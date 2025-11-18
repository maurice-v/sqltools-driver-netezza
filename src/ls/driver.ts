import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import { Connection as NetezzaConnection } from 'node-netezza';

interface CompletionsCache {
  keywords: string[];
  functions: string[];
  dataTypes: string[];
  schemas: Array<{ label: string; detail: string; type: string }>;
  tables: any[];
  columns: any[];
  variables: any[];
}

interface NetezzaConnectionOptions {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean | { ca?: string | Buffer; rejectUnauthorized?: boolean };
  [key: string]: any;
}

export default class NetezzaDriver extends AbstractDriver<any, any> implements IConnectionDriver {
  queries = queries;

  public readonly capabilities = {
    completions: true,
    formatSql: true,
    cancelQuery: true,
    exportData: true
  } as const;

  /**
   * Query parsing configuration for SQLTools
   */
  public readonly identifyStatements = true;

  private currentCatalog: string | null = null;
  private queryTimeout = 30000; // Default 30 seconds
  private netezzaConnection: NetezzaConnection | null = null;
  private runningQueries = new Set<Promise<any>>();
  private queryQueue: Promise<any> = Promise.resolve();
  private completionsCache: CompletionsCache | null = null;

  /**
   * Opens a connection to the Netezza database
   */
  public async open() {
    if (this.connection && this.netezzaConnection) {
      return this.connection;
    }

    this.completionsCache = null;
    console.log('[Netezza Driver] Completions cache cleared for new connection');

    // Set query timeout from connection options (in seconds, convert to ms)
    if (this.credentials.netezzaOptions?.queryTimeout) {
      this.queryTimeout = this.credentials.netezzaOptions.queryTimeout * 1000;
    }
    
    const netezzaOptions: any = {
      host: this.credentials.server,
      port: this.credentials.port || 5480,
      database: this.credentials.database,
      user: this.credentials.username,
      password: this.credentials.password,
      ssl: this.credentials.netezzaOptions?.secureConnection || false,
      ...this.credentials.netezzaOptions
    };

    const conn = new NetezzaConnection(netezzaOptions);
    
    // Connect to the database
    try {
      await conn.connect();
      
      // Workaround for EventEmitter memory leak warning
      try {
        const connWithSocket = conn as any;
        if (connWithSocket.socket && typeof connWithSocket.socket.setMaxListeners === 'function') {
          connWithSocket.socket.setMaxListeners(50);
        }
      } catch (e) {
        // Ignore if we can't access socket
      }
    } catch (err: any) {
      // Provide more context for connection failures
      const errorMsg = err.code === 'ECONNREFUSED' 
        ? `Cannot connect to Netezza at ${this.credentials.server}:${this.credentials.port || 5480}. Please verify the server is running and accessible.`
        : `Connection failed: ${err.message}`;
      throw new Error(errorMsg);
    }

    this.netezzaConnection = conn;
    this.connection = Promise.resolve(conn);
    
    // Set the current catalog to the connection's database
    this.currentCatalog = this.credentials.database;
    console.log(`[Netezza Driver] Connection opened. Current catalog: ${this.currentCatalog}`);
    
    return this.connection;
  }

  /**
   * Closes the connection to the Netezza database
   */
  public async close(): Promise<void> {
    if (!this.connection && !this.netezzaConnection) {
      return;
    }

    try {
      if (this.netezzaConnection) {
        await this.netezzaConnection.close();
      }
    } catch (err) {
      console.error('[Netezza Driver] Error closing connection:', err);
    } finally {
      this.netezzaConnection = null;
      this.connection = null;
    }
  }

  private async queryWithTimeout(query: string, timeoutMs: number, queryInfo?: { index: number; total: number }): Promise<NSDatabase.IResult[]> {
    // Serialize queries to prevent parallel execution issues on Netezza
    return new Promise((resolve, reject) => {
      this.queryQueue = this.queryQueue.then(async () => {
        try {
          const result = await this.executeQueryInternal(query, timeoutMs, queryInfo);
          resolve(result);
        } catch (err) {
          reject(err);
        }
      }).catch(() => {
        // Ignore errors in the queue chain to prevent blocking subsequent queries
      });
    });
  }

  private async executeQueryInternal(query: string, timeoutMs: number, queryInfo?: { index: number; total: number }): Promise<NSDatabase.IResult[]> {
    const conn = await this.open();

    const queryPreview = query.replace(/\s+/g, ' ').trim().substring(0, 50);
    console.log('[Netezza Driver] Executing query:', query);
    const startTime = Date.now();
    
    // Check if this is a SET CATALOG statement and update currentCatalog
    const setCatalogMatch = query.trim().match(/^SET\s+CATALOG\s+(\w+)/i);
    if (setCatalogMatch) {
      const newCatalog = setCatalogMatch[1];
      console.log(`[Netezza Driver] Detected SET CATALOG, updating currentCatalog to: ${newCatalog}`);
      this.currentCatalog = newCatalog;
    }

    try {
      // Create timeout promise
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          reject(new Error(`Query timeout after ${timeoutMs}ms`));
        }, timeoutMs);
      });
      
      // Execute query with timeout
      const queryPromise = conn.execute(query);
      this.runningQueries.add(queryPromise);
      const data = await Promise.race([queryPromise, timeoutPromise]);
      this.runningQueries.delete(queryPromise);
      
      const elapsedTime = Date.now() - startTime;
      console.log(`[Netezza Driver] Query completed in ${elapsedTime}ms`);
      
      if (!data) {
        return [];
      }

      // Handle different response formats from node-netezza
      let cols: any[] = [];
      let rows: any[] = [];
      
      // Check if data has columns and rows properties
      if (data.columns && Array.isArray(data.columns)) {
        cols = data.columns;
        rows = data.rows || [];
      } 
      // Check if data is directly an array of rows
      else if (Array.isArray(data)) {
        rows = data;
        // Extract columns from first row if available
        if (rows.length > 0) {
          cols = Object.keys(rows[0]).map(key => ({ name: key }));
        }
      }
      // Check if data has fields and rows (alternative format)
      else if (data.fields && Array.isArray(data.fields)) {
        cols = data.fields;
        rows = data.rows || [];
      }

      const messages: string[] = [];
      
      // Add current catalog/database
      if (this.currentCatalog) {
        messages.push(`Database: ${this.currentCatalog}`);
      }
      
      // Add the full executed query (normalize whitespace for display)
      const normalizedQuery = query.replace(/\s+/g, ' ').trim();
      messages.push(`Query: ${normalizedQuery}`);
      
      // Add execution time message
      messages.push(`Elapsed time: ${elapsedTime}ms`);
      
      // Limit rows to prevent UI hang with large result sets
      // Use SQLTools pageSize and limit from connection settings
      const pageSize = this.credentials.pageSize || 50;
      const maxRows = this.credentials.previewLimit || pageSize;
      const totalRows = rows.length;
      const isLimited = maxRows > 0 && rows.length > maxRows;
      if (isLimited) {
        rows = rows.slice(0, maxRows);
        messages.push(`Query returned ${totalRows} rows. Showing first ${maxRows} rows. Adjust 'Show records default limit' in connection settings to change this limit.`);
      }
      
      // Add a message about the number of rows affected for non-SELECT queries
      if (!query.trim().toUpperCase().startsWith('SELECT')) {
        if (data.rowCount !== undefined) {
          messages.push(`${data.rowCount} rows affected`);
        }
      }
      
      const result: NSDatabase.IResult = {
        connId: this.getId(),
        requestId: query,
        resultId: generateId(),
        cols: cols.map((c: any) => c.name || c),
        messages: messages,
        query: query,
        results: rows,
      };

      return [result];
    } catch (err: any) {
      const elapsedTime = Date.now() - startTime;
      console.log(`[Netezza Driver] Query failed after ${elapsedTime}ms:`, err.message);
      
      const messages: string[] = [];
      
      // Add current catalog/database
      if (this.currentCatalog) {
        messages.push(`Database: ${this.currentCatalog}`);
      }
      
      // Add the full executed query (normalize whitespace for display)
      const normalizedQuery = query.replace(/\s+/g, ' ').trim();
      messages.push(`Query: ${normalizedQuery}`);
      
      // Add execution time
      messages.push(`Elapsed time: ${elapsedTime}ms`);
      
      // Add prominent error marker
      messages.push(`═══════════════════════════════════════════`);
      messages.push(`❌ QUERY FAILED`);
      messages.push(`═══════════════════════════════════════════`);
      
      // Add detailed error information
      if (err.message && err.message.includes('timeout')) {
        messages.push(`Error Type: Query Timeout`);
        messages.push(`Details: Query execution exceeded the timeout limit of ${timeoutMs}ms`);
        messages.push(`Suggestion: Consider optimizing the query or increasing the timeout setting`);
      } else {
        messages.push(`Error: ${err.message || err}`);
        
        // Add additional error details if available (Netezza-specific)
        if (err.code) {
          messages.push(`Error Code: ${err.code}`);
        }
        if (err.detail) {
          messages.push(`Details: ${err.detail}`);
        }
        if (err.hint) {
          messages.push(`Hint: ${err.hint}`);
        }
        if (err.position) {
          messages.push(`Position: ${err.position}`);
        }
        if (err.where) {
          messages.push(`Where: ${err.where}`);
        }
      }
      
      // Return error in SQLTools result format instead of throwing
      const errorResult: NSDatabase.IResult = {
        connId: this.getId(),
        requestId: query,
        resultId: generateId(),
        cols: ['error'],
        messages: messages,
        error: true,  // Mark as error
        rawError: err,
        query: query,
        results: []
      };
      return [errorResult];
    }
  }

  /**
   * Executes a SQL query with the configured timeout
   * When multiple statements are detected, they are executed sequentially.
   */
  public async query(query: string, opt: any = {}): Promise<NSDatabase.IResult[]> {
    // Log what we received to understand what SQLTools is sending
    console.log('[Netezza Driver] query() called with:');
    console.log('  Query length:', query.length);
    console.log('  Query preview:', JSON.stringify(query.substring(0, 100)));
    console.log('  Options:', JSON.stringify(opt));
    
    // Parse the query to check if it contains multiple statements
    const parsedQueries = await Promise.resolve(this.parse(query));
    console.log(`[Netezza Driver] Parsed ${parsedQueries.length} query/queries`);
    
    // If we have multiple queries, execute them sequentially and return all results
    if (parsedQueries.length > 1) {
      console.log(`[Netezza Driver] Executing ${parsedQueries.length} queries sequentially`);
      const allResults: NSDatabase.IResult[] = [];
      const overallStartTime = Date.now();
      
      for (let i = 0; i < parsedQueries.length; i++) {
        const queryText = parsedQueries[i];
        console.log(`[Netezza Driver] Executing query ${i + 1} of ${parsedQueries.length}`);
        
        // executeQueryInternal always returns results, even for errors
        // No need for try-catch here since errors are returned as error result objects
        const results = await this.queryWithTimeout(queryText, this.queryTimeout, { 
          index: i + 1, 
          total: parsedQueries.length 
        });
        allResults.push(...results);
      }
      
      const totalElapsedTime = Date.now() - overallStartTime;
      
      // Add summary message to the last result
      if (allResults.length > 0) {
        const lastResult = allResults[allResults.length - 1];
        const existingMessages = lastResult.messages || [];
        lastResult.messages = [
          ...existingMessages,
          `─────────────────────────────────────────`,
          `Total execution time for ${parsedQueries.length} queries: ${totalElapsedTime}ms`
        ];
      }
      
      return allResults;
    }
    
    // Execute single query
    return this.queryWithTimeout(query, this.queryTimeout);
  }

  /**
   * Cancels the currently running query by closing and reopening the connection
   * Note: Netezza doesn't support query cancellation, so we close the connection
   */
  public async cancelQuery(): Promise<void> {
    if (this.runningQueries.size === 0) {
      return;
    }

    try {
      // For Netezza, we need to close and reopen the connection to cancel a query
      await this.close();
      this.runningQueries.clear();
    } catch (err: any) {
      throw new Error(`Failed to cancel query: ${err.message}`);
    }
  }

  /**
   * Tests the database connection
   */
  public async testConnection(): Promise<void> {
    await this.open();
    
    try {
      const result = await this.query('SELECT CURRENT_CATALOG, CURRENT_USER, VERSION() AS version');
      
      if (result?.[0]?.results?.[0]) {
        const info = result[0].results[0];
        console.log(`[Netezza Driver] Connected to ${info.current_catalog} as ${info.current_user}`);
        console.log(`[Netezza Driver] Version: ${info.version}`);
      }
    } catch (err) {
      console.warn('[Netezza Driver] Version query failed, falling back to simple test', err);
      await this.query('SELECT 1 AS result');
    }
  }

  /**
   * Parses SQL text and identifies individual query boundaries
   * This allows SQLTools to recognize each semicolon-terminated statement
   * as a separate executable query
   */
  public parse(query: string, driver?: string): Promise<string[]> | string[] {
    console.log(`[Netezza Driver] Parsing query with length: ${query.length}`);
    
    // Split by semicolons but handle strings and comments
    const queries: string[] = [];
    let currentQuery = '';
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inLineComment = false;
    let inBlockComment = false;
    
    for (let i = 0; i < query.length; i++) {
      const char = query[i];
      const nextChar = i + 1 < query.length ? query[i + 1] : '';
      const prevChar = i > 0 ? query[i - 1] : '';
      
      // Handle line comments (-- )
      if (!inSingleQuote && !inDoubleQuote && !inBlockComment && char === '-' && nextChar === '-') {
        inLineComment = true;
        currentQuery += char;
        continue;
      }
      
      // End line comment on newline
      if (inLineComment && char === '\n') {
        inLineComment = false;
        currentQuery += char;
        continue;
      }
      
      // Handle block comments (/* */)
      if (!inSingleQuote && !inDoubleQuote && !inLineComment && char === '/' && nextChar === '*') {
        inBlockComment = true;
        currentQuery += char;
        continue;
      }
      
      if (inBlockComment && char === '*' && nextChar === '/') {
        inBlockComment = false;
        currentQuery += char + nextChar;
        i++; // Skip next character
        continue;
      }
      
      // Handle string literals with proper escaping
      if (!inLineComment && !inBlockComment) {
        if (char === "'" && !inDoubleQuote) {
          // Check for escaped single quote (two consecutive single quotes)
          if (inSingleQuote && nextChar === "'") {
            currentQuery += char + nextChar;
            i++; // Skip next character
            continue;
          }
          inSingleQuote = !inSingleQuote;
        } else if (char === '"' && !inSingleQuote) {
          // Check for escaped double quote
          if (inDoubleQuote && nextChar === '"') {
            currentQuery += char + nextChar;
            i++; // Skip next character
            continue;
          }
          inDoubleQuote = !inDoubleQuote;
        }
      }
      
      // Check for semicolon (statement terminator)
      if (char === ';' && !inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment) {
        currentQuery += char;
        const trimmedQuery = currentQuery.trim();
        if (trimmedQuery.length > 0) {
          console.log(`[Netezza Driver] Found query: ${trimmedQuery.substring(0, 50)}...`);
          queries.push(trimmedQuery);
        }
        currentQuery = '';
        continue;
      }
      
      currentQuery += char;
    }
    
    // Add any remaining query without semicolon
    const trimmedQuery = currentQuery.trim();
    if (trimmedQuery.length > 0) {
      console.log(`[Netezza Driver] Found query (no semicolon): ${trimmedQuery.substring(0, 50)}...`);
      queries.push(trimmedQuery);
    }
    
    console.log(`[Netezza Driver] Parsed ${queries.length} query/queries`);
    return queries.length > 0 ? queries : [query];
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        console.log('[Netezza Driver] Fetching databases...');
        const databases = await this.executeQuery(this.queries.fetchDatabases);
        console.log(`[Netezza Driver] Loaded ${databases.length} database(s)`);
        return databases;
      case ContextValue.DATABASE:
        const dbName = (item as NSDatabase.IDatabase).database;
        console.log(`[Netezza Driver] Fetching schemas for database: ${dbName}`);
        // Note: Not changing currentCatalog - user must explicitly use "Set as Current Catalog" command
        const schemas = await this.executeQuery(this.queries.fetchSchemas);
        console.log(`[Netezza Driver] Loaded ${schemas.length} schema(s)`);
        return schemas;
      case ContextValue.SCHEMA:
        const schemaItem = item as NSDatabase.ISchema;
        console.log(`[Netezza Driver] Fetching objects for schema: ${schemaItem.schema}`);
        
        const [tables, views] = await Promise.all([
          this.executeQuery(this.queries.fetchTables, schemaItem),
          this.executeQuery(this.queries.fetchViews, schemaItem)
        ]);
        
        const enrichedObjects = [...tables, ...views].map(obj => {
          const simpleName = obj.tableName || obj.label;
          const fullyQualified = `${obj.database}.${obj.schema}.${simpleName}`;
          return {
            ...obj,
            tableName: simpleName,
            label: fullyQualified,
            detail: simpleName,
            childType: ContextValue.COLUMN
          };
        });
        
        console.log(`[Netezza Driver] Loaded ${enrichedObjects.length} objects (${tables.length} tables, ${views.length} views)`);
        return enrichedObjects;
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        const table = item as NSDatabase.ITable;
        const tableType = item.type === ContextValue.TABLE ? 'table' : 'view';
        
        console.log(`[Netezza Driver] Fetching columns for ${tableType}: ${table.schema}.${table.label}`);
        const columns = await this.executeQuery(this.queries.fetchColumns, table);
        console.log(`[Netezza Driver] Loaded ${columns.length} columns`);
        
        return columns.map(col => ({ 
          ...col, 
          isLeaf: true,
          childType: ContextValue.NO_CHILD,
          type: ContextValue.COLUMN
        }));
      case ContextValue.COLUMN:
        console.log('[Netezza Driver] Warning: getChildrenForItem called for COLUMN (leaf node)');
        return [];
    }
    return [];
  }

  /**
   * Searches for database items (schemas, tables, views, columns) based on type
   */
  public async searchItems(itemType: ContextValue, search = '', extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    // Extract schema context from various possible parameter formats
    let schemaContext = extraParams.schema || extraParams.database || extraParams.parentName || null;
    
    if (!schemaContext && extraParams.parentLabel) {
      schemaContext = extraParams.parentLabel;
    }
    if (!schemaContext && extraParams.parent?.schema) {
      schemaContext = extraParams.parent.schema;
    }
    
    console.log(`[Netezza Driver] Searching for ${itemType}${schemaContext ? ` in schema "${schemaContext}"` : ''} with term: "${search}"`);
    
    switch (itemType) {
      case ContextValue.DATABASE:
      case ContextValue.SCHEMA:
        // Search for schemas
        const schemas = await this.executeCompletionQuery(
          `SELECT SCHEMA AS label, SCHEMA AS schema
           FROM _V_SCHEMA
           WHERE SCHEMA LIKE '%${search || ''}%'
           ORDER BY SCHEMA`
        );
        return schemas.map((s: any) => ({
          label: s.label,
          type: ContextValue.SCHEMA,
          schema: s.schema,
          database: this.currentCatalog || '',
          childType: ContextValue.TABLE,
          iconId: 'schema',
          detail: 'Schema'
        } as any));

      case ContextValue.TABLE:
        // Search for tables
        const tables = await this.executeCompletionQuery(
          `SELECT TABLENAME AS label, 
                  SCHEMA AS schema,
                  TABLENAME AS table_name
           FROM _V_TABLE
           WHERE TABLENAME LIKE '%${search || ''}%'
           ${schemaContext ? `AND UPPER(SCHEMA) = UPPER('${schemaContext}')` : ''}
           ORDER BY TABLENAME
           LIMIT 100`
        );
        return tables.map((t: any) => ({
          label: t.label,
          type: ContextValue.TABLE,
          schema: t.schema,
          database: this.currentCatalog || '',
          tableName: t.table_name,
          isView: false,
          childType: ContextValue.COLUMN,
          iconId: 'table',
          detail: `Table in ${t.schema}`
        } as any));

      case ContextValue.VIEW:
        // Search for views
        const views = await this.executeCompletionQuery(
          `SELECT VIEWNAME AS label,
                  SCHEMA AS schema,
                  VIEWNAME AS view_name
           FROM _V_VIEW
           WHERE VIEWNAME LIKE '%${search || ''}%'
           ${schemaContext ? `AND UPPER(SCHEMA) = UPPER('${schemaContext}')` : ''}
           ORDER BY VIEWNAME
           LIMIT 100`
        );
        return views.map((v: any) => ({
          label: v.label,
          type: ContextValue.VIEW,
          schema: v.schema,
          database: this.currentCatalog || '',
          tableName: v.view_name,
          isView: true,
          childType: ContextValue.COLUMN,
          iconId: 'view',
          detail: `View in ${v.schema}`
        } as any));

      case ContextValue.COLUMN:
        // Search for columns
        // SQLTools passes table context as: extraParams.tables = [{ label: 'TABLE_NAME', database: 'SCHEMA_NAME' }]
        // In Netezza context, what SQLTools calls 'database' is actually the schema
        let tableFilter = '';
        let schemaFilter = '';
        
        if (extraParams.tables && Array.isArray(extraParams.tables) && extraParams.tables.length > 0) {
          const tableInfo = extraParams.tables[0];
          tableFilter = tableInfo.label || tableInfo.table || tableInfo.tableName || '';
          schemaFilter = tableInfo.database || tableInfo.schema || '';
        } else {
          tableFilter = extraParams.table || '';
          schemaFilter = extraParams.schema || '';
        }
        
        const columns = await this.executeCompletionQuery(
          `SELECT ATTNAME AS label,
                  SCHEMA AS schema,
                  NAME AS table_name,
                  FORMAT_TYPE AS data_type,
                  ATTNOTNULL AS is_nullable
           FROM _V_RELATION_COLUMN
           WHERE ATTNAME LIKE '%${search || ''}%'
           ${schemaFilter ? `AND UPPER(SCHEMA) = UPPER('${schemaFilter}')` : ''}
           ${tableFilter ? `AND UPPER(NAME) = UPPER('${tableFilter}')` : ''}
           ORDER BY ATTNUM
           LIMIT 100`
        );
        
        return columns.map((c: any) => ({
          label: c.label,
          type: ContextValue.COLUMN,
          schema: c.schema,
          database: this.currentCatalog || '',
          table: c.table_name,
          columnName: c.label,
          dataType: c.data_type,
          isNullable: !c.is_nullable,
          iconId: 'column',
          detail: `${c.data_type} - ${c.table_name}`
        } as any));

      case ContextValue.FUNCTION:
        const functions = await this.executeCompletionQuery(
          `SELECT FUNCTION AS label, SCHEMA AS schema
           FROM _V_FUNCTION
           WHERE FUNCTION LIKE '%${search || ''}%'
           ORDER BY FUNCTION`
        );
        return functions.map((f: any) => ({
          label: f.label,
          type: ContextValue.FUNCTION,
          schema: f.schema,
          database: this.currentCatalog || '',
          name: f.label,
          iconId: 'function',
          detail: 'Function'
        } as any));
    }
    
    return [];
  }

  /**
   * Describes a table's structure (columns, types, etc.)
   */
  public async describeTable(table: NSDatabase.ITable, opt: any = {}): Promise<NSDatabase.IResult[]> {
    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.currentCatalog = table.database;
    }
    const queryStr = typeof this.queries.describeTable === 'function' 
      ? this.queries.describeTable(table) 
      : this.queries.describeTable;
    return await this.queryWithTimeout(queryStr as string, this.queryTimeout);
  }

  /**
   * Executes a query function with parameters and returns the results
   */
  private async executeQuery(queryFn: any, params?: any, useConfiguredTimeout = true): Promise<any[]> {
    const queryStr = typeof queryFn === 'function' ? queryFn(params) : queryFn;
    const timeout = useConfiguredTimeout ? this.queryTimeout : 10000;
    const results = await this.queryWithTimeout(queryStr, timeout);
    return results[0]?.results || [];
  }

  /**
   * Returns static completions (keywords, functions, data types)
   * Results are cached for performance
   */
  public getStaticCompletions = async (): Promise<any> => {
    if (!this.completionsCache) {
      this.completionsCache = await this.loadCompletions();
    }
    return this.completionsCache;
  }

  /**
   * Provides context-aware completions based on cursor position and query context
   */
  public async getCompletionsForConnection(params: any): Promise<any[]> {
    const { position, query } = params;
    
    if (!query || position === undefined) {
      return this.getStaticCompletions();
    }
    
    const beforeCursor = query.substring(0, position);
    const lastKeyword = this.getLastKeyword(beforeCursor);
    
    console.log(`[Netezza Driver] Context-aware completion. Last keyword: "${lastKeyword}"`);
    
    // Check if user is typing a schema-qualified table name (e.g., "schema.")
    const schemaMatch = beforeCursor.match(/\b([a-z_][a-z0-9_]*)\.$/i);
    if (schemaMatch) {
      const schemaName = schemaMatch[1];
      console.log(`[Netezza Driver] Schema-qualified table reference detected: ${schemaName}`);
      const tables = await this.searchItems(ContextValue.TABLE, '', { schema: schemaName });
      const views = await this.searchItems(ContextValue.VIEW, '', { schema: schemaName });
      return [...tables, ...views];
    }
    
    switch (lastKeyword) {
      case 'FROM':
      case 'JOIN':
      case 'INTO':
        // Return tables and views
        console.log('[Netezza Driver] Suggesting tables and views');
        const tables = await this.searchItems(ContextValue.TABLE, '', {});
        const views = await this.searchItems(ContextValue.VIEW, '', {});
        return [...tables, ...views];
        
      case 'WHERE':
      case 'SELECT':
      case 'SET':
      case 'ON':
        // Return columns from tables in FROM clause
        const tablesInQuery = this.extractTablesFromQuery(query);
        if (tablesInQuery.length > 0) {
          console.log(`[Netezza Driver] [IntelliSense] Suggesting columns from tables: ${tablesInQuery.join(', ')}`);
          const columnPromises = tablesInQuery.map(table => 
            this.searchItems(ContextValue.COLUMN, '', { table })
          );
          const columnArrays = await Promise.all(columnPromises);
          return columnArrays.flat();
        }
        break;
    }
    
    // Default to static completions
    return this.getStaticCompletions();
  }

  /**
   * Extracts the last SQL keyword before the cursor position
   */
  private getLastKeyword(text: string): string {
    const keywords = text.toUpperCase().split(/[\s,();]+/).filter(k => k.length > 0);
    return keywords[keywords.length - 1] || '';
  }

  /**
   * Extracts table names from FROM and JOIN clauses in a SQL query
   */
  private extractTablesFromQuery(query: string): string[] {
    const fromMatches = query.match(/FROM\s+([^\s,()]+)/gi) || [];
    const joinMatches = query.match(/JOIN\s+([^\s,()]+)/gi) || [];
    
    const tables = [...fromMatches, ...joinMatches]
      .map(m => m.replace(/FROM\s+/i, '').replace(/JOIN\s+/i, ''))
      .filter(t => t.length > 0);
    
    return [...new Set(tables)];
  }

  /**
   * Loads and caches static and dynamic completions
   */
  private async loadCompletions(): Promise<CompletionsCache> {
    const completions: CompletionsCache = {
      keywords: [
        // Netezza-specific keywords
        'DISTRIBUTE', 'ORGANIZE', 'ZONE', 'GROOM', 'GENERATE_STATISTICS',
        'MATERIALIZED', 'EXTERNAL', 'SAMPLED', 'TEMP', 'TEMPORARY',
        // Standard SQL keywords
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL',
        'GROUP', 'ORDER', 'HAVING', 'UNION', 'EXCEPT', 'INTERSECT',
        'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
        'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL',
        'DISTINCT', 'ALL', 'ANY', 'SOME', 'BY', 'ON', 'USING', 'CASE', 'WHEN',
        'THEN', 'ELSE', 'END', 'WITH', 'RECURSIVE', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
        'TABLE', 'VIEW', 'INDEX', 'SEQUENCE', 'DATABASE', 'SCHEMA', 'CONSTRAINT',
        'PRIMARY', 'FOREIGN', 'KEY', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
        'INTO', 'VALUES', 'SET', 'CAST', 'CONVERT'
      ],
      functions: [
        // Netezza-specific functions
        'REGEXP_EXTRACT', 'REGEXP_LIKE', 'REGEXP_REPLACE',
        'TO_CHAR', 'TO_DATE', 'TO_NUMBER', 'TO_TIMESTAMP',
        // Aggregate functions
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
        // String functions
        'CONCAT', 'SUBSTR', 'LENGTH', 'TRIM', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER',
        'REPLACE', 'POSITION', 'STRPOS',
        // Date/Time functions
        'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'NOW', 'EXTRACT',
        'DATE_PART', 'DATE_TRUNC', 'AGE', 'INTERVAL',
        // Numeric functions
        'ABS', 'CEIL', 'FLOOR', 'ROUND', 'TRUNC', 'MOD', 'POWER', 'SQRT', 'EXP', 'LN', 'LOG',
        // Conditional functions
        'COALESCE', 'NULLIF', 'GREATEST', 'LEAST',
        // Window functions
        'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE'
      ],
      dataTypes: [
        'BYTEINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'INT', 'INT1', 'INT2', 'INT4', 'INT8',
        'NUMERIC', 'DECIMAL', 'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION',
        'CHARACTER', 'VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'TEXT',
        'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
        'BOOLEAN', 'BOOL', 'BINARY', 'VARBINARY'
      ],
      // Add dynamic completions placeholders
      schemas: [],
      tables: [],
      columns: [],
      variables: []
    };

    try {
      // Load all schemas for completions
      const schemas = await this.executeCompletionQuery('SELECT DISTINCT SCHEMA FROM _V_SCHEMA ORDER BY SCHEMA');
      completions.schemas = schemas.map((s: any) => ({
        label: s.schema,
        detail: 'Schema',
        type: 'schema'
      }));
      console.log(`[Netezza Driver] [Intellisense] Loaded ${completions.schemas.length} schemas for completions`);

      // Load common functions from system schemas
      try {
        const funcs = await this.executeCompletionQuery(
          `SELECT DISTINCT FUNCTION, SCHEMA 
           FROM _V_FUNCTION 
           WHERE SCHEMA IN ('SYSTEM', 'SQLJ', 'PUBLIC')
           ORDER BY FUNCTION
           LIMIT 100`
        );
        const dynamicFunctions = funcs.map((f: any) => f.function);
        completions.functions = [...new Set([...completions.functions, ...dynamicFunctions])];
        console.log(`[Netezza Driver] [Intellisense] Loaded ${completions.functions.length} total functions (including dynamic)`);
      } catch (err) {
        console.log('[Netezza Driver] [Intellisense] Could not load dynamic functions:', err);
      }

    } catch (err) {
      // If we can't load dynamic completions, just return static ones
      console.error('[Netezza Driver] [Intellisense] Failed to load dynamic completions:', err);
    }

    return completions;
  }

  /**
   * Executes a query for IntelliSense completions with error handling
   */
  private async executeCompletionQuery(query: string): Promise<any[]> {
    try {
      const result = await this.query(query);
      return result?.[0]?.results || [];
    } catch (err) {
      console.error('[Netezza Driver] Completion query error:', err);
      return [];
    }
  }

  /**
   * Fetches records from a table with configured timeout
   */
  public async fetchRecords(params: any): Promise<NSDatabase.IResult[]> {
    const queryStr = this.queries.fetchRecords(params) as string;
    return this.query(queryStr);
  }

  /**
   * Returns a stub result to skip counting records on large Netezza tables
   * COUNT(*) queries can be very slow, and we already limit results in fetchRecords
   */
  public async countRecords(params: any): Promise<NSDatabase.IResult[]> {
    // Return a proper result structure with 0 total instead of running the COUNT query
    const result: NSDatabase.IResult = {
      connId: this.getId(),
      requestId: 'countRecords',
      resultId: generateId(),
      cols: ['total'],
      messages: ['Row counting disabled for performance'],
      query: '-- COUNT query skipped',
      results: [{ total: 0 }],
    };
    return [result];
  }

  /**
   * Executes a user-provided query from the editor
   */
  public async runSingleQuery(query: string): Promise<NSDatabase.IResult> {
    // Use the current catalog (last expanded in object browser) for user queries
    // This allows queries to run in the context of the database being explored
    let catalogMessage = '';
    if (this.currentCatalog) {
      catalogMessage = `Executing query in catalog: ${this.currentCatalog}`;
      console.log(`[Netezza Driver] ${catalogMessage}`);
    }
    
    const results = await this.query(query);
    
    // Add catalog context message to the result
    if (catalogMessage && results[0]) {
      results[0].messages = results[0].messages || [];
      results[0].messages.unshift(catalogMessage);
    }
    
    return results[0];
  }

  /**
   * Generates CREATE TABLE DDL script for a table
   */
  public async getTableCreateScript(table: NSDatabase.ITable, opt: any = {}): Promise<string[]> {
    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.currentCatalog = table.database;
    }
    
    const queryStr = typeof this.queries.getTableCreateScript === 'function' 
      ? this.queries.getTableCreateScript(table) 
      : this.queries.getTableCreateScript;
    
    const results = await this.queryWithTimeout(queryStr as string, this.queryTimeout);
    
    if (results?.[0]?.results?.[0]) {
      const ddl = results[0].results[0].DDL || results[0].results[0].ddl;
      return [ddl];
    }
    
    return ['-- Unable to generate DDL'];
  }

  /**
   * Exports table data in the specified format (CSV by default)
   */
  public async exportData(params: any): Promise<string | any[]> {
    const { table, format = 'CSV' } = params;
    
    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.currentCatalog = table.database;
    }
    
    const tableName = table.schema 
      ? `${table.schema}.${table.tableName || table.label}`
      : (table.tableName || table.label);
    
    const results = await this.query(`SELECT * FROM ${tableName}`);
    
    return format === 'CSV' 
      ? this.resultsToCSV(results[0]) 
      : results[0].results;
  }

  /**
   * Converts query results to CSV format
   */
  private resultsToCSV(result: NSDatabase.IResult): string {
    if (!result.cols || result.cols.length === 0) {
      return '';
    }
    
    const headers = result.cols.join(',');
    
    const rows = result.results.map(row => 
      result.cols.map(col => {
        const value = row[col];
        if (value == null) return '';
        
        const stringValue = String(value);
        // Escape quotes and wrap in quotes if needed
        if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
          return `"${stringValue.replace(/"/g, '""')}"`;
        }
        return stringValue;
      }).join(',')
    ).join('\n');
    
    return `${headers}\n${rows}`;
  }
}
