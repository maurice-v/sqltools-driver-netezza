import AbstractDriver from '@sqltools/base-driver';
import { IConnectionDriver, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import queries from './queries';
import {
  DEFAULT_QUERY_TIMEOUT_MS,
  DEFAULT_PORT,
  ALL_KEYWORDS,
  ALL_FUNCTIONS,
  DATA_TYPES,
} from './constants';
import {
  CompletionsCache,
  NetezzaDriverOptions,
  QueryInfo,
  NetezzaCredentials,
} from './types';
import { PoolManager } from './pool-manager';
import { QueryParser } from './query-parser';
import { ResultBuilder } from './result-builder';
import { QueryTimeoutError } from './errors';

export default class NetezzaDriver
  extends AbstractDriver<any, any>
  implements IConnectionDriver
{
  queries = queries;

  public readonly capabilities = {
    completions: true,
    formatSql: true,
    cancelQuery: true,
    exportData: true,
  } as const;

  /**
   * Query parsing configuration for SQLTools
   */
  public readonly identifyStatements = true;

  // Dependencies
  private poolManager: PoolManager | null = null;
  private readonly queryParser: QueryParser;
  private resultBuilder: ResultBuilder | null = null;

  // State
  private queryTimeout: number = DEFAULT_QUERY_TIMEOUT_MS;
  private runningQueries = new Map<string, { promise: Promise<any>; cancel?: () => void }>();
  private queryQueue: Promise<any> = Promise.resolve();
  private completionsCache: CompletionsCache | null = null;
  private queryIdCounter: number = 0;

  constructor(credentials: NetezzaCredentials, driverOptions?: any) {
    super(credentials as any, driverOptions);
    this.queryParser = new QueryParser();
    this.initializeTimeout();
  }

  /**
   * Initializes the query timeout from credentials
   */
  private initializeTimeout(): void {
    const options = this.credentials.netezzaOptions as NetezzaDriverOptions | undefined;
    if (options?.queryTimeout) {
      this.queryTimeout = options.queryTimeout * 1000;
      console.log(`[Netezza Driver] Query timeout configured to: ${this.queryTimeout}ms`);
    }
  }

  /**
   * Gets the current catalog
   */
  private get currentCatalog(): string | null {
    return this.poolManager?.getCatalog() ?? null;
  }

  /**
   * Opens a connection to the Netezza database
   */
  public async open(): Promise<any> {
    // Always update query timeout from credentials (in case config changed)
    this.initializeTimeout();

    if (this.connection && this.poolManager?.isInitialized) {
      return this.connection;
    }

    this.completionsCache = null;
    console.log('[Netezza Driver] Completions cache cleared for new connection');

    // Create pool manager
    this.poolManager = new PoolManager(
      this.credentials.server,
      this.credentials.port || DEFAULT_PORT,
      this.credentials.database,
      this.credentials.username,
      this.credentials.password,
      this.credentials.netezzaOptions?.secureConnection || false,
      this.credentials.netezzaOptions?.pool
    );

    // Initialize result builder
    this.resultBuilder = new ResultBuilder(this.getId());

    // Initialize pool
    await this.poolManager.initialize();

    // Set the current catalog to the connection's database
    this.poolManager.setCatalog(this.credentials.database);

    this.connection = Promise.resolve(this.poolManager);
    console.log(`[Netezza Driver] Connection pool opened. Current catalog: ${this.currentCatalog}`);

    return this.connection;
  }

  /**
   * Closes the connection to the Netezza database
   */
  public async close(): Promise<void> {
    if (!this.connection && !this.poolManager) {
      return;
    }

    try {
      if (this.poolManager) {
        await this.poolManager.close();
        console.log('[Netezza Driver] Connection pool closed');
      }
    } catch (err) {
      console.error('[Netezza Driver] Error closing pool:', err);
    } finally {
      this.poolManager = null;
      this.connection = null;
    }
  }

  /**
   * Serializes query execution to prevent parallel execution issues
   */
  private async queryWithTimeout(
    query: string,
    timeoutMs: number,
    queryInfo?: QueryInfo,
    bypassLimit = false,
    queryId?: string
  ): Promise<NSDatabase.IResult[]> {
    return new Promise((resolve, reject) => {
      this.queryQueue = this.queryQueue.then(async () => {
        try {
          const result = await this.executeQueryInternal(query, timeoutMs, queryInfo, bypassLimit, queryId);
          resolve(result);
        } catch (err) {
          reject(err);
        }
      }).catch(() => {
        // Ignore errors in the queue chain to prevent blocking subsequent queries
      });
    });
  }

  /**
   * Internal query execution with connection management
   */
  private async executeQueryInternal(
    query: string,
    timeoutMs: number,
    queryInfo?: QueryInfo,
    bypassLimit = false,
    queryId?: string
  ): Promise<NSDatabase.IResult[]> {
    // Ensure pool is initialized
    await this.open();

    if (!this.poolManager || !this.resultBuilder) {
      // Create a temporary result builder if needed for error reporting
      const builder = this.resultBuilder ?? new ResultBuilder(this.getId());
      return [builder.connectionError(query, 'Connection pool not initialized')];
    }

    // Acquire connection from pool
    let conn;
    try {
      conn = await this.poolManager.acquire();
    } catch (err: any) {
      return [this.resultBuilder.connectionError(query, err.message || String(err))];
    }

    const startTime = Date.now();
    console.log('[Netezza Driver] Executing query:', query);
    console.log(`[Netezza Driver] Query timeout set to: ${timeoutMs}ms`);

    try {
      const result = await this.executeWithTimeout(conn, query, timeoutMs, bypassLimit, startTime, queryId);
      await this.poolManager.release(conn);
      
      // Update catalog state only after successful SET CATALOG execution
      const newCatalog = this.queryParser.extractCatalogFromSetStatement(query);
      if (newCatalog) {
        console.log(`[Netezza Driver] SET CATALOG executed successfully, updating currentCatalog to: ${newCatalog}`);
        this.poolManager.setCatalog(newCatalog);
      }
      
      if (queryId) {
        this.runningQueries.delete(queryId);
      }
      return [result];
    } catch (err: any) {
      const elapsedTime = Date.now() - startTime;
      return [this.handleQueryError(conn, query, err, timeoutMs, elapsedTime)];
    }
  }

  /**
   * Executes query with timeout and cancellation support
   */
  private async executeWithTimeout(
    conn: any,
    query: string,
    timeoutMs: number,
    bypassLimit: boolean,
    startTime: number,
    queryId?: string
  ): Promise<NSDatabase.IResult> {
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(new QueryTimeoutError(timeoutMs, query.substring(0, 100)));
      }, timeoutMs);
    });

    let cancelQuery: (() => void) | undefined;
    const queryPromise = conn.execute(query, {
      // Use the new cancellation feature from node-netezza v1.3
      onCancel: (cancelFn: () => void) => {
        cancelQuery = cancelFn;
        // Update the running query with the actual cancel function once it's available
        if (queryId && this.runningQueries.has(queryId)) {
          const queryInfo = this.runningQueries.get(queryId)!;
          queryInfo.cancel = cancelFn;
        }
      }
    });

    // Track the running query (cancel function will be set asynchronously in onCancel)
    if (queryId) {
      this.runningQueries.set(queryId, {
        promise: queryPromise,
        cancel: undefined // Will be set in onCancel callback
      });
    }

    try {
      const data = await Promise.race([queryPromise, timeoutPromise]);
      return this.processQueryResult(query, data, startTime, bypassLimit);
    } finally {
      if (queryId) {
        this.runningQueries.delete(queryId);
      }
    }
  }

  /**
   * Processes successful query result
   */
  private processQueryResult(
    query: string,
    data: any,
    startTime: number,
    bypassLimit: boolean
  ): NSDatabase.IResult {
    const elapsedTime = Date.now() - startTime;
    console.log(`[Netezza Driver] Query completed in ${elapsedTime}ms`);

    if (!data) {
      return this.resultBuilder!.success(query, [], [], [`Elapsed time: ${elapsedTime}ms`]);
    }

    // Handle different response formats from node-netezza
    let cols: any[] = [];
    let rows: any[] = [];

    if (data.columns && Array.isArray(data.columns)) {
      cols = data.columns;
      rows = data.rows || [];
    } else if (Array.isArray(data)) {
      rows = data;
      if (rows.length > 0) {
        cols = Object.keys(rows[0]).map(key => ({ name: key }));
      }
    } else if (data.fields && Array.isArray(data.fields)) {
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
    messages.push(`Elapsed time: ${elapsedTime}ms`);

    // Limit rows to prevent UI hang with large result sets
    const pageSize = this.credentials.pageSize || 50;
    const maxRows = this.credentials.previewLimit || pageSize;
    const totalRows = rows.length;
    const isLimited = !bypassLimit && maxRows > 0 && rows.length > maxRows;
    if (isLimited) {
      rows = rows.slice(0, maxRows);
      messages.push(`Query returned ${totalRows} rows. Showing first ${maxRows} rows. Adjust 'Show records default limit' in connection settings to change this limit.`);
    }

    // Add row count for non-SELECT queries
    if (!query.trim().toUpperCase().startsWith('SELECT') && data.rowCount !== undefined) {
      messages.push(`${data.rowCount} rows affected`);
    }

    return this.resultBuilder!.success(
      query,
      cols.map((c: any) => c.name || c),
      rows,
      messages
    );
  }

  /**
   * Handles query execution errors
   */
  private handleQueryError(
    conn: any,
    query: string,
    err: any,
    timeoutMs: number,
    elapsedTime: number
  ): NSDatabase.IResult {
    console.log(`[Netezza Driver] Query failed after ${elapsedTime}ms:`, err.message);

    const isTimeout = err.message && err.message.includes('timeout');

    // Close the bad connection
    if (conn && this.poolManager) {
      if (isTimeout) {
        this.poolManager.closeConnection(conn);
      } else {
        this.poolManager.closeConnection(conn);
      }
    }

    // For severe errors, reset the pool
    if (isTimeout || err.code === 'ECONNRESET' || err.code === 'ECONNREFUSED') {
      console.log('[Netezza Driver] Severe error detected, recreating connection pool');
      this.resetPool();
    }

    if (isTimeout) {
      return this.resultBuilder!.timeout(query, timeoutMs, elapsedTime, this.currentCatalog);
    }

    const additionalMessages = [
      `Elapsed time: ${elapsedTime}ms`,
      `─────────────────────────────────────────`,
      `Connection closed. Will reconnect for next query.`,
    ];

    return this.resultBuilder!.error(query, err, additionalMessages, this.currentCatalog);
  }

  /**
   * Resets the connection pool after severe errors
   */
  private async resetPool(): Promise<void> {
    try {
      if (this.poolManager) {
        await this.poolManager.close();
      }
      this.poolManager = null;
      this.connection = null;
    } catch (err) {
      console.error('[Netezza Driver] Error resetting pool:', err);
    }
  }



  /**
   * Executes a SQL query with the configured timeout
   */
  public async query(query: string, opt: any = {}): Promise<NSDatabase.IResult[]> {
    const queryId = opt.queryId || `query_${++this.queryIdCounter}_${Date.now()}`;
    
    console.log('[Netezza Driver] query() called with:');
    console.log('  Query ID:', queryId);
    console.log('  Query length:', query.length);
    console.log('  Query preview:', JSON.stringify(query.substring(0, 100)));

    if (!query || query.trim().length === 0) {
      console.log('[Netezza Driver] Empty query received, returning empty result');
      return [];
    }

    try {
      // Parse the query to check if it contains multiple statements
      const parsedQueries = this.queryParser.parse(query);
      console.log(`[Netezza Driver] Parsed ${parsedQueries.length} query/queries`);

      if (parsedQueries.length > 1) {
        return this.executeMultipleQueries(parsedQueries, queryId);
      }

      return this.queryWithTimeout(query, this.queryTimeout, undefined, false, queryId);
    } catch (err: any) {
      console.error('[Netezza Driver] Query execution failed:', err);
      this.runningQueries.delete(queryId);
      if (!this.resultBuilder) {
        this.resultBuilder = new ResultBuilder(this.getId());
      }
      return [this.resultBuilder.error(query, err.message || String(err))];
    }
  }

  /**
   * Executes multiple queries sequentially
   */
  private async executeMultipleQueries(queries: string[], baseQueryId?: string): Promise<NSDatabase.IResult[]> {
    console.log(`[Netezza Driver] Executing ${queries.length} queries sequentially`);
    const allResults: NSDatabase.IResult[] = [];
    const overallStartTime = Date.now();

    for (let i = 0; i < queries.length; i++) {
      const queryText = queries[i];
      const subQueryId = baseQueryId ? `${baseQueryId}_${i + 1}` : `subquery_${i + 1}_${Date.now()}`;
      console.log(`[Netezza Driver] Executing query ${i + 1} of ${queries.length} (ID: ${subQueryId})`);

      const results = await this.queryWithTimeout(queryText, this.queryTimeout, {
        index: i + 1,
        total: queries.length,
      }, false, subQueryId);
      allResults.push(...results);
    }

    const totalElapsedTime = Date.now() - overallStartTime;

    // Add summary message to the last result
    if (allResults.length > 0) {
      const lastResult = allResults[allResults.length - 1];
      lastResult.messages = [
        ...(lastResult.messages || []),
        `─────────────────────────────────────────`,
        `Total execution time for ${queries.length} queries: ${totalElapsedTime}ms`,
      ];
    }

    return allResults;
  }

  /**
   * Parses SQL text and identifies individual query boundaries
   */
  public parse(query: string, driver?: string): Promise<string[]> | string[] {
    console.log(`[Netezza Driver] Parsing query with length: ${query.length}`);
    const queries = this.queryParser.parse(query);
    
    queries.forEach((q, i) => {
      console.log(`[Netezza Driver] Found query ${i + 1}: ${q.substring(0, 50)}...`);
    });
    
    console.log(`[Netezza Driver] Parsed ${queries.length} query/queries`);
    return queries.length > 0 ? queries : [query];
  }

  /**
   * Cancels running queries using the new query-level cancellation feature
   */
  public async cancelQuery(queryId?: string): Promise<void> {
    if (this.runningQueries.size === 0) {
      console.log('[Netezza Driver] No running queries to cancel');
      return;
    }

    try {
      if (queryId && this.runningQueries.has(queryId)) {
        // Cancel specific query
        const queryInfo = this.runningQueries.get(queryId);
        if (queryInfo?.cancel) {
          console.log(`[Netezza Driver] Cancelling query: ${queryId}`);
          console.log(`[Netezza Driver] Current catalog state preserved: ${this.currentCatalog}`);
          queryInfo.cancel();
          this.runningQueries.delete(queryId);
        }
      } else {
        // Cancel all running queries
        console.log(`[Netezza Driver] Cancelling ${this.runningQueries.size} running queries`);
        console.log(`[Netezza Driver] Current catalog state preserved: ${this.currentCatalog}`);
        for (const queryInfo of this.runningQueries.values()) {
          if (queryInfo.cancel) {
            queryInfo.cancel();
          }
        }
        this.runningQueries.clear();
      }
    } catch (err: any) {
      console.error('[Netezza Driver] Failed to cancel query:', err);
      throw err;
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
   * Returns static completions (keywords, functions, data types)
   */
  public getStaticCompletions = async (): Promise<any> => {
    if (!this.completionsCache) {
      this.completionsCache = await this.loadCompletions();
    }
    return this.completionsCache;
  };

  /**
   * Loads and caches static and dynamic completions
   */
  private async loadCompletions(): Promise<CompletionsCache> {
    const completions: CompletionsCache = {
      keywords: [...ALL_KEYWORDS],
      functions: [...ALL_FUNCTIONS],
      dataTypes: [...DATA_TYPES],
      schemas: [],
      tables: [],
      columns: [],
      variables: [],
    };

    try {
      // Load all schemas for completions
      const schemas = await this.executeCompletionQuery('SELECT DISTINCT SCHEMA FROM _V_SCHEMA ORDER BY SCHEMA');
      completions.schemas = schemas.map((s: any) => ({
        label: s.schema,
        detail: 'Schema',
        type: 'schema',
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
   * Provides context-aware completions based on cursor position and query context
   */
  public async getCompletionsForConnection(params: any): Promise<any[]> {
    const { position, query } = params;

    if (!query || position === undefined) {
      const cache = await this.getStaticCompletions();
      return [...cache.keywords, ...cache.functions, ...cache.dataTypes];
    }

    const beforeCursor = query.substring(0, position);
    const lastKeyword = this.queryParser.getLastKeyword(beforeCursor);

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
        console.log('[Netezza Driver] Suggesting tables and views');
        const tables = await this.searchItems(ContextValue.TABLE, '', {});
        const views = await this.searchItems(ContextValue.VIEW, '', {});
        return [...tables, ...views];

      case 'WHERE':
      case 'SELECT':
      case 'SET':
      case 'ON':
        const tablesInQuery = this.queryParser.extractTables(query);
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

    const cache = await this.getStaticCompletions();
    return [...cache.keywords, ...cache.functions, ...cache.dataTypes];
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
        await this.queryWithTimeout(`SET CATALOG ${dbName};`, this.queryTimeout, undefined, false, `set_catalog_${Date.now()}`);
        this.poolManager?.setCatalog(dbName);
        console.log(`[Netezza Driver] Set current catalog to: ${dbName}`);
        const schemas = await this.executeQuery(this.queries.fetchSchemas, { database: dbName });
        console.log(`[Netezza Driver] Loaded ${schemas.length} schema(s)`);
        return schemas;

      case ContextValue.SCHEMA:
        const schemaItem = item as NSDatabase.ISchema;
        console.log(`[Netezza Driver] Fetching objects for schema: ${schemaItem.schema}`);

        const [schemaTables, schemaViews] = await Promise.all([
          this.executeQuery(this.queries.fetchTables, schemaItem),
          this.executeQuery(this.queries.fetchViews, schemaItem),
        ]);

        const enrichedObjects = [...schemaTables, ...schemaViews].map(obj => {
          const simpleName = obj.tableName || obj.label;
          const fullyQualified = `${obj.database}.${obj.schema}.${simpleName}`;
          return {
            ...obj,
            tableName: simpleName,
            label: fullyQualified,
            detail: simpleName,
            childType: ContextValue.COLUMN,
          };
        });

        console.log(`[Netezza Driver] Loaded ${enrichedObjects.length} objects (${schemaTables.length} tables, ${schemaViews.length} views)`);
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
          type: ContextValue.COLUMN,
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
        const schemas = await this.executeCompletionQuery(
          (this.queries.searchSchemas as any)({ search })
        );
        return schemas.map((s: any) => ({
          label: s.label,
          type: ContextValue.SCHEMA,
          schema: s.schema,
          database: this.currentCatalog || '',
          childType: ContextValue.TABLE,
          iconId: 'schema',
          detail: 'Schema',
        } as any));

      case ContextValue.TABLE:
        const tables = await this.executeCompletionQuery(
          (this.queries.searchTablesInSchema as any)({ search, schemaContext })
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
          detail: `Table in ${t.schema}`,
        } as any));

      case ContextValue.VIEW:
        const views = await this.executeCompletionQuery(
          (this.queries.searchViewsInSchema as any)({ search, schemaContext })
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
          detail: `View in ${v.schema}`,
        } as any));

      case ContextValue.COLUMN:
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

        const columnsResult = await this.executeCompletionQuery(
          (this.queries.searchColumnsInTable as any)({ search, schemaFilter, tableFilter })
        );

        return columnsResult.map((c: any) => ({
          label: c.label,
          type: ContextValue.COLUMN,
          schema: c.schema,
          database: this.currentCatalog || '',
          table: c.table_name,
          columnName: c.label,
          dataType: c.data_type,
          isNullable: !c.is_nullable,
          iconId: 'column',
          detail: `${c.data_type} - ${c.table_name}`,
        } as any));

      case ContextValue.FUNCTION:
        const functions = await this.executeCompletionQuery(
          (this.queries.searchFunctions as any)({ search })
        );
        return functions.map((f: any) => ({
          label: f.label,
          type: ContextValue.FUNCTION,
          schema: f.schema,
          database: this.currentCatalog || '',
          name: f.label,
          iconId: 'function',
          detail: 'Function',
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
      this.poolManager?.setCatalog(table.database);
    }
    const queryStr = typeof this.queries.describeTable === 'function'
      ? this.queries.describeTable(table)
      : this.queries.describeTable;
    return await this.queryWithTimeout(queryStr as string, this.queryTimeout, undefined, false, `describe_table_${Date.now()}`);
  }

  /**
   * Executes a query function with parameters and returns the results
   */
  private async executeQuery(queryFn: any, params?: any, useConfiguredTimeout = true): Promise<any[]> {
    const queryStr = typeof queryFn === 'function' ? queryFn(params) : queryFn;
    const timeout = useConfiguredTimeout ? this.queryTimeout : 10000;
    const results = await this.queryWithTimeout(queryStr, timeout, undefined, true, `internal_${Date.now()}`);
    return results[0]?.results || [];
  }

  /**
   * Fetches records from a table
   */
  public async fetchRecords(params: any): Promise<NSDatabase.IResult[]> {
    const queryStr = this.queries.fetchRecords(params) as string;
    return this.query(queryStr);
  }

  /**
   * Returns a stub result to skip counting records on large Netezza tables
   */
  public async countRecords(params: any): Promise<NSDatabase.IResult[]> {
    if (!this.resultBuilder) {
      await this.open();
    }
    return [this.resultBuilder!.countStub()];
  }

  /**
   * Executes a user-provided query from the editor
   */
  public async runSingleQuery(query: string): Promise<NSDatabase.IResult> {
    let catalogMessage = '';
    if (this.currentCatalog) {
      catalogMessage = `Executing query in catalog: ${this.currentCatalog}`;
      console.log(`[Netezza Driver] ${catalogMessage}`);
    }

    const results = await this.query(query);

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
      this.poolManager?.setCatalog(table.database);
    }

    const queryStr = typeof this.queries.getTableCreateScript === 'function'
      ? this.queries.getTableCreateScript(table)
      : this.queries.getTableCreateScript;

    const results = await this.queryWithTimeout(queryStr as string, this.queryTimeout, undefined, false, `show_records_${Date.now()}`);

    if (results?.[0]?.results?.[0]) {
      const ddl = results[0].results[0].DDL || results[0].results[0].ddl;
      return [ddl];
    }

    return ['-- Unable to generate DDL'];
  }

  /**
   * Exports table data in the specified format
   */
  public async exportData(params: any): Promise<string | any[]> {
    const { table, format = 'CSV' } = params;

    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.poolManager?.setCatalog(table.database);
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
        if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
          return `"${stringValue.replace(/"/g, '""')}"`;
        }
        return stringValue;
      }).join(',')
    ).join('\n');

    return `${headers}\n${rows}`;
  }
}
