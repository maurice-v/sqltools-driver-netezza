import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import { Connection as NetezzaConnection } from 'node-netezza';

export default class NetezzaDriver extends AbstractDriver<any, any> implements IConnectionDriver {
  queries = queries;
  private currentCatalog: string | null = null;
  private queryTimeout: number = 30000; // Default 30 seconds
  private netezzaConnection: any = null; // Store the actual connection object
  private runningQueries: Set<Promise<any>> = new Set(); // Track multiple running queries
  private queryQueue: Promise<any> = Promise.resolve(); // Queue to serialize queries

  public async open() {
    if (this.connection && this.netezzaConnection) {
      return this.connection;
    }

    // Set query timeout from connection options (in seconds, convert to ms)
    if (this.credentials.netezzaOptions?.queryTimeout) {
      this.queryTimeout = this.credentials.netezzaOptions.queryTimeout * 1000;
    }
    
    const netezzaOptions = {
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
      throw new Error(`Connection failed: ${err.message}`);
    }

    this.netezzaConnection = conn;
    this.connection = Promise.resolve(conn);
    return this.connection;
  }

  public async close() {
    if (!this.connection && !this.netezzaConnection) return Promise.resolve();

    try {
      if (this.netezzaConnection) {
        await this.netezzaConnection.close();
      }
    } catch (err) {
      console.error('[Netezza Driver] Error closing connection:', err);
    }
    this.netezzaConnection = null;
    this.connection = null;
  }

  private async queryWithTimeout(query: string, timeoutMs: number): Promise<NSDatabase.IResult[]> {
    // Serialize queries to prevent parallel execution issues on Netezza
    return new Promise((resolve, reject) => {
      this.queryQueue = this.queryQueue.then(async () => {
        try {
          const result = await this.executeQueryInternal(query, timeoutMs);
          resolve(result);
        } catch (err) {
          reject(err);
        }
      }).catch(() => {
        // Ignore errors in the queue chain to prevent blocking subsequent queries
      });
    });
  }

  private async executeQueryInternal(query: string, timeoutMs: number): Promise<NSDatabase.IResult[]> {
    const conn = await this.open();

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
      
      // Limit rows to prevent UI hang with large result sets
      // Use SQLTools' built-in previewLimit (default 50)
      const maxRows = this.credentials.previewLimit || 50;
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
      // Check if it's a timeout error
      if (err.message && err.message.includes('timeout')) {
        throw new Error(`Query execution timeout (${timeoutMs}ms). The query took too long to complete.`);
      }
      
      throw new Error(err.message);
    }
  }

  public async query(query: string, opt = {}): Promise<NSDatabase.IResult[]> {
    return this.queryWithTimeout(query, this.queryTimeout);
  }

  /**
   * Cancel/abort the currently running query
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

  public async testConnection() {
    await this.open();
    await this.query('SELECT 1 AS result', {});
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.executeQuery(this.queries.fetchDatabases);
      case ContextValue.DATABASE:
        // Switch to the selected database using SET CATALOG
        const dbName = (item as NSDatabase.IDatabase).database;
        await this.query(`SET CATALOG ${dbName}`);
        this.currentCatalog = dbName;
        return this.executeQuery(this.queries.fetchSchemas);
      case ContextValue.SCHEMA:
        const tables = await this.executeQuery(this.queries.fetchTables, item as NSDatabase.ISchema);
        const views = await this.executeQuery(this.queries.fetchViews, item as NSDatabase.ISchema);
        
        // Set label to fully qualified name for drag and drop, keep simple name in detail
        const enriched = [...tables, ...views].map(t => {
          const simpleName = (t as any).tableName || t.label;
          const fullyQualified = `${(t as any).database}.${t.schema}.${simpleName}`;
          return {
            ...t,
            tableName: simpleName,  // Ensure tableName is preserved for column queries
            label: fullyQualified,
            detail: simpleName  // Show simple name as detail
          };
        });
        
        return enriched;
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        // Switch to the correct catalog before fetching columns
        const table = item as NSDatabase.ITable;
        if (table.database) {
          await this.query(`SET CATALOG ${table.database}`);
          this.currentCatalog = table.database;
        }
        const columns = await this.executeQuery(this.queries.fetchColumns, table);
        // Explicitly mark columns as leaf nodes
        return columns.map(col => ({ ...col, isLeaf: true }));
      case ContextValue.COLUMN:
        return [];
    }
    return [];
  }

  public async searchItems(itemType: ContextValue, search: string, extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    // Search across all catalogs by iterating through databases
    const databases = await this.executeQuery(this.queries.fetchDatabases);
    const allResults: NSDatabase.SearchableItem[] = [];
    const originalCatalog = this.currentCatalog;
    
    for (const db of databases) {
      const dbName = db.database || db.label;
      try {
        await this.query(`SET CATALOG ${dbName}`);
        this.currentCatalog = dbName;
        
        let results: any[] = [];
        switch (itemType) {
          case ContextValue.TABLE:
            results = await this.executeQuery(this.queries.searchTables, { search: `%${search.toLowerCase()}%` });
            break;
          case ContextValue.COLUMN:
            results = await this.executeQuery(this.queries.searchColumns, { search: `%${search.toLowerCase()}%`, ...extraParams });
            break;
        }
        
        allResults.push(...results);
      } catch (err: any) {
        // Continue with next database even if one fails
      }
    }
    
    // Restore original catalog if it was set
    if (originalCatalog) {
      try {
        await this.query(`SET CATALOG ${originalCatalog}`);
        this.currentCatalog = originalCatalog;
      } catch (err: any) {
        // Ignore catalog restore errors
      }
    }
    
    return allResults;
  }

  public async describeTable(table: NSDatabase.ITable, opt = {}): Promise<NSDatabase.IResult[]> {
    // Switch to the correct catalog before describing the table
    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.currentCatalog = table.database;
    }
    const queryStr = typeof this.queries.describeTable === 'function' 
      ? this.queries.describeTable(table) 
      : this.queries.describeTable;
    return await this.queryWithTimeout(queryStr as string, this.queryTimeout);
  }

  private async executeQuery(queryFn: any, params?: any, useConfiguredTimeout: boolean = true): Promise<any[]> {
    const queryStr = typeof queryFn === 'function' ? queryFn(params) : queryFn;
    const timeout = useConfiguredTimeout ? this.queryTimeout : 10000;
    const results = await this.queryWithTimeout(queryStr, timeout);
    return results[0]?.results || [];
  }

  public getStaticCompletions = async () => {
    return {};
  }

  /**
   * Override to use configured timeout for fetching records
   */
  public async fetchRecords(params: any): Promise<NSDatabase.IResult[]> {
    const queryStr = this.queries.fetchRecords(params);
    return this.query(queryStr as string);
  }

  /**
   * Override to skip counting records to avoid timeouts on large tables
   * SQLTools calls this in parallel with fetchRecords, but COUNT(*) on large Netezza tables
   * can be very slow and cause timeouts. Since we already limit results, counting is unnecessary.
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
   * This method is responsible for running queries provided by the user.
   * SQLTools will call this method when the user executes a query from the editor.
   */
  public async runSingleQuery(query: string): Promise<NSDatabase.IResult> {
    const results = await this.query(query);
    return results[0];
  }

  /**
   * Generate and show the CREATE TABLE DDL for a table
   * This method is called when the user selects "Show Table DDL" or similar from the context menu
   */
  public async getTableCreateScript(table: NSDatabase.ITable, opt = {}): Promise<string[]> {
    // Switch to the correct catalog before generating DDL
    if (table.database) {
      await this.query(`SET CATALOG ${table.database}`);
      this.currentCatalog = table.database;
    }
    
    const queryStr = typeof this.queries.getTableCreateScript === 'function' 
      ? this.queries.getTableCreateScript(table) 
      : this.queries.getTableCreateScript;
    
    const results = await this.queryWithTimeout(queryStr as string, this.queryTimeout);
    
    // Extract the DDL string from the result
    if (results && results[0] && results[0].results && results[0].results.length > 0) {
      const ddl = results[0].results[0].DDL || results[0].results[0].ddl;
      return [ddl];
    }
    
    return ['-- Unable to generate DDL'];
  }
}
