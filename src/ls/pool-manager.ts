import { NetezzaConnectionOptions, PoolOptions } from './types';
import { 
  ConnectionError, 
  PoolNotInitializedError, 
  PoolAcquireError,
  CatalogSwitchError 
} from './errors';
import {
  DEFAULT_PORT,
  DEFAULT_POOL_MIN,
  DEFAULT_POOL_MAX,
  DEFAULT_IDLE_TIMEOUT_MS,
} from './constants';

/**
 * Manages the connection pool for Netezza database connections
 * Uses the node-netezza library's Pool implementation
 */
export class PoolManager {
  private pool: any | null = null;
  private currentCatalog: string | null = null;

  constructor(
    private readonly host: string,
    private readonly port: number = DEFAULT_PORT,
    private readonly database: string,
    private readonly user: string,
    private readonly password: string,
    private readonly ssl: boolean | object = false,
    private readonly poolOptions: PoolOptions = {}
  ) {}

  /**
   * Creates connection options for the pool
   */
  private createPoolOptions(): any {
    return {
      host: this.host,
      port: this.port,
      database: this.database,
      user: this.user,
      password: this.password,
      ssl: this.ssl,
      min: this.poolOptions.min ?? DEFAULT_POOL_MIN,
      max: this.poolOptions.max ?? DEFAULT_POOL_MAX,
      idleTimeoutMillis: this.poolOptions.idleTimeoutMillis ?? DEFAULT_IDLE_TIMEOUT_MS,
    };
  }

  /**
   * Initializes the connection pool
   */
  async initialize(): Promise<void> {
    if (this.pool) {
      return; // Already initialized
    }

    try {
      // Dynamically import node-netezza
      const { Pool } = await import('node-netezza');
      
      const options = this.createPoolOptions();
      this.pool = new Pool(options);

      // Test the pool by acquiring and releasing a connection
      const testConn = await this.pool.acquire();
      await this.pool.release(testConn);
      
      console.log('[PoolManager] Connection pool initialized successfully');
    } catch (err: any) {
      this.pool = null;
      throw new ConnectionError(this.host, this.port, err);
    }
  }

  /**
   * Acquires a connection from the pool
   */
  async acquire(): Promise<any> {
    if (!this.pool) {
      throw new PoolNotInitializedError();
    }

    try {
      const conn = await this.pool.acquire();
      
      // Set catalog on the connection if one is active
      if (this.currentCatalog) {
        await this.setCatalogOnConnection(conn, this.currentCatalog);
      }
      
      return conn;
    } catch (err: any) {
      throw new PoolAcquireError(err);
    }
  }

  /**
   * Releases a connection back to the pool
   */
  async release(conn: any): Promise<void> {
    if (conn && this.pool) {
      try {
        await this.pool.release(conn);
      } catch (err) {
        console.error('[PoolManager] Error releasing connection:', err);
      }
    }
  }

  /**
   * Closes a specific connection (for error recovery)
   */
  closeConnection(conn: any): void {
    if (conn && typeof conn.close === 'function') {
      try {
        conn.close();
      } catch (err) {
        console.error('[PoolManager] Error closing connection:', err);
      }
    }
  }

  /**
   * Sets the current catalog for all future connections
   */
  setCatalog(catalog: string): void {
    this.currentCatalog = catalog;
    console.log(`[PoolManager] Catalog set to: ${catalog}`);
  }

  /**
   * Gets the current catalog
   */
  getCatalog(): string | null {
    return this.currentCatalog;
  }

  /**
   * Sets the catalog on a specific connection
   */
  private async setCatalogOnConnection(conn: any, catalog: string): Promise<void> {
    try {
      await conn.execute(`SET CATALOG ${catalog}`);
      console.log(`[PoolManager] Catalog set to ${catalog} on connection`);
    } catch (err: any) {
      throw new CatalogSwitchError(catalog, err);
    }
  }

  /**
   * Closes all connections in the pool
   */
  async close(): Promise<void> {
    if (!this.pool) {
      return;
    }

    try {
      await this.pool.end();
      console.log('[PoolManager] Connection pool closed');
    } finally {
      this.pool = null;
      this.currentCatalog = null;
    }
  }

  /**
   * Checks if the pool is initialized
   */
  get isInitialized(): boolean {
    return this.pool !== null;
  }

  /**
   * Drains and reinitializes the pool (for query cancellation)
   */
  async reset(): Promise<void> {
    await this.close();
    await this.initialize();
  }
}
