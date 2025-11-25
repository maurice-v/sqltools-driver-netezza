/**
 * Base error class for all Netezza driver errors
 */
export class NetezzaError extends Error {
  constructor(
    message: string,
    public readonly code?: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'NetezzaError';
    
    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, this.constructor);
    }
  }
}

/**
 * Error thrown when connection to Netezza fails
 */
export class ConnectionError extends NetezzaError {
  constructor(
    public readonly host: string,
    public readonly port: number,
    cause?: Error
  ) {
    const message = cause?.message
      ? `Cannot connect to Netezza at ${host}:${port}. ${cause.message}`
      : `Cannot connect to Netezza at ${host}:${port}`;
    super(message, 'CONNECTION_ERROR', cause);
    this.name = 'ConnectionError';
  }
}

/**
 * Error thrown when a query exceeds the configured timeout
 */
export class QueryTimeoutError extends NetezzaError {
  constructor(
    public readonly timeoutMs: number,
    public readonly queryPreview?: string
  ) {
    const preview = queryPreview ? ` Query: ${queryPreview}` : '';
    super(
      `Query execution exceeded timeout of ${timeoutMs}ms.${preview}`,
      'QUERY_TIMEOUT'
    );
    this.name = 'QueryTimeoutError';
  }
}

/**
 * Error thrown when pool is not initialized
 */
export class PoolNotInitializedError extends NetezzaError {
  constructor() {
    super('Connection pool is not initialized. Call open() first.', 'POOL_NOT_INITIALIZED');
    this.name = 'PoolNotInitializedError';
  }
}

/**
 * Error thrown when acquiring a connection from pool fails
 */
export class PoolAcquireError extends NetezzaError {
  constructor(cause?: Error) {
    super(
      `Failed to acquire connection from pool. ${cause?.message ?? ''}`,
      'POOL_ACQUIRE_ERROR',
      cause
    );
    this.name = 'PoolAcquireError';
  }
}

/**
 * Error thrown when query execution fails
 */
export class QueryExecutionError extends NetezzaError {
  constructor(
    public readonly query: string,
    cause?: Error
  ) {
    const queryPreview = query.replace(/\s+/g, ' ').trim().substring(0, 100);
    super(
      `Query execution failed: ${cause?.message ?? 'Unknown error'}. Query: ${queryPreview}`,
      'QUERY_EXECUTION_ERROR',
      cause
    );
    this.name = 'QueryExecutionError';
  }
}

/**
 * Error thrown when catalog switch fails
 */
export class CatalogSwitchError extends NetezzaError {
  constructor(
    public readonly catalog: string,
    cause?: Error
  ) {
    super(
      `Failed to switch to catalog "${catalog}". ${cause?.message ?? ''}`,
      'CATALOG_SWITCH_ERROR',
      cause
    );
    this.name = 'CatalogSwitchError';
  }
}
