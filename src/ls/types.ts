import { NSDatabase } from '@sqltools/types';

/**
 * Cache structure for IntelliSense completions
 */
export interface CompletionsCache {
  keywords: string[];
  functions: string[];
  dataTypes: string[];
  schemas: SchemaCompletion[];
  tables: TableCompletion[];
  columns: ColumnCompletion[];
  variables: any[];
}

export interface SchemaCompletion {
  label: string;
  detail: string;
  type: string;
}

export interface TableCompletion {
  label: string;
  schema?: string;
  detail?: string;
  type: string;
}

export interface ColumnCompletion {
  label: string;
  table?: string;
  schema?: string;
  dataType?: string;
  type: string;
}

/**
 * Connection credentials for Netezza
 */
export interface NetezzaCredentials {
  server: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  previewLimit?: number;
  netezzaOptions?: NetezzaDriverOptions;
}

/**
 * Connection options for Netezza
 */
export interface NetezzaConnectionOptions {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean | SslOptions;
  [key: string]: any;
}

export interface SslOptions {
  ca?: string | Buffer;
  rejectUnauthorized?: boolean;
}

/**
 * Pool configuration options
 */
export interface PoolOptions {
  min?: number;
  max?: number;
  idleTimeoutMillis?: number;
}

/**
 * Netezza-specific driver options
 */
export interface NetezzaDriverOptions {
  queryTimeout?: number;
  secureConnection?: boolean;
  pool?: PoolOptions;
}

/**
 * Options for creating result objects
 */
export interface ResultOptions {
  query: string;
  cols?: string[];
  results?: any[];
  messages?: string[];
  error?: boolean;
  rawError?: Error;
}

/**
 * Query execution info for logging and tracking
 */
export interface QueryInfo {
  index: number;
  total: number;
}

/**
 * Parser state for SQL parsing
 */
export interface ParserState {
  inSingleQuote: boolean;
  inDoubleQuote: boolean;
  inLineComment: boolean;
  inBlockComment: boolean;
}

/**
 * Completion request parameters
 */
export interface CompletionParams {
  position: number;
  query: string;
}
