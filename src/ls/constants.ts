/**
 * Default configuration values for the Netezza driver
 */
export const DEFAULT_QUERY_TIMEOUT_MS = 30000;
export const DEFAULT_PORT = 5480;
export const DEFAULT_POOL_MIN = 1;
export const DEFAULT_POOL_MAX = 5;
export const DEFAULT_IDLE_TIMEOUT_MS = 30000;
export const COMPLETION_QUERY_TIMEOUT_MS = 10000;

/**
 * Netezza-specific SQL keywords
 */
export const NETEZZA_KEYWORDS = [
  'DISTRIBUTE', 'ORGANIZE', 'ZONE', 'GROOM', 'GENERATE_STATISTICS',
  'MATERIALIZED', 'EXTERNAL', 'SAMPLED', 'TEMP', 'TEMPORARY',
] as const;

/**
 * Standard SQL keywords
 */
export const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL',
  'GROUP', 'ORDER', 'HAVING', 'UNION', 'EXCEPT', 'INTERSECT',
  'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP', 'TRUNCATE',
  'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL',
  'DISTINCT', 'ALL', 'ANY', 'SOME', 'BY', 'ON', 'USING', 'CASE', 'WHEN',
  'THEN', 'ELSE', 'END', 'WITH', 'RECURSIVE', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
  'TABLE', 'VIEW', 'INDEX', 'SEQUENCE', 'DATABASE', 'SCHEMA', 'CONSTRAINT',
  'PRIMARY', 'FOREIGN', 'KEY', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT',
  'INTO', 'VALUES', 'SET', 'CAST', 'CONVERT',
] as const;

/**
 * All supported keywords (Netezza + Standard SQL)
 */
export const ALL_KEYWORDS = [...NETEZZA_KEYWORDS, ...SQL_KEYWORDS] as const;

/**
 * Netezza-specific functions
 */
export const NETEZZA_FUNCTIONS = [
  'REGEXP_EXTRACT', 'REGEXP_LIKE', 'REGEXP_REPLACE',
  'TO_CHAR', 'TO_DATE', 'TO_NUMBER', 'TO_TIMESTAMP',
] as const;

/**
 * Standard SQL functions
 */
export const SQL_FUNCTIONS = [
  // Aggregate functions
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
  // String functions
  'CONCAT', 'SUBSTR', 'LENGTH', 'TRIM', 'LTRIM', 'RTRIM', 'UPPER', 'LOWER',
  'REPLACE', 'POSITION', 'STRPOS',
  // Date/Time functions
  'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'NOW', 'EXTRACT',
  'DATE_PART', 'DATE_TRUNC', 'AGE', 'INTERVAL',
  // Math functions
  'ABS', 'CEIL', 'FLOOR', 'ROUND', 'TRUNC', 'MOD', 'POWER', 'SQRT', 'EXP', 'LN', 'LOG',
  // Conditional functions
  'COALESCE', 'NULLIF', 'GREATEST', 'LEAST',
  // Window functions
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE',
] as const;

/**
 * All supported functions (Netezza + Standard SQL)
 */
export const ALL_FUNCTIONS = [...NETEZZA_FUNCTIONS, ...SQL_FUNCTIONS] as const;

/**
 * Netezza data types
 */
export const DATA_TYPES = [
  // Integer types
  'BYTEINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'INT', 'INT1', 'INT2', 'INT4', 'INT8',
  // Numeric types
  'NUMERIC', 'DECIMAL', 'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION',
  // String types
  'CHARACTER', 'VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'TEXT',
  // Date/Time types
  'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
  // Other types
  'BOOLEAN', 'BOOL', 'BINARY', 'VARBINARY',
] as const;
