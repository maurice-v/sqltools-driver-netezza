import { IBaseQueries, ContextValue } from '@sqltools/types';

/**
 * Escapes and qualifies a table name for use in SQL queries
 */
function escapeTableName(table: any): string {
  const simpleName = table.tableName || table.label?.split('.').pop() || table;
  
  // Generate fully qualified name
  if (table.database && table.schema) {
    return `${table.database}.${table.schema}.${simpleName}`;
  }
  if (table.schema) {
    return `${table.schema}.${simpleName}`;
  }
  return simpleName;
}

/**
 * Generates a SET CATALOG statement for switching the current catalog/database
 */
export function generateSetCatalogQuery(catalog: string): string {
  return `SET CATALOG ${catalog};`;
}

/**
 * SQL queries for interacting with Netezza database metadata
 * Uses Netezza system views (_V_*) to retrieve schema information
 */
const queries: IBaseQueries = {
  describeTable: (params) => {
    const tableName = params.tableName || params.label?.split('.').pop();
    return `
    SELECT 
      ATTNAME AS label,
      ATTNAME AS "columnName",
      FORMAT_TYPE AS "dataType",
      ATTNOTNULL AS "isNullable",
      ATTTYPMOD AS "size",
      DESCRIPTION AS detail,
      '${ContextValue.COLUMN}' as "type",
      true AS "isLeaf",
      CASE 
        WHEN FORMAT_TYPE LIKE '%CHAR%' THEN 'value'
        WHEN FORMAT_TYPE LIKE '%INT%' THEN '0'
        WHEN FORMAT_TYPE LIKE '%NUMERIC%' THEN '0.0'
        WHEN FORMAT_TYPE LIKE '%DECIMAL%' THEN '0.0'
        WHEN FORMAT_TYPE LIKE '%FLOAT%' THEN '0.0'
        WHEN FORMAT_TYPE LIKE '%DOUBLE%' THEN '0.0'
        WHEN FORMAT_TYPE LIKE '%DATE%' THEN '2024-01-01'
        WHEN FORMAT_TYPE LIKE '%TIME%' THEN '00:00:00'
        WHEN FORMAT_TYPE LIKE '%BOOL%' THEN 'false'
        ELSE NULL
      END AS "defaultValue"
    FROM 
      _V_RELATION_COLUMN
    WHERE 
      UPPER(NAME) = UPPER('${tableName}')
      AND UPPER(SCHEMA) = UPPER('${params.schema}')
    ORDER BY 
      ATTNUM`;
  },

  fetchColumns: (params) => {
    const tableName = params.tableName || params.label?.split('.').pop();
    return `
    SELECT 
      C.ATTNAME AS label,
      C.ATTNAME AS "columnName",
      C.FORMAT_TYPE AS "dataType",
      C.ATTNOTNULL AS "isNullable",
      C.ATTTYPMOD AS "size",
      C.DESCRIPTION AS detail,
      C.SCHEMA AS "schema",
      C.NAME AS "table",
      '${ContextValue.COLUMN}' as "type",
      C.ATTNUM AS "columnPosition",
      true AS "isLeaf",
      '${ContextValue.NO_CHILD}' AS "childType",
      CASE 
        WHEN C.FORMAT_TYPE LIKE '%CHAR%' THEN 'value'
        WHEN C.FORMAT_TYPE LIKE '%INT%' THEN '0'
        WHEN C.FORMAT_TYPE LIKE '%NUMERIC%' THEN '0.0'
        WHEN C.FORMAT_TYPE LIKE '%DECIMAL%' THEN '0.0'
        WHEN C.FORMAT_TYPE LIKE '%FLOAT%' THEN '0.0'
        WHEN C.FORMAT_TYPE LIKE '%DOUBLE%' THEN '0.0'
        WHEN C.FORMAT_TYPE LIKE '%DATE%' THEN '2024-01-01'
        WHEN C.FORMAT_TYPE LIKE '%TIME%' THEN '00:00:00'
        WHEN C.FORMAT_TYPE LIKE '%BOOL%' THEN 'false'
        ELSE NULL
      END AS "defaultValue"
    FROM 
      _V_RELATION_COLUMN C
    WHERE 
      UPPER(C.SCHEMA) = UPPER('${params.schema}')
      AND UPPER(C.NAME) = UPPER('${tableName}')
    ORDER BY 
      C.ATTNUM`;
  },

  fetchRecords: (params) => `SELECT * FROM ${escapeTableName(params.table)} LIMIT ${params.limit || 50}`,

  countRecords: (params) => `SELECT COUNT(*) AS total FROM ${escapeTableName(params.table)}`,

  fetchTables: (params) => `
    SELECT 
      TABLENAME AS label,
      '${ContextValue.TABLE}' AS "type",
      '${ContextValue.RESOURCE_GROUP}.${ContextValue.TABLE}' AS "contextValue",
      OBJID AS id,
      SCHEMA AS "schema",
      CURRENT_CATALOG AS "database",
      TABLENAME AS tableName,
      CURRENT_CATALOG || '.' || SCHEMA AS description
    FROM 
      _V_TABLE
    WHERE 
      SCHEMA = '${params.schema}'
    ORDER BY 
      TABLENAME`,

  fetchViews: (params) => `
    SELECT 
      VIEWNAME AS label,
      '${ContextValue.VIEW}' AS "type",
      '${ContextValue.RESOURCE_GROUP}.${ContextValue.VIEW}' AS "contextValue",
      OBJID AS id,
      SCHEMA AS "schema",
      CURRENT_CATALOG AS "database",
      VIEWNAME AS tableName,
      CURRENT_CATALOG || '.' || SCHEMA AS description
    FROM 
      _V_VIEW
    WHERE 
      SCHEMA = '${params.schema}'
    ORDER BY 
      VIEWNAME`,

  searchTables: (params) => `
    SELECT 
      TABLENAME AS label,
      '${ContextValue.TABLE}' AS "type",
      SCHEMA AS "schema",
      CURRENT_CATALOG AS "database",
      TABLENAME AS tableName
    FROM 
      _V_TABLE
    WHERE 
      LOWER(SCHEMA || '.' || TABLENAME) LIKE '${params.search}'
    ORDER BY 
      TABLENAME
    LIMIT ${params.limit || 100}`,

  searchColumns: (params) => `
    SELECT 
      C.ATTNAME AS label,
      C.FORMAT_TYPE AS datatype,
      C.SCHEMA AS "schema",
      C.NAME AS "table",
      CURRENT_CATALOG AS "database",
      '${ContextValue.COLUMN}' as "type",
      true AS "isLeaf"
    FROM 
      _V_RELATION_COLUMN C
    WHERE 
      LOWER(C.SCHEMA || '.' || C.NAME || '.' || C.ATTNAME) LIKE '${params.search}'
    ORDER BY 
      C.NAME, C.ATTNUM
    LIMIT ${params.limit || 100}`,

  fetchSchemas: (params) => {
    // If a database is specified, we need to query it directly
    // Note: In Netezza, _V_SCHEMA doesn't have a DATABASE column,
    // so we have to trust that CURRENT_CATALOG is set appropriately
    // The driver will handle setting the catalog if needed
    const database = params?.database || '';
    return `
    SELECT 
      SCHEMA AS label,
      SCHEMA AS "schema",
      CURRENT_CATALOG AS "database",
      '${ContextValue.SCHEMA}' AS "type"
    FROM 
      _V_SCHEMA
    WHERE 
      SCHEMA NOT IN ('SYSTEM', 'DEFINITION_SCHEMA')
    ORDER BY 
      SCHEMA`;
  },

  fetchDatabases: (params) => `
    SELECT 
      DATABASE AS label,
      DATABASE AS "database",
      '${ContextValue.DATABASE}' AS "type"
    FROM 
      _V_DATABASE
    ORDER BY 
      DATABASE`,

  getTableCreateScript: (params) => {
    // Extract table name from various possible property names
    let tableName = params?.tableName || (params as any)?.name;
    
    // If still no table name, try to extract from label
    if (!tableName && params?.label) {
      const parts = params.label.split('.');
      tableName = parts[parts.length - 1];
    }
    
    // Extract schema
    let schema = params?.schema;
    
    // If no schema, try to extract from label (database.schema.table format)
    if (!schema && params?.label && params.label.split('.').length >= 2) {
      const parts = params.label.split('.');
      if (parts.length === 3) {
        schema = parts[1];
      } else if (parts.length === 2) {
        schema = parts[0];
      }
    }
    
    if (!tableName || !schema) {
      throw new Error(`Missing required parameters for getTableCreateScript. tableName: ${tableName}, schema: ${schema}`);
    }
    
    return `
WITH
table_cols AS (
    SELECT 
        SCHEMA,
        TABLENAME,
        trim(trailing ',' FROM trim(trailing chr(10) FROM replace(replace(replace (replace (
                XMLserialize(
                    XMLagg(
                        XMLElement(
                            'X',
                            chr(34) || ATTNAME || chr(34) || ' ' || FORMAT_TYPE || CASE WHEN ATTNOTNULL = 't' THEN ' NOT NULL' ELSE '' END || '^CRLF'
                        ) 
                    )
                )               
                ,'<X>','') ,'</X>',',') ,'&quot;','"') ,'^CRLF,', ',' ||chr(10))
            )) AS TABLE_COLUMNS
    FROM (
        SELECT 
            SCHEMA,
            NAME AS TABLENAME,
            ATTNAME,
            FORMAT_TYPE,
            ATTNOTNULL
        FROM _V_RELATION_COLUMN
        WHERE 
            UPPER(SCHEMA) = UPPER('${schema}')
            AND UPPER(NAME) = UPPER('${tableName}')
        ORDER BY 
            ATTNUM
        ) x
    GROUP BY
        SCHEMA,
        TABLENAME
),
dist_cols AS (
    SELECT
        SCHEMA,
        TABLENAME,
        trim(trailing ',' FROM replace(replace(replace (
            XMLserialize(
                XMLagg(
                    XMLElement(
                        'X',
                        chr(34) || ATTNAME || chr(34)
                    )
                )
            )               
            , '<X>','' ),'</X>' ,',' ),'&quot;','"')
        ) AS DIST_COLUMNS
    FROM (
        SELECT
            SCHEMA,
            TABLENAME,
            ATTNAME,
            DISTSEQNO
        FROM 
            _V_TABLE_DIST_MAP
        WHERE 
            DISTSEQNO IS NOT NULL
            AND UPPER(SCHEMA) = UPPER('${schema}')
            AND UPPER(TABLENAME) = UPPER('${tableName}')
        ORDER BY 
            DISTSEQNO
    ) x
    GROUP BY
        SCHEMA,
        TABLENAME
),
org_cols AS(
    SELECT
        SCHEMA,
        TABLENAME,
        trim(trailing ',' FROM replace(replace(replace (
            XMLserialize(
                XMLagg(
                    XMLElement(
                        'X',
                        chr(34) || ATTNAME || chr(34)
                    )
                )
            )               
            , '<X>','' ),'</X>' ,',' ),'&quot;','"')
        ) AS ORG_COLUMNS
    FROM (
        SELECT
            SCHEMA,
            TABLENAME,
            ATTNAME,
            ORGSEQNO
        FROM 
            _V_TABLE_ORGANIZE_COLUMN
        WHERE 
            ORGSEQNO IS NOT NULL
            AND UPPER(SCHEMA) = UPPER('${schema}')
            AND UPPER(TABLENAME) = UPPER('${tableName}')
        ORDER BY 
            ORGSEQNO
    ) x
    GROUP BY
        SCHEMA,
        TABLENAME
)
SELECT 
    'CREATE TABLE ' || CURRENT_CATALOG || '.' || table_cols.schema || '.' || table_cols.tablename || ' (' || CHR(10)
    || table_cols.TABLE_COLUMNS || CHR(10)
    || ')' || CHR(10)
    || COALESCE ('DISTRIBUTE ON ('|| dist_cols.DIST_COLUMNS ||') ', 'DISTRIBUTE ON RANDOM ') || CHR(10)
    || COALESCE ('ORGANIZE ON ('|| org_cols.ORG_COLUMNS ||')', '') 
    || ';' AS "DDL"
FROM table_cols
LEFT JOIN dist_cols 
    ON table_cols.SCHEMA = dist_cols.SCHEMA
    AND table_cols.TABLENAME = dist_cols.TABLENAME
LEFT JOIN org_cols 
    ON table_cols.SCHEMA = org_cols.SCHEMA
    AND table_cols.TABLENAME = org_cols.TABLENAME
`;
  },
  setCatalog: generateSetCatalogQuery
};

export default queries;
