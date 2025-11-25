import { ParserState } from './types';

/**
 * SQL Query Parser for splitting multi-statement queries
 * Handles string literals, comments, and special Netezza syntax
 */
export class QueryParser {
  /**
   * Parses a SQL string and splits it into individual statements
   * @param query - The SQL query string to parse
   * @returns Array of individual SQL statements
   */
  parse(query: string): string[] {
    if (!query || query.trim().length === 0) {
      return [];
    }

    const queries: string[] = [];
    let currentQuery = '';
    const state: ParserState = {
      inSingleQuote: false,
      inDoubleQuote: false,
      inLineComment: false,
      inBlockComment: false,
    };

    for (let i = 0; i < query.length; i++) {
      const char = query[i];
      const nextChar = query[i + 1] ?? '';
      const prevChar = i > 0 ? query[i - 1] : '';

      // Update parser state
      this.updateState(state, char, nextChar, prevChar);

      // Check if this semicolon ends a statement
      if (this.isStatementEnd(char, state)) {
        const trimmed = currentQuery.trim();
        if (trimmed.length > 0) {
          queries.push(trimmed);
        }
        currentQuery = '';
        continue;
      }

      currentQuery += char;
    }

    // Add any remaining query text
    const remaining = currentQuery.trim();
    if (remaining.length > 0) {
      queries.push(remaining);
    }

    return queries.length > 0 ? queries : [query.trim()].filter(q => q.length > 0);
  }

  /**
   * Updates the parser state based on the current character
   */
  private updateState(
    state: ParserState,
    char: string,
    nextChar: string,
    prevChar: string
  ): void {
    // Handle newlines (end line comments)
    if (char === '\n' && state.inLineComment) {
      state.inLineComment = false;
      return;
    }

    // Skip state changes inside comments
    if (state.inLineComment) {
      return;
    }

    // Handle block comment end
    if (state.inBlockComment) {
      if (prevChar === '*' && char === '/') {
        state.inBlockComment = false;
      }
      return;
    }

    // Handle string quotes (not in any string or comment)
    if (!state.inSingleQuote && !state.inDoubleQuote) {
      // Check for comment starts
      if (char === '-' && nextChar === '-') {
        state.inLineComment = true;
        return;
      }
      if (char === '/' && nextChar === '*') {
        state.inBlockComment = true;
        return;
      }
      
      // Check for string starts
      if (char === "'") {
        state.inSingleQuote = true;
      } else if (char === '"') {
        state.inDoubleQuote = true;
      }
    } else {
      // Handle string ends (check for escaped quotes)
      if (state.inSingleQuote) {
        if (char === "'" && prevChar !== '\\') {
          // Check for doubled quotes (escape in SQL)
          if (nextChar !== "'") {
            state.inSingleQuote = false;
          }
        }
      } else if (state.inDoubleQuote) {
        if (char === '"' && prevChar !== '\\') {
          if (nextChar !== '"') {
            state.inDoubleQuote = false;
          }
        }
      }
    }
  }

  /**
   * Determines if the current character marks the end of a statement
   */
  private isStatementEnd(char: string, state: ParserState): boolean {
    return (
      char === ';' &&
      !state.inSingleQuote &&
      !state.inDoubleQuote &&
      !state.inLineComment &&
      !state.inBlockComment
    );
  }

  /**
   * Extracts table names from FROM and JOIN clauses
   */
  extractTables(query: string): string[] {
    const tables: string[] = [];

    // Match FROM clause tables
    const fromRegex = /FROM\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)/gi;
    let match: RegExpExecArray | null;
    
    while ((match = fromRegex.exec(query)) !== null) {
      tables.push(match[1]);
    }

    // Match JOIN clause tables
    const joinRegex = /JOIN\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)/gi;
    
    while ((match = joinRegex.exec(query)) !== null) {
      tables.push(match[1]);
    }

    // Return unique table names
    return [...new Set(tables)];
  }

  /**
   * Gets the last SQL keyword before a given position in the query
   */
  getLastKeyword(text: string): string {
    const keywords = text
      .toUpperCase()
      .split(/\s+/)
      .filter(k => k.length > 0);
    return keywords[keywords.length - 1] || '';
  }

  /**
   * Checks if a query is a SET CATALOG statement and extracts the catalog name
   */
  extractCatalogFromSetStatement(query: string): string | null {
    const match = query.trim().match(/^SET\s+CATALOG\s+(\w+)/i);
    return match ? match[1] : null;
  }
}
