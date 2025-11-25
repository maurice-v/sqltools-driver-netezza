import { NSDatabase } from '@sqltools/types';
import { v4 as generateId } from 'uuid';
import { ResultOptions } from './types';

/**
 * Builder class for creating standardized query results
 */
export class ResultBuilder {
  constructor(private readonly connectionId: string) {}

  /**
   * Creates a standard result object
   */
  create(options: ResultOptions): NSDatabase.IResult {
    return {
      connId: this.connectionId,
      requestId: options.query,
      resultId: generateId(),
      cols: options.cols || [],
      messages: options.messages || [],
      query: options.query,
      results: options.results || [],
      ...(options.error && { error: true, rawError: options.rawError }),
    };
  }

  /**
   * Creates a success result with data
   */
  success(
    query: string,
    cols: string[],
    results: any[],
    messages: string[] = []
  ): NSDatabase.IResult {
    return this.create({
      query,
      cols,
      results,
      messages,
    });
  }

  /**
   * Creates an error result
   */
  error(
    query: string,
    error: Error,
    additionalMessages: string[] = [],
    catalog?: string | null
  ): NSDatabase.IResult {
    const queryPreview = query.replace(/\s+/g, ' ').trim();
    
    return this.create({
      query,
      cols: ['error'],
      messages: [
        ...(catalog ? [`Database: ${catalog}`] : []),
        `Query: ${queryPreview}`,
        `═══════════════════════════════════════════`,
        `❌ QUERY FAILED`,
        `═══════════════════════════════════════════`,
        `Error: ${error.message}`,
        ...additionalMessages,
      ],
      error: true,
      rawError: error,
    });
  }

  /**
   * Creates a timeout error result
   */
  timeout(
    query: string, 
    timeoutMs: number, 
    elapsedMs: number,
    catalog?: string | null
  ): NSDatabase.IResult {
    const queryPreview = query.replace(/\s+/g, ' ').trim();
    
    return this.create({
      query,
      cols: ['error'],
      messages: [
        ...(catalog ? [`Database: ${catalog}`] : []),
        `Query: ${queryPreview}`,
        `═══════════════════════════════════════════`,
        `⏱️ QUERY TIMEOUT`,
        `═══════════════════════════════════════════`,
        `Query exceeded timeout of ${timeoutMs}ms`,
        `Elapsed time: ${elapsedMs}ms`,
        `─────────────────────────────────────────`,
        `The query was cancelled. Connection will be reset.`,
      ],
      error: true,
      rawError: new Error(`Query timeout after ${timeoutMs}ms`),
    });
  }

  /**
   * Creates a connection error result
   */
  connectionError(query: string, errorMessage: string): NSDatabase.IResult {
    return this.create({
      query,
      cols: ['error'],
      messages: [
        `═══════════════════════════════════════════`,
        `❌ CONNECTION ERROR`,
        `═══════════════════════════════════════════`,
        errorMessage,
      ],
      error: true,
      rawError: new Error(errorMessage),
    });
  }

  /**
   * Creates an empty result (for skipped operations)
   */
  empty(query: string, message: string): NSDatabase.IResult {
    return this.create({
      query,
      cols: [],
      results: [],
      messages: [message],
    });
  }

  /**
   * Creates a count result stub (for performance optimization)
   */
  countStub(): NSDatabase.IResult {
    return this.create({
      query: 'COUNT(*)',
      cols: ['total'],
      results: [{ total: 0 }],
      messages: ['Row count skipped for performance'],
    });
  }
}
