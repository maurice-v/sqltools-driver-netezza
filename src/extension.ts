import * as vscode from 'vscode';
import { IExtension, IExtensionPlugin, IDriverExtensionApi } from '@sqltools/types';
import { ExtensionContext } from 'vscode';
import { DRIVER_ALIASES } from './constants';
import queries from './ls/queries';
const { publisher, name } = require('../package.json');

export async function activate(extContext: ExtensionContext): Promise<IDriverExtensionApi> {
  const sqltools = vscode.extensions.getExtension<IExtension>('mtxr.sqltools');
  
  if (!sqltools) {
    throw new Error('SQLTools extension not found. Please install SQLTools first.');
  }

  await sqltools.activate();

  const api = sqltools.exports;

  // Helper function to parse queries (matches driver's parse logic)
  function parseQueries(text: string): { query: string; startOffset: number; endOffset: number }[] {
    const queries: { query: string; startOffset: number; endOffset: number }[] = [];
    let currentQuery = '';
    let queryStartOffset = 0;
    let inSingleQuote = false;
    let inDoubleQuote = false;
    let inLineComment = false;
    let inBlockComment = false;
    
    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      const nextChar = i + 1 < text.length ? text[i + 1] : '';
      
      if (!inSingleQuote && !inDoubleQuote && !inBlockComment && char === '-' && nextChar === '-') {
        inLineComment = true;
        currentQuery += char;
        continue;
      }
      
      if (inLineComment && char === '\n') {
        inLineComment = false;
        currentQuery += char;
        continue;
      }
      
      if (!inSingleQuote && !inDoubleQuote && !inLineComment && char === '/' && nextChar === '*') {
        inBlockComment = true;
        currentQuery += char;
        continue;
      }
      
      if (inBlockComment && char === '*' && nextChar === '/') {
        inBlockComment = false;
        currentQuery += char + nextChar;
        i++;
        continue;
      }
      
      if (!inLineComment && !inBlockComment) {
        if (char === "'" && !inDoubleQuote) {
          if (inSingleQuote && nextChar === "'") {
            currentQuery += char + nextChar;
            i++;
            continue;
          }
          inSingleQuote = !inSingleQuote;
        } else if (char === '"' && !inSingleQuote) {
          if (inDoubleQuote && nextChar === '"') {
            currentQuery += char + nextChar;
            i++;
            continue;
          }
          inDoubleQuote = !inDoubleQuote;
        }
      }
      
      if (char === ';' && !inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment) {
        currentQuery += char;
        const trimmedQuery = currentQuery.trim();
        if (trimmedQuery.length > 0) {
          queries.push({
            query: trimmedQuery,
            startOffset: queryStartOffset,
            endOffset: i + 1
          });
        }
        currentQuery = '';
        queryStartOffset = i + 1;
        continue;
      }
      
      currentQuery += char;
    }
    
    const trimmedQuery = currentQuery.trim();
    if (trimmedQuery.length > 0) {
      queries.push({
        query: trimmedQuery,
        startOffset: queryStartOffset,
        endOffset: text.length
      });
    }
    
    return queries;
  }

  // CodeLens provider for Execute Query at Cursor
  class NetezzaCodeLensProvider implements vscode.CodeLensProvider {
    public provideCodeLenses(document: vscode.TextDocument): vscode.CodeLens[] {
      console.log('[Netezza CodeLens] provideCodeLenses called for:', document.uri.toString(), 'language:', document.languageId);
      
      if (document.languageId !== 'sql') {
        console.log('[Netezza CodeLens] Skipping non-SQL document');
        return [];
      }

      const text = document.getText();
      const queries = parseQueries(text);
      const codeLenses: vscode.CodeLens[] = [];

      console.log(`[Netezza CodeLens] Found ${queries.length} queries`);

      for (const queryInfo of queries) {
        // Find the first line of actual SQL code (skip leading whitespace and comments)
        const queryText = queryInfo.query;
        let sqlStartOffset = 0;
        const lines = queryText.split('\n');
        
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i].trim();
          // Skip empty lines and comments
          if (line && !line.startsWith('--') && !line.startsWith('/*')) {
            // This is the first line with actual SQL
            sqlStartOffset = queryText.indexOf(lines[i]);
            break;
          }
        }
        
        const actualStartOffset = queryInfo.startOffset + sqlStartOffset;
        const startPos = document.positionAt(actualStartOffset);
        const range = new vscode.Range(startPos, startPos);
        
        const codeLens = new vscode.CodeLens(range, {
          title: '▶ Execute Query',
          command: 'sqltools.driver.netezza.executeQueryAtCursor',
          tooltip: 'Execute this query (Ctrl+Alt+E)',
          arguments: [queryInfo]
        });
        
        codeLenses.push(codeLens);
        console.log(`[Netezza CodeLens] Added CodeLens at offset ${actualStartOffset} (query starts at ${queryInfo.startOffset})`);
      }

      return codeLenses;
    }
  }

  // Register the CodeLens provider for both file and untitled schemes
  extContext.subscriptions.push(
    vscode.languages.registerCodeLensProvider(
      [
        { language: 'sql', scheme: 'file' },
        { language: 'sql', scheme: 'untitled' }
      ],
      new NetezzaCodeLensProvider()
    )
  );

  // Register command to execute query at cursor position
  extContext.subscriptions.push(
    vscode.commands.registerCommand('sqltools.driver.netezza.executeQueryAtCursor', async (queryInfo?: { query: string; startOffset: number; endOffset: number }) => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) {
        void vscode.window.showErrorMessage('No active editor');
        return;
      }

      const document = editor.document;
      if (document.languageId !== 'sql') {
        void vscode.window.showWarningMessage('This command only works in SQL files');
        return;
      }

      const fullText = document.getText();
      const selection = editor.selection;
      
      // If called from CodeLens with queryInfo, use that
      if (queryInfo) {
        const startPos = document.positionAt(queryInfo.startOffset);
        const endPos = document.positionAt(queryInfo.endOffset);
        editor.selection = new vscode.Selection(startPos, endPos);
        
        try {
          await vscode.commands.executeCommand('sqltools.executeQuery');
        } catch (error: any) {
          void vscode.window.showErrorMessage(`Failed to execute query: ${error.message || error}`);
        }
        return;
      }
      
      // If user has a selection, use it directly
      if (!selection.isEmpty) {
        const selectedText = document.getText(selection).trim();
        if (selectedText) {
          console.log(`[Netezza Extension] Using user selection: ${selectedText.substring(0, 50)}...`);
          // Keep the existing selection and execute
          await vscode.commands.executeCommand('sqltools.executeQuery');
          return;
        }
      }
      
      // No selection - find query at cursor position
      const cursorOffset = document.offsetAt(editor.selection.active);
      
      // Parse queries
      const queries = parseQueries(fullText);
      
      if (queries.length === 0) {
        void vscode.window.showWarningMessage('No query found');
        return;
      }
      
      // Find which query contains the cursor
      let targetQueryInfo: { query: string; startOffset: number; endOffset: number } | null = null;
      
      for (const queryInfo of queries) {
        if (cursorOffset >= queryInfo.startOffset && cursorOffset <= queryInfo.endOffset) {
          targetQueryInfo = queryInfo;
          console.log(`[Netezza Extension] Found query at cursor: ${queryInfo.query.substring(0, 50)}...`);
          break;
        }
      }
      
      if (!targetQueryInfo) {
        // Cursor not in any query, use the first one
        targetQueryInfo = queries[0];
        console.log(`[Netezza Extension] Cursor not in query, using first query`);
      }
      
      // Select the query text in the editor so SQLTools will execute only that
      const startPos = document.positionAt(targetQueryInfo.startOffset);
      const endPos = document.positionAt(targetQueryInfo.endOffset);
      editor.selection = new vscode.Selection(startPos, endPos);
      
      // Execute using SQLTools' executeQuery command which respects selection
      try {
        await vscode.commands.executeCommand('sqltools.executeQuery');
      } catch (error: any) {
        void vscode.window.showErrorMessage(`Failed to execute query: ${error.message || error}`);
      }
    })
  );

  // Register the "Generate CREATE Script" command
  extContext.subscriptions.push(
    vscode.commands.registerCommand('sqltools.driver.netezza.showCreateTable', async (treeItem: any) => {
      try {
        if (!treeItem) {
          void vscode.window.showErrorMessage('Please select a table or view');
          return;
        }

        const table = treeItem;
        const connId = table.conn || table.connectionId;
        
        if (!connId) {
          void vscode.window.showErrorMessage('No active connection found. Please connect to the database first.');
          return;
        }
        
        // Build proper table object with schema and tableName
        let schema = table.schema;
        let tableName = table.tableName;
        let database = table.database;
        
        // If schema or tableName are missing, try to extract from label
        if ((!schema || !tableName) && table.label) {
          const parts = table.label.split('.');
          if (parts.length === 3) {
            database = database || parts[0];
            schema = schema || parts[1];
            tableName = tableName || parts[2];
          } else if (parts.length === 2) {
            schema = schema || parts[0];
            tableName = tableName || parts[1];
          } else if (parts.length === 1) {
            tableName = tableName || parts[0];
          }
        }
        
        const tableInfo = {
          schema: schema,
          tableName: tableName,
          label: table.label,
          database: database
        };
        
        // Generate the query with proper parameters
        const query = typeof queries.getTableCreateScript === 'function' 
          ? queries.getTableCreateScript(tableInfo)
          : queries.getTableCreateScript;
        
        // Execute query through SQLTools
        const results = await vscode.commands.executeCommand('sqltools.executeQuery', query, connId);
        
        // Extract DDL from results
        let ddl = '';
        if (results && Array.isArray(results) && results.length > 0) {
          const result = results[0];
          if (result.results && result.results.length > 0) {
            ddl = result.results[0].DDL || result.results[0].ddl || '';
          }
        }
        
        if (ddl) {
          const doc = await vscode.workspace.openTextDocument({
            content: ddl,
            language: 'sql'
          });
          await vscode.window.showTextDocument(doc);
        } else {
          void vscode.window.showWarningMessage('No DDL could be generated for this table');
        }
      } catch (error: any) {
        void vscode.window.showErrorMessage(`Failed to generate DDL: ${error.message || error}`);
      }
    })
  );

  const extensionId = `${publisher}.${name}`;
  const plugin: IExtensionPlugin = {
    extensionId,
    name: 'Netezza Plugin',
    type: 'driver',
    async register(extension) {
      // Register icons
      extension.resourcesMap().set(`driver/${DRIVER_ALIASES[0].value}/icons`, {
        active: extContext.asAbsolutePath('icons/active.png'),
        default: extContext.asAbsolutePath('icons/default.png'),
        inactive: extContext.asAbsolutePath('icons/inactive.png'),
      });
      
      // Register each driver alias with schemas
      DRIVER_ALIASES.forEach(({ value }) => {
        extension.resourcesMap().set(`driver/${value}/extension-id`, extensionId);
        extension.resourcesMap().set(`driver/${value}/connection-schema`, extContext.asAbsolutePath('connection.schema.json'));
        extension.resourcesMap().set(`driver/${value}/ui-schema`, extContext.asAbsolutePath('ui.schema.json'));
      });
      
      // Register the language server plugin
      await extension.client.sendRequest('ls/RegisterPlugin', { 
        path: extContext.asAbsolutePath('out/ls/plugin.js') 
      });
    }
  };

  api.registerPlugin(plugin);

  return {
    driverName: DRIVER_ALIASES[0].value,
    parseBeforeSaveConnection: ({ connInfo }) => {
      return connInfo;
    },
    parseBeforeEditConnection: ({ connInfo }) => {
      return connInfo;
    },
    driverAliases: DRIVER_ALIASES,
  };
}

export function deactivate() {}
