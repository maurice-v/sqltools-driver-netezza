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
