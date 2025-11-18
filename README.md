# SQLTools Netezza Driver

A Visual Studio Code extension that adds Netezza database support to [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools).

## Features

- Connect to Netezza databases
- Execute SQL queries
- Browse database schemas, tables, views, and columns
- Autocomplete for database objects
- View table data
- Format SQL queries
- **Multi-statement support**: Write multiple queries in one file, separated by semicolons
- **Smart query parsing**: Execute individual queries by placing cursor on them

## Installation

1. Install [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) extension
2. Install this Netezza driver extension
3. Reload VS Code

## Requirements

- SQLTools extension installed
- Access to a Netezza database
- Netezza database server running and accessible

## Configuration

Create a new SQLTools connection with the following parameters:

- **Connection Method**: Server and Port
- **Server Address**: Your Netezza server hostname or IP address
- **Port**: Default is 5480
- **Database**: The database name to connect to
- **Username**: Your Netezza username
- **Password**: Your Netezza password
- **Secure Connection**: Enable SSL/TLS connection (optional)

### Example Connection Settings

```json
{
  "name": "Netezza Production",
  "driver": "Netezza",
  "server": "netezza.example.com",
  "port": 5480,
  "database": "mydb",
  "username": "admin",
  "password": "password",
  "netezzaOptions": {
    "secureConnection": false,
    "queryTimeout": 30
  }
}
```

## Usage

1. Open the SQLTools sidebar in VS Code
2. Click "Add New Connection"
3. Select "Netezza" as the driver
4. Fill in your connection details
5. Click "Test Connection" to verify
6. Click "Save Connection"
7. Connect to your database and start querying!

## Supported Features

- ✅ Connection management
- ✅ Query execution
- ✅ Multi-statement files (semicolon-separated queries)
- ✅ Smart query identification ("Run on active query")
- ✅ Database browsing
- ✅ Schema browsing
- ✅ Table browsing
- ✅ View browsing
- ✅ Column browsing
- ✅ Table data preview
- ✅ Query history
- ✅ Generate CREATE script (right-click on table/view)
- ✅ SSL/TLS connections
- ✅ Configurable query timeout

## Known Issues

Please report issues on the [GitHub repository](https://github.com/maurice-v/sqltools-driver-netezza/issues).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT

## Credits

This driver uses [node-netezza](https://github.com/maurice-v/node-netezza) to connect to Netezza databases.

Built for use with [SQLTools](https://vscode-sqltools.mteixeira.dev/).
