# Changelog

All notable changes to the SQLTools Netezza Driver will be documented in this file.

## [1.2.0] - 2025-11-18

### Added
- **Multi-statement query support**: Each semicolon-terminated statement is now recognized as a separate query
- **Smart query execution**: "Run on active query" now works for each individual statement in a file
- **Intelligent query parsing**: Correctly handles SQL comments (line and block) and string literals
- **CodeLens integration**: Visual "Execute Query" buttons appear above each query in SQL files
- **Enhanced keyboard shortcuts**: Added Ctrl+Alt+E (Windows) / Cmd+Alt+E (Mac) for executing query at cursor
- **Comprehensive documentation**: Added detailed guides for multi-statement usage and query parsing

### Changed
- Improved query execution: Cursor position now intelligently identifies which query to execute
- Enhanced query parsing with proper string literal and comment handling
- Better error reporting with execution timing and query context
- Sequential execution of multiple queries with individual result reporting

### Technical Improvements
- Implemented `identifyStatements` flag for SQLTools integration
- Added robust query splitting algorithm that respects SQL syntax
- Enhanced extension with CodeLens provider for better user experience
- Improved logging and debugging capabilities

## [1.1.1] - 2025-11-17

### Fixed
- Fixed query execution failure when database/schema is expanded in tree view
- Query context now properly resets to connection default database before execution

## [1.0.1] - 2025-11-14

### Fixed
- Fixed missing node_modules dependencies in packaged extension
- Driver now properly loads in SQLTools

## [1.0.0] - 2025-11-14

### Added
- Initial release of SQLTools Netezza driver
- Support for connecting to Netezza databases
- Database, schema, table, and column browsing
- Query execution and query history
- SSL/TLS connection support
- Configurable query timeout
- Generate CREATE script command for tables and views
- Autocomplete for database objects
- Table data preview with configurable row limits
