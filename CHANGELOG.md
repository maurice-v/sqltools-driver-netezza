# Changelog

All notable changes to the SQLTools Netezza Driver will be documented in this file.

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
