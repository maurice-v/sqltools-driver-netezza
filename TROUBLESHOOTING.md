# Troubleshooting Query Execution

## Issue: Executing a Specific Query from a Multi-Query File

### Expected Behavior (After Fix)
When your SQL file contains multiple queries:
- **Without text selection**: All queries execute sequentially, and you'll see all results
- **With text selection**: Only the selected query text executes

### How to Execute a Specific Query

#### Method 1: Select the Query Text ⭐ **Recommended**
1. **Highlight/Select** the specific query you want to execute (including the semicolon)
   ```sql
   SELECT current_user;  ← Select this entire line
   ```
2. Use the execute command:
   - **Keyboard**: `Ctrl+E Ctrl+E` (Windows/Linux) or `Cmd+E Cmd+E` (Mac)
   - **Right-click**: Select "Run Selected Query"
   - **Command Palette**: `SQLTools: Run Selected Query`

#### Method 2: Execute All Queries
1. Don't select any text (or select all text)
2. Execute - all queries will run sequentially
3. Results for all queries will be displayed

#### Method 3: Use Separate Files
For frequently-used queries, create separate `.sql` files:
- `get_date.sql` → `SELECT current_date;`
- `get_timestamp.sql` → `SELECT current_timestamp;`
- `get_user.sql` → `SELECT current_user;`

### Why Cursor Position Alone Doesn't Work

**SQLTools Architecture Limitation**:
- When you execute without a text selection, SQLTools sends the **entire file content** to the driver
- The driver receives only: `query` (full file text) and `opt` (request metadata)
- **No cursor position** information is included in `opt`
- Therefore, the driver cannot determine which specific query you intended to execute

**Current Driver Behavior**:
- Parses the file into individual queries (split by semicolons)
- If multiple queries found → executes **all of them sequentially**
- Returns results for each query in order

### Previous Behavior (Before Fix)
- Multiple queries detected → only the **first query** executed
- This caused confusion when cursor was on Query 2 or 3

### Comparison with Other SQL Tools

| Tool | Cursor-Based Execution |
|------|----------------------|
| **SQL Server Management Studio** | ✅ Yes (analyzes cursor position) |
| **DataGrip** | ✅ Yes (analyzes cursor position) |
| **Azure Data Studio** | ✅ Yes (analyzes cursor position) |
| **SQLTools (VS Code)** | ❌ No (requires text selection) |

### Future Enhancement
A potential enhancement would require:
1. Intercepting the execute command at the VS Code extension level
2. Getting the active editor and cursor position
3. Parsing queries using the driver's `parse()` method
4. Determining which query contains the cursor
5. Sending only that query to SQLTools

This would require significant architectural changes and is not currently implemented.
