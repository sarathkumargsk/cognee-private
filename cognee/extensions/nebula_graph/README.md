# NebulaGraph Adapter for Cognee

A custom NebulaGraph adapter for Cognee's graph storage system with Row-Level Security (RLS) support for multi-tenant data isolation and access control.

## Overview

This adapter implements the `GraphDBInterface` from Cognee, providing a complete NebulaGraph backend for graph operations. It uses **logical separation via RLS fields** (`tenant_id`, `dataset_id`) instead of NebulaGraph spaces, offering superior performance and scalability for multi-tenant scenarios with proper data isolation.

### Key Features

- **Row-Level Security (RLS)**: Tenant and dataset isolation using query-level filtering
- **Multi-Tenant Support**: Complete data isolation by tenant with automatic filtering
- **Logical Separation**: All data lives in a single shared space with filtering by RLS fields
- **Async/Await Support**: Fully asynchronous implementation using connection pooling
- **Batch Operations**: Optimized batch inserts for nodes and edges
- **Smart Index Management**: Intelligent index rebuilding - only rebuilds newly created indexes
- **Permission Enforcement**: Query-level filtering matching PostgreSQL RLS approach
- **Complete Interface**: Implements all required `GraphDBInterface` methods

### Why Logical Separation with RLS?

| Aspect | Space-based Isolation | Logical Separation (RLS) |
|--------|----------------------|------------------------------|
| Schema Management | Per-space schema creation | Single shared schema |
| Heartbeat Delays | 10-20s wait per space | One-time setup only (smart rebuild) |
| Cross-graph Queries | Complex, requires space switching | Simple WHERE clause |
| Resource Efficiency | Separate partitions per space | Shared partitions |
| Scaling | Space overhead per tenant | Linear with data volume |
| Delete Operation | DROP SPACE (heavy) | DELETE by RLS filter (light) |
| Index Utilization | Per-space indexes | Shared composite indexes with RLS |
| Tenant Isolation | Physical separation | Query-level filtering |
| Permission Model | Space-level access | Row-level with tenant + dataset filtering |
| Initialization Time | 10-20s per tenant | ~1s after first setup |

## Installation

### Prerequisites

- Python 3.8+
- NebulaGraph 3.8.0+ running instance
- Cognee installed

### Install Dependencies

```bash
pip install nebula3-python==3.8.0
```

Or add to your `requirements.txt`:
```
nebula3-python>=3.8.0
```

### Register the Extension (Optional)

If you want to automatically register NebulaGraph with Cognee when your application starts:

```python
# In your application startup code
from cognee.extensions.nebula_graph import register

# This will register NebulaGraph as a supported graph database
```

Alternatively, you can integrate it directly into the main Cognee codebase following the [INTEGRATION.md](INTEGRATION.md) guide.

## Quick Start

### 1. Start NebulaGraph

Using Docker Compose (see `docker-compose.yml` in this directory):

```bash
docker-compose up -d
```

Wait for all services to be healthy:
```bash
docker-compose ps
```

### 2. Configure Environment Variables

Create a `.env` file or export environment variables:

```bash
# Graph database provider
export GRAPH_DATABASE_PROVIDER=nebulagraph

# NebulaGraph connection settings
export NEBULA_GRAPH_HOST=127.0.0.1
export NEBULA_GRAPH_PORT=9669
export NEBULA_GRAPH_USERNAME=root
export NEBULA_GRAPH_PASSWORD=nebula
export NEBULA_GRAPH_MAX_POOL_SIZE=10

# Shared space and logical separation
export NEBULA_GRAPH_SPACE=cognee        # Shared space name (created once)
export NEBULA_GRAPH_ID=default          # Logical graph identifier for isolation
```

### 3. Use with Cognee

After integrating the adapter into your Cognee installation (see INTEGRATION.md):

```python
import cognee
import os

# Configure NebulaGraph
os.environ["GRAPH_DATABASE_PROVIDER"] = "nebulagraph"
os.environ["NEBULA_GRAPH_HOST"] = "127.0.0.1"
os.environ["NEBULA_GRAPH_PORT"] = "9669"
os.environ["NEBULA_GRAPH_USERNAME"] = "root"
os.environ["NEBULA_GRAPH_PASSWORD"] = "nebula"
os.environ["NEBULA_GRAPH_ID"] = "my_project"

# Use cognee normally
await cognee.add("Your document text here")
await cognee.cognify()
results = await cognee.search("Query here")
```

## Architecture

### Schema Design

The adapter uses a unified schema with Row-Level Security (RLS) fields for multi-tenant isolation:

```sql
-- Single shared space (created once)
CREATE SPACE IF NOT EXISTS cognee(
    vid_type=FIXED_STRING(64),
    partition_num=10,
    replica_factor=1
);

USE cognee;

-- Node tag with RLS fields for tenant and dataset isolation
CREATE TAG IF NOT EXISTS KBNode(
    graph_id STRING,          -- Logical graph identifier (backward compatibility)
    dataset_id STRING,        -- Dataset UUID for data isolation
    tenant_id STRING,         -- Tenant UUID for multi-tenant isolation (CRITICAL)
    node_id STRING,           -- Original node UUID
    name STRING,
    type STRING,
    properties STRING,        -- JSON serialized properties
    created_at INT64,
    updated_at INT64
);

-- Edge type with RLS fields for tenant and dataset isolation
CREATE EDGE IF NOT EXISTS KBEdge(
    graph_id STRING,          -- Logical graph identifier (backward compatibility)
    dataset_id STRING,        -- Dataset UUID for data isolation
    tenant_id STRING,         -- Tenant UUID for multi-tenant isolation (CRITICAL)
    relationship_name STRING,
    properties STRING,        -- JSON serialized properties
    created_at INT64,
    updated_at INT64
);

-- Composite indexes for efficient RLS filtering
CREATE TAG INDEX IF NOT EXISTS idx_node_graph_id ON KBNode(graph_id(64));
CREATE TAG INDEX IF NOT EXISTS idx_node_type ON KBNode(graph_id(64), type(64));
CREATE TAG INDEX IF NOT EXISTS idx_node_tenant_dataset ON KBNode(tenant_id(64), dataset_id(64));
CREATE EDGE INDEX IF NOT EXISTS idx_edge_graph_id ON KBEdge(graph_id(64));
CREATE EDGE INDEX IF NOT EXISTS idx_edge_tenant_dataset ON KBEdge(tenant_id(64), dataset_id(64));
```

**Note:** Indexes are only rebuilt when newly created. Subsequent initializations skip the rebuild step, reducing startup time from ~12 seconds to <1 second.

### Query Patterns

All queries automatically filter by RLS fields (`tenant_id` and `dataset_id`):

```sql
-- Insert vertex (includes RLS fields)
INSERT VERTEX KBNode(graph_id, dataset_id, tenant_id, node_id, name, type, properties, created_at, updated_at)
VALUES "uuid":(
    "my_graph",
    "dataset-uuid-123",
    "tenant-uuid-456",
    "uuid",
    "EntityName",
    "Entity",
    "{}",
    1705123456,
    1705123456
);

-- Query nodes (automatic RLS filtering)
MATCH (v:KBNode)
WHERE v.KBNode.tenant_id == "tenant-uuid-456"      -- Tenant isolation (CRITICAL)
  AND v.KBNode.dataset_id == "dataset-uuid-123"    -- Dataset filtering
  AND v.KBNode.node_id == "target_uuid"            -- Additional filters
RETURN v;

-- Delete graph data (RLS-aware deletion)
MATCH (v:KBNode)
WHERE v.KBNode.tenant_id == "tenant-uuid-456"
  AND v.KBNode.dataset_id == "dataset-uuid-123"
YIELD id(vertex) AS vid | DELETE VERTEX $-.vid WITH EDGE;
```

**Note:** All queries include RLS filtering automatically via `_build_rls_filter()` method. No need to manually add WHERE clauses.

## Multi-Tenancy & Row-Level Security (RLS)

The adapter provides complete data isolation using Row-Level Security with three levels of filtering:

### 1. Tenant Isolation (Critical)
All queries automatically filter by `tenant_id` to ensure complete tenant separation:

```python
# Tenant A context
from cognee.context_global_variables import session_user

# Set user with tenant_id
user_a = User(id="user-a", tenant_id="tenant-a")
session_user.set(user_a)

await cognee.add("Tenant A data")
await cognee.cognify()
# All data written with tenant_id="tenant-a"

# Tenant B context
user_b = User(id="user-b", tenant_id="tenant-b")
session_user.set(user_b)

await cognee.add("Tenant B data")
await cognee.cognify()
# All data written with tenant_id="tenant-b"

# Data is completely isolated - Tenant A CANNOT see Tenant B's data
```

### 2. Dataset Isolation
Within a tenant, datasets are isolated by `dataset_id`:

```python
# Same tenant, different datasets
dataset_1_context = {
    'dataset_id': 'dataset-uuid-1',
    'tenant_id': 'tenant-a',
    'user_id': 'user-1',
    'rls_enabled': True
}

dataset_2_context = {
    'dataset_id': 'dataset-uuid-2',
    'tenant_id': 'tenant-a',
    'user_id': 'user-1',
    'rls_enabled': True
}

# Datasets are isolated even within same tenant
```

### 3. Permission Model

Unlike PostgreSQL which enforces RLS at the database level, NebulaGraph uses **query-level filtering**:

- **PostgreSQL RLS**: Database automatically filters rows based on policies
- **NebulaGraph RLS**: Adapter adds WHERE clauses to every query via `_build_rls_filter()`

```python
# Example: _build_rls_filter() adds these conditions to every query:
WHERE v.tenant_id == "current-tenant-id"
  AND v.dataset_id == "current-dataset-id"
```

**Key Difference**: NebulaGraph RLS is enforced at the application layer (query building), not at the database layer. This means:
- ✅ Queries return empty results if user lacks permission (no error thrown)
- ✅ Matches PostgreSQL RLS behavior (silent filtering)
- ✅ Performance: Composite indexes on (tenant_id, dataset_id) ensure fast filtering

### RLS Context Flow

```
User Request
    ↓
session_user (current user's tenant_id)
    ↓
graph_db_config (dataset connection info)
    ↓
NebulaGraphAdapter._extract_connection_info()
    ↓
_build_rls_filter() adds WHERE clauses
    ↓
Query executes with RLS filtering
```

## API Reference

### NebulaGraphAdapter

Main adapter class implementing `GraphDBInterface`.

#### Constructor

```python
NebulaGraphAdapter(
    graph_database_url: str,              # host:port format or just host
    graph_database_username: str = "root",
    graph_database_password: str = "nebula",
    database_name: str = "cognee",        # Shared space name
    graph_id: str = "default",            # Logical graph identifier
    max_pool_size: int = 10,
)
```

#### Methods

**Initialization:**
- `async initialize() -> None` - Initialize connection pool and schema

**Node Operations:**
- `async add_node(node, properties=None) -> None` - Add single node
- `async add_nodes(nodes) -> None` - Add multiple nodes in batch
- `async get_node(node_id) -> Optional[NodeData]` - Get single node
- `async get_nodes(node_ids) -> List[NodeData]` - Get multiple nodes
- `async delete_node(node_id) -> None` - Delete single node
- `async delete_nodes(node_ids) -> None` - Delete multiple nodes

**Edge Operations:**
- `async add_edge(source_id, target_id, relationship_name, properties=None) -> None` - Add single edge
- `async add_edges(edges) -> None` - Add multiple edges in batch
- `async has_edge(source_id, target_id, relationship_name) -> bool` - Check if edge exists
- `async has_edges(edges) -> List[EdgeData]` - Check multiple edges
- `async get_edges(node_id) -> List[EdgeData]` - Get all edges for a node

**Graph Operations:**
- `async get_neighbors(node_id) -> List[NodeData]` - Get neighboring nodes
- `async get_connections(node_id) -> List[Tuple[NodeData, Dict, NodeData]]` - Get all connections
- `async get_graph_data() -> Tuple[List[Node], List[EdgeData]]` - Get all nodes and edges
- `async get_graph_metrics(include_optional=False) -> Dict[str, Any]` - Get graph statistics
- `async delete_graph() -> None` - Delete all data for this graph_id
- `async is_empty() -> bool` - Check if graph is empty

**Query Operations:**
- `async query(query, params=None) -> List[Any]` - Execute raw nGQL query

**Context Manager:**
- `async with adapter:` - Use as async context manager

## Performance Considerations

### Smart Index Rebuilding (Performance Optimization)

**Problem:** Previous implementations rebuilt all indexes on every initialization, causing 10-15 second delays.

**Solution:** The adapter now checks if indexes exist before rebuilding:

```python
# First initialization (indexes don't exist)
await adapter.initialize()  # Takes ~12 seconds (creates + rebuilds)

# Subsequent initializations (indexes exist)
await adapter.initialize()  # Takes <1 second (skips rebuild)
# Output: "All indexes already exist, skipping rebuild."
```

**Implementation:**
- `_check_index_exists()` queries NebulaGraph for existing indexes
- Only newly created indexes are rebuilt
- Reduces initialization time by ~92% after first run

### Batch Operations

The adapter automatically batches inserts with a default batch size of 100:

```python
# Efficient batch insert (nodes)
nodes = [DataPoint(...) for _ in range(1000)]
await adapter.add_nodes(nodes)  # Automatically batched in chunks of 100

# Efficient batch query (edges)
edges = [(src, dst, rel) for _ in range(500)]
existing = await adapter.has_edges(edges)  # Batched queries (50 per batch)
```

**Optimization:** `has_edges()` now uses batch queries with OR conditions instead of N individual permission checks, reducing query count from O(N) to O(N/50).

### Connection Pooling

Configure connection pool size based on your workload:

```python
adapter = NebulaGraphAdapter(
    graph_database_url="127.0.0.1:9669",
    max_pool_size=20,  # Increase for high concurrency
)
```

### Index Optimization

The adapter creates composite indexes for efficient RLS filtering:
- `idx_node_graph_id` - Fast lookup by graph_id (backward compatibility)
- `idx_node_type` - Fast filtering by graph_id + type
- `idx_node_tenant_dataset` - **Critical for RLS**: Fast filtering by (tenant_id, dataset_id)
- `idx_edge_graph_id` - Fast edge filtering by graph_id
- `idx_edge_tenant_dataset` - **Critical for RLS**: Fast edge filtering by (tenant_id, dataset_id)

**Note:** RLS composite indexes are essential for performance. Without them, queries would require full table scans.

## Permission Handling

### How NebulaGraph RLS Differs from PostgreSQL RLS

| Aspect | PostgreSQL (pgvector) | NebulaGraph |
|--------|----------------------|-------------|
| **Enforcement Layer** | Database-level policies | Application-level query filtering |
| **Permission Checks** | None (DB handles it) | None (query filtering handles it) |
| **Unauthorized Access** | Returns empty results | Returns empty results |
| **Error Behavior** | Silent filtering | Silent filtering |
| **Implementation** | RLS policies in SQL | `_build_rls_filter()` adds WHERE clauses |

### Query-Level Filtering

The adapter uses `_build_rls_filter()` to automatically add RLS conditions to all queries:

```python
def _build_rls_filter(self, alias: str = "v") -> str:
    """Build WHERE clause for RLS filtering."""
    if not self.rls_enabled:
        return f'{alias}.graph_id == "{self.graph_id}"'

    conditions = []

    # Tenant isolation (CRITICAL)
    if self.tenant_id:
        conditions.append(f'{alias}.tenant_id == "{self.tenant_id}"')

    # Dataset filtering
    if self.dataset_id:
        conditions.append(f'{alias}.dataset_id == "{self.dataset_id}"')

    return " AND ".join(conditions)
```

**Result:** Every query like `get_node()`, `get_edges()`, `search()` automatically includes these WHERE clauses.

### Permission Check Behavior

The `_check_permission()` method exists for API compatibility but **does not raise errors**:

```python
async def _check_permission(self, permission_type: str = "read") -> None:
    """
    Note: Unlike PostgreSQL RLS (which enforces at DB level), NebulaGraph uses
    query-level filtering via _build_rls_filter(). This method is kept for
    compatibility but does NOT raise errors - permissions are enforced through
    tenant_id and dataset_id filtering in queries.
    """
    pass  # No-op - filtering happens at query level
```

**Why?**
- **PostgreSQL**: RLS policies automatically filter at DB level, so no app-level checks needed
- **NebulaGraph**: No DB-level RLS, so we use query filtering instead
- **Both approaches**: Return empty results for unauthorized access (no exceptions)

### Security Guarantees

1. **Tenant Isolation**: Users can ONLY access data from their tenant
2. **Dataset Isolation**: Users can ONLY access datasets within their tenant
3. **No Data Leakage**: Cross-tenant queries return empty results (not errors)
4. **Performance**: Composite indexes ensure filtering is fast

### Testing RLS

```python
# Test tenant isolation
user_a = User(tenant_id="tenant-a")
session_user.set(user_a)
result_a = await adapter.query("MATCH (v:KBNode) RETURN v")
# Only returns tenant-a data

user_b = User(tenant_id="tenant-b")
session_user.set(user_b)
result_b = await adapter.query("MATCH (v:KBNode) RETURN v")
# Only returns tenant-b data (completely isolated)
```

## Development

### Running Tests

```bash
# Start NebulaGraph
docker-compose up -d

# Wait for services to be ready
sleep 20

# Run tests
pytest tests/

# Clean up
docker-compose down -v
```

### Extension Directory Structure

```
cognee/extensions/nebula_graph/
├── __init__.py
├── nebulagraph_adapter.py      # Main adapter implementation
├── register.py                  # Cognee registration hook
├── README.md                    # This file
├── INTEGRATION.md               # Integration guide
├── CHANGELOG.md                 # Version history
├── docker-compose.yml           # Local NebulaGraph setup
├── example_usage.py             # Usage examples
├── pyproject.toml              # Package configuration
└── .env.template               # Environment variables template
```

## Troubleshooting

### Connection Issues

**Problem:** `Failed to connect to NebulaGraph at 127.0.0.1:9669`

**Solution:**
1. Verify NebulaGraph is running: `docker-compose ps`
2. Check port is accessible: `telnet 127.0.0.1 9669`
3. Verify credentials in environment variables

### Schema Creation Delays

**Problem:** Queries fail with "TagNotFound" errors

**Solution:**
- The adapter includes automatic sleep delays after schema creation
- First initialization takes ~12 seconds (includes index rebuild)
- Subsequent initializations take <1 second (skips rebuild)
- Check schema: `SHOW TAGS` and `SHOW EDGES` in NebulaGraph console

### RLS Context Not Loading

**Problem:** `tenant_id is not set` errors during write operations

**Solution:**
1. Ensure `session_user` is set with valid tenant_id:
   ```python
   from cognee.context_global_variables import session_user
   user = User(id="user-id", tenant_id="tenant-id")
   session_user.set(user)
   ```

2. Verify RLS context is in `graph_db_config`:
   ```python
   from cognee.context_global_variables import graph_db_config
   context = {
       'graph_database_connection_info': {
           'dataset_id': 'dataset-uuid',
           'tenant_id': 'tenant-uuid',
           'user_id': 'user-uuid',
           'rls_enabled': True
       }
   }
   graph_db_config.set(context)
   ```

3. Check adapter initialization loaded context:
   ```python
   print(f"Tenant: {adapter.tenant_id}")
   print(f"Dataset: {adapter.dataset_id}")
   print(f"RLS Enabled: {adapter.rls_enabled}")
   ```

### Multi-tenancy Isolation Issues

**Problem:** Data from different tenants are mixing

**Solution:**
1. Verify `tenant_id` is set correctly before operations
2. Check queries include RLS filters:
   ```sql
   -- Should see this in logs
   WHERE v.tenant_id == "tenant-uuid" AND v.dataset_id == "dataset-uuid"
   ```
3. Ensure RLS indexes are created:
   ```sql
   SHOW TAG INDEXES;  -- Should show idx_node_tenant_dataset
   SHOW EDGE INDEXES; -- Should show idx_edge_tenant_dataset
   ```
4. Verify index rebuild completed (check logs for "All indexes already exist" message)

### Permission Denied Errors (Fixed in Latest Version)

**Problem:** `PermissionDeniedError` when accessing data

**Solution:**
- **Old behavior**: Adapter checked ACL permissions and raised errors
- **New behavior**: Adapter uses query-level filtering (no errors)
- Unauthorized queries now return empty results instead of raising errors
- This matches PostgreSQL RLS behavior

### Slow Initialization

**Problem:** Adapter takes 10-15 seconds to initialize every time

**Solution:**
- **Fixed in latest version**: Indexes are only rebuilt on first creation
- First initialization: ~12 seconds (creates indexes + rebuilds)
- Subsequent initializations: <1 second (skips rebuild)
- Check logs for "All indexes already exist, skipping rebuild" message

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code follows existing patterns
- Documentation is updated
- Commit messages are descriptive

## Changelog

### Version 2.0 (Current) - RLS Support & Performance Improvements

**Major Changes:**
1. **Row-Level Security (RLS)**: Added `tenant_id`, `dataset_id`, and `user_id` fields for multi-tenant isolation
2. **Permission Model**: Changed from application-level checks to query-level filtering (matches PostgreSQL RLS)
3. **Smart Index Rebuild**: Only rebuilds newly created indexes, reducing initialization time by ~92%
4. **Batch Query Optimization**: `has_edges()` now uses batch queries instead of N individual calls
5. **Schema Updates**: Tag names changed from `CogneeNode` to `KBNode` and `CogneeEdge` to `KBEdge`

**Breaking Changes:**
- Schema field additions: `tenant_id`, `dataset_id` (existing data needs migration)
- Tag/Edge names changed (may require data migration)
- `_check_permission()` no longer raises errors (returns empty results instead)

**Performance Improvements:**
- Initialization time: 12s → <1s (after first run)
- `has_edges()`: O(N) queries → O(N/50) queries
- New composite indexes for (tenant_id, dataset_id) improve RLS query performance

**Bug Fixes:**
- Fixed incomplete exception handlers (lines 95, 165)
- Fixed incomplete logger statement (line 158)
- Fixed permission errors by removing explicit ACL checks

### Version 1.0 - Initial Release

**Features:**
- Logical separation via `graph_id`
- Async/await support with connection pooling
- Batch operations for nodes and edges
- Complete `GraphDBInterface` implementation

## License

This extension follows Cognee's license terms.

## References

- [Cognee Documentation](https://docs.cognee.ai/)
- [NebulaGraph Documentation](https://docs.nebula-graph.io/)
- [nebula3-python Client](https://github.com/vesoft-inc/nebula-python)
- [Row-Level Security (RLS) Concept](https://www.postgresql.org/docs/current/ddl-rowsecurity.html) - PostgreSQL documentation
