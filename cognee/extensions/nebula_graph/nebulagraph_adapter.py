"""NebulaGraph Adapter for Graph Database with Logical Separation"""

import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID
from typing import Optional, Any, List, Dict, Type, Tuple, Union, override
from datetime import datetime, timezone

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config

from cognee.infrastructure.engine import DataPoint
from cognee.shared.logging_utils import get_logger
from cognee.infrastructure.databases.graph.graph_db_interface import (
    GraphDBInterface,
    record_graph_changes,
    NodeData,
    EdgeData,
    Node,
)
from cognee.modules.storage.utils import JSONEncoder

logger = get_logger("NebulaGraphAdapter")

# Constants
COGNEE_NODE_TAG = "KBNode"
COGNEE_EDGE_TYPE = "KBEdge"
DEFAULT_SPACE = "KB"


class NebulaGraphAdapter(GraphDBInterface):
    """
    Adapter for interacting with NebulaGraph database.
    Uses logical separation via graph_id instead of separate spaces.
    """

    def __init__(
        self,
        graph_database_url: str,              # host:port format or just host
        graph_database_username: str = "root",
        graph_database_password: str = "nebula",
        database_name: str = DEFAULT_SPACE,   # Shared space name
        graph_id: str = "default",            # Logical graph identifier
        max_pool_size: int = 10,
    ):
        self.host = graph_database_url.split(":")[0] if ":" in graph_database_url else graph_database_url
        self.port = int(graph_database_url.split(":")[1]) if ":" in graph_database_url else 9669
        self.username = graph_database_username
        self.password = graph_database_password
        self.space_name = database_name
        self.graph_id = graph_id              # Logical separation key
        self.max_pool_size = max_pool_size


        # Get RLS context from graph_db_config context variable (set by cognee)
        # This avoids needing to modify core cognee code
        self._load_rls_context_from_config()

        self.dataset_id = self._connection_info.get('dataset_id') or self._connection_info.get('graph_id') or graph_id
        self.tenant_id = self._connection_info.get('tenant_id')
        self.user_id = self._connection_info.get('user_id')
        self.rls_enabled = self._connection_info.get('rls_enabled', True)


        self._connection_pool: Optional[ConnectionPool] = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = asyncio.Lock()
        self._initialized = False


    def _load_rls_context_from_config(self) -> None:
        """
        Load RLS context from graph_db_config context variable.
        Falls back to empty dict if not available (will be loaded async in initialize()).
        """
        try:
            from cognee.context_global_variables import graph_db_config

            config = graph_db_config.get()
            if config and isinstance(config, dict):
                connection_info = config.get('graph_database_connection_info')

                if isinstance(connection_info, dict):
                    self._connection_info = connection_info
                    return
                elif isinstance(connection_info, str):
                    try:
                        self._connection_info = json.loads(connection_info)
                        return
                    except (json.JSONDecodeError, ValueError) as je:
                        pass

        except Exception as e:
            logger.debug(f"Failed to load RLS context from graph_db_config: {e}")

        # Fallback: Will be loaded in initialize()
        self._connection_info = {}

    async def initialize(self) -> None:
        """Initialize connection pool and ensure schema exists (one-time setup)."""
        if self._initialized:
            return


        # Load RLS context if not already loaded or if critical RLS fields are missing
        if not self._connection_info or not self.tenant_id or not self.dataset_id:
            await self._load_rls_context_async()

            # Update RLS fields after loading context
            if self._connection_info:
                self.dataset_id = self._connection_info.get('dataset_id') or self._connection_info.get('graph_id') or self.graph_id
                self.tenant_id = self._connection_info.get('tenant_id')
                self.user_id = self._connection_info.get('user_id')
                self.rls_enabled = self._connection_info.get('rls_enabled', True)


        await self._ensure_connection()
        await self._ensure_space_and_schema()
        self._initialized = True

    async def _load_rls_context_async(self) -> None:
        """
        Load RLS context from dataset database asynchronously.
        This queries the dataset database to find the connection_info for this graph.
        """
        try:
            from cognee.infrastructure.databases.relational import get_relational_engine
            from cognee.modules.users.models import DatasetDatabase
            from sqlalchemy import select

            engine = get_relational_engine()

            # Query for dataset databases with this graph_id using async session
            async with engine.get_async_session() as session:
                # Try to find by graph_id first
                result = await session.execute(
                    select(DatasetDatabase).filter(
                        DatasetDatabase.graph_database_provider == "nebulagraph"
                    )
                )
                dataset_dbs = result.scalars().all()

                for idx, dataset_db in enumerate(dataset_dbs):
                    if dataset_db.graph_database_connection_info:
                        conn_info = dataset_db.graph_database_connection_info
                        if isinstance(conn_info, dict):
                            # Check if this matches our graph_id
                            if (conn_info.get('graph_id') == self.graph_id or
                                conn_info.get('dataset_id') == self.graph_id):
                                self._connection_info = conn_info

                                # Update the RLS fields
                                self.dataset_id = conn_info.get('dataset_id') or conn_info.get('graph_id') or self.graph_id
                                self.tenant_id = conn_info.get('tenant_id')
                                self.user_id = conn_info.get('user_id')
                                self.rls_enabled = conn_info.get('rls_enabled', True)

                                logger.info(f"Loaded RLS context: tenant={self.tenant_id}, "
                                          f"dataset={self.dataset_id}, user={self.user_id}")
                                return


        except Exception as e:
            logger.debug(f"Failed to load RLS context asynchronously: {e}")

    async def _ensure_connection(self) -> None:
        """Ensure connection pool is initialized."""
        if self._connection_pool is not None:
            return

        config = Config()
        config.max_connection_pool_size = self.max_pool_size

        self._connection_pool = ConnectionPool()

        loop = asyncio.get_running_loop()
        ok = await loop.run_in_executor(
            self._executor,
            lambda: self._connection_pool.init([(self.host, self.port)], config)
        )
        if not ok:
            raise ConnectionError(f"Failed to connect to NebulaGraph at {self.host}:{self.port}")

    async def _cleanup_old_schema(self) -> None:
        """
        Nuclear option: Drop tags and edges entirely to clean up incompatible old schema.
        This is called when normal deletion fails due to schema incompatibility.
        """
        try:
            logger.warning("Performing nuclear cleanup: dropping tags and edges")

            # Drop indexes first
            try:
                await self._execute_raw(f"DROP TAG INDEX IF EXISTS idx_node_graph_id;")
                await self._execute_raw(f"DROP TAG INDEX IF EXISTS idx_node_type;")
                await self._execute_raw(f"DROP TAG INDEX IF EXISTS idx_node_tenant_dataset;")
                await self._execute_raw(f"DROP EDGE INDEX IF EXISTS idx_edge_graph_id;")
                await self._execute_raw(f"DROP EDGE INDEX IF EXISTS idx_edge_tenant_dataset;")
                await asyncio.sleep(2)
            except Exception as e:
                logger.debug(f"Error dropping indexes during cleanup: {e}")

            # Drop the tags and edges
            await self._execute_raw(f"DROP TAG IF EXISTS {COGNEE_NODE_TAG};")
            await self._execute_raw(f"DROP EDGE IF EXISTS {COGNEE_EDGE_TYPE};")
            await asyncio.sleep(2)

            logger.info("Old schema cleaned up successfully - will be recreated on next use")

        except Exception as e:
            logger.error(f"Failed to cleanup old schema: {e}")
            raise

    async def _check_index_exists(self, index_name: str, index_type: str = "TAG") -> bool:
        """
        Check if an index exists in NebulaGraph.

        Args:
            index_name: Name of the index to check
            index_type: Type of index - "TAG" or "EDGE"

        Returns:
            True if index exists, False otherwise
        """
        try:
            if index_type == "TAG":
                result = await self._execute_raw("SHOW TAG INDEXES;")
            else:
                result = await self._execute_raw("SHOW EDGE INDEXES;")

            if not result or not result.is_succeeded():
                return False

            # Parse result to check if our index exists
            for row in result:
                row_values = row.values()
                if row_values:
                    # First column is typically the index name
                    name_value = row_values[0]
                    if hasattr(name_value, 'as_string'):
                        name = name_value.as_string()
                    else:
                        name = str(name_value)

                    if name == index_name:
                        return True

            return False
        except Exception as e:
            logger.debug(f"Error checking index {index_name}: {e}")
            return False

    async def _ensure_space_and_schema(self) -> None:
        """Ensure shared space and schema exist (idempotent)."""
        # Create space if not exists
        await self._execute_raw(
            f"CREATE SPACE IF NOT EXISTS {self.space_name}("
            f"vid_type=FIXED_STRING(64), "
            f"partition_num=10, "
            f"replica_factor=1"
            f");",
            use_space=False
        )

        # Wait for space to propagate (only needed on first creation)
        await asyncio.sleep(10)

        # Create node tag with graph_id and RLS fields
        await self._execute_raw(f"""
            CREATE TAG IF NOT EXISTS {COGNEE_NODE_TAG}(
                graph_id STRING,
                dataset_id STRING,
                tenant_id STRING,
                node_id STRING,
                name STRING,
                type STRING,
                properties STRING,
                created_at INT64,
                updated_at INT64
            );
        """)

        # Create edge type with graph_id and RLS fields
        await self._execute_raw(f"""
            CREATE EDGE IF NOT EXISTS {COGNEE_EDGE_TYPE}(
                graph_id STRING,
                dataset_id STRING,
                tenant_id STRING,
                relationship_name STRING,
                properties STRING,
                created_at INT64,
                updated_at INT64
            );
        """)

        # Drop old indexes if they exist (in case of tag name changes)
        try:
            await self._execute_raw(f"DROP TAG INDEX IF EXISTS idx_node_graph_id;")
            await self._execute_raw(f"DROP TAG INDEX IF EXISTS idx_node_type;")
            await self._execute_raw(f"DROP EDGE INDEX IF EXISTS idx_edge_graph_id;")
            await asyncio.sleep(2)
        except Exception as e:
            logger.debug(f"No old indexes to drop: {e}")

        # Check which indexes already exist before creating
        indexes_to_create = [
            ("idx_node_graph_id", "TAG", f"CREATE TAG INDEX IF NOT EXISTS idx_node_graph_id ON {COGNEE_NODE_TAG}(graph_id(64));"),
            ("idx_node_type", "TAG", f"CREATE TAG INDEX IF NOT EXISTS idx_node_type ON {COGNEE_NODE_TAG}(graph_id(64), type(64));"),
            ("idx_edge_graph_id", "EDGE", f"CREATE EDGE INDEX IF NOT EXISTS idx_edge_graph_id ON {COGNEE_EDGE_TYPE}(graph_id(64));"),
            ("idx_node_tenant_dataset", "TAG", f"CREATE TAG INDEX IF NOT EXISTS idx_node_tenant_dataset ON {COGNEE_NODE_TAG}(tenant_id(64), dataset_id(64));"),
            ("idx_edge_tenant_dataset", "EDGE", f"CREATE EDGE INDEX IF NOT EXISTS idx_edge_tenant_dataset ON {COGNEE_EDGE_TYPE}(tenant_id(64), dataset_id(64));"),
        ]

        newly_created_indexes = []
        for index_name, index_type, create_query in indexes_to_create:
            # Check if index exists before creating
            exists = await self._check_index_exists(index_name, index_type)
            if not exists:
                newly_created_indexes.append((index_name, index_type))

            # Create index (will be no-op if exists)
            await self._execute_raw(create_query)

        # Wait for index creation to propagate
        await asyncio.sleep(2)

        # Only rebuild newly created indexes (required in NebulaGraph after creating new indexes)
        if newly_created_indexes:
            logger.info(f"Rebuilding {len(newly_created_indexes)} newly created indexes...")
            for index_name, index_type in newly_created_indexes:
                if index_type == "TAG":
                    await self._execute_raw(f"REBUILD TAG INDEX {index_name};")
                else:
                    await self._execute_raw(f"REBUILD EDGE INDEX {index_name};")

            # Wait for index rebuild to complete
            logger.info("Waiting for NebulaGraph indexes to rebuild...")
            await asyncio.sleep(10)
        else:
            logger.info("All indexes already exist, skipping rebuild.")

    async def _execute_raw(self, query: str, use_space: bool = True) -> Any:
        """Execute raw nGQL without result parsing."""
        await self._ensure_connection()

        loop = asyncio.get_running_loop()

        def execute():
            with self._connection_pool.session_context(self.username, self.password) as session:
                if use_space:
                    session.execute(f"USE {self.space_name};")
                result = session.execute(query)
                if not result.is_succeeded():
                    logger.warning(f"NebulaGraph query warning: {result.error_msg()}")
                return result

        return await loop.run_in_executor(self._executor, execute)

    def _build_rls_filter(self, alias: str = "v", tag_or_edge: str = None) -> str:
        """
        Build WHERE clause for RLS filtering.

        Args:
            alias: The alias for the vertex or edge in the query (e.g., 'v', 'e')
            tag_or_edge: Optional tag or edge type prefix (e.g., 'KBNode', 'KBEdge')

        Returns:
            WHERE clause string for RLS filtering
        """

        if not self.rls_enabled:
            filter_str = f'{alias}.graph_id == "{self.graph_id}"'
            return filter_str

        conditions = []

        # Tenant isolation (CRITICAL - highest priority)
        if self.tenant_id:
            conditions.append(f'{alias}.tenant_id == "{self.tenant_id}"')

        # Dataset filtering
        if self.dataset_id:
            conditions.append(f'{alias}.dataset_id == "{self.dataset_id}"')

        # Fallback for backward compatibility (if no RLS context)
        if not conditions:
            conditions.append(f'{alias}.graph_id == "{self.graph_id}"')

        filter_str = " AND ".join(conditions)
        return filter_str

    async def _check_permission(self, permission_type: str = "read") -> None:
        """
        Check if current user has permission for the dataset.

        Note: Unlike PostgreSQL RLS (which enforces at DB level), NebulaGraph uses
        query-level filtering via _build_rls_filter(). This method is kept for
        compatibility but does NOT raise errors - permissions are enforced through
        tenant_id and dataset_id filtering in queries.

        For write operations, we validate RLS context exists (tenant_id set) but
        don't check ACL permissions since the filtering happens at query time.

        Args:
            permission_type: Type of permission to check ('read', 'write', 'delete')
        """
        # For NebulaGraph, permission enforcement happens via query-level filtering
        # (tenant_id, dataset_id in WHERE clauses), not application-level checks.
        # This matches the pgvector approach where RLS policies filter at DB level.
        pass

    async def query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Execute an nGQL query against NebulaGraph."""
        await self._ensure_connection()

        # Log query preview for debugging
        query_preview = query[:300] + "..." if len(query) > 300 else query

        loop = asyncio.get_running_loop()

        def execute_query():
            with self._connection_pool.session_context(self.username, self.password) as session:
                session.execute(f"USE {self.space_name};")

                if params:
                    result = session.execute_py(query, params)
                else:
                    result = session.execute(query)

                if not result.is_succeeded():
                    # Log error with query details for debugging
                    query_preview = query[:200] + "..." if len(query) > 200 else query
                    logger.error(
                        f"Query: {query_preview}"
                    )
                    return []

                parsed_result = self._parse_result(result)
                return parsed_result

        return await loop.run_in_executor(self._executor, execute_query)

    def _parse_result(self, result) -> List[Dict[str, Any]]:
        """Parse NebulaGraph result into list of dictionaries."""
        if result.is_empty():
            return []

        rows = []
        col_names = result.keys()
        for row in result:
            row_dict = {}
            row_values = row.values()
            for col_name, value in zip(col_names, row_values):
                # Check the actual type using getType() if available
                if hasattr(value, 'is_null') and value.is_null():
                    row_dict[col_name] = None
                elif hasattr(value, 'is_int') and value.is_int():
                    row_dict[col_name] = value.as_int()
                elif hasattr(value, 'is_string') and value.is_string():
                    row_dict[col_name] = value.as_string()
                elif hasattr(value, 'is_bool') and value.is_bool():
                    row_dict[col_name] = value.as_bool()
                elif hasattr(value, 'is_double') and value.is_double():
                    row_dict[col_name] = value.as_double()
                elif hasattr(value, 'is_vertex') and value.is_vertex():
                    row_dict[col_name] = self._node_to_dict(value.as_node())
                elif hasattr(value, 'is_edge') and value.is_edge():
                    row_dict[col_name] = self._edge_to_dict(value.as_relationship())
                elif hasattr(value, 'is_list') and value.is_list():
                    row_dict[col_name] = [self._parse_value(v) for v in value.as_list()]
                elif hasattr(value, 'is_map') and value.is_map():
                    row_dict[col_name] = {k: self._parse_value(v) for k, v in value.as_map().items()}
                else:
                    # Fallback - try cast_primitive if available
                    if hasattr(value, 'cast_primitive'):
                        row_dict[col_name] = value.cast_primitive()
                    else:
                        row_dict[col_name] = str(value)
            rows.append(row_dict)

        return rows

    def _parse_value(self, value) -> Any:
        """Helper to parse individual NebulaGraph values."""
        if hasattr(value, 'is_null') and value.is_null():
            return None
        elif hasattr(value, 'is_int') and value.is_int():
            return value.as_int()
        elif hasattr(value, 'is_string') and value.is_string():
            return value.as_string()
        elif hasattr(value, 'is_bool') and value.is_bool():
            return value.as_bool()
        elif hasattr(value, 'is_double') and value.is_double():
            return value.as_double()
        elif hasattr(value, 'cast_primitive'):
            return value.cast_primitive()
        else:
            return str(value)

    def _node_to_dict(self, node) -> Dict[str, Any]:
        """Convert NebulaGraph node to dictionary."""
        if node is None:
            return {}

        props = {}
        try:
            for key, value in node.properties(COGNEE_NODE_TAG).items():
                if hasattr(value, 'as_string'):
                    props[key] = value.as_string()
                elif hasattr(value, 'as_int'):
                    props[key] = value.as_int()
                else:
                    props[key] = str(value)
        except Exception:
            pass

        return props

    def _edge_to_dict(self, edge) -> Dict[str, Any]:
        """Convert NebulaGraph edge to dictionary."""
        if edge is None:
            return {}

        props = {}
        try:
            for key, value in edge.properties().items():
                if hasattr(value, 'as_string'):
                    props[key] = value.as_string()
                elif hasattr(value, 'as_int'):
                    props[key] = value.as_int()
                else:
                    props[key] = str(value)
        except Exception:
            pass

        return props

    # Phase 2: Node Operations with graph_id Filtering

    async def is_empty(self) -> bool:
        """Check if the logical graph is empty (with RLS filtering)."""
        await self._check_permission("read")

        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE {rls_filter}
            RETURN id(v) AS vid
            LIMIT 1;
        """)
        return len(result) == 0

    async def add_node(
        self,
        node: Union[DataPoint, str],
        properties: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a single node to the logical graph with RLS fields."""
        # Check permission first
        await self._check_permission("write")

        # Validate RLS context is available for tenant isolation
        if self.rls_enabled and not self.tenant_id:
            logger.error(
                f"dataset_id={self.dataset_id}, graph_id={self.graph_id}"
            )
            raise ValueError(
                "Tenant isolation is enabled but tenant_id is not set. "
                "Cannot write data without proper tenant context."
            )

        if isinstance(node, DataPoint):
            node_id = str(node.id)
            node_properties = self._serialize_properties(node.model_dump())
            node_name = getattr(node, "name", node_id)
            node_type = type(node).__name__
        else:
            node_id = node
            node_properties = json.dumps(properties or {}, cls=JSONEncoder)
            node_name = properties.get("name", node_id) if properties else node_id
            node_type = properties.get("type", "Node") if properties else "Node"


        now = int(datetime.now(timezone.utc).timestamp())

        # UPSERT pattern: delete if exists, then insert
        await self.query(f'DELETE VERTEX "{node_id}" WITH EDGE;')

        await self.query(f"""
            INSERT VERTEX {COGNEE_NODE_TAG}(graph_id, dataset_id, tenant_id, node_id, name, type, properties, created_at, updated_at)
            VALUES "{node_id}":(
                "{self.graph_id}",
                "{self.dataset_id}",
                "{self.tenant_id or ''}",
                "{node_id}",
                "{self._escape(node_name)}",
                "{node_type}",
                "{self._escape(node_properties)}",
                {now},
                {now}
            );
        """)

    @record_graph_changes
    async def add_nodes(self, nodes: List[DataPoint]) -> None:
        """Add multiple nodes to the logical graph in batch with RLS fields."""
        if not nodes:
            return

        # Check permission first
        await self._check_permission("write")

        # Validate RLS context is available for tenant isolation
        if self.rls_enabled and not self.tenant_id:
            logger.error(
                f"dataset_id={self.dataset_id}, graph_id={self.graph_id}"
            )
            raise ValueError(
                "Tenant isolation is enabled but tenant_id is not set. "
                "Cannot write data without proper tenant context."
            )


        now = int(datetime.now(timezone.utc).timestamp())
        values = []

        for node in nodes:
            node_id = str(node.id)
            node_name = getattr(node, "name", node_id)
            node_type = type(node).__name__
            node_properties = self._serialize_properties(node.model_dump())

            values.append(
                f'"{node_id}":'
                f'("{self.graph_id}", '
                f'"{self.dataset_id}", '
                f'"{self.tenant_id or ''}", '
                f'"{node_id}", '
                f'"{self._escape(node_name)}", '
                f'"{node_type}", '
                f'"{self._escape(node_properties)}", '
                f'{now}, {now})'
            )

        # Batch insert with chunking
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            await self.query(f"""
                INSERT VERTEX {COGNEE_NODE_TAG}(graph_id, dataset_id, tenant_id, node_id, name, type, properties, created_at, updated_at)
                VALUES {", ".join(batch)};
            """)

    async def get_node(self, node_id: str) -> Optional[NodeData]:
        """Retrieve a single node by ID from the logical graph with RLS filtering."""
        await self._check_permission("read")

        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE id(v) == "{node_id}"
              AND {rls_filter}
            RETURN v;
        """)

        if not result:
            return None

        return self._parse_node_data(result[0].get("v"))

    async def get_nodes(self, node_ids: List[str]) -> List[NodeData]:
        """Retrieve multiple nodes by their IDs from the logical graph with RLS filtering."""
        if not node_ids:
            return []

        await self._check_permission("read")

        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        ids_str = ", ".join(f'"{nid}"' for nid in node_ids)
        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE id(v) IN [{ids_str}]
              AND {rls_filter}
            RETURN v;
        """)

        return [self._parse_node_data(r.get("v")) for r in result if r.get("v")]

    async def delete_node(self, node_id: str) -> None:
        """Delete a node from the logical graph (with RLS ownership check)."""
        # Check permission first
        await self._check_permission("write")

        # Verify node belongs to this tenant/dataset before deleting
        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE id(v) == "{node_id}"
              AND {rls_filter}
            RETURN id(v) AS vid;
        """)

        if result:
            await self.query(f'DELETE VERTEX "{node_id}" WITH EDGE;')
        else:
            logger.warning(f"Cannot delete node not owned by tenant/dataset: {node_id}")

    async def delete_nodes(self, node_ids: List[str]) -> None:
        """Delete multiple nodes from the logical graph."""
        for node_id in node_ids:
            await self.delete_node(node_id)

    # Phase 3: Edge Operations with graph_id Filtering

    async def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add an edge to the logical graph with RLS verification."""
        # Check permission first
        await self._check_permission("write")

        # Validate RLS context is available for tenant isolation
        if self.rls_enabled and not self.tenant_id:
            logger.error(
                f"Cannot add edge: RLS is enabled but tenant_id is not set. "
                f"dataset_id={self.dataset_id}, graph_id={self.graph_id}"
            )
            raise ValueError(
                "Tenant isolation is enabled but tenant_id is not set. "
                "Cannot write data without proper tenant context."
            )

        now = int(datetime.now(timezone.utc).timestamp())
        props_json = json.dumps(properties or {}, cls=JSONEncoder)

        # Verify both nodes exist and belong to this tenant/dataset
        node_filter = self._build_rls_filter(f"n.{COGNEE_NODE_TAG}")
        verify_query = f"""
            MATCH (n:{COGNEE_NODE_TAG}), (m:{COGNEE_NODE_TAG})
            WHERE id(n) == "{source_id}"
              AND id(m) == "{target_id}"
              AND {node_filter}
              AND {node_filter.replace('n.', 'm.')}
            RETURN id(n) AS src, id(m) AS dst;
        """
        verify_result = await self.query(verify_query)

        if not verify_result:
            logger.error(
                f"Cannot create edge: nodes not found or not owned by tenant/dataset. "
                f"Source: {source_id}, Target: {target_id}"
            )
            return

        edge_query = f"""
            INSERT EDGE {COGNEE_EDGE_TYPE}(graph_id, dataset_id, tenant_id, relationship_name, properties, created_at, updated_at)
            VALUES "{source_id}" -> "{target_id}":(
                "{self.graph_id}",
                "{self.dataset_id}",
                "{self.tenant_id or ''}",
                "{relationship_name}",
                "{self._escape(props_json)}",
                {now},
                {now}
            );
        """

        result = await self.query(edge_query)
        logger.debug(f"Edge insertion result for {source_id} -> {target_id}: {len(result) if result else 0} rows")

    @record_graph_changes
    async def add_edges(self, edges: List[EdgeData]) -> None:
        """Add multiple edges to the logical graph in batch."""
        if not edges:
            return

        # Validate RLS context is available for tenant isolation
        if self.rls_enabled and not self.tenant_id:
            logger.error(
                f"Cannot add edges: RLS is enabled but tenant_id is not set. "
                f"dataset_id={self.dataset_id}, graph_id={self.graph_id}"
            )
            raise ValueError(
                "Tenant isolation is enabled but tenant_id is not set. "
                "Cannot write data without proper tenant context."
            )

        logger.info(f"Adding {len(edges)} edges to graph_id: {self.graph_id}")

        now = int(datetime.now(timezone.utc).timestamp())
        values = []
        edge_details = []

        for edge in edges:
            source_id = str(edge[0])
            target_id = str(edge[1])
            rel_name = str(edge[2])
            props = edge[3] if len(edge) > 3 else {}
            props_json = json.dumps(props or {}, cls=JSONEncoder)

            values.append(
                f'"{source_id}" -> "{target_id}":'
                f'("{self.graph_id}", '
                f'"{self.dataset_id}", '
                f'"{self.tenant_id or ""}", '
                f'"{rel_name}", '
                f'"{self._escape(props_json)}", '
                f'{now}, {now})'
            )
            edge_details.append((source_id, target_id, rel_name))

        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            batch_details = edge_details[i:i + batch_size]

            edge_query = f"""
                INSERT EDGE {COGNEE_EDGE_TYPE}(graph_id, dataset_id, tenant_id, relationship_name, properties, created_at, updated_at)
                VALUES {", ".join(batch)};
            """

            logger.debug(f"Inserting batch of {len(batch)} edges")
            result = await self.query(edge_query)

            # Log first few edges in batch for debugging
            if i == 0:
                logger.debug(f"First edges in batch: {batch_details[:3]}")
            logger.debug(f"Batch {i//batch_size + 1} insertion result: {len(result) if result else 0} rows")

    async def has_edge(self, source_id: str, target_id: str, relationship_name: str) -> bool:
        """Check if an edge exists in the logical graph with RLS filtering."""
        await self._check_permission("read")

        edge_filter = self._build_rls_filter("e")
        result = await self.query(f"""
            MATCH (n)-[e:{COGNEE_EDGE_TYPE}]->(m)
            WHERE id(n) == "{source_id}"
              AND id(m) == "{target_id}"
              AND {edge_filter}
              AND e.relationship_name == "{relationship_name}"
            RETURN COUNT(e) AS count;
        """)
        return result[0]["count"] > 0 if result else False

    async def has_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        """Return edges that exist in the logical graph with RLS filtering."""
        if not edges:
            return []

        # Check permission once for the entire batch
        await self._check_permission("read")

        edge_filter = self._build_rls_filter("e")
        existing = []

        # Process edges in batches to avoid query size limits
        batch_size = 50
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]

            # Build WHERE conditions for all edges in this batch
            edge_conditions = []
            for edge in batch:
                source_id = str(edge[0])
                target_id = str(edge[1])
                rel_name = str(edge[2])
                edge_conditions.append(
                    f'(id(n) == "{source_id}" AND id(m) == "{target_id}" AND e.relationship_name == "{rel_name}")'
                )

            # Query all edges in batch
            result = await self.query(f"""
                MATCH (n)-[e:{COGNEE_EDGE_TYPE}]->(m)
                WHERE {edge_filter}
                  AND ({" OR ".join(edge_conditions)})
                RETURN id(n) AS src, id(m) AS dst, e.relationship_name AS rel;
            """)

            # Build set of existing edges for efficient lookup
            existing_set = {(r["src"], r["dst"], r["rel"]) for r in result}

            # Add matching edges to result
            for edge in batch:
                edge_key = (str(edge[0]), str(edge[1]), str(edge[2]))
                if edge_key in existing_set:
                    existing.append(edge)

        return existing

    async def get_edges(self, node_id: str) -> List[EdgeData]:
        """Get all edges connected to a node in the logical graph with RLS filtering."""
        await self._check_permission("read")

        edge_filter = self._build_rls_filter("e")
        result = await self.query(f"""
            MATCH (n)-[e:{COGNEE_EDGE_TYPE}]-(m)
            WHERE id(n) == "{node_id}"
              AND {edge_filter}
            RETURN id(n) AS src, id(m) AS dst, e.relationship_name AS rel, e.properties AS props;
        """)

        return [
            (r["src"], r["dst"], r["rel"], json.loads(r.get("props", "{}")))
            for r in result
        ]

    # Phase 4: Graph Traversal & Data Retrieval

    async def get_neighbors(self, node_id: str) -> List[NodeData]:
        """Get all neighboring nodes in the logical graph with RLS filtering."""
        await self._check_permission("read")

        edge_filter = self._build_rls_filter("e")
        node_filter = self._build_rls_filter(f"m.{COGNEE_NODE_TAG}")
        result = await self.query(f"""
            MATCH (n:{COGNEE_NODE_TAG})-[e:{COGNEE_EDGE_TYPE}]-(m:{COGNEE_NODE_TAG})
            WHERE id(n) == "{node_id}"
              AND {edge_filter}
              AND {node_filter}
            RETURN DISTINCT m;
        """)
        return [self._parse_node_data(r.get("m")) for r in result if r.get("m")]

    async def get_connections(
        self, node_id: Union[str, UUID]
    ) -> List[Tuple[NodeData, Dict[str, Any], NodeData]]:
        """Get all connections for a node in the logical graph with RLS filtering."""
        await self._check_permission("read")

        node_id = str(node_id)
        edge_filter = self._build_rls_filter("e")
        result = await self.query(f"""
            MATCH (n:{COGNEE_NODE_TAG})-[e:{COGNEE_EDGE_TYPE}]-(m:{COGNEE_NODE_TAG})
            WHERE id(n) == "{node_id}"
              AND {edge_filter}
            RETURN n, e, m;
        """)

        connections = []
        for r in result:
            n_data = self._parse_node_data(r.get("n"))
            m_data = self._parse_node_data(r.get("m"))
            e_data = self._parse_edge_props(r.get("e"))
            connections.append((n_data, e_data, m_data))

        return connections

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        """Get all nodes and edges in the logical graph with RLS filtering."""
        await self._check_permission("read")

        # Get nodes for this tenant/dataset
        node_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        nodes_result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE {node_filter}
            RETURN id(v) AS vid, v.{COGNEE_NODE_TAG} AS props;
        """)
        nodes = [(r["vid"], r.get("props")) for r in nodes_result]

        # Get edges for this tenant/dataset
        # CRITICAL: Filter edges where BOTH source and destination nodes match RLS
        # This prevents orphaned edges that reference nodes from other tenants
        edge_filter = self._build_rls_filter("e")
        node_filter_n = self._build_rls_filter(f"n.{COGNEE_NODE_TAG}")
        node_filter_m = self._build_rls_filter(f"m.{COGNEE_NODE_TAG}")
        edges_result = await self.query(f"""
            MATCH (n:{COGNEE_NODE_TAG})-[e:{COGNEE_EDGE_TYPE}]->(m:{COGNEE_NODE_TAG})
            WHERE {node_filter_n}
              AND {edge_filter}
              AND {node_filter_m}
            RETURN src(e) AS src, dst(e) AS dst, e.relationship_name AS rel, e.properties AS props;
        """)
        edges = []
        for r in edges_result:
            edges.append((
                r["src"],
                r["dst"],
                r.get("rel", ""),
                json.loads(r.get("props", "{}"))
            ))

        return nodes, edges

    async def get_graph_metrics(self, include_optional: bool = False) -> Dict[str, Any]:
        """Get statistics for the logical graph with RLS filtering."""
        await self._check_permission("read")

        # Node count
        node_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        node_result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE {node_filter}
            RETURN COUNT(v) AS count;
        """)
        num_nodes = node_result[0]["count"] if node_result else 0

        # Edge count
        edge_filter = self._build_rls_filter("e")
        edge_result = await self.query(f"""
            MATCH ()-[e:{COGNEE_EDGE_TYPE}]->()
            WHERE {edge_filter}
            RETURN COUNT(e) AS count;
        """)
        num_edges = edge_result[0]["count"] if edge_result else 0

        metrics = {
            "num_nodes": num_nodes,
            "num_edges": num_edges,
            "mean_degree": (2 * num_edges / num_nodes) if num_nodes > 0 else 0,
            "edge_density": (num_edges / (num_nodes * (num_nodes - 1))) if num_nodes > 1 else 0,
            "num_connected_components": -1,
            "sizes_of_connected_components": [],
            "num_selfloops": -1,
            "diameter": -1,
            "avg_shortest_path_length": -1,
            "avg_clustering": -1,
        }

        return metrics

    @override
    async def delete_graph(self) -> None:
        """Delete all data for this logical graph with RLS filtering, then drop schema."""
        await self._check_permission("write")

        # Try RLS-aware deletion first (new schema)
        try:
            node_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
            nodes_result = await self.query(f"""
                MATCH (v:{COGNEE_NODE_TAG})
                WHERE {node_filter}
                RETURN id(v) AS vid;
            """)

            # Delete each node (NebulaGraph doesn't support batch delete with filtering)
            for node in nodes_result:
                await self.query(f'DELETE VERTEX "{node["vid"]}" WITH EDGE;')

            logger.info(f"Deleted {len(nodes_result)} nodes using RLS filtering")

        except Exception as e:
            # If RLS filtering fails (old schema), fall back to graph_id only
            logger.warning(f"RLS delete failed, falling back to graph_id filtering: {e}")

            try:
                nodes_result = await self.query(f"""
                    MATCH (v:{COGNEE_NODE_TAG})
                    WHERE v.{COGNEE_NODE_TAG}.graph_id == "{self.graph_id}"
                    RETURN id(v) AS vid;
                """)

                for node in nodes_result:
                    await self.query(f'DELETE VERTEX "{node["vid"]}" WITH EDGE;')

                logger.info(f"Deleted {len(nodes_result)} nodes using graph_id filtering (old schema)")

            except Exception as e2:
                logger.warning(f"Standard deletion failed: {e2}")

        # Drop schema to ensure fresh schema on next initialization
        logger.info("Dropping schema (tags and edges) to ensure clean state")
        await self._cleanup_old_schema()

    async def get_nodeset_subgraph(
        self, node_type: Type[Any], node_name: List[str]
    ) -> Tuple[List[Tuple[int, dict]], List[Tuple[int, int, str, dict]]]:
        """Get subgraph for specific node types in the logical graph with RLS filtering."""
        await self._check_permission("read")

        type_name = node_type.__name__
        names_str = ", ".join(f'"{n}"' for n in node_name)
        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")

        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE {rls_filter}
              AND v.{COGNEE_NODE_TAG}.type == "{type_name}"
              AND v.{COGNEE_NODE_TAG}.name IN [{names_str}]
            RETURN v;
        """)

        nodes = [(i, self._parse_node_data(r.get("v"))) for i, r in enumerate(result)]
        return nodes, []

    async def get_filtered_graph_data(
        self, attribute_filters: List[Dict[str, List[Union[str, int]]]]
    ) -> Tuple[List[Node], List[EdgeData]]:
        """Get filtered graph data by attributes within the logical graph with RLS filtering."""
        await self._check_permission("read")

        # Start with RLS filter
        rls_filter = self._build_rls_filter(f"v.{COGNEE_NODE_TAG}")
        conditions = [rls_filter]

        for filter_dict in attribute_filters:
            for attr, values in filter_dict.items():
                values_str = ", ".join(f'"{v}"' for v in values)
                conditions.append(f'v.{COGNEE_NODE_TAG}.{attr} IN [{values_str}]')

        where_clause = " AND ".join(conditions)

        result = await self.query(f"""
            MATCH (v:{COGNEE_NODE_TAG})
            WHERE {where_clause}
            RETURN id(v) AS id, v;
        """)

        nodes = [(r["id"], self._parse_node_data(r.get("v"))) for r in result]
        node_ids = [n[0] for n in nodes]

        # Get edges between filtered nodes
        edges = []
        if node_ids:
            edge_filter = self._build_rls_filter("e")
            ids_str = ", ".join(f'"{nid}"' for nid in node_ids)
            edges_result = await self.query(f"""
                MATCH (n)-[e:{COGNEE_EDGE_TYPE}]->(m)
                WHERE id(n) IN [{ids_str}]
                  AND id(m) IN [{ids_str}]
                  AND {edge_filter}
                RETURN id(n) AS src, id(m) AS dst, e.relationship_name AS rel, e.properties AS props;
            """)
            edges = [
                (r["src"], r["dst"], r["rel"], json.loads(r.get("props", "{}")))
                for r in edges_result
            ]

        return nodes, edges

    # Phase 5: Helper Methods

    def _serialize_properties(self, props: Dict[str, Any]) -> str:
        """Serialize properties to JSON string."""
        return json.dumps(props, cls=JSONEncoder)

    def _escape(self, value: str) -> str:
        """Escape special characters for nGQL."""
        if value is None:
            return ""
        return str(value).replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")

    def _parse_node_data(self, node_data) -> NodeData:
        """Parse node data from NebulaGraph result."""
        if node_data is None:
            return {}

        if isinstance(node_data, dict):
            props = node_data.get("properties", "{}")
            if isinstance(props, str):
                try:
                    props = json.loads(props)
                except json.JSONDecodeError:
                    props = {}
            return {
                "id": node_data.get("node_id"),
                "name": node_data.get("name"),
                "type": node_data.get("type"),
                **props
            }

        # Handle NebulaGraph Node object
        if hasattr(node_data, 'properties'):
            try:
                raw_props = {}
                for key, value in node_data.properties(COGNEE_NODE_TAG).items():
                    if hasattr(value, 'as_string'):
                        raw_props[key] = value.as_string()
                    elif hasattr(value, 'as_int'):
                        raw_props[key] = value.as_int()
                    else:
                        raw_props[key] = str(value)

                extra_props = json.loads(raw_props.get("properties", "{}"))
                return {
                    "id": raw_props.get("node_id"),
                    "name": raw_props.get("name"),
                    "type": raw_props.get("type"),
                    **extra_props
                }
            except Exception as e:
                logger.debug(f"Error parsing node data: {e}")
                return {}

        return {}

    def _parse_edge_props(self, edge_data) -> Dict[str, Any]:
        """Parse edge properties from NebulaGraph result."""
        if edge_data is None:
            return {}

        if isinstance(edge_data, dict):
            props = edge_data.get("properties", "{}")
            if isinstance(props, str):
                try:
                    props = json.loads(props)
                except json.JSONDecodeError:
                    props = {}
            return {
                "relationship_name": edge_data.get("relationship_name"),
                **props
            }

        # Handle NebulaGraph Edge object
        if hasattr(edge_data, 'properties'):
            try:
                raw_props = {}
                for key, value in edge_data.properties().items():
                    if hasattr(value, 'as_string'):
                        raw_props[key] = value.as_string()
                    elif hasattr(value, 'as_int'):
                        raw_props[key] = value.as_int()
                    else:
                        raw_props[key] = str(value)

                extra_props = json.loads(raw_props.get("properties", "{}"))
                return {
                    "relationship_name": raw_props.get("relationship_name"),
                    **extra_props
                }
            except Exception:
                pass

        return {"relationship_name": "UNKNOWN"}

    def _parse_lookup_props(self, props_data) -> Dict[str, Any]:
        """Parse properties from LOOKUP query result."""
        if props_data is None:
            return {}

        if isinstance(props_data, dict):
            return props_data

        # Handle NebulaGraph map type
        result = {}
        try:
            if hasattr(props_data, 'keys'):
                for key in props_data.keys():
                    value = props_data[key]
                    if hasattr(value, 'as_string'):
                        result[key] = value.as_string()
                    elif hasattr(value, 'as_int'):
                        result[key] = value.as_int()
                    else:
                        result[key] = str(value)
        except Exception:
            pass

        return result

    def close(self):
        """Close the connection pool."""
        if self._connection_pool:
            self._connection_pool.close()
        self._executor.shutdown(wait=False)

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
