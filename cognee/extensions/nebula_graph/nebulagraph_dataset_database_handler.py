"""
NebulaGraph Dataset Database Handler.

This handler manages per-dataset graph instances for NebulaGraph,
enabling multi-user access control and tenant isolation through
logical separation using graph_id.
"""

from typing import Optional, Any, Dict
from uuid import UUID

from cognee.modules.users.models import User
from cognee.modules.users.models import DatasetDatabase
from cognee.infrastructure.databases.dataset_database_handler import DatasetDatabaseHandlerInterface
from cognee.shared.logging_utils import get_logger

logger = get_logger(__name__)


class NebulaGraphDatasetDatabaseHandler(DatasetDatabaseHandlerInterface):
    """
    Handler for NebulaGraph dataset database management.

    NebulaGraph uses logical separation via graph_id to provide
    dataset and tenant isolation within a shared space.
    """

    @classmethod
    async def create_dataset(cls, dataset_id: Optional[UUID], user: Optional[User]) -> Dict[str, Any]:
        """
        Create a new NebulaGraph logical graph instance for the dataset.
        Returns connection info that will be mapped to the dataset.

        Args:
            dataset_id: Dataset UUID
            user: User object who owns the dataset and is making the request

        Returns:
            dict: Connection details for the created NebulaGraph instance
        """
        from cognee.infrastructure.databases.graph.config import get_graph_config

        graph_config = get_graph_config(

        )

        if graph_config.graph_database_provider != "nebulagraph":
            raise ValueError(
                "NebulaGraphDatasetDatabaseHandler can only be used with "
                "NebulaGraph graph database provider."
            )

        # Use dataset_id as the logical graph identifier for isolation
        graph_id = str(dataset_id) if dataset_id else "default"

        # Get tenant_id and user_id for access control context
        tenant_id: Optional[str] = None
        user_id: Optional[str] = None

        if user is not None:
            user_tenant_id = getattr(user, "tenant_id", None)
            user_id_val = getattr(user, "id", None)

            if user_tenant_id is not None:
                tenant_id = str(user_tenant_id)
            elif user_id_val is not None:
                tenant_id = str(user_id_val)

            if user_id_val is not None:
                user_id = str(user_id_val)

        logger.info(
            f"Creating NebulaGraph dataset entry: "
            f"dataset={dataset_id}, graph_id={graph_id}, tenant={tenant_id}, user={user_id}"
        )

        return {
            "graph_database_name": graph_config.graph_database_name or "cognee",
            "graph_database_url": graph_config.graph_database_url,
            "graph_database_provider": "nebulagraph",
            "graph_database_key": graph_config.graph_database_key,
            "graph_dataset_database_handler": "nebulagraph",
            "graph_database_connection_info": {
                "graph_database_username": "root",
                "graph_database_password": "nebula",
                "graph_id": graph_id,
                "dataset_id": str(dataset_id),
                "tenant_id": tenant_id,
                "user_id": user_id,
                "rls_enabled": True,
            },
        }

    @classmethod
    async def resolve_dataset_connection_info(
        cls, dataset_database: DatasetDatabase
    ) -> DatasetDatabase:
        """
        Resolve runtime connection details for NebulaGraph dataset.

        Args:
            dataset_database: DatasetDatabase row from the relational database

        Returns:
            DatasetDatabase: Updated instance with resolved connection info
        """
        if dataset_database.graph_database_connection_info is None:
            dataset_database.graph_database_connection_info = {}

        # Ensure graph_id is present for logical separation
        connection_info: Dict[str, Any] = dataset_database.graph_database_connection_info

        if "graph_id" not in connection_info:
            # Fallback to dataset_id if graph_id not set
            connection_info["graph_id"] = "default"

        return dataset_database

    @classmethod
    async def delete_dataset(cls, dataset_database: DatasetDatabase) -> None:
        """
        Delete the NebulaGraph logical graph for the given dataset.

        This deletes all nodes and edges belonging to the graph_id,
        not the entire space.

        Args:
            dataset_database: DatasetDatabase row containing connection info
        """
        from cognee.infrastructure.databases.graph.config import get_graph_config
        from .nebulagraph_adapter import NebulaGraphAdapter

        connection_info = dataset_database.graph_database_connection_info or {}

        # Fall back to current config if stored connection info is empty
        graph_config = get_graph_config()

        # Get URL - prefer stored, fall back to config
        graph_url = dataset_database.graph_database_url
        if not graph_url:
            graph_url = graph_config.graph_database_url
        if not graph_url:
            logger.warning(
                f"No graph database URL available for dataset deletion, skipping. "
                f"graph_id={connection_info.get('graph_id')}"
            )
            return

        # Get credentials - prefer stored, fall back to config, then defaults
        username = connection_info.get("graph_database_username")
        if not username:
            username = graph_config.graph_database_username or "root"

        password = connection_info.get("graph_database_password")
        if not password:
            password = graph_config.graph_database_password or "nebula"

        database_name = dataset_database.graph_database_name
        if not database_name:
            database_name = graph_config.graph_database_name or "cognee"

        graph_id = connection_info.get("graph_id", "default")

        logger.info(
            f"Deleting NebulaGraph dataset: graph_id={graph_id}, "
            f"url={graph_url}, database={database_name}, username={username}"
        )

        try:
            # Instantiate adapter directly to pass graph_id for proper logical separation
            graph_engine = NebulaGraphAdapter(
                graph_database_url=graph_url,
                graph_database_username=username,
                graph_database_password=password,
                database_name=database_name,
                graph_id=graph_id,
            )

            await graph_engine.initialize()
            await graph_engine.delete_graph()
            graph_engine.close()

            logger.info(f"Deleted NebulaGraph dataset: graph_id={graph_id}")
        except Exception as e:
            logger.error(f"Failed to delete NebulaGraph dataset graph_id={graph_id}: {e}")
            raise
