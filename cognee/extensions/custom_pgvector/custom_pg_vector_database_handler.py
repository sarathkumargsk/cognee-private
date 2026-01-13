"""
CustomPGVector Dataset Database Handler.

This handler manages per-dataset database instances for CustomPGVector,
enabling multi-user access control and tenant isolation.
"""

from typing import override, Any, Dict
from uuid import UUID

from cognee.modules.users.models import User
from cognee.modules.users.models import DatasetDatabase
from cognee.infrastructure.databases.vector import get_vectordb_config
from cognee.infrastructure.databases.relational import get_relational_engine
from cognee.infrastructure.databases.dataset_database_handler import DatasetDatabaseHandlerInterface
from cognee.shared.logging_utils import LoggerInterface, get_logger

logger: LoggerInterface = get_logger(__name__)


class CustomPGVectorDatasetDatabaseHandler(DatasetDatabaseHandlerInterface):
    """
    Handler for CustomPGVector dataset database management.

    CustomPGVector uses a unified table (custom_embeddings) with RLS policies
    to provide dataset and tenant isolation.
    """

    @override
    @classmethod
    async def create_dataset(cls, dataset_id: UUID | None, user: User | None) -> dict[str, Any]:
        vector_config = get_vectordb_config()

        if vector_config.vector_db_provider != "custompgvector":
            raise ValueError(
                "CustomPGVectorDatasetDatabaseHandler can only be used with " +
                "custompgvector vector database provider."
            )

        relational_engine = get_relational_engine()
        db_url: str = str(relational_engine.engine.url)
        db_name: str = db_url.split("/")[-1].split("?")[0] if "/" in db_url else "cognee"

        tenant_id: str | None = None

        if user is not None:
            # Using getattr avoids the "Column vsp None" overlap error in BasedPyright
            # as it treats the returned value as Any/Unknown at access time.
            user_tenant_id = getattr(user, "tenant_id", None)
            user_id = getattr(user, "id", None)

            if user_tenant_id is not None:
                tenant_id = str(user_tenant_id)
            elif user_id is not None:
                tenant_id = str(user_id)

        logger.info(
            f"Creating CustomPGVector dataset entry: " +
            f"dataset={dataset_id}, tenant={tenant_id}"
        )

        return {
            "vector_database_provider": "custompgvector",
            "vector_database_url": db_url,
            "vector_database_key": vector_config.vector_db_key,
            "vector_database_name": db_name,
            "vector_dataset_database_handler": "custompgvector",
            "vector_database_connection_info": {
                "dataset_id": str(dataset_id) if dataset_id is not None else None,
                "tenant_id": tenant_id,
                "rls_enabled": True,
            },
        }

    @override
    @classmethod
    async def resolve_dataset_connection_info(
        cls, dataset_database: DatasetDatabase
    ) -> DatasetDatabase:
        """
        Resolve runtime connection details for CustomPGVector dataset.
        """
        if dataset_database.vector_database_connection_info is None:
            dataset_database.vector_database_connection_info = {}

        # Ensure RLS context fields are present in the connection info dict
        connection_info: dict[str, Any] = dataset_database.vector_database_connection_info

        if "rls_enabled" not in connection_info:
            connection_info["rls_enabled"] = True

        return dataset_database

    @override
    @classmethod
    async def delete_dataset(cls, dataset_database: DatasetDatabase) -> None:
        """
        Delete vector data for the given dataset.
        """
        pass
