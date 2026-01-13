from .custom_pg_vector_adapter import CustomPGVectorAdapter
from .custom_pg_vector_database_handler import CustomPGVectorDatasetDatabaseHandler

try:
    print("===== Registering CustomPGVector Adapter and Dataset Database Handler =====")
    from cognee.infrastructure.databases.vector.supported_databases import (
        supported_databases,
    )
    from cognee.infrastructure.databases.dataset_database_handler import (
        supported_dataset_database_handlers,
    )

    supported_databases["custompgvector"] = CustomPGVectorAdapter
    supported_dataset_database_handlers["custompgvector"] = {
        "handler_instance": CustomPGVectorDatasetDatabaseHandler,
        "handler_provider": "custompgvector",
    }
except ImportError as ie:
    raise ImportError(
        "cognee is not installed. Please install it with: pip install cognee"
    ) from ie

