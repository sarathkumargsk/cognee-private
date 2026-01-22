from .nebulagraph_adapter import NebulaGraphAdapter
from .nebulagraph_dataset_database_handler import NebulaGraphDatasetDatabaseHandler

try:
    print("===== Registering NebulaGraph Adapter and Dataset Database Handler =====")
    from cognee.infrastructure.databases.graph import use_graph_adapter
    from cognee.infrastructure.databases.dataset_database_handler import (
        supported_dataset_database_handlers,
    )

    use_graph_adapter("nebulagraph", NebulaGraphAdapter)

    supported_dataset_database_handlers["nebulagraph"] = {
        "handler_instance": NebulaGraphDatasetDatabaseHandler,
        "handler_provider": "nebulagraph",
    }


except ImportError as ie:
    raise ImportError(
        "cognee is not installed. Please install it with: pip install cognee"
    ) from ie
