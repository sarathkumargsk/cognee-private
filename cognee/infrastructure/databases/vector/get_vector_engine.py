from .config import get_vectordb_context_config
from .create_vector_engine import create_vector_engine


def get_vector_engine():
    config = get_vectordb_context_config()

    # Filter out vector_database_connection_info as it's not a parameter for create_vector_engine
    # It should only be stored in context for adapters to access
    engine_config = {k: v for k, v in config.items() if k != 'vector_database_connection_info'}

    return create_vector_engine(**engine_config)
