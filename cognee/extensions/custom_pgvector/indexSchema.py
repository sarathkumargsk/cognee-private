from cognee.infrastructure.engine.models.DataPoint import DataPoint


class IndexSchema(DataPoint):
    """
    Define a schema for indexing data points with a text field.

    This class inherits from the DataPoint class and specifies the structure of a single
    data point that includes a text attribute. It also includes a metadata field that
    indicates which fields should be indexed.
    """

    text: str

    metadata: dict[str, list[str]] = {"index_fields": ["text"]}