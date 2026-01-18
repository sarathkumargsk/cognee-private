import asyncio
from contextlib import asynccontextmanager
from sqlite3 import ProgrammingError
from typing import List, Union, Optional, Any, override, get_type_hints, AsyncGenerator
from uuid import UUID
from asyncpg import DeadlockDetectedError, DuplicateTableError, UniqueViolationError
from sqlalchemy import delete, select, func, inspect, Column, JSON, MetaData, Table, text, UUID as SQLUUID
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.postgresql import insert

from distributed.utils import override_distributed
from distributed.tasks.queued_add_data_points import queued_add_data_points
from .indexSchema import IndexSchema
from cognee.infrastructure.databases.exceptions import MissingQueryParameterError
from cognee.infrastructure.databases.relational import get_relational_engine, Base
from cognee.infrastructure.databases.relational.sqlalchemy.SqlAlchemyAdapter import SQLAlchemyAdapter
from cognee.infrastructure.databases.vector.models.ScoredResult import ScoredResult
from cognee.infrastructure.databases.vector.pgvector.serialize_data import serialize_data
from cognee.infrastructure.databases.vector.utils import normalize_distances
from cognee.infrastructure.databases.vector.vector_db_interface import VectorDBInterface
from cognee.infrastructure.engine.models.DataPoint import DataPoint
from cognee.infrastructure.engine.utils import parse_id
from cognee.shared.logging_utils import get_logger, LoggerInterface
from cognee.infrastructure.databases.vector.embeddings import EmbeddingEngine
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from cognee.infrastructure.databases.vector.exceptions import CollectionNotFoundError

logger: LoggerInterface = get_logger("CustomPGVectorAdapter")

class CustomPGVectorAdapter(SQLAlchemyAdapter, VectorDBInterface):

    def __init__(
            self,
            url: str,
            api_key: Optional[str],
            embedding_engine: EmbeddingEngine,
            database_name: Optional[str] = None,
    ):
        self.api_key = api_key
        self.embedding_engine = embedding_engine
        self.db_uri: str = url
        self.VECTOR_DB_LOCK = asyncio.Lock()

        relational_db = get_relational_engine()

        # If postgreSQL is used we must use the same engine and sessionmaker
        if relational_db.engine.dialect.name == "postgresql":
            self.engine = relational_db.engine
            self.sessionmaker = relational_db.sessionmaker
        else:
            # If not create new instances of engine and sessionmaker
            self.engine = create_async_engine(self.db_uri)
            self.sessionmaker = async_sessionmaker(bind=self.engine, expire_on_commit=False)

        # Has to be imported at class level
        # Functions reading tables from database need to know what a Vector column type is
        from pgvector.sqlalchemy import Vector

        self.Vector = Vector

    def _extract_connection_info(self) -> dict:
        """
        Extract RLS context from vector_database_connection_info and session_user.

        This information flows from:
        1. DatasetDatabase.vector_database_connection_info (stored in DB)
        2. Populated by CustomPGVectorDatasetDatabaseHandler during dataset creation
        3. Made available through vector_db_config context variable
        4. IMPORTANT: user_id and tenant_id are taken from session_user (current user)
           not from stored connection_info (which has the original creator's info)

        Returns:
            dict with keys: dataset_id, tenant_id, user_id, rls_enabled
        """
        from cognee.context_global_variables import vector_db_config, session_user

        # Get context config which includes connection_info
        context_config = vector_db_config.get() if vector_db_config.get() else {}

        # Extract connection info (dataset_id and rls_enabled from stored config)
        connection_info = context_config.get('vector_database_connection_info', {})

        # Get the current session user - this is the user making the request,
        # NOT the user who originally created the dataset
        current_user = session_user.get()

        # Use current user's info for RLS checks (user_id and tenant_id)
        # Fall back to stored connection_info only if session_user is not set
        if current_user is not None:
            user_id = str(current_user.id) if current_user.id else None
            tenant_id = str(current_user.tenant_id) if current_user.tenant_id else None
            logger.debug(
                f"RLS context from session_user: user_id={user_id}, tenant_id={tenant_id}"
            )
        else:
            user_id = connection_info.get('user_id')
            tenant_id = connection_info.get('tenant_id')
            logger.debug(
                f"RLS context from connection_info (no session_user): "
                f"user_id={user_id}, tenant_id={tenant_id}"
            )

        return {
            'dataset_id': connection_info.get('dataset_id'),
            'tenant_id': tenant_id,
            'user_id': user_id,
            'rls_enabled': connection_info.get('rls_enabled', True)
        }

    @asynccontextmanager
    async def get_async_session_with_rls(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Provide an async session with RLS context variables set.

        Uses SET LOCAL to ensure variables are transaction-scoped and automatically
        cleared when the transaction ends.
        """
        async with self.get_async_session() as session:
            # Extract RLS context
            context = self._extract_connection_info()

            # Only set session variables if using PostgreSQL
            if self.engine.dialect.name == "postgresql":
                # Set session variables using SET LOCAL (transaction-scoped)
                if context['dataset_id']:
                    await session.execute(text(f"SET LOCAL app.dataset_id = '{context['dataset_id']}';"))

                if context['tenant_id']:
                    await session.execute(text(f"SET LOCAL app.tenant_id = '{context['tenant_id']}';"))

                if context['user_id']:
                    await session.execute(text(f"SET LOCAL app.user_id = '{context['user_id']}';"))

                # Set RLS enabled flag
                rls_enabled = 'true' if context['rls_enabled'] else 'false'
                await session.execute(text(f"SET LOCAL app.rls_enabled = '{rls_enabled}';"))

            try:
                yield session
            finally:
                # SET LOCAL variables are automatically cleared on transaction end
                pass

    async def _create_rls_policies(self, connection, table_name: str):
        """
        Create RLS policies for a collection table.

        Policy Logic:
        1. Tenant isolation: current_setting('app.tenant_id') = tenant_id
        2. Access control: Owner OR has ACL read permission
        3. Read-only enforcement at DB level
        """
        # Only create RLS policies for PostgreSQL
        if self.engine.dialect.name != "postgresql":
            return

        try:
            # Grant SELECT on tables used in RLS policy subqueries
            # This is required because RLS policy subqueries run with the
            # permissions of the current user, not a superuser
            # We use try/except for each grant as some may already exist or fail
            for table in ['datasets', 'acls', 'permissions', 'user_roles']:
                try:
                    await connection.execute(text(f"GRANT SELECT ON {table} TO PUBLIC;"))
                except Exception as grant_error:
                    logger.debug(f"Grant on {table} skipped (may already exist): {grant_error}")

            # Enable RLS on the table
            await connection.execute(text(f'ALTER TABLE "{table_name}" ENABLE ROW LEVEL SECURITY;'))

            # Drop existing policies if they exist (for idempotency)
            await connection.execute(text(f'DROP POLICY IF EXISTS "{table_name}_rls_policy" ON "{table_name}";'))

            # Create comprehensive RLS policy for SELECT operations
            # Supports permission inheritance: User → Role → Tenant
            rls_policy_sql = f"""
            CREATE POLICY "{table_name}_rls_policy" ON "{table_name}"
            FOR SELECT
            USING (
                -- Check if RLS is enabled for this request
                CASE
                    WHEN current_setting('app.rls_enabled', true) = 'false' THEN true
                    ELSE (
                        -- Tenant isolation
                        (tenant_id::text = current_setting('app.tenant_id', true))
                        AND
                        -- Access control: owner OR has read permission (direct, via role, or via tenant)
                        (
                            -- Check if user is the dataset owner
                            EXISTS (
                                SELECT 1 FROM datasets
                                WHERE datasets.id = "{table_name}".dataset_id
                                AND datasets.owner_id::text = current_setting('app.user_id', true)
                            )
                            OR
                            -- Check if user has direct ACL read permission
                            EXISTS (
                                SELECT 1 FROM acls
                                JOIN permissions ON acls.permission_id = permissions.id
                                WHERE acls.dataset_id = "{table_name}".dataset_id
                                AND acls.principal_id::text = current_setting('app.user_id', true)
                                AND permissions.name = 'read'
                            )
                            OR
                            -- Check if user has ACL read permission via role membership
                            EXISTS (
                                SELECT 1 FROM acls
                                JOIN permissions ON acls.permission_id = permissions.id
                                JOIN user_roles ON user_roles.role_id = acls.principal_id
                                WHERE acls.dataset_id = "{table_name}".dataset_id
                                AND user_roles.user_id::text = current_setting('app.user_id', true)
                                AND permissions.name = 'read'
                            )
                            OR
                            -- Check if tenant has ACL read permission
                            EXISTS (
                                SELECT 1 FROM acls
                                JOIN permissions ON acls.permission_id = permissions.id
                                WHERE acls.dataset_id = "{table_name}".dataset_id
                                AND acls.principal_id::text = current_setting('app.tenant_id', true)
                                AND permissions.name = 'read'
                            )
                        )
                    )
                END
            );
            """
            await connection.execute(text(rls_policy_sql))

            # Create restrictive policies for INSERT/UPDATE/DELETE (application-controlled)
            # These ensure writes can only happen when RLS is explicitly disabled
            await connection.execute(text(f"""
            DROP POLICY IF EXISTS "{table_name}_write_policy" ON "{table_name}";
            """))

            await connection.execute(text(f"""
            CREATE POLICY "{table_name}_write_policy" ON "{table_name}"
            FOR ALL
            USING (current_setting('app.rls_enabled', true) = 'false');
            """))

            logger.info(f"RLS policies created successfully for table: {table_name}")
        except Exception as e:
            logger.warning(f"Failed to create RLS policies for {table_name}: {e}")
            # Don't fail the entire operation if RLS policy creation fails
            # This allows the extension to work even without RLS support

    @override
    async def embed_data(self, data: list[str]) -> list[list[float]]:
        """
        Embed a list of texts into vectors using the specified embedding engine.

        Parameters:
        -----------

            - data (list[str]): A list of strings to be embedded into vectors.

        Returns:
        --------

            - list[list[float]]: A list of lists of floats representing embedded vectors.
        """
        return await self.embedding_engine.embed_text(data)

    @override
    async def has_collection(self, collection_name: str) -> bool:
        """
        Check if a specified collection exists in the database.

        Parameters:
        -----------

            - collection_name (str): The name of the collection to check for existence.

        Returns:
        --------

            - bool: Returns True if the collection exists, False otherwise.
        """
        async with self.engine.begin() as connection:
            # Create a MetaData instance to load table information
            metadata = MetaData()
            # Load table information from schema into MetaData
            await connection.run_sync(metadata.reflect)

            if collection_name in metadata.tables:
                return True
            else:
                return False


    @retry(
        retry=retry_if_exception_type(
            (DuplicateTableError, UniqueViolationError, ProgrammingError)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=1, max=6),
    )
    @override
    async def create_collection(self, collection_name: str, payload_schema=None):
        data_point_types = get_type_hints(DataPoint)
        vector_size = self.embedding_engine.get_vector_size()

        if not await self.has_collection(collection_name):
            async with self.VECTOR_DB_LOCK:
                if not await self.has_collection(collection_name):

                    class CustomPGVectorDataPoint(Base):
                        """
                        Represent a point in a vector data space with associated data and vector representation.

                        This class inherits from Base and is associated with a database table defined by
                        __tablename__. It maintains the following public methods and instance variables:

                        - __init__(self, id, dataset_id, tenant_id, payload, vector): Initializes a new CustomPGVectorDataPoint instance.

                        Instance variables:
                        - id: Identifier for the data point, defined by data_point_types.
                        - dataset_id: UUID of the dataset this data point belongs to (for RLS).
                        - tenant_id: UUID of the tenant this data point belongs to (for RLS).
                        - payload: JSON data associated with the data point.
                        - vector: Vector representation of the data point, with size defined by vector_size.
                        """

                        __tablename__ = collection_name
                        __table_args__ = {"extend_existing": True}
                        # PGVector requires one column to be the primary key
                        id: Mapped[data_point_types["id"]] = mapped_column(primary_key=True)
                        # RLS columns for tenant and dataset isolation
                        dataset_id = Column(SQLUUID, nullable=False, index=True)
                        tenant_id = Column(SQLUUID, nullable=True, index=True)
                        # Data columns
                        payload = Column(JSON)
                        vector = Column(self.Vector(vector_size))

                        def __init__(self, id, dataset_id, tenant_id, payload, vector):
                            self.id = id
                            self.dataset_id = dataset_id
                            self.tenant_id = tenant_id
                            self.payload = payload
                            self.vector = vector

                    async with self.engine.begin() as connection:
                        if len(Base.metadata.tables.keys()) > 0:
                            await connection.run_sync(
                                Base.metadata.create_all, tables=[CustomPGVectorDataPoint.__table__]
                            )

                    # Create RLS policies after table creation (in separate connection context)
                    async with self.engine.begin() as connection:
                        await self._create_rls_policies(connection, collection_name)

    @retry(
        retry=retry_if_exception_type(DeadlockDetectedError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=6),
    )
    @override_distributed(queued_add_data_points)
    async def create_data_points(self, collection_name: str, data_points: List[DataPoint]):
        data_point_types = get_type_hints(DataPoint)
        if not await self.has_collection(collection_name):
            await self.create_collection(
                collection_name=collection_name,
                payload_schema=type(data_points[0]),
            )

        data_vectors = await self.embed_data(
            [DataPoint.get_embeddable_data(data_point) for data_point in data_points]
        )

        vector_size = self.embedding_engine.get_vector_size()

        # Extract context for dataset_id and tenant_id
        context = self._extract_connection_info()
        dataset_id = context['dataset_id']
        tenant_id = context['tenant_id']

        class CustomPGVectorDataPoint(Base):
            """
            Represents a data point in a PGVector database. This class maps to a table defined by
            the SQLAlchemy ORM.

            It contains the following public instance variables:
            - id: An identifier for the data point.
            - dataset_id: UUID of the dataset this data point belongs to (for RLS).
            - tenant_id: UUID of the tenant this data point belongs to (for RLS).
            - payload: A JSON object containing additional data related to the data point.
            - vector: A vector representation of the data point, configured to the specified size.
            """

            __tablename__ = collection_name
            __table_args__ = {"extend_existing": True}
            # PGVector requires one column to be the primary key
            id: Mapped[data_point_types["id"]] = mapped_column(primary_key=True)
            dataset_id = Column(SQLUUID, nullable=False, index=True)
            tenant_id = Column(SQLUUID, nullable=True, index=True)
            payload = Column(JSON)
            vector = Column(self.Vector(vector_size))

            def __init__(self, id, dataset_id, tenant_id, payload, vector):
                self.id = id
                self.dataset_id = dataset_id
                self.tenant_id = tenant_id
                self.payload = payload
                self.vector = vector

        async with self.get_async_session_with_rls() as session:
            pgvector_data_points = []

            for data_index, data_point in enumerate(data_points):
                pgvector_data_points.append(
                    CustomPGVectorDataPoint(
                        id=data_point.id,
                        dataset_id=dataset_id,
                        tenant_id=tenant_id,
                        vector=data_vectors[data_index],
                        payload=serialize_data(data_point.model_dump()),
                    )
                )

            def to_dict(obj):
                return {
                    column.key: getattr(obj, column.key)
                    for column in inspect(obj).mapper.column_attrs
                }

            insert_statement = insert(CustomPGVectorDataPoint).values(
                [to_dict(data_point) for data_point in pgvector_data_points]
            )
            insert_statement = insert_statement.on_conflict_do_nothing(index_elements=["id"])
            await session.execute(insert_statement)
            await session.commit()

    @override
    async def create_vector_index(self, index_name: str, index_property_name: str):
        await self.create_collection(f"{index_name}_{index_property_name}")

    @override
    async def index_data_points(
            self, index_name: str, index_property_name: str, data_points: list[DataPoint]
    ):
        await self.create_data_points(
            f"{index_name}_{index_property_name}",
            [
                IndexSchema(
                    id=data_point.id,
                    text=DataPoint.get_embeddable_data(data_point),
                )
                for data_point in data_points
            ],
        )

    @override
    async def get_table(self, collection_name: str) -> Table:
        """
        Dynamically loads a table using the given collection name
        with an async engine.
        """
        async with self.engine.begin() as connection:
            # Create a MetaData instance to load table information
            metadata = MetaData()
            # Load table information from schema into MetaData
            await connection.run_sync(metadata.reflect)
            if collection_name in metadata.tables:
                return metadata.tables[collection_name]
            else:
                raise CollectionNotFoundError(
                    f"Collection '{collection_name}' not found!",
                )

    @override
    async def retrieve(self, collection_name: str, data_point_ids: List[str]):
        # Get PGVectorDataPoint Table from database
        PGVectorDataPoint = await self.get_table(collection_name)

        async with self.get_async_session_with_rls() as session:
            results = await session.execute(
                select(PGVectorDataPoint).where(PGVectorDataPoint.c.id.in_(data_point_ids))
            )
            results = results.all()

            return [
                ScoredResult(id=parse_id(result.id), payload=result.payload, score=0)
                for result in results
            ]

    @override
    async def search(
            self,
            collection_name: str,
            query_text: Optional[str] = None,
            query_vector: Optional[List[float]] = None,
            limit: Optional[int] = 15,
            with_vector: bool = False,
    ) -> List[ScoredResult]:
        if query_text is None and query_vector is None:
            raise MissingQueryParameterError()

        if query_text and not query_vector:
            query_vector = (await self.embedding_engine.embed_text([query_text]))[0]

        # Get PGVectorDataPoint Table from database
        PGVectorDataPoint = await self.get_table(collection_name)

        if limit is None:
            async with self.get_async_session_with_rls() as session:
                query = select(func.count()).select_from(PGVectorDataPoint)
                result = await session.execute(query)
                limit = result.scalar_one()

        # If limit is still 0, no need to do the search, just return empty results
        if limit <= 0:
            return []

        # NOTE: This needs to be initialized in case search doesn't return a value
        closest_items = []

        # Use RLS-enabled session for reads
        async with self.get_async_session_with_rls() as session:
            query = select(
                PGVectorDataPoint,
                PGVectorDataPoint.c.vector.cosine_distance(query_vector).label("similarity"),
            ).order_by("similarity")

            if limit > 0:
                query = query.limit(limit)

            # Find closest vectors to query_vector
            closest_items = await session.execute(query)

        vector_list = []

        # Extract distances and find min/max for normalization
        for vector in closest_items.all():
            vector_list.append(
                {
                    "id": parse_id(str(vector.id)),
                    "payload": vector.payload,
                    "_distance": vector.similarity,
                }
            )

        if len(vector_list) == 0:
            return []

        # Normalize vector distance and add this as score information to vector_list
        normalized_values = normalize_distances(vector_list)
        for i in range(0, len(normalized_values)):
            vector_list[i]["score"] = normalized_values[i]

        # Create and return ScoredResult objects
        return [
            ScoredResult(id=row.get("id"), payload=row.get("payload"), score=row.get("score"))
            for row in vector_list
        ]

    @override
    async def batch_search(
            self,
            collection_name: str,
            query_texts: List[str],
            limit: int = None,
            with_vectors: bool = False,
    ):
        query_vectors = await self.embedding_engine.embed_text(query_texts)

        return await asyncio.gather(
            *[
                self.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=limit,
                    with_vector=with_vectors,
                )
                for query_vector in query_vectors
            ]
        )

    @override
    async def delete_data_points(self, collection_name: str, data_point_ids: list[str]):
        async with self.get_async_session_with_rls() as session:
            # Get PGVectorDataPoint Table from database
            PGVectorDataPoint = await self.get_table(collection_name)
            results = await session.execute(
                delete(PGVectorDataPoint).where(PGVectorDataPoint.c.id.in_(data_point_ids))
            )
            await session.commit()
            return results

    @override
    async def prune(self):
        # Clean up the database if it was set up as temporary
        await self.delete_database()
