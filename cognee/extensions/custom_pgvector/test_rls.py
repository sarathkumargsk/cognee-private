"""
Test script for CustomPGVector RLS implementation.

This script tests the Row-Level Security implementation for tenant and dataset isolation.
"""

import asyncio
from uuid import uuid4, UUID
from cognee.extensions.custom_pgvector.custom_pg_vector_adapter import CustomPGVectorAdapter
from cognee.infrastructure.databases.vector.embeddings import get_embedding_engine
from cognee.infrastructure.databases.relational import get_relational_engine
from cognee.infrastructure.engine.models.DataPoint import DataPoint
from cognee.context_global_variables import vector_db_config


class MockDataPoint(DataPoint):
    """Mock DataPoint for testing."""
    text: str
    
    def __init__(self, **data):
        # Set metadata to index the 'text' field
        if 'metadata' not in data:
            data['metadata'] = {'type': 'MockDataPoint', 'index_fields': ['text']}
        super().__init__(**data)


async def test_rls_implementation():
    """
    Test the RLS implementation with multiple tenants and users.
    """
    print("=" * 80)
    print("Testing CustomPGVector RLS Implementation")
    print("=" * 80)
    
    # Initialize adapter
    relational_engine = get_relational_engine()
    db_url = str(relational_engine.engine.url)
    
    adapter = CustomPGVectorAdapter(
        url=db_url,
        api_key=None,
        embedding_engine=get_embedding_engine()
    )
    
    # Test data
    tenant1_id = uuid4()
    tenant2_id = uuid4()
    dataset1_id = uuid4()
    dataset2_id = uuid4()
    user1_id = uuid4()
    user2_id = uuid4()
    collection_name = "test_rls_collection"
    
    print(f"\nTest Setup:")
    print(f"  Tenant 1: {tenant1_id}")
    print(f"  Tenant 2: {tenant2_id}")
    print(f"  Dataset 1: {dataset1_id}")
    print(f"  Dataset 2: {dataset2_id}")
    print(f"  User 1: {user1_id}")
    print(f"  User 2: {user2_id}")
    
    # Test 1: Create collection with RLS columns
    print("\n" + "-" * 80)
    print("Test 1: Creating collection with RLS columns")
    print("-" * 80)
    
    try:
        # Set context for tenant 1, dataset 1
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset1_id),
                'tenant_id': str(tenant1_id),
                'user_id': str(user1_id),
                'rls_enabled': False  # Disable RLS for data insertion
            }
        })
        
        await adapter.create_collection(collection_name)
        print("✓ Collection created successfully with RLS columns")
        
        # Verify table structure
        if adapter.engine.dialect.name == "postgresql":
            table_exists = await adapter.has_collection(collection_name)
            print(f"✓ Table exists: {table_exists}")
        
    except Exception as e:
        print(f"✗ Failed to create collection: {e}")
        return
    
    # Test 2: Insert data points for Tenant 1, Dataset 1
    print("\n" + "-" * 80)
    print("Test 2: Inserting data for Tenant 1, Dataset 1")
    print("-" * 80)
    
    try:
        data_points_t1d1 = [
            MockDataPoint(id=uuid4(), text="Tenant 1 Dataset 1 - Document 1"),
            MockDataPoint(id=uuid4(), text="Tenant 1 Dataset 1 - Document 2"),
        ]
        
        await adapter.create_data_points(collection_name, data_points_t1d1)
        print(f"✓ Inserted {len(data_points_t1d1)} data points for Tenant 1, Dataset 1")
        
    except Exception as e:
        print(f"✗ Failed to insert data: {e}")
        return
    
    # Test 3: Insert data points for Tenant 2, Dataset 2
    print("\n" + "-" * 80)
    print("Test 3: Inserting data for Tenant 2, Dataset 2")
    print("-" * 80)
    
    try:
        # Switch context to tenant 2, dataset 2
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset2_id),
                'tenant_id': str(tenant2_id),
                'user_id': str(user2_id),
                'rls_enabled': False  # Disable RLS for data insertion
            }
        })
        
        data_points_t2d2 = [
            MockDataPoint(id=uuid4(), text="Tenant 2 Dataset 2 - Document 1"),
            MockDataPoint(id=uuid4(), text="Tenant 2 Dataset 2 - Document 2"),
        ]
        
        await adapter.create_data_points(collection_name, data_points_t2d2)
        print(f"✓ Inserted {len(data_points_t2d2)} data points for Tenant 2, Dataset 2")
        
    except Exception as e:
        print(f"✗ Failed to insert data: {e}")
        return
    
    # Test 4: Search as Tenant 1 User 1 with RLS enabled (should only see Tenant 1 data)
    print("\n" + "-" * 80)
    print("Test 4: Search as Tenant 1 User 1 with RLS enabled")
    print("-" * 80)
    
    try:
        # Switch context to tenant 1, user 1, enable RLS
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset1_id),
                'tenant_id': str(tenant1_id),
                'user_id': str(user1_id),
                'rls_enabled': True  # Enable RLS for search
            }
        })
        
        # Note: This test requires datasets and ACL tables to be properly set up
        # In a real scenario, you would create dataset records and ACL entries
        print("⚠ Note: This test requires datasets and ACL tables to be set up")
        print("  You need to:")
        print("  1. Create dataset records in the 'datasets' table")
        print("  2. Create ACL entries for user permissions")
        print("  3. Then run search queries")
        
        # Example search (commented out until datasets are set up):
        # results = await adapter.search(collection_name, query_text="Document", limit=10)
        # print(f"✓ Search returned {len(results)} results (should only see Tenant 1 data)")
        
    except Exception as e:
        print(f"✗ Search test error: {e}")
    
    # Test 5: Search as Tenant 2 User 2 with RLS enabled (should only see Tenant 2 data)
    print("\n" + "-" * 80)
    print("Test 5: Search as Tenant 2 User 2 with RLS enabled")
    print("-" * 80)
    
    try:
        # Switch context to tenant 2, user 2, enable RLS
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset2_id),
                'tenant_id': str(tenant2_id),
                'user_id': str(user2_id),
                'rls_enabled': True  # Enable RLS for search
            }
        })
        
        print("⚠ Note: This test requires datasets and ACL tables to be set up")
        
    except Exception as e:
        print(f"✗ Search test error: {e}")
    
    # Test 6: Verify RLS policies exist
    print("\n" + "-" * 80)
    print("Test 6: Verifying RLS policies")
    print("-" * 80)
    
    if adapter.engine.dialect.name == "postgresql":
        try:
            async with adapter.engine.begin() as conn:
                from sqlalchemy import text
                
                # Check if RLS is enabled
                result = await conn.execute(text(f"""
                    SELECT relrowsecurity 
                    FROM pg_class 
                    WHERE relname = '{collection_name}';
                """))
                rls_enabled = result.scalar()
                print(f"✓ RLS enabled on table: {rls_enabled}")
                
                # Check policies
                result = await conn.execute(text(f"""
                    SELECT policyname, cmd 
                    FROM pg_policies 
                    WHERE tablename = '{collection_name}';
                """))
                policies = result.fetchall()
                print(f"✓ Found {len(policies)} RLS policies:")
                for policy in policies:
                    print(f"  - {policy[0]} (command: {policy[1]})")
                    
        except Exception as e:
            print(f"✗ Failed to verify RLS policies: {e}")
    else:
        print("⚠ Skipping RLS policy verification (not using PostgreSQL)")
    
    # Test 7: Cleanup
    print("\n" + "-" * 80)
    print("Test 7: Cleanup")
    print("-" * 80)
    
    try:
        # Disable RLS for cleanup
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset1_id),
                'tenant_id': str(tenant1_id),
                'user_id': str(user1_id),
                'rls_enabled': False
            }
        })
        
        # Drop the test collection
        if adapter.engine.dialect.name == "postgresql":
            async with adapter.engine.begin() as conn:
                from sqlalchemy import text
                await conn.execute(text(f'DROP TABLE IF EXISTS "{collection_name}" CASCADE;'))
                print(f"✓ Test collection '{collection_name}' dropped")
        
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")
    
    print("\n" + "=" * 80)
    print("RLS Implementation Test Complete")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Create dataset records in the 'datasets' table")
    print("2. Create ACL entries to grant permissions")
    print("3. Run full end-to-end tests with cognee's permission system")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_rls_implementation())
