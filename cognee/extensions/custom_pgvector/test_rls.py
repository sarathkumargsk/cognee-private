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
    
    # Test 6: Role-based permission access
    print("\n" + "-" * 80)
    print("Test 6: Role-based permission access")
    print("-" * 80)

    try:
        role_id = uuid4()
        user_with_role_id = uuid4()

        print("⚠ Note: This test requires the following setup:")
        print("  1. Create a role in the 'roles' table")
        print("  2. Add an ACL entry where principal_id = role_id")
        print("  3. Add user_roles entry linking user to role")
        print("")
        print("  Example SQL setup:")
        print(f"    INSERT INTO roles (id, name, tenant_id) VALUES ('{role_id}', 'test_role', '{tenant1_id}');")
        print(f"    INSERT INTO user_roles (user_id, role_id) VALUES ('{user_with_role_id}', '{role_id}');")
        print(f"    INSERT INTO acls (principal_id, dataset_id, permission_id) ")
        print(f"      SELECT '{role_id}', '{dataset1_id}', id FROM permissions WHERE name = 'read';")
        print("")
        print("  Then search as user_with_role_id (who doesn't have direct permission)")
        print("  User should be able to access data through role inheritance")

    except Exception as e:
        print(f"✗ Role-based permission test error: {e}")

    # Test 7: Tenant-based permission access
    print("\n" + "-" * 80)
    print("Test 7: Tenant-based permission access")
    print("-" * 80)

    try:
        user_in_tenant_id = uuid4()

        print("⚠ Note: This test requires the following setup:")
        print("  1. Add an ACL entry where principal_id = tenant_id")
        print("  2. A user that belongs to the tenant but has no direct/role permission")
        print("")
        print("  Example SQL setup:")
        print(f"    INSERT INTO acls (principal_id, dataset_id, permission_id) ")
        print(f"      SELECT '{tenant1_id}', '{dataset1_id}', id FROM permissions WHERE name = 'read';")
        print("")
        print("  Then search as any user in tenant1 (even without direct permission)")
        print("  User should be able to access data through tenant-level permission")

    except Exception as e:
        print(f"✗ Tenant-based permission test error: {e}")

    # Test 8: Permission denied test
    print("\n" + "-" * 80)
    print("Test 8: Permission denied (no access)")
    print("-" * 80)

    try:
        unauthorized_user_id = uuid4()

        print("⚠ Note: This test verifies that users without any permission path cannot access data")
        print("  1. User has no direct permission")
        print("  2. User has no role with permission")
        print("  3. User's tenant has no permission")
        print("")
        print("  Expected: Search returns empty results (data is filtered by RLS)")

        # Switch context to an unauthorized user (same tenant but no permissions)
        vector_db_config.set({
            'vector_database_connection_info': {
                'dataset_id': str(dataset1_id),
                'tenant_id': str(tenant1_id),
                'user_id': str(unauthorized_user_id),
                'rls_enabled': True
            }
        })

        print(f"  Set context: unauthorized user {unauthorized_user_id}")
        print("  Note: Actual search test requires datasets table to exist")

    except Exception as e:
        print(f"✗ Permission denied test error: {e}")

    # Test 9: Verify RLS policies exist
    print("\n" + "-" * 80)
    print("Test 9: Verifying RLS policies")
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
    
    # Test 10: Cleanup
    print("\n" + "-" * 80)
    print("Test 10: Cleanup")
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
    print("\nPermission Inheritance Hierarchy:")
    print("  User → Role → Tenant")
    print("")
    print("Access is granted if ANY of these conditions are met:")
    print("  1. User is the dataset owner")
    print("  2. User has direct ACL 'read' permission")
    print("  3. User's role has ACL 'read' permission")
    print("  4. User's tenant has ACL 'read' permission")
    print("")
    print("Next Steps to fully test:")
    print("  1. Create dataset records in the 'datasets' table")
    print("  2. Create role records in the 'roles' table")
    print("  3. Create user_roles entries to link users to roles")
    print("  4. Create ACL entries for users, roles, or tenants")
    print("  5. Run search queries to verify permission inheritance")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_rls_implementation())
