"""
Schema management for Neo4j Graph RAG database
"""

from typing import List, Dict, Any, Optional
from .connection import Neo4jConnection


class SchemaManager:
    """Manages Neo4j schema constraints, indexes, and validation"""
    
    def __init__(self, connection: Neo4jConnection):
        self.connection = connection
    
    def create_constraints(self) -> bool:
        """Create all required uniqueness constraints"""
        constraints = [
            ("person_id", "Person", "es_id"),
            ("org_id", "Organization", "es_id"),
            ("project_id", "Project", "es_id"),
            ("pub_id", "Publication", "es_id"),
            ("serial_id", "Serial", "es_id"),
        ]
        
        print("🔒 Creating uniqueness constraints...")
        success = True
        
        with self.connection.get_session() as session:
            for constraint_name, label, property_name in constraints:
                try:
                    query = f"""
                    CREATE CONSTRAINT {constraint_name} 
                    IF NOT EXISTS 
                    FOR (n:{label}) 
                    REQUIRE n.{property_name} IS UNIQUE
                    """
                    session.run(query)
                    print(f"  ✓ {constraint_name}: {label}.{property_name}")
                except Exception as e:
                    print(f"  ❌ {constraint_name}: {e}")
                    success = False
        
        return success
    
    def create_property_indexes(self) -> bool:
        """Create property indexes for common queries"""
        indexes = [
            ("person_name_idx", "Person", "display_name"),
            ("org_name_idx", "Organization", "display_name_eng"),
            ("pub_title_idx", "Publication", "title"),
            ("pub_year_idx", "Publication", "year"),
            ("project_title_idx", "Project", "title_eng"),
            ("serial_title_idx", "Serial", "title"),
        ]
        
        print("🔍 Creating property indexes...")
        success = True
        
        with self.connection.get_session() as session:
            for index_name, label, property_name in indexes:
                try:
                    query = f"""
                    CREATE INDEX {index_name} 
                    IF NOT EXISTS 
                    FOR (n:{label}) 
                    ON (n.{property_name})
                    """
                    session.run(query)
                    print(f"  ✓ {index_name}: {label}.{property_name}")
                except Exception as e:
                    print(f"  ❌ {index_name}: {e}")
                    success = False
        
        return success
    
    def create_vector_indexes(self) -> bool:
        """Create vector indexes for embeddings"""
        # Note: Vector indexes require Neo4j 5.x+
        vector_indexes = [
            ("person_embedding_idx", "Person", "embedding", 1536),
            ("org_embedding_idx", "Organization", "embedding", 1536),
            ("pub_embedding_idx", "Publication", "embedding", 1536),
            ("project_embedding_idx", "Project", "embedding", 1536),
            ("serial_embedding_idx", "Serial", "embedding", 1536),
        ]
        
        print("🎯 Creating vector indexes...")
        success = True
        
        with self.connection.get_session() as session:
            for index_name, label, property_name, dimensions in vector_indexes:
                try:
                    query = f"""
                    CALL db.index.vector.createNodeIndex(
                        '{index_name}',
                        '{label}',
                        '{property_name}',
                        {dimensions},
                        'cosine'
                    )
                    """
                    session.run(query)
                    print(f"  ✓ {index_name}: {label}.{property_name} ({dimensions}d)")
                except Exception as e:
                    # Vector indexes might not be available in all Neo4j versions
                    if "procedure not found" in str(e).lower() or "vector" in str(e).lower():
                        print(f"  ⚠️ {index_name}: Vector indexes not supported in this Neo4j version")
                    else:
                        print(f"  ❌ {index_name}: {e}")
                        success = False
        
        return success
    
    def drop_constraints(self) -> bool:
        """Drop all constraints (for cleanup)"""
        constraint_names = [
            "person_id", "org_id", "project_id", "pub_id", "serial_id"
        ]
        
        print("🔓 Dropping constraints...")
        success = True
        
        with self.connection.get_session() as session:
            for constraint_name in constraint_names:
                try:
                    query = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
                    session.run(query)
                    print(f"  ✓ Dropped {constraint_name}")
                except Exception as e:
                    print(f"  ❌ {constraint_name}: {e}")
                    success = False
        
        return success
    
    def drop_indexes(self) -> bool:
        """Drop all indexes (for cleanup)"""
        index_names = [
            "person_name_idx", "org_name_idx", "pub_title_idx", 
            "pub_year_idx", "project_title_idx", "serial_title_idx",
            "person_embedding_idx", "org_embedding_idx", "pub_embedding_idx",
            "project_embedding_idx", "serial_embedding_idx"
        ]
        
        print("🗑️ Dropping indexes...")
        success = True
        
        with self.connection.get_session() as session:
            for index_name in index_names:
                try:
                    query = f"DROP INDEX {index_name} IF EXISTS"
                    session.run(query)
                    print(f"  ✓ Dropped {index_name}")
                except Exception as e:
                    print(f"  ❌ {index_name}: {e}")
                    success = False
        
        return success
    
    def get_schema_info(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get current schema information"""
        with self.connection.get_session() as session:
            # Get constraints
            constraints_result = session.run("SHOW CONSTRAINTS")
            constraints = [dict(record) for record in constraints_result]
            
            # Get indexes
            indexes_result = session.run("SHOW INDEXES")
            indexes = [dict(record) for record in indexes_result]
            
            # Get node labels
            labels_result = session.run("CALL db.labels()")
            labels = [record["label"] for record in labels_result]
            
            # Get relationship types
            rel_types_result = session.run("CALL db.relationshipTypes()")
            rel_types = [record["relationshipType"] for record in rel_types_result]
        
        return {
            "constraints": constraints,
            "indexes": indexes,
            "node_labels": labels,
            "relationship_types": rel_types
        }
    
    def validate_schema(self) -> Dict[str, bool]:
        """Validate that all required schema elements exist"""
        schema_info = self.get_schema_info()
        
        # Required constraints
        required_constraints = {
            "person_id", "org_id", "project_id", "pub_id", "serial_id"
        }
        
        # Required indexes
        required_indexes = {
            "person_name_idx", "org_name_idx", "pub_title_idx", 
            "pub_year_idx", "project_title_idx", "serial_title_idx"
        }
        
        # Required node labels
        required_labels = {
            "Person", "Organization", "Project", "Publication", "Serial"
        }
        
        # Check constraints
        existing_constraints = {c.get("name", "") for c in schema_info["constraints"]}
        constraints_valid = required_constraints.issubset(existing_constraints)
        
        # Check indexes
        existing_indexes = {i.get("name", "") for i in schema_info["indexes"]}
        indexes_valid = required_indexes.issubset(existing_indexes)
        
        # Check labels (only if data exists)
        existing_labels = set(schema_info["node_labels"])
        labels_valid = True  # Labels are created when data is inserted
        
        return {
            "constraints": constraints_valid,
            "indexes": indexes_valid,
            "labels": labels_valid,
            "overall": constraints_valid and indexes_valid and labels_valid
        }
    
    def setup_schema(self) -> bool:
        """Complete schema setup - constraints and indexes"""
        print("🏗️ Setting up Neo4j schema...")
        
        success = True
        success &= self.create_constraints()
        success &= self.create_property_indexes()
        success &= self.create_vector_indexes()
        
        if success:
            print("✅ Schema setup completed successfully")
        else:
            print("❌ Schema setup completed with errors")
        
        return success
    
    def reset_schema(self) -> bool:
        """Reset schema - drop and recreate all constraints and indexes"""
        print("🔄 Resetting Neo4j schema...")
        
        success = True
        success &= self.drop_indexes()
        success &= self.drop_constraints()
        
        if success:
            success &= self.setup_schema()
        
        if success:
            print("✅ Schema reset completed successfully")
        else:
            print("❌ Schema reset completed with errors")
        
        return success