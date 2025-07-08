#!/usr/bin/env python3
"""
Database connection and configuration module for Neo4j
"""

import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Neo4jConnection:
    def __init__(self):
        self.uri = os.getenv('NEO4J_URI')
        self.username = os.getenv('NEO4J_USERNAME')
        self.password = os.getenv('NEO4J_PASSWORD')
        self.database = os.getenv('NEO4J_DATABASE')
        
        if not all([self.uri, self.username, self.password]):
            raise ValueError("Missing required Neo4j environment variables")
        
        self.driver = GraphDatabase.driver(
            self.uri, 
            auth=(self.username, self.password)
        )
    
    def close(self):
        """Close the database connection"""
        if self.driver:
            self.driver.close()
    
    def get_session(self):
        """Get a new database session"""
        return self.driver.session(database=self.database)
    
    def test_connection(self):
        """Test the database connection"""
        try:
            with self.get_session() as session:
                result = session.run("RETURN 1 as test")
                return result.single()["test"] == 1
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def clear_database(self):
        """Clear all nodes and relationships from the database"""
        with self.get_session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("🧹 Database cleared")
    
    def get_stats(self):
        """Get database statistics"""
        with self.get_session() as session:
            # Get node counts by label
            node_result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY label
            """)
            
            # Get relationship counts by type
            rel_result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(r) as count
                ORDER BY type
            """)
            
            return {
                'nodes': [(record['label'], record['count']) for record in node_result],
                'relationships': [(record['type'], record['count']) for record in rel_result]
            }
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()