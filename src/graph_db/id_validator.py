from typing import List
from pybloom_live import BloomFilter

class IDValidator:
    def __init__(self, expected_count: int, error_rate: float = 0.001):
        self.bloom_filters = {}
        self.expected_count = expected_count
        self.error_rate = error_rate
    
    def build_from_neo4j(self, neo4j_conn, entity_types: List[str]):
        """Build bloom filters from Neo4j data"""
        for entity_type in entity_types:
            self.bloom_filters[entity_type] = BloomFilter(
                capacity=self.expected_count,
                error_rate=self.error_rate
            )
            
            # Query Neo4j for all IDs of this type
            query = f"MATCH (n:{entity_type}) RETURN n.es_id as id"
            with neo4j_conn.get_session() as session:
                result = session.run(query)
                for record in result:
                    if record['id']:
                        self.bloom_filters[entity_type].add(record['id'])
    
    def exists(self, entity_type: str, es_id: str) -> bool:
        """Check if ID exists (with small false positive rate)"""
        if entity_type in self.bloom_filters:
            return es_id in self.bloom_filters[entity_type]
        return False