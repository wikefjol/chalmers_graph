# es_client/base_extractor.py
from abc import ABC, abstractmethod
from typing import Iterator, Generator, Dict, Any, List
from .client import ElasticsearchClient

class BaseStreamingExtractor(ABC):
    def __init__(self, es_client, batch_size: int = 1000):
        self.es_client = es_client
        self.batch_size = batch_size
        self.index_name = self.get_index_name()
    
    @abstractmethod
    def get_index_name(self) -> str:
        """Return the index name for this extractor"""
        pass
    
    @abstractmethod
    def get_query(self) -> Dict[str, Any]:
        """Return the query for extracting data"""
        pass
    
    def extract_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield batches of documents"""
        query = self.get_query()
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def set_batch_size(self, new_batch_size: int) -> None:
        """Update batch size for this extractor"""
        self.batch_size = new_batch_size
    
    def extract_sample_batch(self, size: int = 100) -> List[Dict[str, Any]]:
        """Extract a single sample batch"""
        query = self.get_query()
        result = self.es_client.search(
            index=self.index_name,
            body={"query": query, "size": size}
        )
        return [hit['_source'] for hit in result['hits']['hits']]
    
class BaseExtractor:
    """Base class for Elasticsearch data extractors"""
    
    def __init__(self, es_client: ElasticsearchClient, index_name: str):
        self.es = es_client
        self.index_name = index_name
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents"""
        result = self.es.get_sample_documents(self.index_name, size)
        return [hit['_source'] for hit in result['hits']['hits']]
    
    def extract_all(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Extract all documents using scroll API"""
        body = {"query": {"match_all": {}}}
        
        # Initial scroll request
        result = self.es.scroll(
            index=self.index_name, 
            body=body, 
            scroll='5m', 
            size=batch_size
        )
        
        scroll_id = result['_scroll_id']
        hits = result['hits']['hits']
        
        # Yield first batch
        for hit in hits:
            yield hit['_source']
        
        # Continue scrolling
        while hits:
            result = self.es.scroll_continue(scroll_id, scroll='5m')
            scroll_id = result['_scroll_id']
            hits = result['hits']['hits']
            
            for hit in hits:
                yield hit['_source']
    
    def count_total(self) -> int:
        """Count total documents in the index"""
        return self.es.count_documents(self.index_name)
    
    def extract_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        """Extract documents by their IDs"""
        body = {
            "query": {
                "terms": {
                    "_id": ids
                }
            }
        }
        result = self.es.search(self.index_name, body)
        return [hit['_source'] for hit in result['hits']['hits']]