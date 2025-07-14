"""
Elasticsearch client for connecting to research database
"""

import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from typing import Optional, Dict, Any

# Load environment variables
load_dotenv()


class ElasticsearchClient:
    """
    Client for connecting to Elasticsearch research database
    """
    
    def __init__(self):
        self.host = os.getenv('ES_HOST')
        self.username = os.getenv('ES_USER')
        self.password = os.getenv('ES_PASS')
        
        if not all([self.host, self.username, self.password]):
            raise ValueError("Missing required Elasticsearch environment variables: ES_HOST, ES_USER, ES_PASS")
        
        self.client = Elasticsearch(
            hosts=[self.host],
            http_auth=(self.username, self.password),
            verify_certs=False
        )
    
    def ping(self) -> bool:
        """Test connection to Elasticsearch"""
        try:
            return self.client.ping()
        except Exception as e:
            print(f"Connection failed: {e}")
            return False
    
    def get_info(self) -> Dict[str, Any]:
        """Get Elasticsearch cluster info"""
        return self.client.info()
    
    def test_connection(self) -> bool:
        """Test connection and print status"""
        if self.ping():
            print("✓ Connected to Elasticsearch")
            info = self.get_info()
            print(f"  Version: {info['version']['number']}")
            print(f"  Host: {self.host}")
            return True
        else:
            print("✗ Could not connect to Elasticsearch")
            return False
    
    def get_index_info(self, index_name: str) -> Dict[str, Any]:
        """Get information about a specific index"""
        try:
            return self.client.indices.get(index=index_name)
        except Exception as e:
            print(f"Error getting index info for {index_name}: {e}")
            return {}
    
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """Get statistics for a specific index"""
        try:
            return self.client.indices.stats(index=index_name)
        except Exception as e:
            print(f"Error getting index stats for {index_name}: {e}")
            return {}
    
    def search(self, index: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a search query"""
        return self.client.search(index=index, body=body)
    
    def scroll(self, index: str, body: Dict[str, Any], scroll: str = '5m', size: int = 1000):
        """Execute a scroll query for large result sets"""
        return self.client.search(index=index, body=body, scroll=scroll, size=size)
    
    def scroll_continue(self, scroll_id: str, scroll: str = '5m'):
        """Continue scrolling through results"""
        return self.client.scroll(scroll_id=scroll_id, scroll=scroll)
    
    def scan_documents(self, index: str, query: Dict[str, Any] = None, 
                   batch_size: int = 1000, scroll_timeout: str = '5m'):
        """
        Generator that yields batches of documents using scroll API
        """
        if query is None:
            query = {"match_all": {}}
        
        # Initial search
        response = self.client.search(
            index=index,
            body={"query": query},
            scroll=scroll_timeout,
            size=batch_size
        )
        
        scroll_id = response['_scroll_id']
        
        try:
            while True:
                hits = response['hits']['hits']
                if not hits:
                    break
                    
                # Yield current batch
                yield [hit['_source'] for hit in hits]
                
                # Get next batch
                response = self.client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll_timeout
                )
        except Exception as e:
            # Log error but don't re-raise immediately - cleanup first
            print(f"    ⚠️ ES scroll error: {e}")
            raise
        finally:
            # Always clean up scroll context, even on errors
            try:
                self.client.clear_scroll(scroll_id=scroll_id)
            except Exception as cleanup_error:
                # Don't let cleanup errors mask original errors
                print(f"    ⚠️ Failed to cleanup scroll context: {cleanup_error}")
                pass
    
    def clear_all_scroll_contexts(self) -> bool:
        """Clear all active scroll contexts - useful for cleanup after errors"""
        try:
            result = self.client.clear_scroll(scroll_id="_all")
            print(f"    ✅ Cleared all scroll contexts")
            return True
        except Exception as e:
            print(f"    ⚠️ Failed to clear all scroll contexts: {e}")
            return False
    
    def check_cluster_health(self) -> Dict[str, Any]:
        """Check ES cluster health and return status info"""
        try:
            # Get cluster health
            health = self.client.cluster.health()
            
            # Get basic node info
            info = self.client.info()
            
            # Get cluster stats for memory info
            stats = self.client.cluster.stats()
            
            return {
                'status': 'healthy',
                'cluster_name': info.get('cluster_name', 'unknown'),
                'version': info.get('version', {}).get('number', 'unknown'),
                'health_status': health.get('status', 'unknown'),
                'active_shards': health.get('active_shards', 0),
                'nodes': {
                    'total': health.get('number_of_nodes', 0),
                    'data': health.get('number_of_data_nodes', 0)
                },
                'memory_usage': stats.get('nodes', {}).get('jvm', {}).get('mem', {}),
                'indices_count': health.get('active_primary_shards', 0)
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def is_cluster_ready_for_import(self) -> bool:
        """Check if cluster is ready for large import operations"""
        health_info = self.check_cluster_health()
        
        if health_info['status'] != 'healthy':
            print(f"    ❌ Cluster unhealthy: {health_info.get('error', 'Unknown error')}")
            return False
        
        # Check cluster status
        if health_info['health_status'] not in ['green', 'yellow']:
            print(f"    ⚠️ Cluster status: {health_info['health_status']} (not green/yellow)")
            return False
        
        # Check if we have data nodes
        if health_info['nodes']['data'] == 0:
            print(f"    ❌ No data nodes available")
            return False
        
        print(f"    ✅ Cluster ready: {health_info['health_status']} status, {health_info['nodes']['total']} nodes")
        return True

    def count_documents(self, index: str, query: Optional[Dict[str, Any]] = None) -> int:
        """Count documents in an index"""
        body = {"query": query} if query else {"query": {"match_all": {}}}
        result = self.client.count(index=index, body=body)
        return result['count']
    
    def count_batches(self, index: str, batch_size: int = 1000) -> int:
        """Calculate number of batches for progress tracking"""
        total = self.count_documents(index)
        return (total + batch_size - 1) // batch_size

    def get_sample_documents(self, index: str, size: int = 10) -> Dict[str, Any]:
        """Get sample documents from an index"""
        body = {
            "query": {"match_all": {}},
            "size": size
        }
        return self.search(index=index, body=body)
    
    def close(self):
        """Close the Elasticsearch connection"""
        # Elasticsearch client doesn't need explicit closing
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()