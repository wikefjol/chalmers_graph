"""
Sample extraction and caching for formatting evaluation.
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from es_client.client import ElasticsearchClient


class SampleExtractor:
    """Extracts and caches representative samples from Elasticsearch for offline testing."""
    
    ENTITY_INDEX_MAP = {
        'persons': 'research-persons-static',
        'publications': 'research-publications-static', 
        'projects': 'research-projects-static',
        'organizations': 'research-organizations-static',
        'serials': 'research-serials-static'
    }
    
    def __init__(self, es_client: ElasticsearchClient, cache_dir: str = "data/formatting_samples"):
        self.es_client = es_client
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def extract_samples(self, entity_type: str, count: int = 10, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Extract sample documents for an entity type."""
        cache_file = os.path.join(self.cache_dir, f"{entity_type}_samples.json")
        
        # Check cache first
        if not force_refresh and os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                    if len(cached_data.get('samples', [])) >= count:
                        print(f"Using cached samples for {entity_type}")
                        return cached_data['samples'][:count]
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Extract fresh samples
        print(f"Extracting {count} samples for {entity_type} from ES...")
        samples = self._extract_from_es(entity_type, count)
        
        # Cache the results
        cache_data = {
            'entity_type': entity_type,
            'extracted_at': datetime.now().isoformat(),
            'count': len(samples),
            'samples': samples
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        print(f"Cached {len(samples)} {entity_type} samples")
        return samples
    
    def _extract_from_es(self, entity_type: str, count: int) -> List[Dict[str, Any]]:
        """Extract samples from Elasticsearch."""
        index_name = self.ENTITY_INDEX_MAP.get(entity_type)
        if not index_name:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        try:
            # Get diverse samples using random scoring
            query = {
                "query": {
                    "function_score": {
                        "query": {"match_all": {}},
                        "random_score": {"seed": int(time.time())}
                    }
                },
                "size": count
            }
            
            result = self.es_client.search(index_name, query)
            
            samples = []
            for hit in result['hits']['hits']:
                samples.append({
                    'id': hit['_id'],
                    'source': hit['_source']
                })
            
            return samples
            
        except Exception as e:
            print(f"Error extracting {entity_type} samples: {e}")
            return []
    
    def get_cached_samples(self, entity_type: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached samples if available."""
        cache_file = os.path.join(self.cache_dir, f"{entity_type}_samples.json")
        
        if not os.path.exists(cache_file):
            return None
            
        try:
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                return cached_data.get('samples', [])
        except (json.JSONDecodeError, KeyError):
            return None
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached samples."""
        cache_info = {}
        
        for entity_type in self.ENTITY_INDEX_MAP.keys():
            cache_file = os.path.join(self.cache_dir, f"{entity_type}_samples.json")
            
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r') as f:
                        cached_data = json.load(f)
                        cache_info[entity_type] = {
                            'count': cached_data.get('count', 0),
                            'extracted_at': cached_data.get('extracted_at', 'unknown'),
                            'file_size': os.path.getsize(cache_file)
                        }
                except (json.JSONDecodeError, KeyError):
                    cache_info[entity_type] = {'error': 'corrupted cache'}
            else:
                cache_info[entity_type] = {'status': 'not cached'}
        
        return cache_info
    
    def clear_cache(self, entity_type: Optional[str] = None):
        """Clear cached samples."""
        if entity_type:
            cache_file = os.path.join(self.cache_dir, f"{entity_type}_samples.json")
            if os.path.exists(cache_file):
                os.remove(cache_file)
                print(f"Cleared cache for {entity_type}")
        else:
            # Clear all caches
            for et in self.ENTITY_INDEX_MAP.keys():
                cache_file = os.path.join(self.cache_dir, f"{et}_samples.json")
                if os.path.exists(cache_file):
                    os.remove(cache_file)
            print("Cleared all sample caches")