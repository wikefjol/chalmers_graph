"""
Data extractors for each Elasticsearch index with streaming support
"""
from typing import Iterator, List, Dict, Any, Optional, Generator
from datetime import datetime
from .client import ElasticsearchClient
from .base_extractor import BaseStreamingExtractor, BaseExtractor


class PersonExtractor(BaseStreamingExtractor):
    """Extractor for research-persons-static index with streaming support"""
    
    def __init__(self, es_client: ElasticsearchClient, batch_size: int = 1000):
        super().__init__(es_client, batch_size)
        # For backward compatibility
        self.es = es_client
    
    def get_index_name(self) -> str:
        """Return the index name for persons"""
        return 'research-persons-static'
    
    def get_query(self) -> Dict[str, Any]:
        """Return the default query for extracting all persons"""
        return {"match_all": {}}
    
    # Backward compatibility methods that now use streaming
    def extract_all(self) -> Iterator[Dict[str, Any]]:
        """Extract all documents (backward compatibility)"""
        for batch in self.extract_batches():
            for doc in batch:
                yield doc
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents (backward compatibility)"""
        return self.extract_sample_batch(size)
    
    # Specialized extraction methods
    def extract_active_persons_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract only active persons in batches"""
        query = {
            "bool": {
                "must": [
                    {"term": {"IsActive": True}},
                    {"term": {"IsDeleted": False}}
                ]
            }
        }
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def extract_persons_with_affiliations_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract persons who have organizational affiliations in batches"""
        query = {
            "bool": {
                "must": [
                    {"term": {"HasOrganizationHome": True}},
                    {"range": {"OrganizationHomeCount": {"gt": 0}}}
                ]
            }
        }
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    # Iterator versions for backward compatibility
    def extract_active_persons(self) -> Iterator[Dict[str, Any]]:
        """Extract only active persons (backward compatibility iterator)"""
        for batch in self.extract_active_persons_batches():
            for doc in batch:
                yield doc
    
    def extract_persons_with_affiliations(self) -> Iterator[Dict[str, Any]]:
        """Extract persons with affiliations (backward compatibility iterator)"""
        for batch in self.extract_persons_with_affiliations_batches():
            for doc in batch:
                yield doc


class OrganizationExtractor(BaseStreamingExtractor):
    """Extractor for research-organizations-static index with streaming support"""
    
    def __init__(self, es_client: ElasticsearchClient, batch_size: int = 1000):
        super().__init__(es_client, batch_size)
        self.es = es_client
    
    def get_index_name(self) -> str:
        """Return the index name for organizations"""
        return 'research-organizations-static'
    
    def get_query(self) -> Dict[str, Any]:
        """Return the default query for extracting all organizations"""
        return {"match_all": {}}
    
    # Backward compatibility methods
    def extract_all(self) -> Iterator[Dict[str, Any]]:
        """Extract all documents (backward compatibility)"""
        for batch in self.extract_batches():
            for doc in batch:
                yield doc
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents (backward compatibility)"""
        return self.extract_sample_batch(size)
    
    # Specialized extraction methods
    def extract_active_organizations_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract only active organizations in batches"""
        query = {"term": {"IsActive": True}}
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def extract_active_organizations(self) -> Iterator[Dict[str, Any]]:
        """Extract only active organizations (backward compatibility iterator)"""
        for batch in self.extract_active_organizations_batches():
            for doc in batch:
                yield doc


class PublicationExtractor(BaseStreamingExtractor):
    """Extractor for research-publications-static index with streaming support"""
    
    def __init__(self, es_client: ElasticsearchClient, batch_size: int = 1000):
        super().__init__(es_client, batch_size)
        self.es = es_client
    
    def get_index_name(self) -> str:
        """Return the index name for publications"""
        return 'research-publications-static'
    
    def get_query(self) -> Dict[str, Any]:
        """Return the default query for extracting all publications"""
        return {"match_all": {}}
    
    # Backward compatibility methods
    def extract_all(self) -> Iterator[Dict[str, Any]]:
        """Extract all documents (backward compatibility)"""
        for batch in self.extract_batches():
            for doc in batch:
                yield doc
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents (backward compatibility)"""
        return self.extract_sample_batch(size)
    
    # Specialized extraction methods
    def extract_published_publications_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract only published (non-draft, non-deleted) publications in batches"""
        query = {
            "bool": {
                "must": [
                    {"term": {"IsDraft": False}},
                    {"term": {"IsDeleted": False}}
                ]
            }
        }
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def extract_by_year_range_batches(self, start_year: int, end_year: int) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract publications within a year range in batches"""
        query = {
            "bool": {
                "must": [
                    {"range": {"Year": {"gte": start_year, "lte": end_year}}},
                    {"term": {"IsDraft": False}},
                    {"term": {"IsDeleted": False}}
                ]
            }
        }
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    # Iterator versions for backward compatibility
    def extract_published_publications(self) -> Iterator[Dict[str, Any]]:
        """Extract published publications (backward compatibility iterator)"""
        for batch in self.extract_published_publications_batches():
            for doc in batch:
                yield doc
    
    def extract_by_year_range(self, start_year: int, end_year: int) -> Iterator[Dict[str, Any]]:
        """Extract publications by year range (backward compatibility iterator)"""
        for batch in self.extract_by_year_range_batches(start_year, end_year):
            for doc in batch:
                yield doc


class ProjectExtractor(BaseStreamingExtractor):
    """Extractor for research-projects-static index with streaming support"""
    
    def __init__(self, es_client: ElasticsearchClient, batch_size: int = 1000):
        super().__init__(es_client, batch_size)
        self.es = es_client
    
    def get_index_name(self) -> str:
        """Return the index name for projects"""
        return 'research-projects-static'
    
    def get_query(self) -> Dict[str, Any]:
        """Return the default query for extracting all projects"""
        return {"match_all": {}}
    
    # Backward compatibility methods
    def extract_all(self) -> Iterator[Dict[str, Any]]:
        """Extract all documents (backward compatibility)"""
        for batch in self.extract_batches():
            for doc in batch:
                yield doc
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents (backward compatibility)"""
        return self.extract_sample_batch(size)
    
    # Specialized extraction methods
    def extract_active_projects_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract projects with publish status indicating they're active in batches"""
        query = {
            "bool": {
                "must": [
                    {"range": {"PublishStatus": {"gte": 1}}}
                ]
            }
        }
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def extract_active_projects(self) -> Iterator[Dict[str, Any]]:
        """Extract active projects (backward compatibility iterator)"""
        for batch in self.extract_active_projects_batches():
            for doc in batch:
                yield doc


class SerialExtractor(BaseStreamingExtractor):
    """Extractor for research-serials-static index with streaming support"""
    
    def __init__(self, es_client: ElasticsearchClient, batch_size: int = 1000):
        super().__init__(es_client, batch_size)
        self.es = es_client
    
    def get_index_name(self) -> str:
        """Return the index name for serials"""
        return 'research-serials-static'
    
    def get_query(self) -> Dict[str, Any]:
        """Return the default query for extracting all serials"""
        return {"match_all": {}}
    
    # Backward compatibility methods
    def extract_all(self) -> Iterator[Dict[str, Any]]:
        """Extract all documents (backward compatibility)"""
        for batch in self.extract_batches():
            for doc in batch:
                yield doc
    
    def extract_sample(self, size: int = 10) -> List[Dict[str, Any]]:
        """Extract sample documents (backward compatibility)"""
        return self.extract_sample_batch(size)
    
    # Specialized extraction methods
    def extract_active_serials_batches(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Extract non-deleted serials in batches"""
        query = {"term": {"IsDeleted": False}}
        
        for batch in self.es_client.scan_documents(
            index=self.index_name,
            query=query,
            batch_size=self.batch_size
        ):
            yield batch
    
    def extract_active_serials(self) -> Iterator[Dict[str, Any]]:
        """Extract active serials (backward compatibility iterator)"""
        for batch in self.extract_active_serials_batches():
            for doc in batch:
                yield doc


# Helper function to get the count of documents for each extractor
def get_extraction_counts(es_client: ElasticsearchClient) -> Dict[str, int]:
    """Get document counts for all indices"""
    extractors = {
        'persons': PersonExtractor(es_client),
        'organizations': OrganizationExtractor(es_client),
        'publications': PublicationExtractor(es_client),
        'projects': ProjectExtractor(es_client),
        'serials': SerialExtractor(es_client)
    }
    
    counts = {}
    for name, extractor in extractors.items():
        counts[name] = es_client.count_documents(extractor.index_name)
    
    return counts