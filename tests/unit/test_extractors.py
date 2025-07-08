"""
Unit tests for Elasticsearch extractors
"""

import pytest
from unittest.mock import Mock, MagicMock
from typing import Dict, Any

from es_client.extractors import (
    BaseExtractor,
    PersonExtractor,
    OrganizationExtractor,
    PublicationExtractor,
    ProjectExtractor,
    SerialExtractor
)


class TestBaseExtractor:
    """Test cases for BaseExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test extractor initialization"""
        extractor = BaseExtractor(mock_es_client, 'test-index')
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'test-index'
    
    def test_extract_sample(self, mock_es_client, mock_es_search_response):
        """Test extract_sample method"""
        mock_es_client.get_sample_documents.return_value = mock_es_search_response
        
        extractor = BaseExtractor(mock_es_client, 'test-index')
        result = extractor.extract_sample(size=2)
        
        expected = [
            {'Id': '1', 'Title': 'Test Doc 1'},
            {'Id': '2', 'Title': 'Test Doc 2'}
        ]
        assert result == expected
        mock_es_client.get_sample_documents.assert_called_once_with('test-index', 2)
    
    def test_count_total(self, mock_es_client):
        """Test count_total method"""
        mock_es_client.count_documents.return_value = 1000
        
        extractor = BaseExtractor(mock_es_client, 'test-index')
        result = extractor.count_total()
        
        assert result == 1000
        mock_es_client.count_documents.assert_called_once_with('test-index')
    
    def test_extract_by_ids(self, mock_es_client, mock_es_search_response):
        """Test extract_by_ids method"""
        mock_es_client.search.return_value = mock_es_search_response
        
        extractor = BaseExtractor(mock_es_client, 'test-index')
        result = extractor.extract_by_ids(['1', '2'])
        
        expected = [
            {'Id': '1', 'Title': 'Test Doc 1'},
            {'Id': '2', 'Title': 'Test Doc 2'}
        ]
        assert result == expected
        
        # Verify the search query
        expected_body = {
            "query": {
                "terms": {
                    "_id": ['1', '2']
                }
            }
        }
        mock_es_client.search.assert_called_once_with('test-index', expected_body)
    
    def test_extract_all_single_batch(self, mock_es_client):
        """Test extract_all method with single batch"""
        # Mock scroll response with no more results
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'Title': 'Doc 1'}},
                    {'_source': {'Id': '2', 'Title': 'Doc 2'}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        # Second call returns empty hits (end of scroll)
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = BaseExtractor(mock_es_client, 'test-index')
        results = list(extractor.extract_all(batch_size=1000))
        
        expected = [
            {'Id': '1', 'Title': 'Doc 1'},
            {'Id': '2', 'Title': 'Doc 2'}
        ]
        assert results == expected


class TestPersonExtractor:
    """Test cases for PersonExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test PersonExtractor initialization"""
        extractor = PersonExtractor(mock_es_client)
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'research-persons-static'
    
    def test_extract_active_persons(self, mock_es_client):
        """Test extract_active_persons method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'IsActive': True, 'IsDeleted': False}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = PersonExtractor(mock_es_client)
        results = list(extractor.extract_active_persons())
        
        assert len(results) == 1
        assert results[0]['Id'] == '1'
        assert results[0]['IsActive'] is True
        
        # Verify the query structure
        expected_body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"IsActive": True}},
                        {"term": {"IsDeleted": False}}
                    ]
                }
            }
        }
        mock_es_client.scroll.assert_called_once_with(
            'research-persons-static', 
            expected_body, 
            scroll='5m', 
            size=1000
        )
    
    def test_extract_persons_with_affiliations(self, mock_es_client):
        """Test extract_persons_with_affiliations method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'HasOrganizationHome': True, 'OrganizationHomeCount': 2}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = PersonExtractor(mock_es_client)
        results = list(extractor.extract_persons_with_affiliations())
        
        assert len(results) == 1
        assert results[0]['HasOrganizationHome'] is True
        assert results[0]['OrganizationHomeCount'] == 2


class TestOrganizationExtractor:
    """Test cases for OrganizationExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test OrganizationExtractor initialization"""
        extractor = OrganizationExtractor(mock_es_client)
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'research-organizations-static'
    
    def test_extract_active_organizations(self, mock_es_client):
        """Test extract_active_organizations method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'IsActive': True}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = OrganizationExtractor(mock_es_client)
        results = list(extractor.extract_active_organizations())
        
        assert len(results) == 1
        assert results[0]['IsActive'] is True
        
        # Verify the query structure
        expected_body = {
            "query": {
                "term": {"IsActive": True}
            }
        }
        mock_es_client.scroll.assert_called_once_with(
            'research-organizations-static', 
            expected_body, 
            scroll='5m', 
            size=1000
        )


class TestPublicationExtractor:
    """Test cases for PublicationExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test PublicationExtractor initialization"""
        extractor = PublicationExtractor(mock_es_client)
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'research-publications-static'
    
    def test_extract_published_publications(self, mock_es_client):
        """Test extract_published_publications method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'IsDraft': False, 'IsDeleted': False}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = PublicationExtractor(mock_es_client)
        results = list(extractor.extract_published_publications())
        
        assert len(results) == 1
        assert results[0]['IsDraft'] is False
        assert results[0]['IsDeleted'] is False
    
    def test_extract_by_year_range(self, mock_es_client):
        """Test extract_by_year_range method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'Year': 2020, 'IsDraft': False, 'IsDeleted': False}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = PublicationExtractor(mock_es_client)
        results = list(extractor.extract_by_year_range(2020, 2022))
        
        assert len(results) == 1
        assert results[0]['Year'] == 2020
        
        # Verify the query structure includes year range
        expected_body = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"Year": {"gte": 2020, "lte": 2022}}},
                        {"term": {"IsDraft": False}},
                        {"term": {"IsDeleted": False}}
                    ]
                }
            }
        }
        mock_es_client.scroll.assert_called_once_with(
            'research-publications-static', 
            expected_body, 
            scroll='5m', 
            size=1000
        )


class TestProjectExtractor:
    """Test cases for ProjectExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test ProjectExtractor initialization"""
        extractor = ProjectExtractor(mock_es_client)
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'research-projects-static'
    
    def test_extract_active_projects(self, mock_es_client):
        """Test extract_active_projects method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'ID': 1, 'PublishStatus': 3}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = ProjectExtractor(mock_es_client)
        results = list(extractor.extract_active_projects())
        
        assert len(results) == 1
        assert results[0]['PublishStatus'] == 3


class TestSerialExtractor:
    """Test cases for SerialExtractor"""
    
    def test_initialization(self, mock_es_client):
        """Test SerialExtractor initialization"""
        extractor = SerialExtractor(mock_es_client)
        
        assert extractor.es == mock_es_client
        assert extractor.index_name == 'research-serials-static'
    
    def test_extract_active_serials(self, mock_es_client):
        """Test extract_active_serials method"""
        mock_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'IsDeleted': False}}
                ]
            },
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_empty_response = {
            'hits': {'hits': []},
            '_scroll_id': 'test_scroll_id'
        }
        
        mock_es_client.scroll.return_value = mock_response
        mock_es_client.scroll_continue.return_value = mock_empty_response
        
        extractor = SerialExtractor(mock_es_client)
        results = list(extractor.extract_active_serials())
        
        assert len(results) == 1
        assert results[0]['IsDeleted'] is False