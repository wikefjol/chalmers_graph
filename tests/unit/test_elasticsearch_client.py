"""
Unit tests for Elasticsearch client
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from elasticsearch import Elasticsearch

from es_client.client import ElasticsearchClient


class TestElasticsearchClient:
    """Test cases for ElasticsearchClient"""
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_client_initialization_success(self):
        """Test successful client initialization with valid environment variables"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            client = ElasticsearchClient()
            
            assert client.host == 'test-host.com'
            assert client.username == 'test_user'
            assert client.password == 'test_pass'
            
            mock_es.assert_called_once_with(
                hosts=['test-host.com'],
                http_auth=('test_user', 'test_pass'),
                verify_certs=False
            )
    
    @patch.dict(os.environ, {}, clear=True)
    def test_client_initialization_missing_env_vars(self):
        """Test client initialization fails with missing environment variables"""
        with pytest.raises(ValueError, match="Missing required Elasticsearch environment variables"):
            ElasticsearchClient()
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_ping_success(self):
        """Test successful ping"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.ping.return_value = True
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.ping()
            
            assert result is True
            mock_client.ping.assert_called_once()
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_ping_failure(self):
        """Test ping failure with exception"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.ping.side_effect = Exception("Connection failed")
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.ping()
            
            assert result is False
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_get_info(self):
        """Test get_info method"""
        expected_info = {
            'version': {'number': '6.8.23'},
            'cluster_name': 'test_cluster'
        }
        
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.info.return_value = expected_info
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.get_info()
            
            assert result == expected_info
            mock_client.info.assert_called_once()
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_test_connection_success(self, capsys):
        """Test test_connection method with successful connection"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.ping.return_value = True
            mock_client.info.return_value = {
                'version': {'number': '6.8.23'}
            }
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.test_connection()
            
            assert result is True
            captured = capsys.readouterr()
            assert "✓ Connected to Elasticsearch" in captured.out
            assert "Version: 6.8.23" in captured.out
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_count_documents(self):
        """Test count_documents method"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.count.return_value = {'count': 1000}
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.count_documents('test-index')
            
            assert result == 1000
            mock_client.count.assert_called_once_with(
                index='test-index',
                body={'query': {'match_all': {}}}
            )
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_get_sample_documents(self):
        """Test get_sample_documents method"""
        expected_response = {
            'hits': {
                'hits': [
                    {'_source': {'Id': '1', 'Title': 'Test Doc 1'}},
                    {'_source': {'Id': '2', 'Title': 'Test Doc 2'}}
                ]
            }
        }
        
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.search.return_value = expected_response
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            result = client.get_sample_documents('test-index', size=2)
            
            assert result == expected_response
            mock_client.search.assert_called_once_with(
                index='test-index',
                body={
                    'query': {'match_all': {}},
                    'size': 2
                }
            )
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_scroll_methods(self):
        """Test scroll and scroll_continue methods"""
        scroll_response = {
            'hits': {'hits': [{'_source': {'Id': '1'}}]},
            '_scroll_id': 'test_scroll_id'
        }
        
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_client.search.return_value = scroll_response
            mock_client.scroll.return_value = scroll_response
            mock_es.return_value = mock_client
            
            client = ElasticsearchClient()
            
            # Test scroll
            body = {'query': {'match_all': {}}}
            result = client.scroll('test-index', body, scroll='5m', size=1000)
            assert result == scroll_response
            
            # Test scroll_continue
            result = client.scroll_continue('test_scroll_id', scroll='5m')
            assert result == scroll_response
            mock_client.scroll.assert_called_with(
                scroll_id='test_scroll_id',
                scroll='5m'
            )
    
    @patch.dict(os.environ, {
        'ES_HOST': 'test-host.com',
        'ES_USER': 'test_user',
        'ES_PASS': 'test_pass'
    })
    def test_context_manager(self):
        """Test client as context manager"""
        with patch('es_client.client.Elasticsearch') as mock_es:
            mock_client = MagicMock()
            mock_es.return_value = mock_client
            
            with ElasticsearchClient() as client:
                assert isinstance(client, ElasticsearchClient)
            
            # close() should be called (though it's a no-op for ES client)