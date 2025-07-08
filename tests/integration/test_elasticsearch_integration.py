"""
Integration tests for Elasticsearch connectivity and data extraction
"""

import pytest
import os
from typing import Dict, Any

from es_client.client import ElasticsearchClient
from es_client.extractors import (
    PersonExtractor,
    OrganizationExtractor,
    PublicationExtractor,
    ProjectExtractor,
    SerialExtractor
)


@pytest.mark.integration
class TestElasticsearchIntegration:
    """Integration tests for Elasticsearch connectivity"""
    
    def test_connection_to_real_elasticsearch(self):
        """Test connection to actual Elasticsearch instance"""
        # Skip if environment variables not set (CI/CD, etc.)
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        assert client.test_connection() is True
        
        # Test basic info retrieval
        info = client.get_info()
        assert 'version' in info
        assert 'number' in info['version']
    
    def test_index_access_and_counts(self):
        """Test access to all required indexes and document counts"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        
        indexes = [
            'research-persons-static',
            'research-organizations-static',
            'research-publications-static',
            'research-projects-static',
            'research-serials-static'
        ]
        
        for index in indexes:
            try:
                count = client.count_documents(index)
                assert count > 0, f"Index {index} should have documents"
                print(f"✓ {index}: {count:,} documents")
            except Exception as e:
                pytest.fail(f"Failed to access index {index}: {e}")
    
    def test_person_extractor_real_data(self):
        """Test PersonExtractor with real data"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        extractor = PersonExtractor(client)
        
        # Test sample extraction
        sample = extractor.extract_sample(size=5)
        assert len(sample) <= 5
        
        # Verify sample structure matches expected fields
        if sample:
            person = sample[0]
            required_fields = ['Id', 'FirstName', 'LastName', 'DisplayName']
            for field in required_fields:
                assert field in person, f"Required field {field} missing from person document"
        
        # Test count
        total = extractor.count_total()
        assert total > 0
        print(f"✓ Total persons: {total:,}")
    
    def test_organization_extractor_real_data(self):
        """Test OrganizationExtractor with real data"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        extractor = OrganizationExtractor(client)
        
        # Test sample extraction
        sample = extractor.extract_sample(size=5)
        assert len(sample) <= 5
        
        # Verify sample structure
        if sample:
            org = sample[0]
            required_fields = ['Id', 'NameSwe', 'NameEng', 'DisplayNameSwe', 'DisplayNameEng']
            for field in required_fields:
                assert field in org, f"Required field {field} missing from organization document"
        
        # Test count
        total = extractor.count_total()
        assert total > 0
        print(f"✓ Total organizations: {total:,}")
    
    def test_publication_extractor_real_data(self):
        """Test PublicationExtractor with real data"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        extractor = PublicationExtractor(client)
        
        # Test sample extraction
        sample = extractor.extract_sample(size=5)
        assert len(sample) <= 5
        
        # Verify sample structure
        if sample:
            pub = sample[0]
            # Note: Some fields might be optional, so we check for existence of key identifiers
            assert 'Id' in pub or '_id' in pub, "Publication should have an ID field"
            
            # Check for common publication fields (not all may be present)
            common_fields = ['Title', 'Abstract', 'Year', 'Persons']
            fields_present = [field for field in common_fields if field in pub]
            assert len(fields_present) > 0, "Publication should have at least one common field"
        
        # Test count
        total = extractor.count_total()
        assert total > 0
        print(f"✓ Total publications: {total:,}")
    
    def test_project_extractor_real_data(self):
        """Test ProjectExtractor with real data"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        extractor = ProjectExtractor(client)
        
        # Test sample extraction
        sample = extractor.extract_sample(size=5)
        assert len(sample) <= 5
        
        # Verify sample structure
        if sample:
            project = sample[0]
            # Projects should have either ID or Id field
            assert 'ID' in project or 'Id' in project, "Project should have an ID field"
        
        # Test count
        total = extractor.count_total()
        assert total > 0
        print(f"✓ Total projects: {total:,}")
    
    def test_serial_extractor_real_data(self):
        """Test SerialExtractor with real data"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        extractor = SerialExtractor(client)
        
        # Test sample extraction
        sample = extractor.extract_sample(size=5)
        assert len(sample) <= 5
        
        # Verify sample structure
        if sample:
            serial = sample[0]
            required_fields = ['Id', 'Title']
            for field in required_fields:
                assert field in serial, f"Required field {field} missing from serial document"
        
        # Test count
        total = extractor.count_total()
        assert total > 0
        print(f"✓ Total serials: {total:,}")
    
    def test_relationship_data_extraction(self):
        """Test extraction of relationship data from publications and projects"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        
        # Test authorship relationships from publications
        pub_extractor = PublicationExtractor(client)
        publications = pub_extractor.extract_sample(size=10)
        
        authorship_relationships = 0
        for pub in publications:
            if pub.get('HasPersons') and pub.get('Persons'):
                authorship_relationships += len(pub['Persons'])
        
        print(f"✓ Found {authorship_relationships} authorship relationships in sample")
        
        # Test project participation relationships
        project_extractor = ProjectExtractor(client)
        projects = project_extractor.extract_sample(size=10)
        
        participation_relationships = 0
        for project in projects:
            if project.get('Persons'):
                participation_relationships += len(project['Persons'])
        
        print(f"✓ Found {participation_relationships} project participation relationships in sample")
    
    def test_data_quality_checks(self):
        """Test basic data quality checks"""
        if not all([os.getenv('ES_HOST'), os.getenv('ES_USER'), os.getenv('ES_PASS')]):
            pytest.skip("Elasticsearch credentials not available")
        
        client = ElasticsearchClient()
        
        # Check for persons with valid identifiers
        person_extractor = PersonExtractor(client)
        persons = person_extractor.extract_sample(size=20)
        
        persons_with_identifiers = 0
        for person in persons:
            if person.get('HasIdentifiers') and person.get('Identifiers'):
                persons_with_identifiers += 1
        
        print(f"✓ {persons_with_identifiers}/20 persons have identifiers")
        
        # Check for organizations with geographic data
        org_extractor = OrganizationExtractor(client)
        orgs = org_extractor.extract_sample(size=20)
        
        orgs_with_geo = 0
        for org in orgs:
            if org.get('GeoLat') and org.get('GeoLong'):
                orgs_with_geo += 1
        
        print(f"✓ {orgs_with_geo}/20 organizations have geographic coordinates")
        
        # Check for publications with temporal data
        pub_extractor = PublicationExtractor(client)
        pubs = pub_extractor.extract_sample(size=20)
        
        pubs_with_year = 0
        for pub in pubs:
            if pub.get('Year') and pub['Year'] > 0:
                pubs_with_year += 1
        
        print(f"✓ {pubs_with_year}/20 publications have valid year data")