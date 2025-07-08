#!/usr/bin/env python3
"""
Test runner and demonstration script for Graph RAG implementation
"""

import sys
import os
sys.path.append('src')

from es_client.client import ElasticsearchClient
from es_client.extractors import (
    PersonExtractor,
    OrganizationExtractor,
    PublicationExtractor,
    ProjectExtractor,
    SerialExtractor
)

def demonstrate_elasticsearch_connectivity():
    """Demonstrate Elasticsearch connectivity and data extraction"""
    print("🔌 Testing Elasticsearch Connectivity")
    print("=" * 50)
    
    try:
        # Initialize client
        client = ElasticsearchClient()
        
        # Test connection
        if not client.test_connection():
            print("❌ Failed to connect to Elasticsearch")
            return False
        
        print("\n📊 Index Statistics:")
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
                print(f"  📄 {index}: {count:,} documents")
            except Exception as e:
                print(f"  ❌ {index}: Error - {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def demonstrate_data_extractors():
    """Demonstrate data extractors with sample data"""
    print("\n🔍 Testing Data Extractors")
    print("=" * 50)
    
    try:
        client = ElasticsearchClient()
        
        # Test Person Extractor
        print("\n👥 Person Extractor Sample:")
        person_extractor = PersonExtractor(client)
        persons = person_extractor.extract_sample(size=3)
        
        for i, person in enumerate(persons, 1):
            name = person.get('DisplayName', 'Unknown')
            identifiers = len(person.get('Identifiers', []))
            active = "✓" if person.get('IsActive') else "✗"
            print(f"  {i}. {name} | {identifiers} identifiers | Active: {active}")
        
        # Test Organization Extractor
        print("\n🏢 Organization Extractor Sample:")
        org_extractor = OrganizationExtractor(client)
        orgs = org_extractor.extract_sample(size=3)
        
        for i, org in enumerate(orgs, 1):
            name = org.get('DisplayNameEng', 'Unknown')
            country = org.get('Country', 'Unknown')
            active = "✓" if org.get('IsActive') else "✗"
            print(f"  {i}. {name} | {country} | Active: {active}")
        
        # Test Publication Extractor
        print("\n📄 Publication Extractor Sample:")
        pub_extractor = PublicationExtractor(client)
        pubs = pub_extractor.extract_sample(size=3)
        
        for i, pub in enumerate(pubs, 1):
            title = pub.get('Title', 'Unknown')[:50] + "..."
            year = pub.get('Year', 'Unknown')
            authors = len(pub.get('Persons', []))
            print(f"  {i}. {title} | {year} | {authors} authors")
        
        # Test Project Extractor  
        print("\n🔬 Project Extractor Sample:")
        project_extractor = ProjectExtractor(client)
        projects = project_extractor.extract_sample(size=3)
        
        for i, project in enumerate(projects, 1):
            title = project.get('ProjectTitleEng', project.get('ProjectTitleSwe', 'Unknown'))[:50] + "..."
            start_date = project.get('StartDate', 'Unknown')
            participants = len(project.get('Persons', []))
            print(f"  {i}. {title} | Start: {start_date} | {participants} participants")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def demonstrate_relationship_extraction():
    """Demonstrate relationship data extraction"""
    print("\n🔗 Testing Relationship Extraction")
    print("=" * 50)
    
    try:
        client = ElasticsearchClient()
        
        # Extract authorship relationships
        print("\n📚 Authorship Relationships:")
        pub_extractor = PublicationExtractor(client)
        pubs = pub_extractor.extract_sample(size=2)
        
        for pub in pubs:
            title = pub.get('Title', 'Unknown')[:40] + "..."
            print(f"\n  Publication: {title}")
            
            if pub.get('HasPersons') and pub.get('Persons'):
                for person_rel in pub['Persons'][:3]:  # Show first 3 authors
                    person_data = person_rel.get('PersonData', {})
                    role = person_rel.get('Role', {})
                    order = person_rel.get('Order', 'N/A')
                    
                    name = person_data.get('DisplayName', 'Unknown')
                    role_name = role.get('NameEng', 'Unknown')
                    
                    print(f"    {order}. {name} ({role_name})")
            else:
                print("    No authors found")
        
        # Extract project participation relationships
        print("\n🔬 Project Participation Relationships:")
        project_extractor = ProjectExtractor(client)
        projects = project_extractor.extract_sample(size=2)
        
        for project in projects:
            title = project.get('ProjectTitleEng', project.get('ProjectTitleSwe', 'Unknown'))[:40] + "..."
            print(f"\n  Project: {title}")
            
            if project.get('Persons'):
                for person_rel in project['Persons'][:3]:  # Show first 3 participants
                    person = person_rel.get('Person', {})
                    role_name = person_rel.get('PersonRoleName_en', 'Unknown Role')
                    
                    name = person.get('DisplayName', 'Unknown')
                    print(f"    • {name} ({role_name})")
            else:
                print("    No participants found")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main demonstration function"""
    print("🧪 Graph RAG Protocol-Based Implementation Demo")
    print("=" * 60)
    
    success = True
    
    # Test Elasticsearch connectivity
    success &= demonstrate_elasticsearch_connectivity()
    
    # Test data extractors
    if success:
        success &= demonstrate_data_extractors()
    
    # Test relationship extraction
    if success:
        success &= demonstrate_relationship_extraction()
    
    if success:
        print("\n✅ All demonstrations completed successfully!")
        print("\n📝 Next Steps:")
        print("  1. Implement concrete model classes following protocols")
        print("  2. Add data validation and transformation")
        print("  3. Integrate with Neo4j for graph database")
        print("  4. Add embedding generation and RAG functionality")
    else:
        print("\n❌ Some tests failed. Check your configuration.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)