"""
Relationship extractors for Neo4j Graph RAG
Extracts relationships from nested Elasticsearch document structures
"""

from typing import Dict, List, Any, Iterator, Tuple
from datetime import datetime
import json


class RelationshipExtractor:
    """Base class for extracting relationships from Elasticsearch documents"""
    
    def __init__(self, documents: Dict[str, List[Dict[str, Any]]]):
        """
        Initialize with document collections
        
        Args:
            documents: Dict mapping document type to list of documents
                      e.g., {'persons': [...], 'publications': [...]}
        """
        self.documents = documents
        self.relationships = {
            'AUTHORED': [],
            'INVOLVED_IN': [],
            'AFFILIATED': [],
            'PARTNER': [],
            'OUTPUT': [],
            'PUBLISHED_IN': [],
            'PARENT_OF': []
        }
    
    def extract_all_relationships(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract all relationships from the documents"""
        print("🔗 Extracting relationships from documents...")
        
        # Extract each relationship type
        self._extract_authored_relationships()
        self._extract_involved_in_relationships()
        self._extract_affiliated_relationships()
        self._extract_partner_relationships()
        self._extract_output_relationships()
        self._extract_published_in_relationships()
        self._extract_parent_of_relationships()
        
        # Print summary
        for rel_type, rels in self.relationships.items():
            if rels:
                print(f"  ✓ {rel_type}: {len(rels)} relationships found")
        
        return self.relationships
    
    def _extract_authored_relationships(self):
        """Extract AUTHORED relationships from Publication.persons"""
        for pub in self.documents.get('publications', []):
            pub_id = pub.get('es_id')
            if not pub_id:
                continue
            
            # Get persons from the publication
            persons = pub.get('persons', [])
            if isinstance(persons, str):
                try:
                    persons = json.loads(persons)
                except:
                    persons = []
            
            for idx, person_data in enumerate(persons):
                if isinstance(person_data, dict):
                    # Person ID is directly in PersonId field
                    person_id = person_data.get('PersonId')
                    if person_id:
                        # Role information is in the Role object
                        role = person_data.get('Role', {})
                        rel = {
                            'source_id': person_id,
                            'target_id': pub_id,
                            'properties': {
                                'order': person_data.get('Order', idx),
                                'role_id': role.get('Id', ''),
                                'role_name_swe': role.get('NameSwe', ''),
                                'role_name_eng': role.get('NameEng', '')
                            }
                        }
                        self.relationships['AUTHORED'].append(rel)
    
    def _extract_involved_in_relationships(self):
        """Extract INVOLVED_IN relationships from Project.persons"""
        for project in self.documents.get('projects', []):
            project_id = project.get('es_id')
            if not project_id:
                continue
            
            # Get persons from the project
            persons = project.get('persons', [])
            if isinstance(persons, str):
                try:
                    persons = json.loads(persons)
                except:
                    persons = []
            
            for person_data in persons:
                if isinstance(person_data, dict):
                    # Person ID is in PersonID field (uppercase ID)
                    person_id = person_data.get('PersonID')
                    if person_id:
                        rel = {
                            'source_id': person_id,
                            'target_id': project_id,
                            'properties': {
                                'role_id': person_data.get('PersonRoleID', ''),
                                'role_name_swe': person_data.get('PersonRoleName_sv', ''),
                                'role_name_eng': person_data.get('PersonRoleName_en', ''),
                                'organization_id': person_data.get('OrganizationID', '')
                            }
                        }
                        self.relationships['INVOLVED_IN'].append(rel)
    
    def _extract_affiliated_relationships(self):
        """Extract AFFILIATED relationships from Person.organization_home"""
        for person in self.documents.get('persons', []):
            person_id = person.get('es_id')
            if not person_id:
                continue
            
            # Get organization_home from the person
            org_home = person.get('organization_home', [])
            if isinstance(org_home, str):
                try:
                    org_home = json.loads(org_home)
                except:
                    org_home = []
            
            for org_data in org_home:
                if isinstance(org_data, dict):
                    org_id = org_data.get('OrganizationId') or org_data.get('organization_id')
                    if org_id:
                        rel = {
                            'source_id': person_id,
                            'target_id': org_id,
                            'properties': {
                                'role': org_data.get('Role', ''),
                                'start_date': org_data.get('StartDate', ''),
                                'end_date': org_data.get('EndDate', '')
                            }
                        }
                        self.relationships['AFFILIATED'].append(rel)
    
    def _extract_partner_relationships(self):
        """Extract PARTNER relationships from Project.organizations"""
        for project in self.documents.get('projects', []):
            project_id = project.get('es_id')
            if not project_id:
                continue
            
            # Get organizations from the project
            organizations = project.get('organizations', [])
            if isinstance(organizations, str):
                try:
                    organizations = json.loads(organizations)
                except:
                    organizations = []
            
            for org_data in organizations:
                if isinstance(org_data, dict):
                    # Organization ID is in OrganizationID field
                    org_id = org_data.get('OrganizationID')
                    if org_id:
                        rel = {
                            'source_id': org_id,
                            'target_id': project_id,
                            'properties': {
                                'role_id': org_data.get('OrganizationRoleID', ''),
                                'role_name_swe': org_data.get('OrganizationRoleNameSv', ''),
                                'role_name_eng': org_data.get('OrganizationRoleNameEn', '')
                            }
                        }
                        self.relationships['PARTNER'].append(rel)
    
    def _extract_output_relationships(self):
        """Extract OUTPUT relationships from Publication.project"""
        for pub in self.documents.get('publications', []):
            pub_id = pub.get('es_id')
            if not pub_id:
                continue
            
            # Get project from the publication
            project_data = pub.get('project')
            if isinstance(project_data, str):
                try:
                    project_data = json.loads(project_data)
                except:
                    project_data = None
            
            if isinstance(project_data, dict):
                project_id = project_data.get('ProjectId') or project_data.get('project_id')
                if project_id:
                    rel = {
                        'source_id': str(project_id),
                        'target_id': pub_id,
                        'properties': {}
                    }
                    self.relationships['OUTPUT'].append(rel)
    
    def _extract_published_in_relationships(self):
        """Extract PUBLISHED_IN relationships from Publication.series"""
        for pub in self.documents.get('publications', []):
            pub_id = pub.get('es_id')
            if not pub_id:
                continue
            
            # Get series from the publication
            series = pub.get('series')
            if isinstance(series, str):
                try:
                    series = json.loads(series)
                except:
                    series = None
            
            # Series is a list of series items
            if isinstance(series, list):
                for series_item in series:
                    if isinstance(series_item, dict):
                        serial_data = series_item.get('SerialItem', {})
                        serial_id = serial_data.get('Id')
                        if serial_id:
                            rel = {
                                'source_id': pub_id,
                                'target_id': serial_id,
                                'properties': {
                                    'serial_number': series_item.get('SerialNumber', '')
                                }
                            }
                            self.relationships['PUBLISHED_IN'].append(rel)
    
    def _extract_parent_of_relationships(self):
        """Extract PARENT_OF relationships from Organization.organization_parents"""
        for org in self.documents.get('organizations', []):
            org_id = org.get('es_id')
            if not org_id:
                continue
            
            # Get organization_parents
            org_parents = org.get('organization_parents', [])
            if isinstance(org_parents, str):
                try:
                    org_parents = json.loads(org_parents)
                except:
                    org_parents = []
            
            for parent_data in org_parents:
                if isinstance(parent_data, dict):
                    parent_id = parent_data.get('OrganizationId') or parent_data.get('organization_id')
                    if parent_id:
                        rel = {
                            'source_id': parent_id,
                            'target_id': org_id,
                            'properties': {
                                'level': parent_data.get('Level', 0)
                            }
                        }
                        self.relationships['PARENT_OF'].append(rel)

class StreamingRelationshipExtractor:
    def __init__(self, neo4j_conn, es_client, batch_size: int = 1000):
        self.neo4j_conn = neo4j_conn
        self.es_client = es_client
        self.batch_size = batch_size
    
    def extract_relationships_stream(self):
        """
        Extract relationships in streaming fashion
        """
        # Process each entity type that has relationships
        relationship_sources = {
            'publications': ['persons', 'organizations', 'project', 'series'],
            'projects': ['persons', 'organizations'],
            'persons': ['organization_home'],
            'organizations': ['organization_parents']
        }
        
        for source_type, rel_types in relationship_sources.items():
            self._process_entity_relationships(source_type, rel_types)
    
    def _process_entity_relationships(self, source_type: str, rel_types: List[str]):
        """Process relationships for a specific entity type"""
        extractor = self._get_extractor(source_type)
        
        for batch in extractor.extract_batches():
            relationships = []
            
            for doc in batch:
                # Extract relationships from this document
                doc_relationships = self._extract_doc_relationships(
                    doc, source_type, rel_types
                )
                relationships.extend(doc_relationships)
            
            # Import relationships batch
            if relationships:
                self._import_relationship_batch(relationships)