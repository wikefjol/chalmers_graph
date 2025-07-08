"""
GraphDB compatibility validation for formatted documents.
"""

import json
from typing import Dict, List, Any, Optional, Set


class CompatibilityValidator:
    """Validates formatted documents for GraphDB compatibility."""
    
    # Required fields for each entity type
    REQUIRED_FIELDS = {
        'persons': {'es_id', 'name'},
        'publications': {'es_id', 'title'},
        'projects': {'es_id', 'title'},
        'organizations': {'es_id', 'name'},
        'serials': {'es_id', 'name'}
    }
    
    # Recommended fields for complete functionality
    RECOMMENDED_FIELDS = {
        'persons': {'cpl_id', 'scopus_id', 'first_name', 'last_name'},
        'publications': {'year', 'publication_type', 'doi'},
        'projects': {'start_year', 'end_year', 'description'},
        'organizations': {'organization_type', 'level'},
        'serials': {'serial_type', 'issn'}
    }
    
    # Fields that should be numeric
    NUMERIC_FIELDS = {
        'year', 'start_year', 'end_year', 'order', 'level',
        'publication_year', 'founded_year'
    }
    
    # Fields that should be boolean
    BOOLEAN_FIELDS = {
        'is_active', 'is_peer_reviewed', 'is_open_access'
    }
    
    # Fields that contain JSON strings
    JSON_STRING_FIELDS = {
        'identifiers_json', 'keywords_json', 'categories_json',
        'publication_type_json', 'source_json'
    }
    
    def __init__(self):
        pass
    
    def validate_document(self, doc: Dict[str, Any], entity_type: str) -> Dict[str, Any]:
        """Validate a single formatted document for GraphDB compatibility."""
        validation_result = {
            'entity_type': entity_type,
            'valid': True,
            'warnings': [],
            'errors': [],
            'field_issues': [],
            'summary': {
                'required_fields_present': 0,
                'recommended_fields_present': 0,
                'total_fields': len(doc),
                'property_name_issues': 0,
                'type_issues': 0,
                'json_issues': 0
            }
        }
        
        # Check required fields
        required = self.REQUIRED_FIELDS.get(entity_type, set())
        for field in required:
            if field in doc and doc[field] is not None and doc[field] != '':
                validation_result['summary']['required_fields_present'] += 1
            else:
                validation_result['errors'].append(f"Missing required field: {field}")
                validation_result['valid'] = False
        
        # Check recommended fields
        recommended = self.RECOMMENDED_FIELDS.get(entity_type, set())
        for field in recommended:
            if field in doc and doc[field] is not None and doc[field] != '':
                validation_result['summary']['recommended_fields_present'] += 1
            else:
                validation_result['warnings'].append(f"Missing recommended field: {field}")
        
        # Validate individual fields
        for field_name, value in doc.items():
            field_issues = self._validate_field(field_name, value)
            if field_issues:
                validation_result['field_issues'].extend(field_issues)
                
                # Categorize issues
                for issue in field_issues:
                    if 'property name' in issue['issue'].lower():
                        validation_result['summary']['property_name_issues'] += 1
                    elif 'type' in issue['issue'].lower():
                        validation_result['summary']['type_issues'] += 1
                    elif 'json' in issue['issue'].lower():
                        validation_result['summary']['json_issues'] += 1
                
                # Mark as invalid if there are serious issues
                if any('invalid' in issue['severity'] for issue in field_issues):
                    validation_result['valid'] = False
        
        return validation_result
    
    def _validate_field(self, field_name: str, value: Any) -> List[Dict[str, str]]:
        """Validate a single field."""
        issues = []
        
        # Check property name compatibility
        if not self._is_valid_neo4j_property_name(field_name):
            issues.append({
                'field': field_name,
                'issue': f"Property name '{field_name}' may not be compatible with Neo4j",
                'severity': 'warning'
            })
        
        # Check for None values (should be avoided in Neo4j)
        if value is None:
            issues.append({
                'field': field_name,
                'issue': "None values should be avoided in Neo4j properties",
                'severity': 'warning'
            })
            return issues
        
        # Check expected numeric fields
        if field_name in self.NUMERIC_FIELDS:
            if not isinstance(value, (int, float)):
                try:
                    float(value)
                except (ValueError, TypeError):
                    issues.append({
                        'field': field_name,
                        'issue': f"Expected numeric value, got {type(value).__name__}: {value}",
                        'severity': 'error'
                    })
        
        # Check expected boolean fields
        if field_name in self.BOOLEAN_FIELDS:
            if not isinstance(value, bool):
                issues.append({
                    'field': field_name,
                    'issue': f"Expected boolean value, got {type(value).__name__}: {value}",
                    'severity': 'error'
                })
        
        # Check JSON string fields
        if field_name in self.JSON_STRING_FIELDS:
            if not isinstance(value, str):
                issues.append({
                    'field': field_name,
                    'issue': f"Expected JSON string, got {type(value).__name__}",
                    'severity': 'error'
                })
            else:
                try:
                    json.loads(value)
                except json.JSONDecodeError:
                    issues.append({
                        'field': field_name,
                        'issue': "Invalid JSON string format",
                        'severity': 'error'
                    })
        
        # Check for overly long strings (Neo4j property size limits)
        if isinstance(value, str) and len(value) > 10000:
            issues.append({
                'field': field_name,
                'issue': f"String value very long ({len(value)} chars), may hit Neo4j limits",
                'severity': 'warning'
            })
        
        # Check for complex nested objects (should be JSON strings)
        if isinstance(value, (dict, list)) and field_name not in {'_source', '_id'}:
            issues.append({
                'field': field_name,
                'issue': "Complex objects should be serialized as JSON strings for Neo4j",
                'severity': 'warning'
            })
        
        return issues
    
    def _is_valid_neo4j_property_name(self, name: str) -> bool:
        """Check if a property name is valid for Neo4j."""
        # Neo4j property names should start with letter or underscore
        # and contain only letters, numbers, underscores
        if not name:
            return False
        
        if not (name[0].isalpha() or name[0] == '_'):
            return False
        
        return all(c.isalnum() or c == '_' for c in name)
    
    def validate_batch(self, documents: List[Dict[str, Any]], entity_type: str) -> Dict[str, Any]:
        """Validate a batch of documents."""
        batch_result = {
            'entity_type': entity_type,
            'total_documents': len(documents),
            'valid_documents': 0,
            'invalid_documents': 0,
            'documents_with_warnings': 0,
            'common_issues': {},
            'summary_stats': {
                'avg_required_fields': 0,
                'avg_recommended_fields': 0,
                'avg_total_fields': 0,
                'total_errors': 0,
                'total_warnings': 0
            },
            'detailed_results': []
        }
        
        if not documents:
            return batch_result
        
        # Track common issues
        issue_counts = {}
        total_stats = {
            'required_fields': 0,
            'recommended_fields': 0,
            'total_fields': 0,
            'errors': 0,
            'warnings': 0
        }
        
        for doc in documents:
            validation = self.validate_document(doc, entity_type)
            batch_result['detailed_results'].append(validation)
            
            if validation['valid']:
                batch_result['valid_documents'] += 1
            else:
                batch_result['invalid_documents'] += 1
            
            if validation['warnings']:
                batch_result['documents_with_warnings'] += 1
            
            # Aggregate stats
            total_stats['required_fields'] += validation['summary']['required_fields_present']
            total_stats['recommended_fields'] += validation['summary']['recommended_fields_present']
            total_stats['total_fields'] += validation['summary']['total_fields']
            total_stats['errors'] += len(validation['errors'])
            total_stats['warnings'] += len(validation['warnings'])
            
            # Track common issues
            for error in validation['errors']:
                issue_counts[error] = issue_counts.get(error, 0) + 1
            for warning in validation['warnings']:
                issue_counts[warning] = issue_counts.get(warning, 0) + 1
        
        # Calculate averages
        doc_count = len(documents)
        batch_result['summary_stats'] = {
            'avg_required_fields': round(total_stats['required_fields'] / doc_count, 1),
            'avg_recommended_fields': round(total_stats['recommended_fields'] / doc_count, 1),
            'avg_total_fields': round(total_stats['total_fields'] / doc_count, 1),
            'total_errors': total_stats['errors'],
            'total_warnings': total_stats['warnings']
        }
        
        # Identify most common issues
        sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
        batch_result['common_issues'] = dict(sorted_issues[:10])  # Top 10 issues
        
        return batch_result