"""
Document formatting evaluation and comparison.
"""

import json
import time
from typing import Dict, List, Any, Tuple, Optional

from graph_db.streaming_importer import StreamingImportPipeline


class DocumentFormatter:
    """Evaluates document formatting transformations using actual pipeline formatters."""
    
    def __init__(self):
        # Create a minimal streaming pipeline instance for formatting functions
        self.pipeline = StreamingImportPipeline(connection=None, batch_size=100)
    
    def format_document(self, doc: Dict[str, Any], entity_type: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Format a document and return both formatted result and transformation analysis."""
        start_time = time.time()
        
        # Use the actual formatting methods from streaming pipeline
        if entity_type == 'persons':
            formatted = self.pipeline._format_person_document(doc)
        elif entity_type == 'publications':
            formatted = self.pipeline._format_publication_document(doc)
        elif entity_type == 'projects':
            formatted = self.pipeline._format_project_document(doc)
        elif entity_type == 'organizations':
            formatted = self.pipeline._format_organization_document(doc)
        elif entity_type == 'serials':
            formatted = self.pipeline._format_serial_document(doc)
        else:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        format_time = time.time() - start_time
        
        # Analyze transformations
        analysis = self._analyze_transformations(doc, formatted, format_time)
        
        return formatted, analysis
    
    def _analyze_transformations(self, original: Dict[str, Any], formatted: Dict[str, Any], format_time: float) -> Dict[str, Any]:
        """Analyze what transformations occurred during formatting."""
        original_fields = set(original.keys())
        formatted_fields = set(formatted.keys())
        
        added_fields = formatted_fields - original_fields
        removed_fields = original_fields - formatted_fields
        common_fields = original_fields & formatted_fields
        
        transformed_fields = []
        type_changes = []
        
        for field in common_fields:
            orig_val = original[field]
            form_val = formatted[field]
            
            if orig_val != form_val:
                transformed_fields.append({
                    'field': field,
                    'original': self._safe_repr(orig_val),
                    'formatted': self._safe_repr(form_val),
                    'original_type': type(orig_val).__name__,
                    'formatted_type': type(form_val).__name__
                })
                
                if type(orig_val) != type(form_val):
                    type_changes.append({
                        'field': field,
                        'from_type': type(orig_val).__name__,
                        'to_type': type(form_val).__name__
                    })
        
        return {
            'format_time_ms': round(format_time * 1000, 2),
            'field_counts': {
                'original': len(original_fields),
                'formatted': len(formatted_fields),
                'added': len(added_fields),
                'removed': len(removed_fields),
                'transformed': len(transformed_fields)
            },
            'added_fields': list(added_fields),
            'removed_fields': list(removed_fields),
            'transformed_fields': transformed_fields,
            'type_changes': type_changes
        }
    
    def _safe_repr(self, value: Any, max_length: int = 100) -> str:
        """Safe string representation of a value with length limit."""
        try:
            if isinstance(value, (dict, list)):
                repr_str = json.dumps(value, ensure_ascii=False)
            else:
                repr_str = str(value)
            
            if len(repr_str) > max_length:
                return repr_str[:max_length] + "..."
            return repr_str
        except:
            return f"<{type(value).__name__}>"
    
    def compare_documents(self, doc1: Dict[str, Any], doc2: Dict[str, Any], labels: Tuple[str, str] = ("Original", "Formatted")) -> Dict[str, Any]:
        """Compare two documents side by side."""
        all_fields = set(doc1.keys()) | set(doc2.keys())
        
        comparison = {
            'summary': {
                'total_fields': len(all_fields),
                labels[0].lower() + '_only': len(set(doc1.keys()) - set(doc2.keys())),
                labels[1].lower() + '_only': len(set(doc2.keys()) - set(doc1.keys())),
                'different_values': 0,
                'identical_values': 0
            },
            'field_comparison': []
        }
        
        for field in sorted(all_fields):
            val1 = doc1.get(field, '<MISSING>')
            val2 = doc2.get(field, '<MISSING>')
            
            identical = val1 == val2
            if identical and val1 != '<MISSING>':
                comparison['summary']['identical_values'] += 1
            elif not identical and val1 != '<MISSING>' and val2 != '<MISSING>':
                comparison['summary']['different_values'] += 1
            
            comparison['field_comparison'].append({
                'field': field,
                'identical': identical,
                labels[0]: self._safe_repr(val1),
                labels[1]: self._safe_repr(val2),
                'types': [type(val1).__name__ if val1 != '<MISSING>' else 'missing',
                         type(val2).__name__ if val2 != '<MISSING>' else 'missing']
            })
        
        return comparison
    
    def format_evaluation_report(self, entity_type: str, samples: List[Dict[str, Any]], detailed: bool = False) -> Dict[str, Any]:
        """Generate a comprehensive formatting evaluation report."""
        if not samples:
            return {'error': 'No samples provided'}
        
        report = {
            'entity_type': entity_type,
            'sample_count': len(samples),
            'evaluation_timestamp': time.time(),
            'summary': {
                'total_format_time_ms': 0,
                'avg_format_time_ms': 0,
                'errors': 0,
                'field_stats': {
                    'avg_original_fields': 0,
                    'avg_formatted_fields': 0,
                    'avg_added_fields': 0,
                    'avg_removed_fields': 0,
                    'avg_transformed_fields': 0
                }
            },
            'detailed_results': [] if detailed else None
        }
        
        total_stats = {'format_time': 0, 'errors': 0, 'field_counts': {}}
        field_count_keys = ['original', 'formatted', 'added', 'removed', 'transformed']
        for key in field_count_keys:
            total_stats['field_counts'][key] = 0
        
        for i, sample in enumerate(samples):
            try:
                formatted, analysis = self.format_document(sample['source'], entity_type)
                
                total_stats['format_time'] += analysis['format_time_ms']
                for key in field_count_keys:
                    total_stats['field_counts'][key] += analysis['field_counts'][key]
                
                if detailed:
                    report['detailed_results'].append({
                        'sample_id': sample['id'],
                        'formatted_document': formatted,
                        'analysis': analysis
                    })
                    
            except Exception as e:
                total_stats['errors'] += 1
                if detailed:
                    report['detailed_results'].append({
                        'sample_id': sample['id'],
                        'error': str(e)
                    })
        
        # Calculate averages
        successful_samples = len(samples) - total_stats['errors']
        if successful_samples > 0:
            report['summary']['avg_format_time_ms'] = round(total_stats['format_time'] / successful_samples, 2)
            for key in field_count_keys:
                report['summary']['field_stats'][f'avg_{key}_fields'] = round(
                    total_stats['field_counts'][key] / successful_samples, 1
                )
        
        report['summary']['total_format_time_ms'] = round(total_stats['format_time'], 2)
        report['summary']['errors'] = total_stats['errors']
        
        return report