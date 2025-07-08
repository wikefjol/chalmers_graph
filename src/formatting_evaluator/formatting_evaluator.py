"""
Main formatting evaluation orchestrator.
"""

import json
from typing import Dict, List, Any, Optional
from es_client.client import ElasticsearchClient
from .sample_extractor import SampleExtractor
from .document_formatter import DocumentFormatter


class FormattingEvaluator:
    """Main orchestrator for formatting evaluation workflow."""
    
    def __init__(self, es_client: Optional[ElasticsearchClient] = None):
        self.es_client = es_client
        self.sample_extractor = SampleExtractor(es_client) if es_client else None
        self.document_formatter = DocumentFormatter()
    
    def evaluate_entity_type(self, entity_type: str, sample_count: int = 10, 
                           detailed: bool = False, use_cache: bool = True) -> Dict[str, Any]:
        """Complete evaluation of formatting for an entity type."""
        
        # Get samples
        if use_cache and self.sample_extractor:
            samples = self.sample_extractor.get_cached_samples(entity_type)
            if not samples or len(samples) < sample_count:
                if self.es_client:
                    samples = self.sample_extractor.extract_samples(
                        entity_type, sample_count, force_refresh=True
                    )
                else:
                    return {'error': 'No cached samples available and no ES client provided'}
        elif self.sample_extractor and self.es_client:
            samples = self.sample_extractor.extract_samples(entity_type, sample_count)
        else:
            return {'error': 'Cannot extract samples: no ES client provided'}
        
        if not samples:
            return {'error': f'No samples available for {entity_type}'}
        
        # Format documents and get analysis
        format_report = self.document_formatter.format_evaluation_report(
            entity_type, samples[:sample_count], detailed=detailed
        )
        
        # Validate formatted documents for GraphDB compatibility
        formatted_docs = []
        if detailed and format_report.get('detailed_results'):
            for result in format_report['detailed_results']:
                if 'formatted_document' in result:
                    formatted_docs.append(result['formatted_document'])
        else:
            # Format just for validation
            for sample in samples[:sample_count]:
                try:
                    formatted, _ = self.document_formatter.format_document(
                        sample['source'], entity_type
                    )
                    formatted_docs.append(formatted)
                except Exception:
                    continue
        
        validation_report = self.compatibility_validator.validate_batch(
            formatted_docs, entity_type
        )
        
        # Combine results
        evaluation_result = {
            'entity_type': entity_type,
            'evaluation_summary': {
                'samples_processed': len(samples),
                'formatting_errors': format_report['summary']['errors'],
                'avg_format_time_ms': format_report['summary']['avg_format_time_ms'],
                'validation_pass_rate': (
                    validation_report['valid_documents'] / validation_report['total_documents']
                    if validation_report['total_documents'] > 0 else 0
                ),
                'documents_with_warnings': validation_report['documents_with_warnings']
            },
            'formatting_report': format_report,
            'validation_report': validation_report
        }
        
        if detailed:
            evaluation_result['sample_comparisons'] = []
            for i, sample in enumerate(samples[:sample_count]):
                try:
                    formatted, analysis = self.document_formatter.format_document(
                        sample['source'], entity_type
                    )
                    comparison = self.document_formatter.compare_documents(
                        sample['source'], formatted, ("Original", "Formatted")
                    )
                    evaluation_result['sample_comparisons'].append({
                        'sample_id': sample['id'],
                        'comparison': comparison,
                        'analysis': analysis
                    })
                except Exception as e:
                    evaluation_result['sample_comparisons'].append({
                        'sample_id': sample['id'],
                        'error': str(e)
                    })
        
        return evaluation_result
    
    def evaluate_all_types(self, sample_count: int = 10, detailed: bool = False) -> Dict[str, Any]:
        """Evaluate all entity types."""
        entity_types = ['persons', 'publications', 'projects', 'organizations', 'serials']
        
        results = {
            'overall_summary': {
                'total_entity_types': len(entity_types),
                'successful_evaluations': 0,
                'failed_evaluations': 0,
                'avg_validation_pass_rate': 0,
                'total_samples_processed': 0
            },
            'entity_results': {}
        }
        
        total_pass_rate = 0
        successful_count = 0
        
        for entity_type in entity_types:
            try:
                result = self.evaluate_entity_type(
                    entity_type, sample_count, detailed=detailed
                )
                
                if 'error' not in result:
                    results['entity_results'][entity_type] = result
                    results['overall_summary']['successful_evaluations'] += 1
                    results['overall_summary']['total_samples_processed'] += (
                        result['evaluation_summary']['samples_processed']
                    )
                    total_pass_rate += result['evaluation_summary']['validation_pass_rate']
                    successful_count += 1
                else:
                    results['entity_results'][entity_type] = result
                    results['overall_summary']['failed_evaluations'] += 1
                    
            except Exception as e:
                results['entity_results'][entity_type] = {'error': str(e)}
                results['overall_summary']['failed_evaluations'] += 1
        
        if successful_count > 0:
            results['overall_summary']['avg_validation_pass_rate'] = (
                total_pass_rate / successful_count
            )
        
        return results
    
    def quick_test_single_sample(self, entity_type: str, sample_index: int = 0) -> Dict[str, Any]:
        """Quick test of a single sample for development workflow."""
        
        # Get cached sample
        if not self.sample_extractor:
            return {'error': 'No sample extractor available'}
            
        samples = self.sample_extractor.get_cached_samples(entity_type)
        if not samples or len(samples) <= sample_index:
            return {'error': f'No cached sample at index {sample_index} for {entity_type}'}
        
        sample = samples[sample_index]
        
        try:
            # Format the document
            formatted, analysis = self.document_formatter.format_document(
                sample['source'], entity_type
            )
            
            # Compare original vs formatted
            comparison = self.document_formatter.compare_documents(
                sample['source'], formatted
            )
            
            return {
                'sample_id': sample['id'],
                'entity_type': entity_type,
                'original_document': sample['source'],
                'formatted_document': formatted,
                'formatting_analysis': analysis,
                'comparison': comparison,
                'summary': {
                    'format_time_ms': analysis['format_time_ms'],
                    'fields_changed': len(analysis['transformed_fields']),
                    'added_fields': len(analysis['added_fields']),
                    'removed_fields': len(analysis['removed_fields'])
                }
            }
            
        except Exception as e:
            return {
                'sample_id': sample['id'],
                'entity_type': entity_type,
                'error': str(e)
            }