#!/usr/bin/env python3
"""
Command Line Interface for Neo4j Graph RAG Database Operations
"""

import sys
import argparse
import json
import time
from typing import Dict, Any
sys.path.append('src')

from graph_db.connection import Neo4jConnection
from graph_db.schema import SchemaManager
from graph_db.db_manager import DatabaseManager
from graph_db.importer import ImportPipeline
from es_client.base_extractor import BaseStreamingExtractor
from es_client.client import ElasticsearchClient
from es_client.extractors import (
    PersonExtractor, OrganizationExtractor, PublicationExtractor,
    ProjectExtractor, SerialExtractor
)
from es_client.relationship_extractors import RelationshipExtractor
from formatting_evaluator.formatting_evaluator import FormattingEvaluator

class Neo4jCLI:
    """Command line interface for Neo4j operations"""
    
    def __init__(self):
        self.neo4j_conn = None
        self.es_client = None
        self.schema_manager = None
        self.db_manager = None
        self.import_pipeline = None
        self.formatting_evaluator = None
    
    def _initialize_connections(self):
        """Initialize database connections"""
        try:
            self.neo4j_conn = Neo4jConnection()
            self.es_client = ElasticsearchClient()
            self.schema_manager = SchemaManager(self.neo4j_conn)
            self.db_manager = DatabaseManager(self.neo4j_conn)
            self.import_pipeline = ImportPipeline(self.neo4j_conn)
            self.formatting_evaluator = FormattingEvaluator(self.es_client)
            return True
        except Exception as e:
            print(f"❌ Failed to initialize connections: {e}")
            return False
    
    def cmd_test_connections(self, args):
        """Test Neo4j and Elasticsearch connections"""
        print("🔌 Testing Database Connections")
        print("=" * 40)
        
        # Test Neo4j
        print("🗄️ Testing Neo4j connection...")
        if self.neo4j_conn.test_connection():
            print("  ✅ Neo4j connection successful")
        else:
            print("  ❌ Neo4j connection failed")
            return False
        
        # Test Elasticsearch
        print("\n🔍 Testing Elasticsearch connection...")
        if self.es_client.test_connection():
            print("  ✅ Elasticsearch connection successful")
            # Add scan_documents check
            print("\n  Testing scroll API...")
            try:
                # Test with a small batch
                test_extractor = PersonExtractor(self.es_client, batch_size=10)
                batch_count = 0
                for batch in test_extractor.extract_batches():
                    batch_count += 1
                    if batch_count >= 1:  # Just test one batch
                        break
                print("  ✅ Scroll API working correctly")
            except Exception as e:
                print(f"  ⚠️ Scroll API test failed: {e}")
        else:
            print("  ❌ Elasticsearch connection failed")
            return False
        
        print("\n✅ All connections successful!")
        return True
    
    def cmd_schema_setup(self, args):
        """Set up Neo4j schema (constraints and indexes)"""
        print("🏗️ Setting up Neo4j Schema")
        print("=" * 30)
        
        success = self.schema_manager.setup_schema()
        if success:
            print("\n📋 Schema Information:")
            schema_info = self.schema_manager.get_schema_info()
            print(f"  Constraints: {len(schema_info['constraints'])}")
            print(f"  Indexes: {len(schema_info['indexes'])}")
            print(f"  Node Labels: {len(schema_info['node_labels'])}")
        
        return success
    
    def cmd_schema_reset(self, args):
        """Reset Neo4j schema (drop and recreate)"""
        print("🔄 Resetting Neo4j Schema")
        print("=" * 30)
        
        return self.schema_manager.reset_schema()
    
    def cmd_schema_info(self, args):
        """Show current schema information"""
        print("📋 Neo4j Schema Information")
        print("=" * 30)
        
        schema_info = self.schema_manager.get_schema_info()
        
        print(f"\n🔒 Constraints ({len(schema_info['constraints'])}):")
        for constraint in schema_info['constraints']:
            name = constraint.get('name', 'unnamed')
            label = constraint.get('labelsOrTypes', [''])[0]
            props = constraint.get('properties', [])
            print(f"  • {name}: {label}.{','.join(props)}")
        
        print(f"\n🔍 Indexes ({len(schema_info['indexes'])}):")
        for index in schema_info['indexes']:
            name = index.get('name', 'unnamed')
            labels = index.get('labelsOrTypes', [])
            props = index.get('properties', [])
            # Ensure labels and props are lists
            if not isinstance(labels, list):
                labels = [str(labels)] if labels else ['']
            if not isinstance(props, list):
                props = [str(props)] if props else ['']
            print(f"  • {name}: {','.join(labels)}.{','.join(props)}")
        
        print(f"\n🏷️ Node Labels ({len(schema_info['node_labels'])}):")
        for label in schema_info['node_labels']:
            print(f"  • {label}")
        
        print(f"\n🔗 Relationship Types ({len(schema_info['relationship_types'])}):")
        for rel_type in schema_info['relationship_types']:
            print(f"  • {rel_type}")
        
        return True
    
    def cmd_db_stats(self, args):
        """Show database statistics"""
        print("📊 Database Statistics")
        print("=" * 25)
        
        self.db_manager.print_database_stats()
        return True
    
    def cmd_clear_db(self, args):
        """Clear database with safety checks"""
        print("🗑️ Database Clearing")
        print("=" * 20)
        
        if args.force:
            return self.db_manager.clear_database_safe(confirmation=True)
        else:
            return self.db_manager.clear_database_safe(confirmation=False)
    
    def cmd_clear_by_type(self, args):
        """Clear specific node types"""
        labels = args.labels.split(',')
        return self.db_manager.clear_database_by_label(labels, confirmation=args.force)
    
    def cmd_validate_data(self, args):
        """Validate data consistency"""
        print("🔍 Data Validation")
        print("=" * 20)
        
        result = self.db_manager.validate_data_consistency()
        
        if result['status'] == 'clean':
            print("✅ Data validation passed - no issues found")
        else:
            print(f"⚠️ Found {len(result['issues'])} issues:")
            for issue in result['issues']:
                if 'error' in issue:
                    print(f"  ❌ {issue['check']}: {issue['error']}")
                else:
                    print(f"  ⚠️ {issue['check']}: {issue['count']} items")
        
        return result['status'] == 'clean'
    
    def cmd_import_sample(self, args):
        """Import sample data using streaming pipeline"""
        print("🚀 Sample Data Import (Streaming Mode)")
        print("=" * 35)
        print(f"  Sample size: {args.size}")
        print(f"  Batch size: {args.batch_size}")
        
        # Check if we have streaming support
        if not hasattr(self.es_client, 'scan_documents'):
            print("\n⚠️ ERROR: Streaming not available - scan_documents method missing")
            print("  Please update ElasticsearchClient with scroll API support")
            return False
        
        # Prepare streaming extractors
        extractors = self._prepare_streaming_extractors(
            sample_mode=True,
            sample_size=args.size,
            batch_size=args.batch_size
        )
        
        if not extractors:
            print("❌ Failed to prepare streaming extractors")
            return False
        
        # Use streaming import pipeline
        from graph_db.streaming_importer import StreamingImportPipeline
        
        streaming_pipeline = StreamingImportPipeline(
            self.neo4j_conn, 
            batch_size=args.batch_size
        )
        
        return streaming_pipeline.run_streaming_import(
            extractors,
            enable_relationships=True,
            sample_mode=True,
            sample_size=args.size
        )
    
    def cmd_import_full(self, args):
        """Import full dataset using streaming pipeline"""
        print("🚀 Full Data Import (Streaming Mode)")
        print("=" * 35)
        print(f"  Batch size: {args.batch_size}")
        if args.skip_relationships:
            print("  ⚠️  Skipping relationship import")
        
        # Check if we have streaming support
        if not hasattr(self.es_client, 'scan_documents'):
            print("\n⚠️ ERROR: Streaming not available - scan_documents method missing")
            print("  Please update ElasticsearchClient with scroll API support")
            return False
        
        # Prepare streaming extractors
        entity_types = None
        if hasattr(args, 'entity_types') and args.entity_types:
            entity_types = [t.strip() for t in args.entity_types.split(',')]
            print(f"  Entity types: {', '.join(entity_types)}")
        
        extractors = self._prepare_streaming_extractors(
            sample_mode=False,
            batch_size=args.batch_size,
            entity_types=entity_types
        )
        
        if not extractors:
            print("❌ Failed to prepare streaming extractors")
            return False
        
        # Use streaming import pipeline
        from graph_db.streaming_importer import StreamingImportPipeline
        
        streaming_pipeline = StreamingImportPipeline(
            self.neo4j_conn, 
            batch_size=args.batch_size
        )
        
        return streaming_pipeline.run_streaming_import(
            extractors,
            enable_relationships=not args.skip_relationships,
            sample_mode=False
        )
    
    def cmd_format_evaluate(self, args):
        """Evaluate document formatting"""
        print(f"📊 EVALUATING FORMATTING: {args.entity_type}")
        print("=" * 50)
        
        try:
            if args.entity_type == 'all':
                result = self.formatting_evaluator.evaluate_all_types(
                    sample_count=args.samples,
                    detailed=args.detailed
                )
                
                # Print summary
                summary = result['overall_summary']
                print(f"\n📈 OVERALL RESULTS:")
                print(f"  Entity types evaluated: {summary['successful_evaluations']}/{summary['total_entity_types']}")
                print(f"  Total samples processed: {summary['total_samples_processed']}")
                print(f"  Average validation pass rate: {summary['avg_validation_pass_rate']:.1%}")
                
                if args.detailed:
                    print(f"\n📋 DETAILED RESULTS:")
                    for entity_type, entity_result in result['entity_results'].items():
                        if 'error' not in entity_result:
                            eval_summary = entity_result['evaluation_summary']
                            print(f"\n  {entity_type.title()}:")
                            print(f"    Samples: {eval_summary['samples_processed']}")
                            print(f"    Format time: {eval_summary['avg_format_time_ms']:.2f}ms avg")
                            print(f"    Validation: {eval_summary['validation_pass_rate']:.1%} pass rate")
                        else:
                            print(f"\n  {entity_type.title()}: ERROR - {entity_result['error']}")
            else:
                result = self.formatting_evaluator.evaluate_entity_type(
                    entity_type=args.entity_type,
                    sample_count=args.samples,
                    detailed=args.detailed,
                    use_cache=not args.force_refresh
                )
                
                if 'error' in result:
                    print(f"❌ Error: {result['error']}")
                    return False
                
                # Print summary
                eval_summary = result['evaluation_summary']
                print(f"\n📈 RESULTS for {args.entity_type}:")
                print(f"  Samples processed: {eval_summary['samples_processed']}")
                print(f"  Formatting errors: {eval_summary['formatting_errors']}")
                print(f"  Average format time: {eval_summary['avg_format_time_ms']:.2f}ms")
                print(f"  Validation pass rate: {eval_summary['validation_pass_rate']:.1%}")
                print(f"  Documents with warnings: {eval_summary['documents_with_warnings']}")
                
                # Show validation details
                validation = result['validation_report']
                print(f"\n🔍 VALIDATION DETAILS:")
                print(f"  Valid documents: {validation['valid_documents']}/{validation['total_documents']}")
                if validation['common_issues']:
                    print(f"  Common issues:")
                    for issue, count in list(validation['common_issues'].items())[:3]:
                        print(f"    - {issue} ({count} occurrences)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error during evaluation: {e}")
            return False
    
    def cmd_format_test(self, args):
        """Test formatter with a single sample"""
        print(f"🧪 TESTING FORMATTER: {args.entity_type}")
        print("=" * 50)
        
        try:
            result = self.formatting_evaluator.quick_test_single_sample(args.entity_type, args.index)
            
            if 'error' in result:
                print(f"❌ Error: {result['error']}")
                return False
            
            # Print compact results
            summary = result['summary']
            print(f"\n✅ FORMATTING RESULTS:")
            print(f"  Sample ID: {result['sample_id']}")
            print(f"  Format time: {summary['format_time_ms']:.2f}ms")
            print(f"  Fields changed: {summary['fields_changed']}")
            print(f"  Fields added: {summary['added_fields']}")
            print(f"  Fields removed: {summary['removed_fields']}")
            
            # Show documents based on flags
            if args.show_original or args.show_both:
                print(f"\n📄 ORIGINAL ES DOCUMENT:")
                print("=" * 60)
                print(json.dumps(result['original_document'], indent=2, ensure_ascii=False))
            
            if args.show_formatted or args.show_both:
                print(f"\n⚙️ FORMATTED DOCUMENT:")
                print("=" * 60)
                print(json.dumps(result['formatted_document'], indent=2, ensure_ascii=False))
            
            if args.show_both:
                print(f"\n🔍 FIELD-LEVEL CHANGES:")
                print("=" * 60)
                analysis = result['formatting_analysis']
                if analysis['added_fields']:
                    print(f"Added fields: {analysis['added_fields']}")
                if analysis['removed_fields']:
                    print(f"Removed fields: {analysis['removed_fields']}")
                if analysis['transformed_fields']:
                    print(f"Transformed fields ({len(analysis['transformed_fields'])}):")
                    for tf in analysis['transformed_fields'][:10]:  # Show first 10
                        print(f"  {tf['field']}: {tf['original_type']} → {tf['formatted_type']}")
            
            if args.verbose:
                print(f"\n📋 DETAILED COMPARISON:")
                comparison = result['comparison']
                comp_summary = comparison['summary']
                print(f"  Total fields: {comp_summary['total_fields']}")
                print(f"  Identical values: {comp_summary['identical_values']}")
                print(f"  Different values: {comp_summary['different_values']}")
                
                # Show some field changes
                changes = [fc for fc in comparison['field_comparison'] if not fc['identical']][:5]
                if changes:
                    print(f"\n  Sample field changes:")
                    for change in changes:
                        print(f"    {change['field']}: {change['Original'][:50]}... → {change['Formatted'][:50]}...")
            
            return True
            
        except Exception as e:
            print(f"❌ Error during test: {e}")
            return False
    
    def cmd_format_compare(self, args):
        """Compare raw vs formatted documents side by side"""
        print(f"🔍 DOCUMENT COMPARISON: {args.entity_type}")
        print("=" * 60)
        
        # Get cached samples
        samples = self.formatting_evaluator.sample_extractor.get_cached_samples(args.entity_type)
        if not samples:
            print(f"❌ No cached samples found for {args.entity_type}")
            print("   Run 'format extract-samples' first")
            return False
        
        # Limit to requested number
        samples_to_show = samples[:args.count]
        
        for i, sample in enumerate(samples_to_show, 1):
            print(f"\n{'='*80}")
            print(f"SAMPLE {i}/{len(samples_to_show)}")
            print(f"{'='*80}")
            
            # Format the document
            result = self.formatting_evaluator.document_formatter.format_document(args.entity_type, sample)
            
            if result.success:
                self.formatting_evaluator.document_formatter.display_formatting_result(
                    result, show_full_documents=True
                )
            else:
                print(f"❌ Formatting failed: {result.error_message}")
        
        return True
    
    def cmd_format_extract_samples(self, args):
        """Extract and cache samples without evaluation"""
        print(f"📥 EXTRACTING SAMPLES")
        print("=" * 40)
        
        try:
            if args.entity_type == 'all':
                entity_types = ['persons', 'publications', 'projects', 'organizations', 'serials']
            else:
                entity_types = [args.entity_type]
            
            for entity_type in entity_types:
                print(f"\nExtracting {args.samples} samples for {entity_type}...")
                samples = self.formatting_evaluator.sample_extractor.extract_samples(
                    entity_type=entity_type,
                    count=args.samples,
                    force_refresh=args.force_refresh
                )
                print(f"  ✅ Cached {len(samples)} {entity_type} samples")
            
            print(f"\n✅ EXTRACTION COMPLETE")
            return True
            
        except Exception as e:
            print(f"❌ Error during extraction: {e}")
            return False
    
    def cmd_format_list_cache(self, args):
        """List cached samples"""
        print(f"📁 CACHED SAMPLES")
        print("=" * 30)
        
        try:
            cached_info = self.formatting_evaluator.sample_extractor.get_cache_info()
            
            if not cached_info:
                print("No cached samples found")
                return True
            
            for entity_type, info in cached_info.items():
                if 'error' in info:
                    print(f"❌ {entity_type}: Error - {info['error']}")
                elif 'status' in info:
                    print(f"⚪ {entity_type}: {info['status']}")
                else:
                    size_kb = info['file_size'] / 1024
                    print(f"✅ {entity_type}: {info['count']} samples ({size_kb:.1f} KB)")
                    print(f"   Extracted: {info['extracted_at']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error listing cache: {e}")
            return False
    
    def cmd_format_clear_cache(self, args):
        """Clear cached samples"""
        try:
            if args.entity_type == 'all':
                self.formatting_evaluator.sample_extractor.clear_cache()
                print("🗑️ Cleared all cached samples")
            else:
                self.formatting_evaluator.sample_extractor.clear_cache(args.entity_type)
                print(f"🗑️ Cleared cache for: {args.entity_type}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error clearing cache: {e}")
            return False

    def _prepare_streaming_extractors(self, sample_mode: bool = False, 
                                    sample_size: int = 1000, 
                                    batch_size: int = 1000,
                                    entity_types: list = None) -> Dict[str, BaseStreamingExtractor]:
        """Prepare streaming extractors"""
        try:
            print("📡 Preparing streaming extractors...")
            print(f"   Batch size: {batch_size}")
            
            all_extractors = {
                'persons': PersonExtractor(self.es_client, batch_size=batch_size),
                'organizations': OrganizationExtractor(self.es_client, batch_size=batch_size),
                'publications': PublicationExtractor(self.es_client, batch_size=batch_size),
                'projects': ProjectExtractor(self.es_client, batch_size=batch_size),
                'serials': SerialExtractor(self.es_client, batch_size=batch_size),
            }
            
            # Filter extractors based on entity_types if specified
            if entity_types:
                extractors = {}
                for entity_type in entity_types:
                    if entity_type in all_extractors:
                        extractors[entity_type] = all_extractors[entity_type]
                    else:
                        print(f"⚠️ Warning: Unknown entity type '{entity_type}', skipping")
                print(f"   Selected types: {', '.join(extractors.keys())}")
            else:
                extractors = all_extractors
            
            if sample_mode:
                # For sample mode, we'll use the sample methods
                print(f"   Sample mode: {sample_size} items per type")
            
            return extractors
            
        except Exception as e:
            print(f"❌ Error preparing streaming extractors: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def cmd_import_relationships(self, args):
        """Import relationships only without importing nodes"""
        print("🔗 Relationship Import")
        print("=" * 25)
        print(f"  Batch size: {args.batch_size}")
        
        # Parse relationship types if specified
        relationship_types = None
        if hasattr(args, 'types') and args.types:
            relationship_types = [t.strip().upper() for t in args.types.split(',')]
            print(f"  Relationship types: {', '.join(relationship_types)}")
            
            # Validate relationship types
            valid_types = {'AFFILIATED', 'AUTHORED', 'INVOLVED_IN', 'PARTNER', 'PUBLISHED_IN', 'PART_OF'}
            invalid_types = set(relationship_types) - valid_types
            if invalid_types:
                print(f"❌ Invalid relationship types: {', '.join(invalid_types)}")
                print(f"   Valid types: {', '.join(sorted(valid_types))}")
                return False
        
        # Initialize components
        from graph_db.streaming_importer import NodeCentricRelationshipProcessor
        from es_client.client import ElasticsearchClient
        
        es_client = ElasticsearchClient()
        node_relationship_processor = NodeCentricRelationshipProcessor(
            self.neo4j_conn, es_client, f"rel_import_{time.strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Define all relationship types with their labels
        all_relationship_types = [
            ("AFFILIATED", "👥", "Person", "Organization"),
            ("AUTHORED", "📄", "Person", "Publication"), 
            ("INVOLVED_IN", "🔬", "Person", "Project"),
            ("PARTNER", "🏢", "Organization", "Project"),
            ("PUBLISHED_IN", "📚", "Publication", "Serial"),
            ("PART_OF", "🏛️", "Organization", "Organization")
        ]
        
        # Filter relationship types if specified
        if relationship_types:
            types_to_process = [(rel_type, emoji, source, target) for rel_type, emoji, source, target 
                              in all_relationship_types if rel_type in relationship_types]
        else:
            types_to_process = all_relationship_types
        
        print(f"\n🔗 Processing {len(types_to_process)} relationship type(s)...")
        
        total_relationships = 0
        
        for rel_type, emoji, source_label, target_label in types_to_process:
            print(f"\n  {emoji} Processing {rel_type} relationships ({source_label} → {target_label})...")
            
            try:
                rel_count = node_relationship_processor.process_relationship_type(
                    rel_type, source_label, target_label, sample_mode=False
                )
                total_relationships += rel_count
                print(f"    ✓ {rel_type}: {rel_count:,} relationships created")
            except Exception as e:
                print(f"    ❌ Error processing {rel_type}: {e}")
        
        print(f"\n  ✅ Total relationships imported: {total_relationships:,}")
        
        # Show final statistics
        print(f"\n📊 Final Database Statistics:")
        from graph_db.db_manager import DatabaseManager
        db_manager = DatabaseManager(self.neo4j_conn)
        db_manager.print_database_stats()
        
        return True
    
    def cmd_dry_run(self, args):
        """Perform dry run validation"""
        print("🧪 Dry Run Validation")
        print("=" * 25)
        print(f"  Sample size: {args.size}")
        print(f"  Batch size: {args.batch_size}")
        
        # Prepare data extractors with batch size
        data_extractors = self._prepare_data_extractors(
            sample_mode=True, 
            sample_size=args.size,
            batch_size=args.batch_size
        )
        
        if not data_extractors:
            print("❌ Failed to prepare data extractors")
            return False
        
        # Check if dry_run supports batch_size
        try:
            results = self.import_pipeline.dry_run(
                data_extractors, 
                sample_size=args.size,
                batch_size=args.batch_size
            )
        except TypeError:
            # Fallback if batch_size not supported
            results = self.import_pipeline.dry_run(
                data_extractors, 
                sample_size=args.size
            )
        
        print("\n📋 Validation Results:")
        overall_valid = True
        
        for data_type, result in results.items():
            status = "✅" if result['status'] == 'valid' else "⚠️"
            print(f"  {status} {data_type}: {result['sample_size']} samples")
            
            if result['missing_fields']:
                print(f"    Missing fields: {len(result['missing_fields'])}")
                for field in result['missing_fields'][:3]:  # Show first 3
                    print(f"      • {field}")
                if len(result['missing_fields']) > 3:
                    print(f"      ... and {len(result['missing_fields']) - 3} more")
            
            if result['status'] != 'valid':
                overall_valid = False
        
        if overall_valid:
            print("\n✅ All data validation passed")
        else:
            print("\n⚠️ Data validation found issues")
        
        return overall_valid
    
    def _prepare_data_extractors(self, sample_mode: bool = False, sample_size: int = 1000, batch_size: int = 1000) -> Dict[str, Any]:
        """Prepare data extractors from Elasticsearch with batch size support (legacy method)"""
        try:
            print("📡 Preparing data extractors (legacy mode)...")
            print(f"   Batch size: {batch_size}")
            
            extractors = {
                'organizations': OrganizationExtractor(self.es_client, batch_size=batch_size),
                'persons': PersonExtractor(self.es_client, batch_size=batch_size),
                'serials': SerialExtractor(self.es_client, batch_size=batch_size),
                'projects': ProjectExtractor(self.es_client, batch_size=batch_size),
                'publications': PublicationExtractor(self.es_client, batch_size=batch_size),
            }
            
            data_extractors = {}
            formatted_documents = {}  # Store for relationship extraction
            
            for name, extractor in extractors.items():
                if sample_mode:
                    data = extractor.extract_sample(size=sample_size)
                else:
                    # For full mode, we'll use streaming later
                    data = list(extractor.extract_all())
                
                # Convert ES documents to proper format
                formatted_data = []
                for doc in data:
                    formatted_doc = self._format_document(name, doc)
                    if formatted_doc:
                        formatted_data.append(formatted_doc)
                
                data_extractors[name] = formatted_data
                formatted_documents[name] = formatted_data  # Store for relationships
                print(f"  ✓ {name}: {len(formatted_data)} items prepared")
            
            # Extract relationships from the formatted documents
            from es_client.relationship_extractors import RelationshipExtractor
            
            rel_extractor = RelationshipExtractor(formatted_documents)
            relationships = rel_extractor.extract_all_relationships()
            
            # Add relationships to data_extractors
            data_extractors['relationships'] = relationships
            print(f"  ✓ relationships: {len(relationships)} extracted")
            
            return data_extractors
            
        except Exception as e:
            print(f"❌ Error preparing data extractors: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _format_document(self, doc_type: str, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format Elasticsearch document for Neo4j import"""
        if doc_type == 'persons':
            return {
                'es_id': doc.get('Id', ''),
                'first_name': doc.get('FirstName', ''),
                'last_name': doc.get('LastName', ''),
                'display_name': doc.get('DisplayName', ''),
                'birth_year': doc.get('BirthYear', 0),
                'is_active': doc.get('IsActive', False),
                'has_identifiers': doc.get('HasIdentifiers', False),
                'identifiers': json.dumps(doc.get('Identifiers', [])),
                # Keep organization_home for relationship extraction
                'organization_home': doc.get('OrganizationHome', [])
            }
        
        elif doc_type == 'organizations':
            return {
                'es_id': doc.get('Id', ''),
                'name_swe': doc.get('NameSwe', ''),
                'name_eng': doc.get('NameEng', ''),
                'display_name_swe': doc.get('DisplayNameSwe', ''),
                'display_name_eng': doc.get('DisplayNameEng', ''),
                'city': doc.get('City', ''),
                'country': doc.get('Country', ''),
                'geo_lat': float(doc.get('GeoLat', 0)) if doc.get('GeoLat') else 0,
                'geo_long': float(doc.get('GeoLong', 0)) if doc.get('GeoLong') else 0,
                'level': doc.get('Level', 0),
                'is_active': doc.get('IsActive', False),
                'organization_types': json.dumps(doc.get('OrganizationTypes', [])),
                # Keep organization_parents for relationship extraction
                'organization_parents': doc.get('OrganizationParents', [])
            }
        
        elif doc_type == 'publications':
            return {
                'es_id': doc.get('Id', ''),
                'title': doc.get('Title', ''),
                'abstract': doc.get('Abstract', ''),
                'year': doc.get('Year', 0),
                'publication_type': doc.get('PublicationType', ''),
                'source': doc.get('Source', ''),
                'is_draft': doc.get('IsDraft', False),
                'is_deleted': doc.get('IsDeleted', False),
                'keywords': json.dumps(doc.get('Keywords', [])),
                'categories': json.dumps(doc.get('Categories', [])),
                # Keep these for relationship extraction (note the capitalization)
                'persons': doc.get('Persons', []),
                'organizations': doc.get('Organizations', []),
                'project': doc.get('Project'),
                'series': doc.get('Series', [])  # This is a list
            }
        
        elif doc_type == 'projects':
            return {
                'es_id': str(doc.get('ID', '')),
                'title_swe': doc.get('ProjectTitleSwe', ''),
                'title_eng': doc.get('ProjectTitleEng', ''),
                'description_swe': doc.get('ProjectDescriptionSwe', ''),
                'description_eng': doc.get('ProjectDescriptionEng', ''),
                'start_date': doc.get('StartDate', ''),
                'end_date': doc.get('EndDate', ''),
                'publish_status': doc.get('PublishStatus', 0),
                'keywords': json.dumps(doc.get('Keywords', [])),
                'categories': json.dumps(doc.get('Categories', [])),
                # Keep these for relationship extraction (note the capitalization)
                'persons': doc.get('Persons', []),
                'organizations': doc.get('Organizations', [])
            }
        
        elif doc_type == 'serials':
            return {
                'es_id': doc.get('Id', ''),
                'title': doc.get('Title', ''),
                'start_year': doc.get('StartYear', 0),
                'end_year': doc.get('EndYear', 0),
                'publisher': doc.get('Publisher', ''),
                'country': doc.get('Country', ''),
                'is_open_access': doc.get('IsOpenAccess', False),
                'is_peer_reviewed': doc.get('IsPeerReviewed', False),
                'is_deleted': doc.get('IsDeleted', False),
            }
        
        return None

    
    def run(self):
        """Main CLI entry point"""
        parser = argparse.ArgumentParser(
            description="Neo4j Graph RAG Database Operations CLI",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Test connections
        subparsers.add_parser('test', help='Test database connections')
        
        # Schema management
        schema_parser = subparsers.add_parser('schema', help='Schema management')
        schema_subparsers = schema_parser.add_subparsers(dest='schema_action')
        schema_subparsers.add_parser('setup', help='Set up schema')
        schema_subparsers.add_parser('reset', help='Reset schema')
        schema_subparsers.add_parser('info', help='Show schema info')
        
        # Database management
        db_parser = subparsers.add_parser('db', help='Database management')
        db_subparsers = db_parser.add_subparsers(dest='db_action')
        db_subparsers.add_parser('stats', help='Show database statistics')
        
        clear_parser = db_subparsers.add_parser('clear', help='Clear entire database')
        clear_parser.add_argument('--force', action='store_true', help='Skip confirmation')
        
        clear_type_parser = db_subparsers.add_parser('clear-type', help='Clear specific node types')
        clear_type_parser.add_argument('labels', help='Comma-separated list of labels to clear')
        clear_type_parser.add_argument('--force', action='store_true', help='Skip confirmation')
        
        db_subparsers.add_parser('validate', help='Validate data consistency')
        
        # Import operations
        import_parser = subparsers.add_parser('import', help='Data import operations')
        import_subparsers = import_parser.add_subparsers(dest='import_action')
        
        # Sample import
        sample_parser = import_subparsers.add_parser('sample', help='Import sample data')
        sample_parser.add_argument('--size', type=int, default=1000, help='Sample size per type')
        sample_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
        
        # Full import
        full_parser = import_subparsers.add_parser('full', help='Import full dataset')
        full_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
        full_parser.add_argument('--skip-relationships', action='store_true', help='Skip relationship import')
        full_parser.add_argument('--entity-types', type=str, help='Comma-separated list of entity types to import (persons,organizations,publications,projects,serials)')
        
        # Relationships import
        rels_parser = import_subparsers.add_parser('relationships', help='Import relationships only')
        rels_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
        rels_parser.add_argument('--types', type=str, help='Comma-separated list of relationship types (AFFILIATED,AUTHORED,INVOLVED_IN,PARTNER,PUBLISHED_IN,PART_OF)')
        
        # Dry run
        dry_run_parser = import_subparsers.add_parser('dry-run', help='Validate data without importing')
        dry_run_parser.add_argument('--size', type=int, default=100, help='Sample size for validation')
        dry_run_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
        
        # Formatting evaluation operations
        format_parser = subparsers.add_parser('format', help='Document formatting evaluation')
        format_subparsers = format_parser.add_subparsers(dest='format_action')
        
        # Evaluate formatting
        eval_parser = format_subparsers.add_parser('evaluate', help='Evaluate document formatting')
        eval_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                help='Entity type to evaluate')
        eval_parser.add_argument('--samples', type=int, default=5, help='Number of samples per type (default: 5)')
        eval_parser.add_argument('--force-refresh', action='store_true', help='Force refresh of samples from ES')
        eval_parser.add_argument('--detailed', action='store_true', help='Show detailed formatting results')
        
        # Test single sample
        test_parser = format_subparsers.add_parser('test', help='Test formatter with a single sample')
        test_parser.add_argument('entity_type', choices=['persons', 'organizations', 'publications', 'projects', 'serials'],
                                help='Entity type to test')
        test_parser.add_argument('--index', type=int, default=0, help='Sample index to test (default: 0)')
        test_parser.add_argument('--verbose', action='store_true', help='Show detailed comparison')
        test_parser.add_argument('--show-formatted', action='store_true', help='Display complete formatted document')
        test_parser.add_argument('--show-original', action='store_true', help='Display raw ES document')
        test_parser.add_argument('--show-both', action='store_true', help='Display both original and formatted documents side-by-side')
        
        # Compare documents
        compare_parser = format_subparsers.add_parser('compare', help='Compare raw vs formatted documents')
        compare_parser.add_argument('entity_type', choices=['persons', 'organizations', 'publications', 'projects', 'serials'],
                                   help='Entity type to compare')
        compare_parser.add_argument('--count', type=int, default=3, help='Number of samples to compare (default: 3)')
        
        # Extract samples
        extract_parser = format_subparsers.add_parser('extract-samples', help='Extract and cache samples from ES')
        extract_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                   help='Entity type to extract')
        extract_parser.add_argument('--samples', type=int, default=10, help='Number of samples to extract (default: 10)')
        extract_parser.add_argument('--force-refresh', action='store_true', help='Force refresh even if cache exists')
        
        # Cache management
        format_subparsers.add_parser('list-cache', help='List cached samples')
        
        clear_cache_parser = format_subparsers.add_parser('clear-cache', help='Clear cached samples')
        clear_cache_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                       help='Entity type to clear (or all)')
        
        args = parser.parse_args()
        
        if not args.command:
            parser.print_help()
            return 1
        
        # Initialize connections
        if not self._initialize_connections():
            return 1
        
        # Route commands
        try:
            if args.command == 'test':
                success = self.cmd_test_connections(args)
            
            elif args.command == 'schema':
                if args.schema_action == 'setup':
                    success = self.cmd_schema_setup(args)
                elif args.schema_action == 'reset':
                    success = self.cmd_schema_reset(args)
                elif args.schema_action == 'info':
                    success = self.cmd_schema_info(args)
                else:
                    print("❌ Unknown schema action")
                    return 1
            
            elif args.command == 'db':
                if args.db_action == 'stats':
                    success = self.cmd_db_stats(args)
                elif args.db_action == 'clear':
                    success = self.cmd_clear_db(args)
                elif args.db_action == 'clear-type':
                    success = self.cmd_clear_by_type(args)
                elif args.db_action == 'validate':
                    success = self.cmd_validate_data(args)
                else:
                    print("❌ Unknown database action")
                    return 1
            
            elif args.command == 'import':
                if args.import_action == 'sample':
                    success = self.cmd_import_sample(args)
                elif args.import_action == 'full':
                    success = self.cmd_import_full(args)
                elif args.import_action == 'relationships':
                    success = self.cmd_import_relationships(args)
                elif args.import_action == 'dry-run':
                    success = self.cmd_dry_run(args)
                else:
                    print("❌ Unknown import action")
                    return 1
            
            elif args.command == 'format':
                if args.format_action == 'evaluate':
                    success = self.cmd_format_evaluate(args)
                elif args.format_action == 'test':
                    success = self.cmd_format_test(args)
                elif args.format_action == 'compare':
                    success = self.cmd_format_compare(args)
                elif args.format_action == 'extract-samples':
                    success = self.cmd_format_extract_samples(args)
                elif args.format_action == 'list-cache':
                    success = self.cmd_format_list_cache(args)
                elif args.format_action == 'clear-cache':
                    success = self.cmd_format_clear_cache(args)
                else:
                    print("❌ Unknown format action")
                    return 1
            
            else:
                print(f"❌ Unknown command: {args.command}")
                return 1
            
            return 0 if success else 1
            
        except KeyboardInterrupt:
            print("\n⚠️ Operation cancelled by user")
            return 1
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            if self.neo4j_conn:
                self.neo4j_conn.close()


if __name__ == "__main__":
    cli = Neo4jCLI()
    sys.exit(cli.run())