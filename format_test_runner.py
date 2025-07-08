#!/usr/bin/env python3
"""
Interactive formatting test runner for development workflow.

Provides quick testing capabilities for document formatting without
requiring full CLI invocation or database operations.
"""

import sys
import argparse
import json
from typing import Dict, Any, Optional
sys.path.append('src')

from es_client.client import ElasticsearchClient
from formatting_evaluator import FormattingEvaluator


class FormatTestRunner:
    """Interactive test runner for formatting evaluation during development."""
    
    def __init__(self):
        self.es_client = None
        self.evaluator = None
        self._initialize()
    
    def _initialize(self):
        """Initialize connections and evaluator."""
        try:
            print("🔌 Initializing connections...")
            self.es_client = ElasticsearchClient()
            if not self.es_client.test_connection():
                print("❌ Elasticsearch connection failed")
                return False
            
            self.evaluator = FormattingEvaluator(self.es_client)
            print("✅ Ready for testing")
            return True
            
        except Exception as e:
            print(f"❌ Initialization failed: {e}")
            return False
    
    def quick_test(self, entity_type: str, sample_index: int = 0, show_docs: str = None) -> bool:
        """Quick test of a single sample."""
        if not self.evaluator:
            print("❌ Evaluator not initialized")
            return False
        
        print(f"\n🧪 QUICK TEST: {entity_type} (sample {sample_index})")
        print("-" * 50)
        
        try:
            result = self.evaluator.quick_test_single_sample(entity_type, sample_index)
            
            if 'error' in result:
                print(f"❌ {result['error']}")
                return False
            
            # Print compact results
            summary = result['summary']
            print(f"✅ Results:")
            print(f"  Sample: {result['sample_id']}")
            print(f"  Format time: {summary['format_time_ms']:.2f}ms")
            print(f"  Fields changed: {summary['fields_changed']}")
            print(f"  Fields added: {summary['added_fields']}")
            print(f"  Fields removed: {summary['removed_fields']}")
            
            # Show documents if requested
            if show_docs in ['original', 'both']:
                print(f"\n📄 ORIGINAL ES DOCUMENT:")
                print("-" * 40)
                print(json.dumps(result['original_document'], indent=2, ensure_ascii=False))
            
            if show_docs in ['formatted', 'both']:
                print(f"\n⚙️ FORMATTED DOCUMENT:")
                print("-" * 40)
                print(json.dumps(result['formatted_document'], indent=2, ensure_ascii=False))
            
            return True
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
    
    def extract_samples(self, entity_type: str, count: int = 5) -> bool:
        """Extract and cache samples for testing."""
        if not self.evaluator:
            print("❌ Evaluator not initialized")
            return False
        
        print(f"\n📥 EXTRACTING: {count} samples of {entity_type}")
        print("-" * 50)
        
        try:
            samples = self.evaluator.sample_extractor.extract_samples(
                entity_type, count, force_refresh=True
            )
            print(f"✅ Cached {len(samples)} {entity_type} samples")
            return True
            
        except Exception as e:
            print(f"❌ Extraction failed: {e}")
            return False
    
    def show_cache_status(self):
        """Show cache status for all entity types."""
        if not self.evaluator:
            print("❌ Evaluator not initialized")
            return
        
        print(f"\n📁 CACHE STATUS")
        print("-" * 30)
        
        try:
            cache_info = self.evaluator.sample_extractor.get_cache_info()
            
            for entity_type, info in cache_info.items():
                if 'status' in info:
                    print(f"⚪ {entity_type}: {info['status']}")
                elif 'error' in info:
                    print(f"❌ {entity_type}: error")
                else:
                    print(f"✅ {entity_type}: {info['count']} samples")
                    
        except Exception as e:
            print(f"❌ Cache check failed: {e}")
    
    def batch_test(self, entity_types: list = None, sample_count: int = 3) -> Dict[str, bool]:
        """Test all entity types quickly."""
        if not entity_types:
            entity_types = ['persons', 'publications', 'projects', 'organizations', 'serials']
        
        print(f"\n🧪 BATCH TEST: {len(entity_types)} entity types")
        print("=" * 60)
        
        results = {}
        
        for entity_type in entity_types:
            try:
                result = self.evaluator.evaluate_entity_type(
                    entity_type, sample_count=sample_count, detailed=False
                )
                
                if 'error' in result:
                    print(f"❌ {entity_type}: {result['error']}")
                    results[entity_type] = False
                else:
                    summary = result['evaluation_summary']
                    pass_rate = summary['validation_pass_rate']
                    print(f"{'✅' if pass_rate > 0.8 else '⚠️'} {entity_type}: "
                          f"{pass_rate:.1%} pass rate, "
                          f"{summary['avg_format_time_ms']:.1f}ms avg")
                    results[entity_type] = pass_rate > 0.8
                    
            except Exception as e:
                print(f"❌ {entity_type}: error - {e}")
                results[entity_type] = False
        
        # Summary
        passed = sum(results.values())
        total = len(results)
        print(f"\n📊 SUMMARY: {passed}/{total} entity types passed")
        
        return results
    
    def interactive_mode(self):
        """Interactive mode for development."""
        print("\n🔧 INTERACTIVE MODE")
        print("=" * 50)
        print("Commands:")
        print("  test <entity_type> [index] - Test single sample")
        print("  show original <entity_type> [index] - Show raw ES document")
        print("  show formatted <entity_type> [index] - Show formatted document")
        print("  show both <entity_type> [index] - Show both documents")
        print("  extract <entity_type> [count] - Extract samples")
        print("  cache - Show cache status")
        print("  batch - Test all entity types")
        print("  help - Show this help")
        print("  quit - Exit")
        print()
        
        while True:
            try:
                cmd = input("format> ").strip().split()
                
                if not cmd:
                    continue
                
                action = cmd[0].lower()
                
                if action == 'quit' or action == 'exit':
                    break
                elif action == 'help':
                    print("Commands: test, show, extract, cache, batch, help, quit")
                elif action == 'test' and len(cmd) >= 2:
                    entity_type = cmd[1]
                    index = int(cmd[2]) if len(cmd) > 2 else 0
                    self.quick_test(entity_type, index)
                elif action == 'show' and len(cmd) >= 3:
                    show_type = cmd[1]  # original, formatted, both
                    entity_type = cmd[2]
                    index = int(cmd[3]) if len(cmd) > 3 else 0
                    if show_type in ['original', 'formatted', 'both']:
                        self.quick_test(entity_type, index, show_docs=show_type)
                    else:
                        print("❌ show command usage: show [original|formatted|both] <entity_type> [index]")
                elif action == 'extract' and len(cmd) >= 2:
                    entity_type = cmd[1]
                    count = int(cmd[2]) if len(cmd) > 2 else 5
                    self.extract_samples(entity_type, count)
                elif action == 'cache':
                    self.show_cache_status()
                elif action == 'batch':
                    self.batch_test()
                else:
                    print("❌ Unknown command. Type 'help' for commands.")
                    
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"❌ Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='Formatting test runner for development')
    parser.add_argument('--test', metavar='ENTITY_TYPE', 
                       help='Quick test of entity type')
    parser.add_argument('--index', type=int, default=0,
                       help='Sample index for testing (default: 0)')
    parser.add_argument('--extract', metavar='ENTITY_TYPE',
                       help='Extract samples for entity type')
    parser.add_argument('--count', type=int, default=5,
                       help='Number of samples to extract (default: 5)')
    parser.add_argument('--batch', action='store_true',
                       help='Test all entity types')
    parser.add_argument('--interactive', action='store_true',
                       help='Start interactive mode')
    
    args = parser.parse_args()
    
    runner = FormatTestRunner()
    
    if not runner.evaluator:
        sys.exit(1)
    
    if args.test:
        success = runner.quick_test(args.test, args.index)
        sys.exit(0 if success else 1)
    elif args.extract:
        success = runner.extract_samples(args.extract, args.count)
        sys.exit(0 if success else 1)
    elif args.batch:
        results = runner.batch_test()
        passed = sum(results.values())
        total = len(results)
        print(f"\nFinal result: {passed}/{total} entity types passed")
        sys.exit(0 if passed == total else 1)
    elif args.interactive:
        runner.interactive_mode()
    else:
        # Default: show cache and offer interactive mode
        runner.show_cache_status()
        print("\nUse --interactive for interactive mode or --help for options")


if __name__ == "__main__":
    main()