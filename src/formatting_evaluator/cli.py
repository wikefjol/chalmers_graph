"""
CLI commands for formatting evaluation
"""

import argparse
import sys
from pathlib import Path

from .formatting_evaluator import FormattingEvaluator


class FormattingEvaluatorCLI:
    """Command line interface for document formatting evaluation"""
    
    def __init__(self):
        self.evaluator = FormattingEvaluator()
    
    def cmd_evaluate(self, args):
        """Evaluate formatting for one or more entity types"""
        if args.entity_type == 'all':
            return self.evaluator.evaluate_all_types(
                samples_count=args.samples,
                force_refresh=args.force_refresh
            )
        else:
            return self.evaluator.evaluate_entity_type(
                entity_type=args.entity_type,
                samples_count=args.samples,
                force_refresh=args.force_refresh,
                show_details=args.detailed
            )
    
    def cmd_extract_samples(self, args):
        """Extract and cache samples without evaluation"""
        print(f"📥 EXTRACTING SAMPLES")
        print("=" * 40)
        
        entity_types = None if args.entity_type == 'all' else [args.entity_type]
        
        samples = self.evaluator.sample_extractor.extract_samples(
            entity_types=entity_types,
            samples_per_type=args.samples,
            force_refresh=args.force_refresh
        )
        
        print(f"\n✅ EXTRACTION COMPLETE")
        for entity_type, entity_samples in samples.items():
            print(f"  {entity_type}: {len(entity_samples)} samples cached")
        
        return {'success': True, 'samples': samples}
    
    def cmd_list_cache(self, args):
        """List cached samples"""
        print(f"📁 CACHED SAMPLES")
        print("=" * 30)
        
        cached_info = self.evaluator.sample_extractor.list_cached_samples()
        
        if not cached_info:
            print("No cached samples found")
            return {'success': True, 'cached_info': {}}
        
        for entity_type, info in cached_info.items():
            if 'error' in info:
                print(f"❌ {entity_type}: Error - {info['error']}")
            else:
                size_kb = info['file_size'] / 1024
                print(f"✅ {entity_type}: {info['count']} samples ({size_kb:.1f} KB)")
                print(f"   Extracted: {info['extracted_at']}")
        
        return {'success': True, 'cached_info': cached_info}
    
    def cmd_clear_cache(self, args):
        """Clear cached samples"""
        entity_types = None if args.entity_type == 'all' else [args.entity_type]
        
        self.evaluator.clear_cache(entity_types)
        
        if entity_types:
            print(f"🗑️ Cleared cache for: {', '.join(entity_types)}")
        else:
            print("🗑️ Cleared all cached samples")
        
        return {'success': True}
    
    def cmd_validate_only(self, args):
        """Run only validation on cached samples"""
        print(f"🔍 VALIDATION ONLY: {args.entity_type}")
        print("=" * 50)
        
        # Get cached samples
        samples = self.evaluator.sample_extractor.get_cached_samples(args.entity_type)
        if not samples:
            print(f"❌ No cached samples found for {args.entity_type}")
            print("   Run 'extract-samples' first")
            return {'success': False, 'error': 'No cached samples'}
        
        # Format and validate
        formatting_results = self.evaluator.document_formatter.format_samples(args.entity_type, samples)
        
        successful_formats = [r for r in formatting_results if r.success]
        if not successful_formats:
            print("❌ No documents were successfully formatted")
            return {'success': False, 'error': 'Formatting failed'}
        
        validation_results = []
        for result in successful_formats:
            validation = self.evaluator.compatibility_validator.validate_document(
                args.entity_type, result.formatted_document
            )
            validation_results.append(validation)
        
        # Display results
        validation_summary = self.evaluator.compatibility_validator.generate_validation_summary(validation_results)
        
        print(f"\n📊 VALIDATION SUMMARY")
        print("-" * 40)
        self.evaluator._display_validation_summary(validation_summary)
        
        if args.detailed:
            print(f"\n📋 DETAILED VALIDATION RESULTS")
            print("-" * 50)
            for result in validation_results:
                self.evaluator.compatibility_validator.display_validation_result(result)
        
        return {
            'success': True,
            'validation_results': validation_results,
            'validation_summary': validation_summary
        }
    
    def cmd_compare_samples(self, args):
        """Compare raw vs formatted documents side by side"""
        print(f"🔍 DOCUMENT COMPARISON: {args.entity_type}")
        print("=" * 60)
        
        # Get cached samples
        samples = self.evaluator.sample_extractor.get_cached_samples(args.entity_type)
        if not samples:
            print(f"❌ No cached samples found for {args.entity_type}")
            return {'success': False, 'error': 'No cached samples'}
        
        # Limit to requested number
        samples_to_show = samples[:args.count]
        
        for i, sample in enumerate(samples_to_show, 1):
            print(f"\n{'='*80}")
            print(f"SAMPLE {i}/{len(samples_to_show)}")
            print(f"{'='*80}")
            
            # Format the document
            result = self.evaluator.document_formatter.format_document(args.entity_type, sample)
            
            if result.success:
                self.evaluator.document_formatter.display_formatting_result(
                    result, show_full_documents=True
                )
            else:
                print(f"❌ Formatting failed: {result.error_message}")
        
        return {'success': True}
    
    def cmd_save_report(self, args):
        """Save evaluation report to file"""
        filename = args.filename if args.filename else None
        entity_type = args.entity_type if args.entity_type != 'all' else None
        
        report_path = self.evaluator.save_evaluation_report(entity_type, filename)
        
        if report_path:
            print(f"✅ Report saved successfully")
            return {'success': True, 'report_path': report_path}
        else:
            print(f"❌ Failed to save report")
            return {'success': False}
    
    def cmd_test_formatter(self, args):
        """Test the formatter with a single sample"""
        print(f"🧪 TESTING FORMATTER: {args.entity_type}")
        print("=" * 50)
        
        # Get one sample
        samples = self.evaluator.sample_extractor.get_cached_samples(args.entity_type)
        if not samples:
            # Try to extract one sample
            extracted = self.evaluator.sample_extractor.extract_samples(
                entity_types=[args.entity_type],
                samples_per_type=1,
                force_refresh=False
            )
            samples = extracted.get(args.entity_type, [])
        
        if not samples:
            print(f"❌ No samples available for {args.entity_type}")
            return {'success': False}
        
        sample = samples[0]
        
        # Format and validate
        format_result = self.evaluator.document_formatter.format_document(args.entity_type, sample)
        
        if format_result.success:
            validation_result = self.evaluator.compatibility_validator.validate_document(
                args.entity_type, format_result.formatted_document
            )
            
            # Show detailed results
            self.evaluator.document_formatter.display_formatting_result(
                format_result, show_full_documents=True
            )
            self.evaluator.compatibility_validator.display_validation_result(validation_result)
            
            return {
                'success': True,
                'format_result': format_result,
                'validation_result': validation_result
            }
        else:
            print(f"❌ Formatting failed: {format_result.error_message}")
            return {'success': False, 'error': format_result.error_message}
    
    def run(self):
        """Main CLI entry point"""
        parser = argparse.ArgumentParser(
            description="Document Formatting Evaluation CLI",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Evaluate all entity types with 5 samples each
  python -m formatting_evaluator.cli evaluate all --samples 5
  
  # Evaluate just persons with detailed output
  python -m formatting_evaluator.cli evaluate persons --detailed
  
  # Extract and cache samples without evaluation
  python -m formatting_evaluator.cli extract-samples publications --samples 10
  
  # Compare raw vs formatted documents
  python -m formatting_evaluator.cli compare persons --count 3
  
  # Test formatter with a single sample
  python -m formatting_evaluator.cli test-formatter organizations
            """
        )
        
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Evaluate command
        eval_parser = subparsers.add_parser('evaluate', help='Evaluate document formatting')
        eval_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                help='Entity type to evaluate')
        eval_parser.add_argument('--samples', type=int, default=5, help='Number of samples per type (default: 5)')
        eval_parser.add_argument('--force-refresh', action='store_true', help='Force refresh of samples from ES')
        eval_parser.add_argument('--detailed', action='store_true', help='Show detailed formatting results')
        
        # Extract samples command
        extract_parser = subparsers.add_parser('extract-samples', help='Extract and cache samples from ES')
        extract_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                   help='Entity type to extract')
        extract_parser.add_argument('--samples', type=int, default=10, help='Number of samples to extract (default: 10)')
        extract_parser.add_argument('--force-refresh', action='store_true', help='Force refresh even if cache exists')
        
        # List cache command
        subparsers.add_parser('list-cache', help='List cached samples')
        
        # Clear cache command
        clear_parser = subparsers.add_parser('clear-cache', help='Clear cached samples')
        clear_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                 help='Entity type to clear (or all)')
        
        # Validate only command
        validate_parser = subparsers.add_parser('validate-only', help='Run validation on cached samples')
        validate_parser.add_argument('entity_type', choices=['persons', 'organizations', 'publications', 'projects', 'serials'],
                                    help='Entity type to validate')
        validate_parser.add_argument('--detailed', action='store_true', help='Show detailed validation results')
        
        # Compare samples command
        compare_parser = subparsers.add_parser('compare', help='Compare raw vs formatted documents')
        compare_parser.add_argument('entity_type', choices=['persons', 'organizations', 'publications', 'projects', 'serials'],
                                   help='Entity type to compare')
        compare_parser.add_argument('--count', type=int, default=3, help='Number of samples to compare (default: 3)')
        
        # Save report command
        report_parser = subparsers.add_parser('save-report', help='Save evaluation report to file')
        report_parser.add_argument('entity_type', choices=['all', 'persons', 'organizations', 'publications', 'projects', 'serials'],
                                  help='Entity type to save (or all)')
        report_parser.add_argument('--filename', help='Custom filename for report')
        
        # Test formatter command
        test_parser = subparsers.add_parser('test-formatter', help='Test formatter with a single sample')
        test_parser.add_argument('entity_type', choices=['persons', 'organizations', 'publications', 'projects', 'serials'],
                                help='Entity type to test')
        
        args = parser.parse_args()
        
        if not args.command:
            parser.print_help()
            return 1
        
        # Route commands
        try:
            if args.command == 'evaluate':
                result = self.cmd_evaluate(args)
            elif args.command == 'extract-samples':
                result = self.cmd_extract_samples(args)
            elif args.command == 'list-cache':
                result = self.cmd_list_cache(args)
            elif args.command == 'clear-cache':
                result = self.cmd_clear_cache(args)
            elif args.command == 'validate-only':
                result = self.cmd_validate_only(args)
            elif args.command == 'compare':
                result = self.cmd_compare_samples(args)
            elif args.command == 'save-report':
                result = self.cmd_save_report(args)
            elif args.command == 'test-formatter':
                result = self.cmd_test_formatter(args)
            else:
                print(f"❌ Unknown command: {args.command}")
                return 1
            
            return 0 if result.get('success', True) else 1
            
        except KeyboardInterrupt:
            print("\n⚠️ Operation cancelled by user")
            return 1
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == "__main__":
    cli = FormattingEvaluatorCLI()
    sys.exit(cli.run())