#!/usr/bin/env python3
"""
Test runner script for the StackOverflow scraper
Runs all tests and provides detailed reporting
"""

import unittest
import sys
import os
from io import StringIO
import time
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_specific_test_class(test_class_name):
    """Run a specific test class"""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName(f'test_unified_scraper.{test_class_name}')
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result

def run_all_tests():
    """Run all tests with detailed reporting"""
    print("=" * 70)
    print(f"StackOverflow Scraper Test Suite")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Discover and run all tests
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Custom test runner with more details
    stream = StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=2)
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    # Print results
    output = stream.getvalue()
    print(output)
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%" if result.testsRun > 0 else "N/A")
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    
    # Detailed failure/error reporting
    if result.failures:
        print(f"\n{len(result.failures)} FAILURES:")
        print("-" * 50)
        for test, traceback in result.failures:
            print(f"FAIL: {test}")
            print(traceback)
            print("-" * 50)
    
    if result.errors:
        print(f"\n{len(result.errors)} ERRORS:")
        print("-" * 50)
        for test, traceback in result.errors:
            print(f"ERROR: {test}")
            print(traceback)
            print("-" * 50)
    
    # Overall result
    if result.wasSuccessful():
        print(f"\n✅ ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n❌ SOME TESTS FAILED!")
        return 1

def run_coverage_test():
    """Run tests with coverage reporting (requires coverage.py)"""
    try:
        import coverage
        cov = coverage.Coverage()
        cov.start()
        
        result = run_all_tests()
        
        cov.stop()
        cov.save()
        
        print("\n" + "=" * 70)
        print("COVERAGE REPORT")
        print("=" * 70)
        cov.report(show_missing=True)
        
        return result
    except ImportError:
        print("Coverage.py not installed. Install with: pip install coverage")
        print("Running tests without coverage...")
        return run_all_tests()

def main():
    """Main test runner function"""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == '--help' or command == '-h':
            print("StackOverflow Scraper Test Runner")
            print("\nUsage:")
            print("  python run_tests.py                    - Run all tests")
            print("  python run_tests.py --coverage         - Run tests with coverage")
            print("  python run_tests.py --class <name>     - Run specific test class")
            print("  python run_tests.py --list             - List all test classes")
            print("\nAvailable test classes:")
            print("  - TestTextCleaning")
            print("  - TestStackOverflowScraperMethods") 
            print("  - TestScraperIntegration")
            print("  - TestErrorHandling")
            print("  - TestDataValidation")
            return 0
            
        elif command == '--coverage':
            return run_coverage_test()
            
        elif command == '--class' and len(sys.argv) > 2:
            class_name = sys.argv[2]
            print(f"Running test class: {class_name}")
            result = run_specific_test_class(class_name)
            return 0 if result.wasSuccessful() else 1
            
        elif command == '--list':
            print("Available test classes:")
            print("  - TestTextCleaning")
            print("  - TestStackOverflowScraperMethods") 
            print("  - TestScraperIntegration")
            print("  - TestErrorHandling")
            print("  - TestDataValidation")
            return 0
        else:
            print(f"Unknown command: {command}")
            print("Use --help for usage information")
            return 1
    else:
        return run_all_tests()

if __name__ == '__main__':
    sys.exit(main())