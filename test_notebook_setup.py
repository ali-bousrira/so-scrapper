#!/usr/bin/env python3
"""
Test script to verify notebook dependencies and database connectivity
Run this before using the interactive notebook
"""

import sys
import importlib
import os

def test_imports():
    """Test if all required packages are available"""
    print("Testing Python package imports...")
    
    required_packages = [
        'pandas', 'matplotlib', 'seaborn', 'plotly', 
        'wordcloud', 'ipywidgets', 'pymysql', 'requests', 
        'beautifulsoup4', 'selenium', 'python-dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            # Handle special cases
            if package == 'beautifulsoup4':
                importlib.import_module('bs4')
            elif package == 'python-dotenv':
                importlib.import_module('dotenv')
            else:
                importlib.import_module(package)
            print(f"  {package}")
        except ImportError:
            print(f"  {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Install them with: pip install " + " ".join(missing_packages))
        return False
    else:
        print("\nAll required packages are installed!")
        return True

def test_env_file():
    """Test if .env file exists and has required variables"""
    print("\nTesting environment configuration...")
    
    if not os.path.exists('.env'):
        print("  .env file not found")
        print("  Create a .env file with:")
        print("     HOST=your_mariadb_host")
        print("     USER=your_mariadb_user") 
        print("     PASSWORD=your_mariadb_password")
        print("     DATABASE=your_database_name")
        return False
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        required_vars = ['HOST', 'USER', 'PASSWORD', 'DATABASE']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
            else:
                print(f"  {var}")
        
        if missing_vars:
            print(f"  Missing environment variables: {', '.join(missing_vars)}")
            return False
        else:
            print("  All environment variables found!")
            return True
            
    except Exception as e:
        print(f"  Error loading .env file: {e}")
        return False

def test_database_connection():
    """Test MariaDB database connection"""
    print("\nTesting database connection...")
    
    try:
        from mariadb_crud import MariaDBCRUD
        
        with MariaDBCRUD() as crud:
            stats = crud.get_statistics()
            print(f"  Database connection successful!")
            print(f"  Current database stats:")
            print(f"     Questions: {stats['total_questions']:,}")
            print(f"     Authors: {stats['total_authors']:,}")
            print(f"     Tags: {stats['total_tags']:,}")
            
        return True
        
    except Exception as e:
        print(f"  Database connection failed: {e}")
        print("  Make sure MariaDB is running and credentials are correct")
        return False

def test_scraper_functionality():
    """Test basic scraper functionality"""
    print("\nTesting scraper functionality...")
    
    try:
        from unified_scraper import StackOverflowScraper
        
        scraper = StackOverflowScraper()
        print("  Scraper initialization successful!")
        
        # Test API endpoint
        import requests
        response = requests.get("https://api.stackexchange.com/2.3/questions", 
                              params={'site': 'stackoverflow', 'pagesize': 1}, 
                              timeout=10)
        
        if response.status_code == 200:
            print("  Stack Exchange API accessible!")
        else:
            print("  Stack Exchange API may be limited")
            
        return True
        
    except Exception as e:
        print(f"  Scraper test failed: {e}")
        return False

def test_notebook_files():
    """Test if notebook and related files exist"""
    print("\nTesting notebook files...")
    
    required_files = [
        'interactive_scraper_analysis.ipynb',
        'unified_scraper.py',
        'scraper_mariadb.py', 
        'mariadb_crud.py'
    ]
    
    all_exist = True
    
    for file in required_files:
        if os.path.exists(file):
            print(f"  {file}")
        else:
            print(f"  {file} - MISSING")
            all_exist = False
    
    return all_exist

def main():
    """Run all tests"""
    print("NOTEBOOK SETUP TEST")
    print("=" * 50)
    
    tests = [
        ("Package imports", test_imports),
        ("Environment file", test_env_file),
        ("Database connection", test_database_connection),
        ("Scraper functionality", test_scraper_functionality),
        ("Required files", test_notebook_files)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n{test_name} test crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY:")
    
    all_passed = True
    for test_name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"   {test_name:<20} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("ALL TESTS PASSED!")
        print("Your notebook is ready to use!")
        print("\nNext steps:")
        print("   1. Open Jupyter Lab/Notebook")
        print("   2. Load 'interactive_scraper_analysis.ipynb'")
        print("   3. Run the cells to start scraping and analyzing!")
    else:
        print("SOME TESTS FAILED!")
        print("Fix the failing tests before using the notebook")
        print("\nCommon fixes:")
        print("   - Install missing packages: pip install [package_name]")
        print("   - Create/fix .env file with database credentials")
        print("   - Start MariaDB service")
        print("   - Check internet connection for API access")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)