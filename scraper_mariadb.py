#!/usr/bin/env python3
"""
StackOverflow scraper with MariaDB integration
Combines scraping functionality with MariaDB CRUD operations
"""

from unified_scraper import StackOverflowScraper, QuestionData
from mariadb_crud import MariaDBCRUD
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import argparse


class StackOverflowScraperMariaDB(StackOverflowScraper):
    """Enhanced scraper with MariaDB integration"""
    
    def __init__(self):
        super().__init__()
        self.crud = MariaDBCRUD()
    
    def scrape_and_store(self, method: str = "beautifulsoup", num_questions: int = 10, 
                        avoid_duplicates: bool = True, **kwargs) -> Dict[str, Any]:
        """
        Scrape questions and store them in MariaDB
        
        Args:
            method: Scraping method (beautifulsoup, selenium, api)
            num_questions: Number of questions to scrape
            avoid_duplicates: Skip questions that already exist in database
            **kwargs: Additional parameters for scraping methods
            
        Returns:
            Dictionary with scraping results and statistics
        """
        print(f"Starting scraping with {method} method...")
        start_time = datetime.now()
        
        # Scrape questions
        questions = self.scrape_questions(method=method, num_questions=num_questions, **kwargs)
        scrape_time = datetime.now()
        
        if not questions:
            return {
                'success': False,
                'message': 'No questions scraped',
                'scraped_count': 0,
                'stored_count': 0,
                'duplicates_skipped': 0
            }
        
        # Store in database
        if avoid_duplicates:
            stored_ids = self.crud.create_questions_batch(questions, scrape_method=method)
            stored_count = len(stored_ids)
            duplicates_skipped = len(questions) - stored_count
        else:
            stored_ids = []
            duplicates_skipped = 0
            for question in questions:
                try:
                    question_id = self.crud.create_question(question, scrape_method=method)
                    stored_ids.append(question_id)
                except ValueError:
                    duplicates_skipped += 1
                except Exception as e:
                    print(f"Error storing question: {e}")
            stored_count = len(stored_ids)
        
        end_time = datetime.now()
        
        return {
            'success': True,
            'scraped_count': len(questions),
            'stored_count': stored_count,
            'duplicates_skipped': duplicates_skipped,
            'scrape_time': (scrape_time - start_time).total_seconds(),
            'total_time': (end_time - start_time).total_seconds(),
            'method': method,
            'stored_question_ids': stored_ids[:5]  # First 5 IDs for reference
        }
    
    def batch_scrape_and_store(self, methods: List[str], num_questions_per_method: int = 10) -> Dict[str, Any]:
        """
        Scrape using multiple methods and store all results
        
        Args:
            methods: List of scraping methods to use
            num_questions_per_method: Questions to scrape per method
            
        Returns:
            Combined results from all methods
        """
        results = {}
        total_scraped = 0
        total_stored = 0
        total_duplicates = 0
        
        for method in methods:
            print(f"\n--- Scraping with {method.upper()} ---")
            result = self.scrape_and_store(
                method=method, 
                num_questions=num_questions_per_method,
                avoid_duplicates=True
            )
            
            results[method] = result
            total_scraped += result['scraped_count']
            total_stored += result['stored_count']
            total_duplicates += result['duplicates_skipped']
            
            print(f"{method}: {result['stored_count']} stored, {result['duplicates_skipped']} duplicates")
        
        return {
            'methods_used': methods,
            'total_scraped': total_scraped,
            'total_stored': total_stored,
            'total_duplicates_skipped': total_duplicates,
            'results_by_method': results
        }
    
    # Database query methods
    def search_stored_questions(self, search_term: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search questions in database"""
        return self.crud.search_questions(search_term, limit)
    
    def get_questions_by_tag(self, tag: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get questions by specific tag"""
        return self.crud.get_questions(limit=limit, tag=tag)
    
    def get_questions_by_author(self, author_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get questions by specific author"""
        return self.crud.get_questions(limit=limit, author_name=author_name)
    
    def get_popular_tags_with_counts(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get most popular tags with question counts"""
        return self.crud.get_popular_tags(limit)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        return self.crud.get_statistics()
    
    def get_top_authors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get authors with most questions"""
        return self.crud.get_authors(limit=limit)
    
    # Data management methods
    def export_questions_to_json(self, filename: str, limit: int = None, **filters):
        """Export questions from database to JSON"""
        self.crud.export_questions_to_json(filename, limit, **filters)
    
    def cleanup_old_questions(self, days_old: int = 30) -> int:
        """Remove old questions from database"""
        deleted_count = self.crud.delete_old_questions(days_old)
        print(f"Deleted {deleted_count} questions older than {days_old} days")
        return deleted_count
    
    def update_question_text(self, question_id: int) -> bool:
        """Re-scrape and update question text"""
        question = self.crud.get_question_by_id(question_id)
        if not question:
            print(f"Question {question_id} not found")
            return False
        
        try:
            new_text = self._get_question_text_bs4(question['link'])
            success = self.crud.update_question(question_id, text=new_text)
            if success:
                print(f"Updated question {question_id} text")
            return success
        except Exception as e:
            print(f"Error updating question {question_id}: {e}")
            return False
    
    def get_question_details(self, question_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed question information"""
        return self.crud.get_question_by_id(question_id)
    
    def close(self):
        """Close database connection"""
        self.crud.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def main():
    """Command line interface for the MariaDB scraper"""
    parser = argparse.ArgumentParser(description='StackOverflow Scraper with MariaDB')
    parser.add_argument('command', choices=['scrape', 'search', 'stats', 'export', 'cleanup', 'authors', 'tags'], 
                       help='Command to execute')
    
    # Scraping arguments
    parser.add_argument('--method', choices=['beautifulsoup', 'selenium', 'api'], 
                       default='beautifulsoup', help='Scraping method')
    parser.add_argument('--count', type=int, default=10, help='Number of questions to scrape')
    parser.add_argument('--all-methods', action='store_true', help='Use all scraping methods')
    
    # Search arguments
    parser.add_argument('--term', type=str, help='Search term')
    parser.add_argument('--tag', type=str, help='Filter by tag')
    parser.add_argument('--author', type=str, help='Filter by author')
    parser.add_argument('--limit', type=int, default=50, help='Limit results')
    
    # Export arguments
    parser.add_argument('--output', type=str, help='Output filename')
    
    # Cleanup arguments
    parser.add_argument('--days', type=int, default=30, help='Days old for cleanup')
    
    args = parser.parse_args()
    
    # Initialize scraper
    with StackOverflowScraperMariaDB() as scraper:
        
        if args.command == 'scrape':
            if args.all_methods:
                methods = ['beautifulsoup', 'selenium', 'api']
                result = scraper.batch_scrape_and_store(methods, args.count)
                print("\n=== BATCH SCRAPING RESULTS ===")
                print(f"Total scraped: {result['total_scraped']}")
                print(f"Total stored: {result['total_stored']}")
                print(f"Duplicates skipped: {result['total_duplicates_skipped']}")
                
                for method, method_result in result['results_by_method'].items():
                    print(f"  • {method}: {method_result['stored_count']} stored ({method_result['scrape_time']:.1f}s)")
            else:
                result = scraper.scrape_and_store(method=args.method, num_questions=args.count)
                print("\n=== SCRAPING RESULTS ===")
                print(f"Method: {result['method']}")
                print(f"Scraped: {result['scraped_count']}")
                print(f"Stored: {result['stored_count']}")
                print(f"Duplicates skipped: {result['duplicates_skipped']}")
                print(f"Time: {result['total_time']:.2f} seconds")
        
        elif args.command == 'search':
            if args.term:
                questions = scraper.search_stored_questions(args.term, args.limit)
                print(f"\n=== SEARCH RESULTS for '{args.term}' ===")
            elif args.tag:
                questions = scraper.get_questions_by_tag(args.tag, args.limit)
                print(f"\n=== QUESTIONS with tag '{args.tag}' ===")
            elif args.author:
                questions = scraper.get_questions_by_author(args.author, args.limit)
                print(f"\n=== QUESTIONS by author '{args.author}' ===")
            else:
                print("Please provide --term, --tag, or --author for search")
                return
            
            if not questions:
                print("No questions found")
                return
                
            for i, q in enumerate(questions, 1):
                print(f"{i:2d}. {q['title'][:70]}...")
                print(f"    Author: {q['author_name']} ({q['author_reputation']} rep)")
                print(f"    Tags: {', '.join(q['tags'])}")
                print(f"    {q['link']}")
                print()
        
        elif args.command == 'stats':
            stats = scraper.get_database_stats()
            print("\n=== DATABASE STATISTICS ===")
            print(f"Total questions: {stats['total_questions']:,}")
            print(f"Total authors: {stats['total_authors']:,}")
            print(f"Total tags: {stats['total_tags']:,}")
            print(f"Last scraped: {stats['last_scraped']}")
            
            if stats['questions_by_method']:
                print(f"\nQuestions by scrape method:")
                for method, count in stats['questions_by_method'].items():
                    print(f"  • {method}: {count:,}")
            
            print(f"\nPOPULAR TAGS:")
            popular_tags = scraper.get_popular_tags_with_counts(10)
            for tag_data in popular_tags:
                print(f"  • {tag_data['tag']}: {tag_data['count']:,} questions")
        
        elif args.command == 'authors':
            authors = scraper.get_top_authors(args.limit)
            print(f"\n=== TOP AUTHORS ===")
            for i, author in enumerate(authors, 1):
                print(f"{i:2d}. {author['name']}")
                print(f"    Reputation: {author['reputation']:,}" if author['reputation'] else "    Reputation: N/A")
                print(f"    Questions: {author['question_count']}")
                print()
        
        elif args.command == 'tags':
            popular_tags = scraper.get_popular_tags_with_counts(args.limit)
            print(f"\n=== POPULAR TAGS ===")
            for i, tag_data in enumerate(popular_tags, 1):
                print(f"{i:2d}. {tag_data['tag']}: {tag_data['count']:,} questions")
        
        elif args.command == 'export':
            filename = args.output or f"stackoverflow_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filters = {}
            if args.tag:
                filters['tag'] = args.tag
            if args.author:
                filters['author_name'] = args.author
            
            scraper.export_questions_to_json(filename, args.limit, **filters)
        
        elif args.command == 'cleanup':
            deleted = scraper.cleanup_old_questions(args.days)
            print(f"Cleanup completed. Deleted {deleted:,} old questions.")


if __name__ == "__main__":
    main()