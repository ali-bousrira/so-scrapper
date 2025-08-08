import pymysql
from datetime import datetime
from typing import List, Dict, Any, Optional
import json
import os
from dotenv import load_dotenv
from unified_scraper import QuestionData

# Load environment variables
load_dotenv()

class MariaDBCRUD:
    """Complete CRUD operations for StackOverflow scraper data using MariaDB"""
    
    def __init__(self):
        """Initialize MariaDB connection"""
        self.config = {
            'host': os.getenv('HOST'),
            'user': os.getenv('USER'),
            'password': os.getenv('PASSWORD'),
            'database': os.getenv('DATABASE'),
            'charset': 'utf8mb4'
        }
        self.connection = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Establish database connection"""
        try:
            # First try to connect to the specific database
            self.connection = pymysql.connect(**self.config)
            print(f"Connected to MariaDB database: {self.config['database']}")
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1049:  # Unknown database error
                print(f"Database '{self.config['database']}' doesn't exist, creating it...")
                self._create_database()
                # Now connect to the newly created database
                self.connection = pymysql.connect(**self.config)
                print(f"Connected to MariaDB database: {self.config['database']}")
            else:
                print(f"Failed to connect to MariaDB: {e}")
                raise
        except Exception as e:
            print(f"❌ Failed to connect to MariaDB: {e}")
            raise
    
    def _create_database(self):
        """Create the database if it doesn't exist"""
        # Connect without specifying database
        temp_config = self.config.copy()
        database_name = temp_config.pop('database')
        
        temp_connection = None
        try:
            temp_connection = pymysql.connect(**temp_config)
            cursor = temp_connection.cursor()
            
            # Create database with UTF8MB4 charset
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS `{database_name}` 
                CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
            """)
            
            temp_connection.commit()
            print(f"Database '{database_name}' created successfully")
            
        except Exception as e:
            print(f"Error creating database: {e}")
            raise
        finally:
            if temp_connection:
                temp_connection.close()
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        cursor = self.connection.cursor()
        try:
            # Authors table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS authors (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    reputation INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_author_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Tags table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tags (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(50) NOT NULL UNIQUE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_tag_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Questions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS questions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(500) NOT NULL,
                    link VARCHAR(500) NOT NULL UNIQUE,
                    text LONGTEXT,
                    author_id INT,
                    publication_date DATETIME,
                    scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    scrape_method VARCHAR(20),
                    FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE SET NULL,
                    INDEX idx_title (title(100)),
                    INDEX idx_link (link),
                    INDEX idx_publication_date (publication_date),
                    INDEX idx_scraped_at (scraped_at),
                    INDEX idx_scrape_method (scrape_method)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Question-Tags junction table (many-to-many)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS question_tags (
                    question_id INT,
                    tag_id INT,
                    PRIMARY KEY (question_id, tag_id),
                    FOREIGN KEY (question_id) REFERENCES questions(id) ON DELETE CASCADE,
                    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            self.connection.commit()
            print("Database tables created/verified successfully")
            
        except Exception as e:
            print(f"Error creating tables: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def _get_or_create_author(self, cursor, name: str, reputation: Optional[int] = None) -> int:
        """Get existing author ID or create new author"""
        # Check if author exists
        cursor.execute("SELECT id, reputation FROM authors WHERE name = %s", (name,))
        result = cursor.fetchone()
        
        if result:
            author_id, current_rep = result
            # Update reputation if provided and different
            if reputation and current_rep != reputation:
                cursor.execute(
                    "UPDATE authors SET reputation = %s, updated_at = %s WHERE id = %s",
                    (reputation, datetime.now(), author_id)
                )
            return author_id
        else:
            # Create new author
            cursor.execute(
                "INSERT INTO authors (name, reputation) VALUES (%s, %s)",
                (name, reputation)
            )
            return cursor.lastrowid
    
    def _get_or_create_tag(self, cursor, tag_name: str) -> int:
        """Get existing tag ID or create new tag"""
        # Check if tag exists
        cursor.execute("SELECT id FROM tags WHERE name = %s", (tag_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            # Create new tag
            cursor.execute("INSERT INTO tags (name) VALUES (%s)", (tag_name,))
            return cursor.lastrowid
    
    # CREATE operations
    def create_question(self, question_data: QuestionData, scrape_method: str = None) -> int:
        """Create a new question record"""
        cursor = self.connection.cursor()
        try:
            # Check if question already exists
            cursor.execute("SELECT id FROM questions WHERE link = %s", (question_data.link,))
            if cursor.fetchone():
                raise ValueError(f"Question with link '{question_data.link}' already exists")
            
            # Get or create author
            author_id = self._get_or_create_author(cursor, question_data.author_name, question_data.author_reputation)
            
            # Insert question
            cursor.execute("""
                INSERT INTO questions (title, link, text, author_id, publication_date, scrape_method)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                question_data.title,
                question_data.link,
                question_data.text,
                author_id,
                question_data.publication_date,
                scrape_method
            ))
            
            question_id = cursor.lastrowid
            
            # Handle tags
            for tag_name in question_data.tags:
                tag_id = self._get_or_create_tag(cursor, tag_name)
                cursor.execute(
                    "INSERT IGNORE INTO question_tags (question_id, tag_id) VALUES (%s, %s)",
                    (question_id, tag_id)
                )
            
            self.connection.commit()
            return question_id
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def create_question_if_not_exists(self, question_data: QuestionData, scrape_method: str = None) -> tuple[int, bool]:
        """Create question if it doesn't exist, return (question_id, created_flag)"""
        cursor = self.connection.cursor()
        try:
            # Check if question already exists
            cursor.execute("SELECT id FROM questions WHERE link = %s", (question_data.link,))
            existing = cursor.fetchone()
            if existing:
                return existing[0], False
            
            # Create new question
            question_id = self.create_question(question_data, scrape_method)
            return question_id, True
            
        finally:
            cursor.close()
    
    def create_questions_batch(self, questions_data: List[QuestionData], scrape_method: str = None) -> List[int]:
        """Create multiple questions in batch"""
        created_ids = []
        skipped_count = 0
        
        for question_data in questions_data:
            try:
                question_id, created = self.create_question_if_not_exists(question_data, scrape_method)
                if created:
                    created_ids.append(question_id)
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"Error creating question '{question_data.title[:50]}...': {e}")
                continue
        
        print(f"Batch created: {len(created_ids)} new questions, {skipped_count} duplicates skipped")
        return created_ids
    
    # READ operations
    def get_question_by_id(self, question_id: int) -> Optional[Dict[str, Any]]:
        """Get question by ID"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("""
                SELECT q.*, a.name as author_name, a.reputation as author_reputation
                FROM questions q
                LEFT JOIN authors a ON q.author_id = a.id
                WHERE q.id = %s
            """, (question_id,))
            
            question = cursor.fetchone()
            if not question:
                return None
            
            # Get tags
            cursor.execute("""
                SELECT t.name FROM tags t
                JOIN question_tags qt ON t.id = qt.tag_id
                WHERE qt.question_id = %s
            """, (question_id,))
            
            tags = [row['name'] for row in cursor.fetchall()]
            question['tags'] = tags
            
            return question
            
        finally:
            cursor.close()
    
    def get_question_by_link(self, link: str) -> Optional[Dict[str, Any]]:
        """Get question by URL link"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("""
                SELECT q.*, a.name as author_name, a.reputation as author_reputation
                FROM questions q
                LEFT JOIN authors a ON q.author_id = a.id
                WHERE q.link = %s
            """, (link,))
            
            question = cursor.fetchone()
            if not question:
                return None
            
            # Get tags
            cursor.execute("""
                SELECT t.name FROM tags t
                JOIN question_tags qt ON t.id = qt.tag_id
                WHERE qt.question_id = %s
            """, (question['id'],))
            
            tags = [row['name'] for row in cursor.fetchall()]
            question['tags'] = tags
            
            return question
            
        finally:
            cursor.close()
    
    def get_questions(self, limit: int = 100, offset: int = 0, **filters) -> List[Dict[str, Any]]:
        """Get questions with optional filters"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            query = """
                SELECT q.*, a.name as author_name, a.reputation as author_reputation
                FROM questions q
                LEFT JOIN authors a ON q.author_id = a.id
            """
            params = []
            conditions = []
            
            # Apply filters
            if 'author_name' in filters:
                conditions.append("a.name = %s")
                params.append(filters['author_name'])
            
            if 'scrape_method' in filters:
                conditions.append("q.scrape_method = %s")
                params.append(filters['scrape_method'])
            
            if 'tag' in filters:
                query += " JOIN question_tags qt ON q.id = qt.question_id JOIN tags t ON qt.tag_id = t.id"
                conditions.append("t.name = %s")
                params.append(filters['tag'])
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY q.scraped_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            questions = cursor.fetchall()
            
            # Get tags for each question
            for question in questions:
                cursor.execute("""
                    SELECT t.name FROM tags t
                    JOIN question_tags qt ON t.id = qt.tag_id
                    WHERE qt.question_id = %s
                """, (question['id'],))
                question['tags'] = [row['name'] for row in cursor.fetchall()]
            
            return questions
            
        finally:
            cursor.close()
    
    def search_questions(self, search_term: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Search questions by title or text content"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("""
                SELECT q.*, a.name as author_name, a.reputation as author_reputation
                FROM questions q
                LEFT JOIN authors a ON q.author_id = a.id
                WHERE q.title LIKE %s OR q.text LIKE %s
                ORDER BY q.scraped_at DESC
                LIMIT %s
            """, (f"%{search_term}%", f"%{search_term}%", limit))
            
            questions = cursor.fetchall()
            
            # Get tags for each question
            for question in questions:
                cursor.execute("""
                    SELECT t.name FROM tags t
                    JOIN question_tags qt ON t.id = qt.tag_id
                    WHERE qt.question_id = %s
                """, (question['id'],))
                question['tags'] = [row['name'] for row in cursor.fetchall()]
            
            return questions
            
        finally:
            cursor.close()
    
    def get_popular_tags(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get most popular tags with question counts"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("""
                SELECT t.name, COUNT(qt.question_id) as question_count
                FROM tags t
                JOIN question_tags qt ON t.id = qt.tag_id
                GROUP BY t.id, t.name
                ORDER BY question_count DESC
                LIMIT %s
            """, (limit,))
            
            return [{'tag': row['name'], 'count': row['question_count']} for row in cursor.fetchall()]
            
        finally:
            cursor.close()
    
    def get_authors(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all authors"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute("""
                SELECT a.*, COUNT(q.id) as question_count
                FROM authors a
                LEFT JOIN questions q ON a.id = q.author_id
                GROUP BY a.id
                ORDER BY question_count DESC, a.reputation DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            return cursor.fetchall()
            
        finally:
            cursor.close()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            stats = {}
            
            # Total counts
            cursor.execute("SELECT COUNT(*) as count FROM questions")
            stats['total_questions'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM authors")
            stats['total_authors'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM tags")
            stats['total_tags'] = cursor.fetchone()['count']
            
            # Questions by scrape method
            cursor.execute("""
                SELECT scrape_method, COUNT(*) as count
                FROM questions
                WHERE scrape_method IS NOT NULL
                GROUP BY scrape_method
            """)
            stats['questions_by_method'] = {row['scrape_method']: row['count'] for row in cursor.fetchall()}
            
            # Last scraped
            cursor.execute("SELECT MAX(scraped_at) as last_scraped FROM questions")
            result = cursor.fetchone()
            stats['last_scraped'] = result['last_scraped'] if result else None
            
            return stats
            
        finally:
            cursor.close()
    
    # UPDATE operations
    def update_question(self, question_id: int, **updates) -> bool:
        """Update question by ID"""
        cursor = self.connection.cursor()
        try:
            allowed_fields = ['title', 'text', 'publication_date', 'scrape_method']
            set_clauses = []
            params = []
            
            for field, value in updates.items():
                if field in allowed_fields:
                    set_clauses.append(f"{field} = %s")
                    params.append(value)
            
            if not set_clauses:
                return False
            
            params.append(question_id)
            query = f"UPDATE questions SET {', '.join(set_clauses)} WHERE id = %s"
            
            cursor.execute(query, params)
            self.connection.commit()
            
            return cursor.rowcount > 0
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def update_author(self, author_id: int, **updates) -> bool:
        """Update author by ID"""
        cursor = self.connection.cursor()
        try:
            allowed_fields = ['name', 'reputation']
            set_clauses = []
            params = []
            
            for field, value in updates.items():
                if field in allowed_fields:
                    set_clauses.append(f"{field} = %s")
                    params.append(value)
            
            if not set_clauses:
                return False
            
            set_clauses.append("updated_at = %s")
            params.extend([datetime.now(), author_id])
            
            query = f"UPDATE authors SET {', '.join(set_clauses)} WHERE id = %s"
            
            cursor.execute(query, params)
            self.connection.commit()
            
            return cursor.rowcount > 0
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    # DELETE operations
    def delete_question(self, question_id: int) -> bool:
        """Delete question by ID"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("DELETE FROM questions WHERE id = %s", (question_id,))
            self.connection.commit()
            return cursor.rowcount > 0
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def delete_questions_by_author(self, author_name: str) -> int:
        """Delete all questions by a specific author"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                DELETE q FROM questions q
                JOIN authors a ON q.author_id = a.id
                WHERE a.name = %s
            """, (author_name,))
            
            deleted_count = cursor.rowcount
            self.connection.commit()
            return deleted_count
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    def delete_old_questions(self, days_old: int = 30) -> int:
        """Delete questions older than specified days"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                DELETE FROM questions
                WHERE scraped_at < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (days_old,))
            
            deleted_count = cursor.rowcount
            self.connection.commit()
            return deleted_count
            
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()
    
    # UTILITY operations
    def export_questions_to_json(self, filename: str, limit: int = None, **filters):
        """Export questions to JSON file"""
        questions = self.get_questions(limit=limit or 10000, **filters)
        
        # Convert datetime objects to strings
        for question in questions:
            for key, value in question.items():
                if isinstance(value, datetime):
                    question[key] = value.isoformat()
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(questions, f, ensure_ascii=False, indent=2)
        
        print(f"Exported {len(questions)} questions to {filename}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Convenience functions
def get_mariadb_crud() -> MariaDBCRUD:
    """Get MariaDB CRUD instance"""
    return MariaDBCRUD()


if __name__ == "__main__":
    # Example usage and testing
    print("Testing MariaDB CRUD operations...")
    
    with MariaDBCRUD() as crud:
        # Show statistics
        stats = crud.get_statistics()
        print(f"Database statistics: {stats}")
        
        # Show popular tags
        popular_tags = crud.get_popular_tags(5)
        print(f"Popular tags: {popular_tags}")