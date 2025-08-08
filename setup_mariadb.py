#!/usr/bin/env python3
"""
MariaDB database setup script for StackOverflow scraper
Creates database and tables manually
"""

import pymysql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def setup_mariadb_database():
    """Set up MariaDB database and tables for StackOverflow scraper"""
    
    config = {
        'host': os.getenv('HOST', 'localhost'),
        'user': os.getenv('USER', 'root'),
        'password': os.getenv('PASSWORD', ''),
        'charset': 'utf8mb4'
    }
    
    database_name = os.getenv('DATABASE', 'stackoverflow')
    
    print(f"🚀 Setting up MariaDB database: {database_name}")
    print(f"📍 Host: {config['host']}")
    print(f"👤 User: {config['user']}")
    
    connection = None
    try:
        # Connect to MariaDB server (without specific database)
        print("\n1. 🔌 Connecting to MariaDB server...")
        connection = pymysql.connect(**config)
        cursor = connection.cursor()
        
        # Create database
        print(f"2. 🔧 Creating database '{database_name}'...")
        cursor.execute(f"""
            CREATE DATABASE IF NOT EXISTS `{database_name}` 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
        """)
        connection.commit()
        print(f"✅ Database '{database_name}' created successfully")
        
        # Switch to the new database
        cursor.execute(f"USE `{database_name}`")
        
        # Create tables
        print("3. 📋 Creating tables...")
        
        # Authors table
        print("   📝 Creating 'authors' table...")
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
        print("   🏷️  Creating 'tags' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(50) NOT NULL UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tag_name (name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Questions table
        print("   ❓ Creating 'questions' table...")
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
        
        # Question-Tags junction table
        print("   🔗 Creating 'question_tags' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS question_tags (
                question_id INT,
                tag_id INT,
                PRIMARY KEY (question_id, tag_id),
                FOREIGN KEY (question_id) REFERENCES questions(id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        connection.commit()
        print("✅ All tables created successfully")
        
        # Show table information
        print("\n4. 📊 Database structure:")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count = cursor.fetchone()[0]
            print(f"   📋 {table_name}: {count} records")
        
        print(f"\n✅ MariaDB setup complete!")
        print(f"🎯 Ready to use with: python scraper_mariadb.py")
        
    except pymysql.err.OperationalError as e:
        print(f"❌ Connection error: {e}")
        print("💡 Check your MariaDB connection settings in .env file")
        print("   Required variables: HOST, USER, PASSWORD, DATABASE")
    except Exception as e:
        print(f"❌ Setup error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if connection:
            connection.close()
            print("🔌 Database connection closed")


def test_connection():
    """Test MariaDB connection with current settings"""
    
    print("🧪 Testing MariaDB connection...")
    
    config = {
        'host': os.getenv('HOST', 'localhost'),
        'user': os.getenv('USER', 'root'),
        'password': os.getenv('PASSWORD', ''),
        'database': os.getenv('DATABASE', 'stackoverflow'),
        'charset': 'utf8mb4'
    }
    
    print(f"📍 Host: {config['host']}")
    print(f"👤 User: {config['user']}")
    print(f"🗄️  Database: {config['database']}")
    
    try:
        connection = pymysql.connect(**config)
        cursor = connection.cursor()
        
        # Test basic query
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Connected successfully!")
        print(f"📊 MariaDB version: {version}")
        
        # Show database info
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        print(f"🗄️  Current database: {current_db}")
        
        # Show tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"📋 Tables: {len(tables)}")
        for table in tables:
            print(f"   • {table[0]}")
        
        connection.close()
        print("🎉 Connection test passed!")
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        print("💡 Make sure MariaDB is running and credentials are correct")


def drop_database():
    """Drop the database (use with caution!)"""
    
    database_name = os.getenv('DATABASE', 'stackoverflow')
    
    print(f"⚠️  WARNING: This will DELETE the entire database '{database_name}'!")
    confirm = input("Type 'DELETE' to confirm: ").strip()
    
    if confirm != 'DELETE':
        print("❌ Operation cancelled")
        return
    
    config = {
        'host': os.getenv('HOST', 'localhost'),
        'user': os.getenv('USER', 'root'),
        'password': os.getenv('PASSWORD', ''),
        'charset': 'utf8mb4'
    }
    
    try:
        connection = pymysql.connect(**config)
        cursor = connection.cursor()
        
        cursor.execute(f"DROP DATABASE IF EXISTS `{database_name}`")
        connection.commit()
        
        print(f"🗑️  Database '{database_name}' deleted successfully")
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Error dropping database: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'test':
            test_connection()
        elif command == 'drop':
            drop_database()
        elif command == 'setup':
            setup_mariadb_database()
        else:
            print("Usage:")
            print("  python setup_mariadb.py setup  - Set up database and tables")
            print("  python setup_mariadb.py test   - Test database connection")
            print("  python setup_mariadb.py drop   - Drop database (DANGEROUS!)")
    else:
        setup_mariadb_database()