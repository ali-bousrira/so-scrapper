import mariadb
from dotenv import load_dotenv
import os

load_dotenv() 
HOST = os.getenv("HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")

def create_database_if_not_exists():
    try:
        connection = mariadb.connect(
            host=HOST,
            user=USER,
            password=PASSWORD
        )

        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS so_scrapper")
        connection.commit()  
        print("✅ Base 'so_scrapper' créée ou déjà existante.")
    except mariadb.Error as e:
        print(f"❌ Erreur MariaDB : {e}")
    finally:
        cursor.close()
        connection.close()

def drop_table_if_exists(request ,cursor, table_name):
    if request == "y":
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"✅ Table '{table_name}' supprimée avant le scraping.")

def connect_db():
    return mariadb.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

def create_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS questions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title TEXT,
        link TEXT,
        excerpt TEXT,
        tags TEXT,
        author VARCHAR(255),
        pub_date DATETIME
    )
    """)

def insert_data(cursor, data):
    insert_query = """
    INSERT INTO questions (title, link, excerpt, tags, author, pub_date)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    for q in data:
        cursor.execute(insert_query, (
            q['title'],
            q['link'],
            q['excerpt'],
            q['tags'],
            q['author'],
            q['pub_date']
        ))
