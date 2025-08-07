import mariadb
from dotenv import load_dotenv
import os
class Bs4database:
    def __init__(self):
        load_dotenv() 
        self.host = os.getenv("HOST")
        self.user = os.getenv("USER")
        self.password = os.getenv("PASSWORD")
        self.database = os.getenv("DATABASE")

    def create_database_if_not_exists(self):
        try:
            connection = mariadb.connect(
                host=self.host,
                user=self.user,
                password=self.password
        )

            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS so_scrapper")
            connection.commit()  
            print("Base 'so_scrapper' créée ou déjà existante.")
        except mariadb.Error as e:
            print(f"Erreur MariaDB : {e}")
        finally:
            cursor.close()
            connection.close()

    def drop_table_if_exists(self,request, cursor, table_name):
        if request == "y":
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' supprimée avant le scraping.")

    def connect_db(self):
        return mariadb.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def create_table(self,cursor):
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS questions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            link TEXT,
        question TEXT,
        tags TEXT,
        author VARCHAR(255),
        pub_date DATETIME
    )
    """)

    def insert_data(self,cursor, data):
        insert_query = """
        INSERT INTO questions (title, link, question, tags, author, pub_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        for q in data:
            cursor.execute(insert_query, (
                q['title'],
                q['link'],
                q['question'],
                q['tags'],
                q['author'],
                q['pub_date']
            ))
