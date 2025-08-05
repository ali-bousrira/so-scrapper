import mariadb

def create_database_if_not_exists():
    try:
        connection = mariadb.connect(
            host="localhost",
            user="root",
            password="root"
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
        print(f"✅ Table '{table_name}' supprimée si elle existait.")

def connect_db():
    return mariadb.connect(
        host="localhost",
        user="root",
        password="root",
        database="so_scrapper"
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
