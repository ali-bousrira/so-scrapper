from scraper import scrape_stackoverflow_pages
from database import connect_db, create_table, insert_data , create_database_if_not_exists , drop_table_if_exists

def main():
    request = input("Voulez-vous supprimer la base de données existante ? (y/n) : ").strip().lower()
    create_database_if_not_exists()
    


    data = scrape_stackoverflow_pages()
    conn = connect_db()
    cursor = conn.cursor()

    drop_table_if_exists(request, cursor, "questions")
    create_table(cursor)
    insert_data(cursor, data)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
    