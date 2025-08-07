from bs4scraper import scrape_stackoverflow_pages_parallel
from bs4database import connect_db, create_table, insert_data, create_database_if_not_exists, drop_table_if_exists

def main():
    request = input("Voulez-vous supprimer la base de données existante ? (y/n) : ").strip().lower()
    
    create_database_if_not_exists()

    # Configuration du scraping
    max_pages = 2000
    max_workers = 6

    print(f"Lancement du scraping parallèle de {max_pages} pages avec {max_workers} threads...")
    data = scrape_stackoverflow_pages_parallel(max_pages=max_pages, max_workers=max_workers)

    # Connexion à la base de données
    conn = connect_db()
    cursor = conn.cursor()

    # Suppression et création de la table
    drop_table_if_exists(request, cursor, "questions")
    create_table(cursor)

    # Insertion des données
    insert_data(cursor, data)

    # Commit et fermeture
    conn.commit()
    conn.close()

    print(f"✅ Terminé. {len(data)} questions insérées dans la base de données.")

if __name__ == "__main__":
    main()
