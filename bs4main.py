from bs4scraper import scrape_stackoverflow_pages_parallel
import bs4database
def main():
    db = bs4database.Bs4database()

    request = input("Voulez-vous supprimer la base de données existante ? (y/n) : ").strip().lower()
    
    db.create_database_if_not_exists()

    # Configuration du scraping
    max_pages = 1000
    max_workers = 1000

    print(f"Lancement du scraping parallèle de {max_pages} pages avec {max_workers} threads...")
    data = scrape_stackoverflow_pages_parallel(max_pages=max_pages, max_workers=max_workers)

    # Connexion à la base de données
    conn = db.connect_db()
    cursor = conn.cursor()

    # Suppression et création de la table
    db.drop_table_if_exists(request, cursor, "questions")
    db.create_table(cursor)

    # Insertion des données
    db.insert_data(cursor, data)

    # Commit et fermeture
    conn.commit()
    conn.close()

    print(f"Terminé. {len(data)} questions insérées dans la base de données.")

if __name__ == "__main__":
    main()
