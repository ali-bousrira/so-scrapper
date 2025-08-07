import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuration
BASE_URL = "https://stackoverflow.com/questions"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def scrape_page(page_num):
    time.sleep(2)
    url = f"{BASE_URL}?page={page_num}&sort=newest"
    print(f"[Thread] Scraping page {page_num}...")

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la requête vers {url} : {e}")
        return []  # On retourne une liste vide si la page échoue

    soup = BeautifulSoup(response.text, 'html.parser')
    questions = soup.select(".s-post-summary")

    page_results = []
    for q in questions:
        title_tag = q.select_one(".s-link")
        title = title_tag.text.strip() if title_tag else "No Title"
        link = "https://stackoverflow.com" + title_tag['href'] if title_tag else ""

        question_tag = q.select_one(".s-post-summary--content-question")
        question = question_tag.text.strip() if question_tag else ""

        tags = [tag.text for tag in q.select(".s-post-summary--meta-tags .post-tag")]

        author_tag = q.select_one(".s-user-card--link")
        author = author_tag.text.strip() if author_tag else "Unknown"

        date_tag = q.select_one("time")
        pub_date = None
        if date_tag and date_tag.has_attr('datetime'):
            pub_date = datetime.fromisoformat(date_tag['datetime'].replace("Z", "+00:00"))

        page_results.append({
            "title": title,
            "link": link,
            "question": question,
            "tags": ",".join(tags),
            "author": author,
            "pub_date": pub_date
        })
        
    return page_results


def scrape_stackoverflow_pages_parallel(max_pages, max_workers=5):
    all_questions = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(scrape_page, page) for page in range(1, max_pages + 1)]

        for future in as_completed(futures):
            try:
                result = future.result()
                all_questions.extend(result)
            except Exception as e:
                print(f"Erreur lors du traitement d'une page : {e}")

    return all_questions


if __name__ == "__main__":
    results = scrape_stackoverflow_pages_parallel(max_pages=5, max_workers=5)
    print(f"\nScraping terminé. {len(results)} questions récupérées.")
    for q in results[:5]:  # affichage d'exemple
        print("-", q["title"])
