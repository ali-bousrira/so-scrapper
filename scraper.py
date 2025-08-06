import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

def scrape_stackoverflow_pages(max_pages, delay):
    base_url = "https://stackoverflow.com/questions"
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_questions = []

    for page in range(1, max_pages + 1):
        print(f"Scraping page {page}...")
        url = f"{base_url}?page={page}&sort=newest"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ Erreur page {page} : {response.status_code}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        questions = soup.select(".s-post-summary")

        if not questions:
            print("✅ Fin du scraping : plus de questions.")
            break

        for q in questions:
            title_tag = q.select_one(".s-link")
            title = title_tag.text.strip() if title_tag else "No Title"
            link = "https://stackoverflow.com" + title_tag['href'] if title_tag else ""

            excerpt_tag = q.select_one(".s-post-summary--content-excerpt")
            excerpt = excerpt_tag.text.strip() if excerpt_tag else ""

            tags = [tag.text for tag in q.select(".s-post-summary--meta-tags .post-tag")]

            author_tag = q.select_one(".s-user-card--link")
            author = author_tag.text.strip() if author_tag else "Unknown"

            date_tag = q.select_one("time")
            pub_date = None
            if date_tag and date_tag.has_attr('datetime'):
                pub_date = datetime.fromisoformat(date_tag['datetime'].replace("Z", "+00:00"))

            all_questions.append({
                "title": title,
                "link": link,
                "excerpt": excerpt,
                "tags": ",".join(tags),
                "author": author,
                "pub_date": pub_date
            })

        time.sleep(delay)

    return all_questions
