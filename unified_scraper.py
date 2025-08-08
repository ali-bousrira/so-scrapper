import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import json
import re
from typing import List, Dict, Optional
from dataclasses import dataclass


@dataclass
class QuestionData:
    title: str
    link: str
    text: str
    tags: List[str]
    author_name: str
    author_reputation: Optional[int]
    publication_date: Optional[datetime]


class StackOverflowScraper:
    def __init__(self):
        self.base_url = "https://stackoverflow.com"
        self.api_url = "https://api.stackexchange.com/2.3"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def _clean_html_text(self, html_text: str) -> str:
        """Clean HTML text by removing tags and formatting properly"""
        if not html_text:
            return ""
        
        # Create BeautifulSoup object
        soup = BeautifulSoup(html_text, 'html.parser')
        
        # Handle code blocks specially - preserve formatting
        for code in soup.find_all(['code', 'pre']):
            code.string = f"\n```\n{code.get_text()}\n```\n"
        
        # Convert to text while preserving some structure
        text = soup.get_text(separator=' ')
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single space
        text = re.sub(r'\n\s*\n', '\n\n', text)  # Multiple newlines to double newline
        text = text.strip()
        
        return text

    def scrape_questions(self, method: str = "beautifulsoup", num_questions: int = 10, **kwargs) -> List[QuestionData]:
        """
        Main scraping method that delegates to specific scrapers based on method
        
        Args:
            method: "beautifulsoup", "selenium", or "api"
            num_questions: Number of questions to scrape
            **kwargs: Additional parameters for specific methods
        """
        if method == "beautifulsoup":
            return self._scrape_with_beautifulsoup(num_questions, **kwargs)
        elif method == "selenium":
            return self._scrape_with_selenium(num_questions, **kwargs)
        elif method == "api":
            return self._scrape_with_api(num_questions, **kwargs)
        else:
            raise ValueError(f"Unknown method: {method}. Use 'beautifulsoup', 'selenium', or 'api'")

    def _scrape_with_beautifulsoup(self, num_questions: int, delay: float = 1.0) -> List[QuestionData]:
        """Scrape using BeautifulSoup"""
        questions_data = []
        page = 1
        
        while len(questions_data) < num_questions:
            print(f"Scraping page {page} with BeautifulSoup...")
            url = f"{self.base_url}/questions?page={page}&sort=newest"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            questions = soup.select(".s-post-summary")
            
            if not questions:
                print("No more questions found")
                break

            for q in questions:
                if len(questions_data) >= num_questions:
                    break

                # Title and link
                title_tag = q.select_one(".s-link")
                title = title_tag.text.strip() if title_tag else "No Title"
                link = self.base_url + title_tag['href'] if title_tag and title_tag.get('href') else ""

                # Get question text by visiting the question page
                question_text = self._get_question_text_bs4(link)

                # Tags
                tags = [tag.text for tag in q.select(".s-post-summary--meta-tags .post-tag")]

                # Author
                author_tag = q.select_one(".s-user-card--link")
                author_name = author_tag.text.strip() if author_tag else "Unknown"

                # Author reputation (need to visit profile or get from question page)
                author_reputation = self._get_author_reputation_bs4(link)

                # Publication date
                date_tag = q.select_one("time")
                pub_date = None
                if date_tag and date_tag.has_attr('datetime'):
                    try:
                        pub_date = datetime.fromisoformat(date_tag['datetime'].replace("Z", "+00:00"))
                    except:
                        pub_date = None

                questions_data.append(QuestionData(
                    title=title,
                    link=link,
                    text=question_text,
                    tags=tags,
                    author_name=author_name,
                    author_reputation=author_reputation,
                    publication_date=pub_date
                ))

            page += 1
            time.sleep(delay)

        return questions_data[:num_questions]

    def _scrape_with_selenium(self, num_questions: int, headless: bool = True) -> List[QuestionData]:
        """Scrape using Selenium"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(options=chrome_options)
        questions_data = []
        
        try:
            page = 1
            while len(questions_data) < num_questions:
                print(f"Scraping page {page} with Selenium...")
                url = f"{self.base_url}/questions?page={page}&sort=newest"
                driver.get(url)
                
                # Wait for questions to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "s-post-summary"))
                )
                
                questions = driver.find_elements(By.CLASS_NAME, "s-post-summary")
                
                if not questions:
                    break

                for q in questions:
                    if len(questions_data) >= num_questions:
                        break

                    try:
                        # Title and link
                        title_element = q.find_element(By.CSS_SELECTOR, ".s-link")
                        title = title_element.text.strip()
                        link = title_element.get_attribute("href")

                        # Get question text by visiting the question page
                        question_text = self._get_question_text_selenium(driver, link)

                        # Tags
                        tag_elements = q.find_elements(By.CSS_SELECTOR, ".s-post-summary--meta-tags .post-tag")
                        tags = [tag.text for tag in tag_elements]

                        # Author
                        try:
                            author_element = q.find_element(By.CSS_SELECTOR, ".s-user-card--link")
                            author_name = author_element.text.strip()
                        except:
                            author_name = "Unknown"

                        # Author reputation
                        author_reputation = self._get_author_reputation_selenium(driver, link)

                        # Publication date
                        try:
                            date_element = q.find_element(By.TAG_NAME, "time")
                            datetime_attr = date_element.get_attribute("datetime")
                            pub_date = datetime.fromisoformat(datetime_attr.replace("Z", "+00:00")) if datetime_attr else None
                        except:
                            pub_date = None

                        questions_data.append(QuestionData(
                            title=title,
                            link=link,
                            text=question_text,
                            tags=tags,
                            author_name=author_name,
                            author_reputation=author_reputation,
                            publication_date=pub_date
                        ))

                    except Exception as e:
                        print(f"Error processing question: {e}")
                        continue

                page += 1
                time.sleep(1)

        finally:
            driver.quit()

        return questions_data[:num_questions]

    def _scrape_with_api(self, num_questions: int, site: str = "stackoverflow") -> List[QuestionData]:
        """Scrape using Stack Exchange API"""
        questions_data = []
        page_size = min(100, num_questions)  # API max is 100
        page = 1

        while len(questions_data) < num_questions:
            print(f"Fetching page {page} from API...")
            
            # Get questions
            questions_url = f"{self.api_url}/questions"
            params = {
                'order': 'desc',
                'sort': 'creation',
                'site': site,
                'pagesize': page_size,
                'page': page,
                'filter': '!9_bDDxJY5'  # Include body, owner, and creation_date
            }

            try:
                response = requests.get(questions_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if 'items' not in data or not data['items']:
                    break

                for item in data['items']:
                    if len(questions_data) >= num_questions:
                        break

                    # Extract data from API response
                    title = item.get('title', 'No Title')
                    link = item.get('link', '')
                    raw_text = item.get('body', '')
                    text = self._clean_html_text(raw_text)  # Clean HTML from API response
                    tags = item.get('tags', [])
                    
                    # Owner information
                    owner = item.get('owner', {})
                    author_name = owner.get('display_name', 'Unknown')
                    author_reputation = owner.get('reputation')

                    # Publication date
                    creation_date = item.get('creation_date')
                    pub_date = datetime.fromtimestamp(creation_date) if creation_date else None

                    questions_data.append(QuestionData(
                        title=title,
                        link=link,
                        text=text,
                        tags=tags,
                        author_name=author_name,
                        author_reputation=author_reputation,
                        publication_date=pub_date
                    ))

                # Check if we have more pages
                if not data.get('has_more', False):
                    break
                    
                page += 1
                time.sleep(0.1)  # Be nice to the API

            except requests.exceptions.RequestException as e:
                print(f"API request error: {e}")
                break

        return questions_data[:num_questions]

    def _get_question_text_bs4(self, question_url: str) -> str:
        """Get full question text using BeautifulSoup"""
        try:
            response = requests.get(question_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            question_body = soup.select_one('.s-prose')
            if question_body:
                # Get the HTML content and clean it
                html_content = str(question_body)
                return self._clean_html_text(html_content)
            return ""
        except Exception as e:
            print(f"Error getting question text: {e}")
            return ""

    def _get_question_text_selenium(self, driver, question_url: str) -> str:
        """Get full question text using Selenium"""
        try:
            current_url = driver.current_url
            driver.get(question_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "s-prose"))
            )
            question_body = driver.find_element(By.CLASS_NAME, "s-prose")
            # Get HTML content and clean it
            html_content = question_body.get_attribute('innerHTML')
            cleaned_text = self._clean_html_text(html_content)
            driver.get(current_url)  # Go back to the list
            return cleaned_text
        except Exception as e:
            print(f"Error getting question text: {e}")
            return ""

    def _get_author_reputation_bs4(self, question_url: str) -> Optional[int]:
        """Get author reputation using BeautifulSoup"""
        try:
            response = requests.get(question_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            rep_element = soup.select_one('.reputation-score')
            if rep_element:
                rep_text = rep_element.text.strip().replace(',', '')
                return int(rep_text) if rep_text.isdigit() else None
        except Exception as e:
            print(f"Error getting author reputation: {e}")
        return None

    def _get_author_reputation_selenium(self, driver, question_url: str) -> Optional[int]:
        """Get author reputation using Selenium"""
        try:
            current_url = driver.current_url
            driver.get(question_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "reputation-score"))
            )
            rep_element = driver.find_element(By.CLASS_NAME, "reputation-score")
            rep_text = rep_element.text.strip().replace(',', '')
            driver.get(current_url)  # Go back to the list
            return int(rep_text) if rep_text.isdigit() else None
        except Exception as e:
            print(f"Error getting author reputation: {e}")
            return None

    def save_to_json(self, questions: List[QuestionData], filename: str):
        """Save scraped data to JSON file"""
        data = []
        for q in questions:
            data.append({
                'title': q.title,
                'link': q.link,
                'text': q.text,
                'tags': q.tags,
                'author_name': q.author_name,
                'author_reputation': q.author_reputation,
                'publication_date': q.publication_date.isoformat() if q.publication_date else None
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {filename}")


def main():
    """Example usage"""
    scraper = StackOverflowScraper()
    
    # Choose method and parameters
    method = input("Choose scraping method (beautifulsoup/selenium/api): ").strip().lower()
    if method not in ['beautifulsoup', 'selenium', 'api']:
        print("Invalid method, defaulting to beautifulsoup")
        method = 'beautifulsoup'
    
    try:
        num_questions = int(input("Number of questions to scrape: "))
    except ValueError:
        print("Invalid number, defaulting to 10")
        num_questions = 10

    print(f"Starting scraping with {method} method for {num_questions} questions...")
    
    # Scrape questions
    questions = scraper.scrape_questions(method=method, num_questions=num_questions)
    
    # Display results
    print(f"\nScraped {len(questions)} questions:")
    for i, q in enumerate(questions, 1):
        print(f"\n{i}. {q.title}")
        print(f"   Author: {q.author_name} (Rep: {q.author_reputation})")
        print(f"   Tags: {', '.join(q.tags)}")
        print(f"   Date: {q.publication_date}")
        print(f"   Link: {q.link}")
        print(f"   Text preview: {q.text[:100]}...")
    
    # Save to JSON
    filename = f"stackoverflow_questions_{method}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    scraper.save_to_json(questions, filename)


if __name__ == "__main__":
    main()