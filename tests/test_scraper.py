import unittest
from unittest.mock import patch, Mock
from bs4 import BeautifulSoup
from bs4scraper import scrape_page

class TestHTTPRequest(unittest.TestCase):
    @patch('bs4scraper.requests.get')
    def test_http_request_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<html><body><div class="s-post-summary"><a class="s-link" href="/q/123">Title</a></div></body></html>'
        mock_get.return_value = mock_response

        result = scrape_page(1)
        self.assertIsInstance(result, list)
        self.assertGreaterEqual(len(result), 1)


class TestHTMLParsing(unittest.TestCase):
    def test_html_parsing_extract_data(self):
        html = '''
        <div class="s-post-summary">
            <a class="s-link" href="/questions/123">Example title</a>
            <div class="s-post-summary--content-question">Extrait...</div>
            <div class="s-post-summary--meta-tags">
                <a class="post-tag">python</a>
                <a class="post-tag">web</a>
            </div>
            <div class="s-user-card--link">John Doe</div>
            <time datetime="2023-08-01T12:34:56Z"></time>
        </div>
        '''
        soup = BeautifulSoup(html, 'html.parser')
        question = soup.select_one(".s-post-summary")
        self.assertIsNotNone(question)

        title = question.select_one(".s-link").text.strip()
        tags = [tag.text for tag in question.select(".post-tag")]

        self.assertEqual(title, "Example title")
        self.assertEqual(tags, ["python", "web"])
