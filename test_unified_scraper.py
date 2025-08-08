import unittest
from unittest.mock import Mock, patch, MagicMock
import requests
from datetime import datetime
import json
from unified_scraper import StackOverflowScraper, QuestionData


class TestTextCleaning(unittest.TestCase):
    """Test HTML text cleaning functionality"""
    
    def setUp(self):
        self.scraper = StackOverflowScraper()
    
    def test_clean_simple_html(self):
        """Test basic HTML tag removal"""
        html_text = "<p>This is a simple paragraph.</p>"
        result = self.scraper._clean_html_text(html_text)
        self.assertEqual(result, "This is a simple paragraph.")
    
    def test_clean_complex_html(self):
        """Test complex HTML with multiple tags"""
        html_text = """
        <div>
            <p>This is a <strong>question</strong> about Python.</p>
            <p>I need help with <em>list comprehensions</em>.</p>
        </div>
        """
        result = self.scraper._clean_html_text(html_text)
        expected = "This is a question about Python. I need help with list comprehensions ."
        self.assertEqual(result, expected)
    
    def test_preserve_code_blocks(self):
        """Test that code blocks are preserved with markdown formatting"""
        html_text = """
        <p>Here's my code:</p>
        <pre><code>def hello():
    print("Hello World")</code></pre>
        <p>What's wrong with it?</p>
        """
        result = self.scraper._clean_html_text(html_text)
        self.assertIn("```", result)
        self.assertIn("def hello():", result)
    
    def test_clean_empty_text(self):
        """Test cleaning empty or None text"""
        self.assertEqual(self.scraper._clean_html_text(""), "")
        self.assertEqual(self.scraper._clean_html_text(None), "")
    
    def test_clean_whitespace(self):
        """Test whitespace normalization"""
        html_text = "<p>Text   with    multiple     spaces</p>"
        result = self.scraper._clean_html_text(html_text)
        self.assertEqual(result, "Text with multiple spaces")


class TestStackOverflowScraperMethods(unittest.TestCase):
    """Test individual scraping methods with mocked responses"""
    
    def setUp(self):
        self.scraper = StackOverflowScraper()
    
    @patch('requests.get')
    def test_beautifulsoup_scraping_success(self, mock_get):
        """Test BeautifulSoup scraping with mocked response"""
        # Mock the list page response
        mock_list_response = Mock()
        mock_list_response.status_code = 200
        mock_list_response.text = """
        <div class="s-post-summary">
            <h3><a class="s-link" href="/questions/123/test-question">Test Question</a></h3>
            <div class="s-post-summary--content-excerpt">This is a test question</div>
            <div class="s-post-summary--meta-tags">
                <span class="post-tag">python</span>
                <span class="post-tag">testing</span>
            </div>
            <div class="s-user-card--link">TestUser</div>
            <time datetime="2024-01-01T10:00:00Z"></time>
        </div>
        """
        
        # Mock the question detail page response
        mock_detail_response = Mock()
        mock_detail_response.status_code = 200
        mock_detail_response.text = """
        <div class="s-prose">
            <p>This is the full question text with <strong>formatting</strong>.</p>
            <pre><code>print("Hello World")</code></pre>
        </div>
        <div class="reputation-score">1500</div>
        """
        
        mock_get.side_effect = [mock_list_response, mock_detail_response, mock_detail_response]
        
        # Test scraping
        questions = self.scraper._scrape_with_beautifulsoup(num_questions=1, delay=0)
        
        self.assertEqual(len(questions), 1)
        question = questions[0]
        self.assertEqual(question.title, "Test Question")
        self.assertIn("python", question.tags)
        self.assertEqual(question.author_name, "TestUser")
    
    @patch('requests.get')
    def test_api_scraping_success(self, mock_get):
        """Test API scraping with mocked response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'items': [{
                'title': 'API Test Question',
                'link': 'https://stackoverflow.com/questions/123/api-test',
                'body': '<p>This is the <strong>API question</strong> body.</p>',
                'tags': ['python', 'api'],
                'owner': {
                    'display_name': 'API User',
                    'reputation': 2000
                },
                'creation_date': 1640995200  # 2022-01-01
            }],
            'has_more': False
        }
        mock_get.return_value = mock_response
        
        questions = self.scraper._scrape_with_api(num_questions=1)
        
        self.assertEqual(len(questions), 1)
        question = questions[0]
        self.assertEqual(question.title, 'API Test Question')
        self.assertEqual(question.author_name, 'API User')
        self.assertEqual(question.author_reputation, 2000)
        self.assertIn('python', question.tags)
    
    @patch('unified_scraper.webdriver.Chrome')
    def test_selenium_scraping_setup(self, mock_chrome):
        """Test Selenium driver setup"""
        mock_driver = Mock()
        mock_chrome.return_value = mock_driver
        mock_driver.find_elements.return_value = []  # No questions found
        
        questions = self.scraper._scrape_with_selenium(num_questions=1, headless=True)
        
        mock_chrome.assert_called_once()
        mock_driver.quit.assert_called_once()
        self.assertEqual(len(questions), 0)


class TestScraperIntegration(unittest.TestCase):
    """Integration tests for the complete scraper workflow"""
    
    def setUp(self):
        self.scraper = StackOverflowScraper()
    
    def test_method_selection(self):
        """Test that method selection works correctly"""
        with patch.object(self.scraper, '_scrape_with_beautifulsoup') as mock_bs:
            mock_bs.return_value = []
            self.scraper.scrape_questions(method="beautifulsoup", num_questions=5)
            mock_bs.assert_called_once_with(5)
        
        with patch.object(self.scraper, '_scrape_with_selenium') as mock_sel:
            mock_sel.return_value = []
            self.scraper.scrape_questions(method="selenium", num_questions=5)
            mock_sel.assert_called_once_with(5)
        
        with patch.object(self.scraper, '_scrape_with_api') as mock_api:
            mock_api.return_value = []
            self.scraper.scrape_questions(method="api", num_questions=5)
            mock_api.assert_called_once_with(5)
    
    def test_invalid_method(self):
        """Test error handling for invalid methods"""
        with self.assertRaises(ValueError):
            self.scraper.scrape_questions(method="invalid_method", num_questions=5)
    
    def test_save_to_json(self):
        """Test JSON saving functionality"""
        test_questions = [
            QuestionData(
                title="Test Question",
                link="https://example.com",
                text="Test text",
                tags=["python", "test"],
                author_name="TestUser",
                author_reputation=1000,
                publication_date=datetime(2024, 1, 1, 12, 0, 0)
            )
        ]
        
        filename = "test_output.json"
        self.scraper.save_to_json(test_questions, filename)
        
        # Verify file was created and contains correct data
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], "Test Question")
        self.assertEqual(data[0]['tags'], ["python", "test"])
        self.assertEqual(data[0]['author_reputation'], 1000)
        
        # Clean up
        import os
        os.remove(filename)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in various scenarios"""
    
    def setUp(self):
        self.scraper = StackOverflowScraper()
    
    @patch('requests.get')
    def test_network_error_handling(self, mock_get):
        """Test handling of network errors"""
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        questions = self.scraper._scrape_with_beautifulsoup(num_questions=1, delay=0)
        self.assertEqual(len(questions), 0)
    
    @patch('requests.get')
    def test_api_error_handling(self, mock_get):
        """Test handling of API errors"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server error")
        mock_get.return_value = mock_response
        
        questions = self.scraper._scrape_with_api(num_questions=1)
        self.assertEqual(len(questions), 0)
    
    def test_text_extraction_error_handling(self):
        """Test error handling in text extraction methods"""
        # Test with invalid URL
        result = self.scraper._get_question_text_bs4("invalid_url")
        self.assertEqual(result, "")
        
        result = self.scraper._get_author_reputation_bs4("invalid_url")
        self.assertIsNone(result)


class TestDataValidation(unittest.TestCase):
    """Test data validation and formatting"""
    
    def setUp(self):
        self.scraper = StackOverflowScraper()
    
    def test_question_data_structure(self):
        """Test QuestionData structure and types"""
        question = QuestionData(
            title="Test Title",
            link="https://example.com",
            text="Test text",
            tags=["tag1", "tag2"],
            author_name="Test Author",
            author_reputation=1500,
            publication_date=datetime.now()
        )
        
        self.assertIsInstance(question.title, str)
        self.assertIsInstance(question.link, str)
        self.assertIsInstance(question.text, str)
        self.assertIsInstance(question.tags, list)
        self.assertIsInstance(question.author_name, str)
        self.assertIsInstance(question.author_reputation, int)
        self.assertIsInstance(question.publication_date, datetime)
    
    def test_empty_data_handling(self):
        """Test handling of empty or missing data"""
        question = QuestionData(
            title="",
            link="",
            text="",
            tags=[],
            author_name="Unknown",
            author_reputation=None,
            publication_date=None
        )
        
        self.assertEqual(question.title, "")
        self.assertEqual(question.tags, [])
        self.assertIsNone(question.author_reputation)
        self.assertIsNone(question.publication_date)


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)