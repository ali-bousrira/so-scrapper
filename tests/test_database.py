import unittest
import sqlite3
from bs4database import create_table, insert_data

class TestDatabaseInsertion(unittest.TestCase):
    def setUp(self):
        # Création d'une bdd temporaire en memoire
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        create_table(self.cursor)

    def tearDown(self):
        self.conn.close()

    def test_insert_data(self):
        fake_data = [{
            "title": "Test Question",
            "link": "https://stackoverflow.com/q/123",
            "question": "Ceci est un test",
            "tags": "python,unittest",
            "author": "Alice",
            "pub_date": None
        }]
        insert_data(self.cursor, fake_data)
        self.cursor.execute("SELECT * FROM questions")
        rows = self.cursor.fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "Test Question")  

