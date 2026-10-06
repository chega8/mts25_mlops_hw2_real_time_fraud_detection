import unittest
from unittest.mock import patch
from datetime import datetime
from streamlit.testing.v1 import AppTest

class InterfaceTests(unittest.TestCase):
    def test_results_button(self):
        app = AppTest.from_file('interface/app.py', default_timeout=30).run()
        self.assertFalse(app.exception)
        with patch('common.database.results', return_value=([('fraud-001',0.99,1,datetime.now())],[0,0.1,0.5,1],4)):
            app.button[1].click().run()
        self.assertFalse(app.exception)
        self.assertIn('fraud-001',app.dataframe[1].value.transaction_id.values)

    def test_empty_results(self):
        app = AppTest.from_file('interface/app.py', default_timeout=30).run()
        with patch('common.database.results', return_value=([],[],0)):
            app.button[1].click().run()
        self.assertFalse(app.exception)
        self.assertEqual(len(app.info),2)

if __name__ == '__main__':
    unittest.main()
