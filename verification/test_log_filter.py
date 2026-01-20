import logging
import unittest


# Define the filter class essentially as it is in server.py to test the logic
class EndpointAccessFilter(logging.Filter):
    """
    Filter to suppress successful access logs for specific endpoints (e.g., /stats polling).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            log_msg = record.getMessage()
            if "GET /bot/" in log_msg and "/stats" in log_msg and "200" in log_msg:
                return False
            return True
        except Exception:
            return True


class TestLogFilter(unittest.TestCase):
    def setUp(self):
        self.filter = EndpointAccessFilter()
        self.logger = logging.getLogger("test_logger")
        self.logger.setLevel(logging.INFO)
        # Clear existing handlers
        self.logger.handlers = []

        # Create a capturing handler
        self.log_capture_string = io.StringIO()
        self.handler = logging.StreamHandler(self.log_capture_string)
        self.handler.addFilter(self.filter)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)

    def test_filter_spam_logs(self):
        # Retrieve the logger again to ensure we use the one with the handler
        logger = logging.getLogger("test_logger")

        # 1. Spam Log - Should be filtered
        logger.info('127.0.0.1:48882 - "GET /bot/243/stats HTTP/1.1" 200 OK')

        # 2. Normal Log - Should function
        logger.info('127.0.0.1:48882 - "POST /start HTTP/1.1" 200 OK')

        # 3. Error Log (even if stats) - Should function (though 200 check handles this,
        # let's verify a non-200 stats call passes effectively)
        logger.info('127.0.0.1:48882 - "GET /bot/243/stats HTTP/1.1" 500 Internal Server Error')

        log_contents = self.log_capture_string.getvalue()

        # Assertions
        self.assertNotIn('GET /bot/243/stats HTTP/1.1" 200', log_contents, "Spam log was not filtered")
        self.assertIn("POST /start", log_contents, "Normal log was filtered")
        self.assertIn("500 Internal Server Error", log_contents, "Error log was filtered")

        print("Verification passed: Spam logs filtered, others preserved.")


import io

if __name__ == "__main__":
    unittest.main()
