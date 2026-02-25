import unittest
from functions.a1_2.report_failed import lambda_handler


class TestLambdaHandler(unittest.TestCase):
    def test_lambda_handler(self):
        # Create sample event and context
        event = {"key": "value"}
        context = "context"

        # Call lambda_handler function with sample event and context
        result = lambda_handler(event, context)

        # Assert that the result is equal to the event
        self.assertEqual(result, event)


if __name__ == "__main__":
    unittest.main()
