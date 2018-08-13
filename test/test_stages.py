from unittest import TestCase
import json

from tweet_extractor.tweets_filter import filter_datapoint
from parsing.language_parser import LanguageParser
from sentiment.sentiment_analyer import SentimentAnalyzer


def load_jsonstring(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        return f.readline()


class TestFilter(TestCase):

    # Test data
    test_datapoint = load_jsonstring('test_datapoint.txt')

    # Expected results
    test_datapoint_filtered = load_jsonstring('test_datapoint_filtered.txt')

    def test_filter_datapoint(self):
        filtered_datapoint = filter_datapoint(TestFilter.test_datapoint)
        self.assertEqual(filtered_datapoint, TestFilter.test_datapoint_filtered)


class TestParser(TestCase):
    # Test data
    test_datapoint = load_jsonstring('test_datapoint_filtered.txt')

    # Expected results
    test_datapoint_parsed = load_jsonstring('test_datapoint_parsed.txt')

    def test_parse_datapoint(self):
        parser = LanguageParser('en')
        parsed_datapoint = parser.parse_datapoint(TestParser.test_datapoint)
        parsed_datapoint_json_obj = json.loads(parsed_datapoint)
        expected_json_obj = json.loads(TestParser.test_datapoint_parsed)

        self.assertEqual(parsed_datapoint_json_obj.keys(), expected_json_obj.keys())
        for key, value in parsed_datapoint_json_obj.items():
            if key == 'text':
                self.assertEqual(set(value.split()), set(expected_json_obj[key].split()))
            else:
                self.assertEqual(value, expected_json_obj[key])


class TestSentimentAnalyzer(TestCase):
    # Test data
    test_datapoint = load_jsonstring('test_datapoint_parsed.txt')

    # Expected results
    test_datapoint_scored = load_jsonstring('test_datapoint_scored.txt')

    def test_score_datapoint(self):
        analyzer = SentimentAnalyzer()
        parsed_datapoint = analyzer.score_datapoint(TestSentimentAnalyzer.test_datapoint)
        self.assertEqual(parsed_datapoint, TestSentimentAnalyzer.test_datapoint_scored)


