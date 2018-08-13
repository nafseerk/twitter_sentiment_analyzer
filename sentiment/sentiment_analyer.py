import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class SentimentAnalyzer:

    def __init__(self):
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

        # Language supported for sentiment analysis
        self.supported_languages = ['en']

    # Check if the language is supported by the Sentiment Analyzer
    def is_language_supported(self, lang):
        return lang in self.supported_languages

    # Return the sentiment score of the given text in the language
    def get_sentiment_score(self, text, lang):

        if not self.is_language_supported(lang):
            print('Error: Language {0} not supported'.format(lang))
            return None

        sentiment_score = self.sentiment_analyzer.polarity_scores(text)['compound']
        
        if sentiment_score < 0:
            return -1
        elif sentiment_score > 0:
            return 1
        else:
            return 0

    def score_datapoint(self, datapoint):

        if datapoint is None:
            return None

        # Convert datapoint to JSON object
        data_json = json.loads(datapoint)

        # Get the score of the datapoint if possible
        score = None
        if data_json and 'text' in data_json and 'lang' in data_json:
            score = self.get_sentiment_score(data_json['text'], data_json['lang'])

        output = (str(data_json['id']) + ',' +
                  str(score) + ',' +
                  str(data_json['latitude']) + ',' +
                  str(data_json['longitude']) + ',' +
                  str(data_json['timestamp'])
                  )

        return output

    # Score all the datapoints in the given file and save to output file
    def score_datapoints(self, input_file):

        # Assumes that the input_file has .json extension
        output_file = input_file[:-5] + '_scored.csv'

        with open(output_file, 'w') as out_file:

            # Write file header
            out_file.write('id,score,latitude,longitude,timestamp\n')

            scored_count = 0
            for i, line in enumerate(open(input_file, 'r', encoding='utf-8')):
                # Convert string to JSON object
                line_json = json.loads(line)

                # Get the score of the datapoint if possible
                if line_json and 'text' in line_json and 'lang' in line_json:
                    score = self.get_sentiment_score(line_json['text'], line_json['lang'])
                    if score is not None:
                        out_file.write(str(line_json['id']) + ',' +
                                       str(score) + ',' +
                                       str(line_json['latitude']) + ',' +
                                       str(line_json['longitude']) + ',' +
                                       str(line_json['timestamp']) + '\n'
                                       )
                        scored_count += 1
                        
                        if scored_count % 100 == 0:
                            print('Scored {0} datapoints from {1} datapoints'.format(scored_count, i+1))

        print('Scored {0} datapoints from {1} datapoints'.format(scored_count, i+1))


if __name__ == '__main__':
    parsed_data_file = '/Users/apple/twitter_data/FIFA2018/2018-07-09_historic_filtered_parsed.json'

    # Create Sentiment Analyzer
    sentiment_analyzer = SentimentAnalyzer()

    # Parse the tweets in a file and write to output file
    sentiment_analyzer.score_datapoints(parsed_data_file)
