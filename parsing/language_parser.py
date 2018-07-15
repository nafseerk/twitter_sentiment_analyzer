from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords as nltk_stopwords
from parsing.stopwords import STOPWORDS
import json
import re


class LanguageParser:

    def __init__(self, lang):

        # Set the language for the parser. Currently only support a single language
        self.language = lang

        # Set stopwords
        self.stopwords = set(nltk_stopwords.words('english')).union(STOPWORDS)

        # Tokenizer
        self.tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)

    def is_valid_token(self, token):

        return (token not in self.stopwords) and \
               (len(token) > 1) and \
               (token[0] != '#') and \
               (not re.compile(r"http\S+", ).match(token))

    def parse(self, text):

        # Remove whitespaces
        text = text.strip()

        # Tokenize the text
        tokens = self.tokenizer.tokenize(text)

        # Filter valid tokens
        tokens = [token for token in tokens if self.is_valid_token(token)]

        # Eliminate duplicates
        tokens = list(set(tokens))

        return ' '.join(tokens)

    def parse_tweets(self, input_file):

        # Assumes that the input_file has .json extension
        output_file = input_file[:-5] + '_parsed.json'

        # Parse the tweet in each line of input file and write the result to an output file
        with open(output_file, 'w', encoding='utf-8') as out_file:

            parsed_count = 0
            for i, line in enumerate(open(input_file, 'r', encoding='utf-8')):
                
                # Convert string to JSON object
                line_json = json.loads(line)

                if line_json['lang'] == 'en':
                    parsed_text = self.parse(line_json['text'])
                    line_json['text'] = parsed_text

                    out_file.write(json.dumps(line_json))
                    out_file.write('\n')
                    parsed_count += 1

                    if parsed_count % 100 == 0:
                        print('Parsed {0} datapoints from {1} datapoints'.format(parsed_count, i + 1))

        print('Parsed {0} datapoints from {1} datapoints'.format(parsed_count, i + 1))


if __name__ == '__main__':
    filtered_data_file = '/Users/apple/twitter_data/FIFA2018/2018-07-09_historic_filtered.json'

    # Create parser
    parser = LanguageParser('en')

    # Parse the tweets in a file and write to output file
    parser.parse_tweets(filtered_data_file)
