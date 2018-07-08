

class LanguageParser:

    def __init__(self):
        pass

    def parse(self, text):
        tokens = []

        # NLP parsing code to be added here

        return tokens

    def parse_tweets(self, input_file):

        # Assumes that the input_file has .json extension
        output_file = input_file[:-5] + '_parsed.json'

        # Parse the tweet in each line of input file and write the result to an output file


if __name__ == '__main__':
    filtered_twitter_data_file = 'ADD FILE PATH HERE'

    # Create parser
    parser = LanguageParser()

    # Parse the tweets in a file and write to output file
    parser.parse_tweets(filtered_twitter_data_file)
