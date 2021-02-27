from praw.reddit import Submission
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class SymbolTokenizer:

    def __init__(self):
        self._load_symbol_array()
        self._sent_analyzer = SentimentAnalyzer()

    def _load_symbol_array(self):
        with open('symbols.txt') as f:
            self._symbols = [line.rstrip() for line in f]

    def find_symbols(self, submission):
        symbols_found = {}
        # Check submission (title or comment) for ticker symbols
        if isinstance(submission, (Submission,)):
            text = submission.title + ' ' + submission.selftext
        else:
            text = submission.body
        sentiment_score = self._sent_analyzer.get_compound_sentiment_score(text)
        for word in text.split():
            word = self._replace_dollar_sign(word)
            #  Only count each symbol once per comment to avoid spam
            if word not in symbols_found and self._is_symbol(word):
                symbols_found[word] = sentiment_score

        return symbols_found

    @staticmethod
    def _replace_dollar_sign(word):
        return word[1:] if word[0] == '$' else word

    @staticmethod
    def _correct_symbol_length(word):
        return 5 >= len(word) > 1

    def _is_symbol(self, word):
        # ignore = ['for', 'FOR', 'of', 'OF', 'DD', 'and', 'AND']
        return word in self._symbols  # and word not in ignore

    @property
    def symbols(self):
        return self._symbols


class SentimentAnalyzer:

    def __init__(self):
        nltk.download('vader_lexicon')
        self._sia = SentimentIntensityAnalyzer()

    def get_submission_sentiment(self, text):
        return self._sia.polarity_scores(text)

    def get_compound_sentiment_score(self, text):
        return self._sia.polarity_scores(text)['compound']

