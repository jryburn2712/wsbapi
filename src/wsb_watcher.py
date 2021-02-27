import praw

from src import db_functions
from src.processing import SymbolTokenizer
from src.db_constants import login, reddit_login


def main():
    db = db_functions.connect_to_db(login.HOST,
                                    login.USERNAME,
                                    login.PASSWORD,
                                    'wsb')
    if db is None:
        return

    cursor = db.cursor()

    reddit = praw.Reddit(client_id=reddit_login.CLIENT_ID,
                         client_secret=reddit_login.CLIENT_SECRET,
                         user_agent=reddit_login.USER_AGENT)

    tokenizer = SymbolTokenizer()

    sub = reddit.subreddit('wallstreetbets')
    comment_stream = sub.stream.comments(pause_after=-1, skip_existing=True)
    submission_stream = sub.stream.submissions(pause_after=-1,
                                               skip_existing=True)
    while True:
        for comment in comment_stream:
            if comment is None:
                break
            parse_symbols_and_add_to_db(cursor, db, tokenizer, comment)

        for submission in submission_stream:
            if submission is None:
                break
            parse_symbols_and_add_to_db(cursor, db, tokenizer, submission)


def parse_symbols_and_add_to_db(cursor, db, tokenizer, submission):
    #  Returns dict of symbols with the sentiment of the original comment
    found_symbols = tokenizer.find_symbols(submission)
    for symbol, sentiment in found_symbols.items():
        db_functions.update_symbol_mentions(cursor, symbol, sentiment)
        if cursor.rowcount <= 0:
            db_functions.insert_symbol_to_db(cursor, symbol, sentiment)
            db_functions.create_data_table_for_symbol(cursor, symbol,
                                                      submission, sentiment)
        else:
            db_functions.add_reddit_date_mention_to_db(cursor, symbol,
                                                       submission, sentiment)

    db.commit()


if __name__ == '__main__':
    main()
