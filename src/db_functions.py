import mysql.connector
from mysql.connector import Error
from praw.reddit import Submission

from src.db_constants import wsb, symbols, companies


def insert_symbol_to_db(cursor, symbol, sentiment):
    sql_insert = f"INSERT INTO {wsb.TABLE_NAME} ({wsb.SYMBOL}, " \
        f"{wsb.MENTION_COUNT}, " \
        f"{wsb.SENTIMENT_SUM}, " \
        f"{wsb.COMPANY_ID}) " \
        f"VALUES ('{symbol}', 1, {float(sentiment)}, " \
        f"(SELECT {companies.ID} FROM {companies.TABLE_NAME} " \
        f"WHERE {companies.SYMBOL} = '{symbol}'));"

    try:
        cursor.execute(sql_insert)
    except Error as e:
        print(e)
        print(f'Error inserting symbol into database: {symbol}')
        print(sql_insert)


def update_symbol_mentions(cursor, symbol, sentiment):
    sql_update = f"UPDATE {wsb.TABLE_NAME} SET " \
        f"{wsb.MENTION_COUNT} = {wsb.MENTION_COUNT} + 1, " \
        f"{wsb.SENTIMENT_SUM} = {wsb.SENTIMENT_SUM} + {float(sentiment)} " \
        f"WHERE {wsb.SYMBOL} = '{symbol}' " \
        f"AND {wsb.DATE} = CAST(CURRENT_TIMESTAMP AS Date);"

    try:
        cursor.execute(sql_update)
    except Error as e:
        print(f'Error updating symbol mention count: {symbol}')


def create_data_table_for_symbol(cursor, symbol, submission, sentiment):
    statement = f"CREATE TABLE IF NOT EXISTS _{symbol} (" \
        f"{symbols.ID} int(11) NOT NULL auto_increment, " \
        f"{symbols.REDDIT_DATE} varchar(50) NOT NULL, " \
        f"{symbols.USERNAME} varchar(50) NOT NULL, " \
        f"{symbols.SUBMISSION_ID} varchar(100) NOT NULL, " \
        f"{symbols.SUBMISSION_TYPE} varchar(15) NOT NULL, " \
        f"{symbols.SENTIMENT} float(53) NOT NULL, " \
        f"PRIMARY KEY ({symbols.ID})" \
        f");"
    try:
        cursor.execute(statement)
    except Error as e:
        print(f'Error creating table for symbol: {symbol}')

    add_reddit_date_mention_to_db(cursor, symbol, submission, sentiment)


def add_reddit_date_mention_to_db(cursor, symbol, submission, sentiment):
    submission_type = 'post' if isinstance(submission, Submission) else 'comment'
    statement = f"INSERT INTO _{symbol} ({symbols.REDDIT_DATE}, " \
        f"{symbols.USERNAME}, " \
        f"{symbols.SUBMISSION_ID}, " \
        f"{symbols.SUBMISSION_TYPE}, " \
        f"{symbols.SENTIMENT}) " \
        f"VALUES ('{submission.created_utc}', " \
        f"'{submission.author.name}', " \
        f"'{submission.id}', " \
        f"'{submission_type}', " \
        f"{float(sentiment)}" \
        f");"
    try:
        cursor.execute(statement)
    except Error as e:
        print(f'Error adding reddit date to {symbol} table.')
        print(e)
        print(statement)


def connect_to_db(host, user, password, database):
    connection = None
    try:
        connection = mysql.connector.connect(host=host,
                                             user=user,
                                             passwd=password,
                                             database=database)

        print("Successfully connected to DB.")
    except Error as e:
        print(f'Error: {e}')

    return connection
