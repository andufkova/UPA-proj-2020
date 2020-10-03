import requests
import datetime
import argparse
import re
import pandas as pd

from sklearn.preprocessing import StandardScaler
from noSQL import PymongoDatabase
from sqlite import SQLite3database

#global variables and constants
BANK_BASE_URL = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/rok.txt?rok="
LAST_4_MONTHS = 4   # if no arguments is given, or argument is invalid, get last 4 months of data
SQLITE3_DB_FILE = 'sqlitedb.db'


def get_data_from_bank(arguments):
    if arguments[0].date is None:
        return get_last_4_months()
    else:
        date = arguments[0].date.split('.')
        return get_4_months(date)

def get_last_4_months():
    today = datetime.datetime.today()

    bank_url = BANK_BASE_URL + str(today.year)
    bank_currency_data = requests.get(url=bank_url).text

    if today.month < LAST_4_MONTHS:  # handle situation, when months from last year are needed
        bank_url = BANK_BASE_URL + str(today.year - 1)
        result = requests.get(url=bank_url).text
        bank_currency_data = result + "\n".join(bank_currency_data.split("\n")[1:])  # remove header from second GET call so we dont have it twice in our data

    return bank_currency_data

def get_4_months(date):
    """
    :param date: tells me which months are being requested in format [month, year] as strings
    """
    today = datetime.datetime.today()

    bank_url = BANK_BASE_URL + str(date[1])
    bank_currency_data = requests.get(url=bank_url).text

    if int(date[0]) >= 9 and int(date[1]) + 1 <= today.year:
        bank_url = BANK_BASE_URL + str(int(date[1]) + 1)
        result = requests.get(url=bank_url).text
        bank_currency_data = bank_currency_data + "\n".join(
            result.split("\n")[1:])  # remove header from second GET call so we dont have it twice in our

    return bank_currency_data

def transform_text_data_to_dictionary(data):
    """
    transforms text file data from bank server to dictionary(json), which is required by monogo database
    :param data: in form of pure text
    :return: dictionary containing all data
    """

    dict_data = {}
    currency_list = []
    currency_array_tmp = data.split('\n')[0].split('|')[1:]

    for currency in currency_array_tmp:
        currency_info = currency.split(' ')
        currency_list.append(currency_info[1])
        dict_data[currency_info[1]] = [('amount', currency_info[0])]

    for key in dict_data:
        key_index = 0
        for currency_column in bank_data.split('\n')[0].split('|'):
            if key in currency_column:
                for line in bank_data.split('\n')[1:-1]:
                    if "Datum" in line: # for some reason, for some months bank does not provide data for same currencies... skip those data
                        return dict_data
                    line_split = line.split('|')
                    dict_data[key].append((line_split[0], line_split[key_index]))
                break
            else:
                key_index = key_index + 1

    return dict_data


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--date', metavar='T', type=str, help='select first of 4 months (format MM.YYYY) to select data from')
    args = parser.parse_known_args()

    if args[0].date is None:
        return args
    else:
        date = args[0].date.strip()
        if re.match(r"^[0]*[1-9][0-2]*\.[1-2][0-9]{3}$", date):
            today = datetime.datetime.today()
            date = date.split('.')
            if 1 <= int(date[0]) <= 12 and int(date[1]) <= today.year:
                pass
            else:
                args[0].date = None

    return args

def load_to_pandas(raw_data):
    """
    transforms raw file data from mongo database to pandas dataframe, specifies data formats (date and float)
    :param raw_data: data from noSQL DB
    :return: data in pandas dataframe
    """
    df = pd.DataFrame()
    for coll in raw_data:
        for currency, values in coll.items():
            print("... loading ({})".format(currency))
            amount = 0
            if currency != '_id':
                for record in values:
                    if (record[0] == 'amount'):
                        amount = record[1]
                    else:
                        val = float(record[1].replace(',', '.')) / int(amount)
                        df = df.append({'curr': currency, 'date': record[0], 'value': val}, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        return(df)

def prepare_task_1_3(sqlite, df):
    """
    saves data from pandas dataframe to SQLite3
    :param sqlite: SQLite3 DB connection
    :param df: dataframe with data
    """
    q_drop_existing_table = '''DROP TABLE IF EXISTS task_1_3'''
    sqlite.execute_query(q_drop_existing_table)
    sqlite.df_to_sql(df, 'task_1_3')

def prepare_task_2(sqlite, df, selected_curr):
    """
    scales data and saves them from pandas dataframe to SQLite3
    :param sqlite: SQLite3 DB connection
    :param df: dataframe with data
    :param selected_curr: currency chosen to examine
    """
    df2 = df.loc[df['curr'] == selected_curr]
    q_drop_existing_table = '''DROP TABLE IF EXISTS task_2'''
    sqlite.execute_query(q_drop_existing_table)

    df2 = df2.copy(deep=True)
    sc = StandardScaler()
    df2['value'] = sc.fit_transform(df2[["value"]])

    sqlite.df_to_sql(df2, 'task_2')


if __name__ == "__main__":
    args = parse_args()

    bank_data = get_data_from_bank(args)
    bank_data = transform_text_data_to_dictionary(bank_data)

    pymongo = PymongoDatabase()
    pymongo.insert_currency_data(bank_data)

    raw_data = pymongo.read_currency_data()
    df = load_to_pandas(raw_data)

    sqlite = SQLite3database(SQLITE3_DB_FILE)
    prepare_task_1_3(sqlite, df)
    # TODO: how to choose currency for task 2? Maybe in args?
    prepare_task_2(sqlite, df, 'EUR')


