import requests
import datetime
import argparse
import re
import pandas as pd
import dateutil
import math

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
    bank_currency_data = [requests.get(url=bank_url).text]

    if today.month < LAST_4_MONTHS:  # handle situation, when months from last year are needed
        bank_url = BANK_BASE_URL + str(today.year - 1)
        result = requests.get(url=bank_url).text
        bank_currency_data.append(result)

    return bank_currency_data

def get_4_months(date):
    """
    :param date: tells me which months are being requested in format [month, year] as strings
    """

    bank_url = BANK_BASE_URL + str(date[1])
    bank_currency_data = [requests.get(url=bank_url).text]

    if int(date[0]) < LAST_4_MONTHS:
        bank_url = BANK_BASE_URL + str(int(date[1]) - 1)
        result = requests.get(url=bank_url).text
        bank_currency_data.append(result)

    return bank_currency_data

def transform_text_data_to_dictionary(data):
    """
    transforms text file data from bank server to dictionary(json), which is required by monogo database
    :param data: in form of pure text in list
    :return: dictionary containing all data
    """

    processed_data = []
    for currency_year in data:
        for new_block in ["Datum" + block for block in currency_year.split("Datum") if block]:
            processed_data.append(new_block)

    dict_data = {}

    for currency_block in processed_data:
        currency_array_tmp = currency_block.split('\n')[0].split('|')[1:]

        for currency in currency_array_tmp:
            currency_info = currency.split(' ')
            if currency_info[1] not in dict_data:
                dict_data[currency_info[1]] = [('amount', currency_info[0])]

        for key in dict_data:
            key_index = 0
            for currency_column in currency_block.split('\n')[0].split('|'):
                if key in currency_column:
                    for line in currency_block.split('\n')[1:-1]:
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

def load_to_pandas(raw_data, args):
    """
    transforms raw file data from mongo database to pandas dataframe, specifies data formats (date and float)
    :param raw_data: data from noSQL DB
    :return: data in pandas dataframe
    """
    df = pd.DataFrame()
    for coll in raw_data:
        for currency, values in coll.items():
            #print("... loading ({})".format(currency))
            amount = 0
            if currency != '_id':
                for record in values:
                    if (record[0] == 'amount'):
                        amount = record[1]
                    else:
                        val = float(record[1].replace(',', '.')) / int(amount)
                        db_date = datetime.datetime.strptime(record[0], '%d.%m.%Y')

                        args_date_start = datetime.datetime.today() - dateutil.relativedelta.relativedelta(months=4)
                        args_date_end = datetime.datetime.today()

                        if args[0].date is not None:
                            args_date_start = datetime.datetime.strptime('01.'+args[0].date, '%d.%m.%Y')
                            args_date_end = args_date_start + dateutil.relativedelta.relativedelta(months=4)

                        if (db_date >= args_date_start) and (db_date <= args_date_end):
                            df = df.append({'curr': currency, 'date': record[0], 'value': val}, ignore_index=True)

        df["date"] = pd.to_datetime(df["date"], format='%d.%m.%Y')
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

def get_currancy_names(prep_db, tab_name):
    List = list()
    cur = prep_db.connection.cursor()

    qry_get_all_currency_names = '''
    SELECT DISTINCT curr
    FROM ''' + tab_name + ''';'''

    cur.execute(qry_get_all_currency_names)
    rows = cur.fetchall()
    for row in rows:
        List.append(row[0])
    return List

def execute_query_A1(prep_db, tab_name):
    """
    executes query on database that will print priority list based on (increase/decrease) rate of currency
    :prep_db: SQLite3 DB connection
    :tab_name: name of table that is prepared for queries
    """
    cur = prep_db.connection.cursor()
    Dict = {}

    names = get_currancy_names(prep_db, tab_name)

    

    for item in names:
        qry_get_first = '''
        SELECT value
        FROM '''+ tab_name +'''
        WHERE curr = "'''+ item +'''"
        ORDER BY date ASC
        LIMIT 1;
        '''

        cur.execute(qry_get_first)
        first = cur.fetchall()[0][0]

        qry_get_last = '''
        SELECT value
        FROM '''+ tab_name +'''
        WHERE curr = "'''+ item +'''"
        ORDER BY date DESC
        LIMIT 1;
        '''

        cur.execute(qry_get_last)
        last = cur.fetchall()[0][0]

        result = (last - first)/(first/100)
        Dict.update({item : result})

    Dict = {k: v for k, v in sorted(Dict.items(), key=lambda item: item[1], reverse = True)}
    print_query_A1(Dict)

def print_query_A1(Dict):
    """
    prints out results of query A1
    :Dict: dictionary with resulting values of currency rates
    """
    counter = 1
    print("### Priority list of currency rates (query A1) ###")
    for item in Dict: 
        print(str(counter) + ". "+ item + ", changed by: "+ str(Dict[item]) +" %")
        counter += 1
    print("")

def execute_query_A2(prep_db, tab_name):
    """
    executes query on database that will print fluctuation in currencies 
    :prep_db: SQLite3 DB connection
    :tab_name: name of table that is prepared for queries
    """
    cur = prep_db.connection.cursor()
    Dict = {}
    List = list()
    names = get_currancy_names(prep_db, tab_name)



    for name in names:

        qry_get_all_curr = '''
        SELECT value
        FROM '''+ tab_name +'''
        WHERE curr = "'''+ name +'''";
        '''

        cur.execute(qry_get_all_curr)
        res = cur.fetchall()

        for item in res:
            List.append(item[0])

        n_vals = len(res)
        x_avg = sum_list(List) / len(List)

        sum_pow = 0
        for value in List:
            sum_pow = math.pow(value - x_avg, 2)

        Dict.update({name: (math.sqrt((1/(n_vals-1))*sum_pow))})

    Dict = Dict = {k: v for k, v in sorted(Dict.items(), key=lambda item: item[1], reverse = False)}
    print_query_A2(Dict)

def sum_list(List):
    sum = 0
    for value in List:
        sum += value
    return sum

def print_query_A2(Dict):
    """
    prints out results of query A1
    :Dict: dictionary with resulting values of currency rates
    """
    print("### Standard deviation of currency rates (query A2) ###")
    for item in Dict: 
        print(item + ", standard deviation : "+ str(Dict[item]))


if __name__ == "__main__":
    args = parse_args()

    bank_data = get_data_from_bank(args)
    bank_data = transform_text_data_to_dictionary(bank_data)

    pymongo = PymongoDatabase()
    pymongo.insert_currency_data(bank_data)

    raw_data = pymongo.read_currency_data()
    df = load_to_pandas(raw_data, args)

    sqlite = SQLite3database(SQLITE3_DB_FILE)
    prepare_task_1_3(sqlite, df)
    # TODO: how to choose currency for task 2? Maybe in args?
    prepare_task_2(sqlite, df, 'EUR')

    execute_query_A1(sqlite, "task_1_3")
    execute_query_A2(sqlite,"task_1_3")


