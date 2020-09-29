import requests
import datetime

from noSQL import PymongoDatabase

#global variables and constants
BANK_BASE_URL = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/rok.txt?rok="
LAST_4_MONTHS = 4  # according to UPA project description, we should be getting last 4 months of data


def get_data_from_bank():
    today = datetime.datetime.today()

    bank_url = BANK_BASE_URL + str(today.year)
    result = requests.get(url=bank_url)
    bank_currency__data = result.text

    if today.month < LAST_4_MONTHS:     # handle situation, when months from last year are needed
        bank_url = BANK_BASE_URL + str(today.year - 1)
        result = requests.get(url=bank_url).text
        bank_currency__data = result + "\n".join(bank_currency__data.split("\n")[1:])   # remove header from second GET call so we dont have it twice in our data

    return bank_currency__data

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
        currency_id = currency.split(' ')[1]
        currency_list.append(currency_id)
        dict_data[currency_id] = []

    for key in dict_data:
        key_index = 0
        for currency_column in bank_data.split('\n')[0].split('|'):
            if key in currency_column:
                for line in bank_data.split('\n')[1:-1]:
                    line_split = line.split('|')
                    dict_data[key].append((line_split[0], line_split[key_index]))
                break
            else:
                key_index = key_index + 1

    return dict_data

if __name__ == "__main__":
    bank_data = None
    bank_data = get_data_from_bank()

    bank_data = transform_text_data_to_dictionary(bank_data)

    pymongo = PymongoDatabase()
    pymongo.insert_currency_data(bank_data)
