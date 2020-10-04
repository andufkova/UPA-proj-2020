import pymongo

class PymongoDatabase:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.database = self.client['database']
        self.currency_collection = self.database['currency_data']

    def insert_currency_data(self, data):
        self.currency_collection.drop()     # cleanup any data from previous runs
        self.currency_collection.insert_one(data)

    def read_currency_data(self):
        currency_collection = self.currency_collection.find()
        return currency_collection

    def test(self): # debug print for python to show the DB element
        print(self.currency_collection.find_one())
