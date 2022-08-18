from pymongo import MongoClient

#Mongodb connection parameters
CONNECTION_STRING = "mongodb://localhost:27017"

class MongoConnector():

    def __init__(self,dbname='iot'):
        self.client = MongoClient(CONNECTION_STRING)
        self.db = self.client[dbname]
        self.collection = None
    
    def get_database(self):
        return self.db
    
    def create_or_set_collection(self,c_name):
        self.collection = self.db[c_name]
    
    def insert_document(self,document):
        document = self.collection.insert_one(document)
        return document
    
    def get_one_document(self,filter):
        document = self.collection.find_one(filter)
        if document:
            return document
        else:
            return None


