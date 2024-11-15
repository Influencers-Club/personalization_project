import os
import time
from pymongo import MongoClient
from app.logger_manager import CustomLogger


def get_mongo_uri():
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT", 27010)
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASSWORD")
    mongo_uri = f"mongodb://{host}:{port}"
    if user:
        auth = user
        if password:
            auth = f"{user}:{password}"
        mongo_uri = f"mongodb://{auth}@{host}:{port}/"

    return mongo_uri


class MongoDbApi:

    def __init__(self,
                 host="",
                 db="test",
                 # ToDo change the name of the collection to your collection
                 scraper_profile="scraper_profile",
                 scraper_all="scraper_all",
                 read_preference="?readPreference=secondary&maxStalenessSeconds=120000"):

        self.logger = CustomLogger("Mongo")
        self.host = host if host else get_mongo_uri()
        self.db_name = db
        self.scraper_profile = scraper_profile
        self.scraper_all = scraper_all
        self.read_preference = read_preference

    def fill_db_names(self, db, collection):
        if not db:
            db = self.db_name
        if not collection:
            collection = self.scraper_profile
        return db, collection

    def insert_one(self, data={}, db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        with MongoClient(host=self.host, document_class=dict) as client:
            res = client[db][collection].insert_one(document=data)
            return res

    def insert_many(self, lst_data=[], db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        if lst_data:
            with MongoClient(host=self.host, document_class=dict) as client:
                res = client[db][collection].insert_many(documents=lst_data, ordered=False)
                return res

    def update_one(self, _id=None, data=None, db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        if _id and data:
            with MongoClient(host=self.host, document_class=dict) as client:
                res = client[db][collection].update_one({"_id": _id}, {"$set": data})
                return res

    def update_many(self, filter, field, value, db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        with MongoClient(host=self.host, document_class=dict) as client:
            while True:
                try:
                    res = client[db][collection].update_many(filter, {"$set": {field: value}})
                    break
                except Exception as err:
                    self.logger.error(err)
                    time.sleep(10)

            return res

    def upsert_many(self, lst_data=[], column="", db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        update_cnt = 0
        credentials = [x.get(column) for x in lst_data]
        column_not_in_db, column_in_db, _id_and_column_in_db = self.find_in_db_and_not_in_db(
            column_values=credentials, column=column, db=db, collection=collection)
        not_in_db = [x for x in lst_data if x.get(column) in column_not_in_db]
        in_db = [x for x in lst_data if x.get(column) not in column_not_in_db]
        insert_cnt = len(not_in_db)
        self.insert_many(lst_data=not_in_db, db=db, collection=collection)
        for obj in in_db:
            update_cnt += 1
            self.update_one(_id=_id_and_column_in_db.get(obj.get(column)), data=obj, db=db, collection=collection)
        return insert_cnt, update_cnt

    def append_element_to_a_list(self, element, column, _id, db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        if element and _id:
            with MongoClient(host=self.host, document_class=dict) as client:
                client[db][collection].update_one({"_id": _id},
                                                  {"$push": {column: element}})

    def find_one(self, column_name="", column_value="", db=None, collection=None, data={}):
        db, collection = self.fill_db_names(db, collection)
        with MongoClient(host=self.host, document_class=dict) as client:
            while True:
                try:
                    res = client[db][collection].find_one({column_name: column_value}, data)
                    break
                except Exception as err:
                    self.logger.error(err)
                    time.sleep(10)
            return res

    def find_many(self, filter={}, limit=1000, skip=0, points={}, db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        while True:
            try:
                users = []
                with MongoClient(host=self.host, document_class=dict) as client:
                    for data_ in client[db][collection].find(filter, points).skip(skip).limit(
                            limit):
                        users.append(data_)
                return users
            except Exception as e:
                self.logger.error(e)
                time.sleep(10)

    def find_in_db_and_not_in_db(self, column_values=[], column="", db=None, collection=None):
        db, collection = self.fill_db_names(db, collection)
        while True:
            try:
                column_values_in_db = []
                column_value_and__id = {}
                with MongoClient(host=self.host, document_class=dict) as client:
                    for data_ in client[db][collection].find(
                            {column: {"$in": column_values}},
                            {'_id': 1, column: 1}):
                        column_values_in_db.append(data_.get(column))
                        column_value_and__id[data_.get(column)] = data_.get('_id')
                column_values_not_in_db = list(set(column_values) - set(column_values_in_db))
                return column_values_not_in_db, column_values_in_db, column_value_and__id
            except Exception as e:
                self.logger.error(e)
                time.sleep(10)

    def find_users_from_cross_scraping(self, social_media="instagram", credential_name="username"):
        list_of_user_ids = []
        unique_ids = []
        with MongoClient(host=self.host, document_class=dict) as client:
            for data_ in client["scrapers"]["cross_scrapping_users"].find(
                    {"$and": [{"scrape_try_date": None},
                              {"social_media": social_media},
                              {"credential_name": credential_name}
                              ]},
                    {"credential_value": 1, "cross_match_id": 1}).limit(30000):
                username = data_.get("credential_value")
                if username:
                    list_of_user_ids.append(username)
                    unique_ids.append(data_.get("cross_match_id"))
        return list_of_user_ids, unique_ids


mongo_api = MongoDbApi()
