import time
from time import sleep

from elastic_transport import ConnectionTimeout
from elasticsearch import Elasticsearch, helpers
from typing import List, Dict
import json
import datetime
from dateutil.parser import parse
from app.core.config import settings
from app.logger_manager import CustomLogger


def date_converter(input):
    if isinstance(input, datetime.date):
        return input.strftime('%Y-%m-%d')


# ToDo add fields that you would like to export
export_list = ['date_scraped', 'avg_likes', 'avg_comments', 'first_name', 'engagement_percent', 'scraped_from',
               'userid', 'username', 'full_name', 'follower_count', 'following_count', 'media_count', 'biography',
               'external_url', 'email', 'contact_phone_number', 'address_street', 'category', 'city', 'isverified']


def split_into_groups(credentials=[], splitting_factor=1):
    list_of_lists = []
    number_of_credentials_in_a_group = int(len(credentials) / splitting_factor) + 1
    for i in range(0, splitting_factor):
        if i == splitting_factor - 1:
            tmp_list = credentials[i * number_of_credentials_in_a_group:]
            if tmp_list:
                list_of_lists.append(tmp_list)
            continue
        list_of_lists.append(
            credentials[i * number_of_credentials_in_a_group:(i + 1) * number_of_credentials_in_a_group])
    return list_of_lists


class ElasticSearchAPI:
    def __init__(self, host_uri=None, username=None, password=None):
        self.logger = CustomLogger("Elastic search")
        self.logger.info("Initializing Elasticsearch Database")
        try:
            self.host_uri = host_uri.split(",") if host_uri else settings.ELASTIC_HOSTS.split(",")
            self.username = username if username else settings.ELASTIC_USERNAME
            self.password = password if password else settings.ELASTIC_PASSWORD
            self.index = settings.ELASTIC_INDEX
            self.basic_auth = (self.username, self.password)
            self._elasticsearch = Elasticsearch(self.host_uri, basic_auth=self.basic_auth)
        except Exception as e:
            self.logger.error(e)

    def fill_index(self, index):
        return index if index else self.index

    def insert_one_document(self, index: str, data: Dict) -> bool:
        index = self.fill_index(index)
        data_to_insert = json.dumps(data, default=date_converter)
        try:
            res = self._elasticsearch.index(index=index, body=data_to_insert)
            if res['result'] == 'created':
                self.logger.info(f'Document inserted: {res["_id"]}')
                return True
        except Exception as e:
            self.logger.error(f'Error inserting document: {e}')
        return False

    def update_one_document(self, index: str, _id, data: Dict) -> bool:
        index = self.fill_index(index)
        # data dictionary needs to contain _id from elastic_search
        if '_id' in data:
            data.pop('_id')
        res = self._elasticsearch.update(
            index=index,
            id=_id,
            body={
                "doc": data,
                "doc_as_upsert": True
            },
        )
        try:
            if res['result'] == 'updated':
                self.logger.info(f'Document updated')
                return True
            else:
                self.logger.info("Couldn't update document")
        except Exception as e:
            self.logger.error(f'Error updating document: {e}')
        return False

    def find_one(self, index, column, value):
        index = self.fill_index(index)
        column = column + '.keyword'  # This depends on the mapping of the column in the index.
        # Comment the line above if the column is mapped as a keyword

        if column and value:
            try:
                query = {
                    "query": {
                        "term": {
                            column: value
                        }
                    }
                }

                result = self.search(index=index, **query)
                for data_ in result['hits']['hits']:
                    return data_
            except Exception as e:
                self.logger.error(e)
                sleep(10)

    def upsert_one_document(self, index, column, value, data):
        index = self.fill_index(index)
        document = self.find_one(index, column, value)
        if document:
            self.update_one_document(index=index, _id=document.get('_id'), data=data)
        else:
            self.insert_one_document(index=index, data=data)

    def insert_many_documents(self, index: str, data: List[Dict]):
        index = self.fill_index(index)
        retry_counter = 0
        while True:
            try:
                lst_data = [{'_op_type': 'index', '_index': index,
                             '_source': json.dumps(d, default=date_converter)} for d in data]
                success, info = helpers.bulk(self._elasticsearch, lst_data, index=index)
                if success:
                    self.logger.info(f'{success}  documents inserted into {index} in elastic')
                else:
                    self.logger.warning(f'A document failed: {info}')
                return success
            except ConnectionTimeout:
                print('Connection timeout occurred, continuing in 1 sec...')
                retry_counter += 1
                sleep(1)

                if retry_counter == 3:
                    print('Creating a new connection...')
                    self._elasticsearch = Elasticsearch(self.host_uri, basic_auth=self.basic_auth, timeout=20)
                    retry_counter = 0
                    sleep(3)

            except Exception as e:
                self.logger.error(f'Error inserting documents: {e}')
                return

    def update_many_documents(self, index: str, data: List[Dict]):
        index = self.fill_index(index)
        try:
            actions = [{
                "_op_type": "update",
                "_index": index,
                "_id": d.pop('_id'),
                "doc": d
            } for d in data]
            success, errors = helpers.bulk(self._elasticsearch, actions, raise_on_error=False)
            if success:
                self.logger.info(f'{len(data)} documents updated')
            if errors:
                self.logger.error(errors)
        except Exception as e:
            self.logger.error(f'Error updating documents: {e}')

    def search(self, index, *args, **kwargs):
        index = self.fill_index(index)
        while True:
            try:
                return self._elasticsearch.search(index=index, body=kwargs.get('body', None), *args, **kwargs)
            except Exception as e:
                self.logger.error(e)

    def _scroll_search_results(self, index, query, limit=None):
        index = self.fill_index(index)
        try:
            total_hits = 0
            results = []
            if limit:
                condition = len(results) < limit
            else:
                condition = True

            search_results = self._elasticsearch.search(
                index=index,
                body=query,
                size=10000,
                scroll='1m'
            )
            while condition:
                hits = search_results['hits']['hits']
                for hit in hits:
                    results.append(hit['_source'])

                total_hits += len(hits)

                if total_hits >= search_results['hits']['total']['value']:
                    break

                scroll_id = search_results['_scroll_id']

                search_results = self._elasticsearch.scroll(
                    scroll_id=scroll_id,
                    scroll='1m'
                )

                if limit:
                    condition = len(results) < limit
        except Exception as e:
            self.logger.error(e)
            time.sleep(5)
            return self._scroll_search_results(index, query, limit)
        return results

    def find_in_db_and_not_in_db(self, list_of_all=[], column="", index=""):
        index = self.fill_index(index)
        if index:
            try:
                users_in_db = []
                users_dict = {}
                split_factor = int(len(list_of_all) / 10000) + 1
                groups = split_into_groups(list_of_all, split_factor)
                for group in groups:
                    query = {
                        "size": 10000,
                        "query": {
                            "terms": {
                                column: group
                            }
                        },
                        "_source": [column]
                    }
                    result = self.search(index=index, **query)
                    for data_ in result['hits']['hits']:
                        users_in_db.append(data_['_source'][column])
                        users_dict[data_['_source'][column]] = data_['_id']

                users_not_in_db = list(set(list_of_all) - set(users_in_db))
                return users_in_db, users_not_in_db, users_dict
            except Exception as e:
                self.logger.error(e)
                sleep(10)

    def find_users_for_auto_rescrape(self, index, number_of_users=200000, days_before=30):
        index = self.fill_index(index)
        list_of_users = []
        before_date = (datetime.datetime.now() - datetime.timedelta(days=days_before)).strftime('%Y-%m-%d')
        query_filter = {
            "_source": ['username', 'userid'],
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must_not": [
                                                {
                                                    "exists": {
                                                        "field": "scrape_info"
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "bool": {
                                            "must": [
                                                {
                                                    "range": {
                                                        "scrape_info.scrape_try_date": {
                                                            "lt": before_date
                                                        }
                                                    }
                                                },
                                                {
                                                    "range": {
                                                        "scrape_info.scrape_fail_counter": {
                                                            "lt": 3
                                                        }
                                                    }
                                                },
                                            ]
                                        }
                                    },
                                ]
                            },

                        }
                    ]
                }
            }
        }
        result = self._scroll_search_results(index=index, query=query_filter, limit=number_of_users)
        for data_ in result:
            username = data_.get('username')
            userid = data_.get('userid')
            if username:
                list_of_users.append(username)
        self.logger.info(f"Len of users found: {len(list_of_users)}")
        return list_of_users

    def get_user_multi(self, index="", lst_items=[], column_name="", export_list_=None):
        index = self.fill_index(index)
        if not export_list_:
            export_list_ = export_list
        query = {
            "query": {
                "terms": {
                    column_name: lst_items
                }
            },
            "_source": export_list_
        }
        results = self._scroll_search_results(index=index, query=query)
        for result in results:
            yield result

    def update_error_user_scrape_info(self, index="", _id=None):
        index = self.fill_index(index)
        user = self.find_one(index="", column="_id", value=_id)
        if user:
            user = user.get('_source')
        datetime_ = validate_date_field_from_elastic(datetime.datetime.now())
        if user:
            scrape_fail_counter = user.get("scrape_info", {}).get("scrape_fail_counter", 0)
            scrape_fail_counter += 1
            scrape_info = {"scrape_fail_counter": scrape_fail_counter,
                           "scrape_try_date": datetime_}
            user["scrape_info"] = scrape_info
            self.update_one_document(index=index, _id=_id, data=user)
            return scrape_info


elastic_api = ElasticSearchAPI(host_uri="", username="", password="")


def validate_date_field_from_elastic(field):
    if isinstance(field, datetime.datetime):
        return field
    elif isinstance(field, str):
        return parse(field)
    elif isinstance(field, int):
        return datetime.datetime.fromtimestamp(field)


