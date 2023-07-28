import codecs
import datetime
import logging
import pandas as pd
import requests
from app.core.config import settings

UNKNOWN = 0
STATIC = 1
RESIDENTIAL = 2
MOBILE = 3


def get_logger(name=__name__):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(threadName)s | %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(name=name)
    logger.propagate = False
    return logger


def to_json(python_object):
    if isinstance(python_object, bytes):
        return {'__class__': 'bytes',
                '__value__': codecs.encode(python_object, 'base64').decode()}
    raise TypeError(repr(python_object) + ' is not JSON serializable')


def from_json(json_object):
    if '__class__' in json_object and json_object['__class__'] == 'bytes':
        decoded_session_val = codecs.decode(json_object['__value__'].encode(), 'base64')
        return decoded_session_val
    return json_object


def order_dict_keys(new_entry):
    keys = new_entry.keys()
    sorted_keys = sorted(keys, key=lambda x: x.lower())
    for key in sorted_keys:
        new_entry[key] = new_entry.pop(key)
    return new_entry


def gen_read_data_from_csv(file_path="", delimiter=","):
    chunk_size = 100000
    for chunk in pd.read_csv(filepath_or_buffer=file_path,
                             delimiter=delimiter,
                             chunksize=chunk_size,
                             low_memory=False,
                             encoding="utf-8"):

        for record_ in chunk.to_dict("records"):
            yield record_


def calculate_number_of_threads(number_of_ids, ids_in_thread):
    if ids_in_thread != 1:
        return int(number_of_ids / ids_in_thread) + 1
    else:
        return number_of_ids


def split_into_groups(credentials=[], splitting_factor=1):
    list_of_lists = []
    number_of_credentials_in_thread = int(len(credentials) / splitting_factor) + 1
    for i in range(0, splitting_factor):
        if i == splitting_factor - 1:
            tmp_list = credentials[i * number_of_credentials_in_thread:]
            if tmp_list:
                list_of_lists.append(tmp_list)
            continue
        list_of_lists.append(
            credentials[i * number_of_credentials_in_thread:(i + 1) * number_of_credentials_in_thread])
    return list_of_lists


def get_chunked_list(lst_items=[], chunk_size=100):
    return [lst_items[i: i + chunk_size] for i in range(0, len(lst_items), chunk_size)]


def create_csv(file_path="", data=[], is_header=True, mode="w"):
    return pd.DataFrame(data).to_csv(file_path, header=is_header, mode=mode, index=False)


def mongo_export_into_csv(items=None, file_path=""):
    is_header = True
    mode = "w"
    totals = 0
    for lst_db_data in get_chunked_list(lst_items=items, chunk_size=10000):
        totals += len(lst_db_data)
        create_csv(file_path=file_path, data=lst_db_data, is_header=is_header, mode=mode)
        if is_header:
            is_header = False
            mode = "a"
    return totals


def get_static_proxies():
    url = settings.PROXY_MANAGEMENT_SYSTEM_URL + '/api/v1/scrapers/get_proxy_list'
    params = {'project': 'All', 'proxy_type': 'Static'}
    resp = requests.get(url, params=params).json()
    return resp


def get_day_of_month():
    d = datetime.datetime.now()
    # get the day of month
    return d.strftime("%d")


def get_month_of_year():
    d = datetime.datetime.now()
    return d.strftime("%m")


def get_current_hour():
    d = datetime.datetime.now()
    return str((int(d.strftime("%H")) + 2) % 24)


def get_current_minute():
    d = datetime.datetime.now()
    return d.strftime("%M")


def get_x_minute_after_now(x):
    current = int(get_current_minute())
    minutes = (current + x) % 60
    return str(minutes)


