import eventlet
import datetime
import gc
import os
import random
from app import utils
import app.crud as crud
from app.db.mongo_api import mongo_api
from app import schemas
from app.db.queries import find_credentials_for_no_parameters_scrape
from app.kafka.kafka_api import KafkaApi
from app.logger_manager import CustomLogger
from app.scraper_engine.ScraperClass import Scraper
from app.utils import get_logger


def pre_task(entity_id):
    gc.collect()
    object_logger = create_entity_loger(entity_id)
    crud.scraper_entity.update_entity(
        entity_id=entity_id,
        obj_in=schemas.ScraperEntityUpdate(
            dt_start=datetime.datetime.now(),
            status=3
        )
    )
    return object_logger


def main():
    logger = get_logger('initial')
    logger.info('Initial task')


def no_parameters_task(entity_id, **kwargs):
    object_logger = pre_task(entity_id)
    credentials = find_credentials_for_no_parameters_scrape()
    spawn_threads(credentials=credentials, object_logger=object_logger, scrape_tag='', mode="usernames",
                  entity_id=entity_id, do_export=False, pool_size=120)


def multiple_parameters_task(mode, update, file_path, scrape_tag, entity_id=None, do_export=False, **kwargs):
    credentials = get_credentials_from_file(file_path)
    object_logger = pre_task(entity_id)
    crud.scraper_entity.update_entity(
        entity_id=entity_id,
        obj_in=schemas.ScraperEntityUpdate(
            dt_start=datetime.datetime.now(),
            status=3
        )
    )
    #   If we don't want to scrape already scraped users
    if not update:
        # We look for those credentials already existing in db and do not send them to be scraped
        users_not_in_db, users_in_db, users_dict = mongo_api.find_in_db_and_not_in_db(
            column_values=credentials, column="pk")
        object_logger.info(f"Found {len(users_in_db)}/{len(credentials)} in db")
        credentials = users_not_in_db
    spawn_threads(object_logger=object_logger, credentials=credentials, scrape_tag=scrape_tag, mode=mode,
                  entity_id=entity_id, do_export=do_export)
    del users_not_in_db
    del users_in_db
    gc.collect()


def get_credentials_from_file(file_path):
    if os.path.exists(file_path):
        list_of_ids_dict = []
        for dict_info_ in utils.gen_read_data_from_csv(file_path=file_path):
            list_of_ids_dict.append(dict_info_)
        try:
            first_key = list(list_of_ids_dict[0].keys())[0]
        except:
            return []
        credentials = [str(x.get(first_key)) for x in list_of_ids_dict]
        return credentials
    return []


def spawn_threads(object_logger, credentials, scrape_tag, mode, entity_id, do_export, batch_number=300, pool_size=500):
    kafka_api = KafkaApi(logger=object_logger)
    user_ids_groups = pre_spawn(credentials, object_logger, batch_number)
    green_pool = eventlet.GreenPool(size=pool_size)
    for group in user_ids_groups:
        obj = Scraper(scrape_tag=scrape_tag, entity_id=entity_id, logger=object_logger,
                      kafka_api=kafka_api, export=do_export)
        f = green_pool.spawn(obj.scrape_users, credentials=group, mode=mode)
        f.link(fin)
    green_pool.waitall()
    kafka_api.flush_messages()
    del user_ids_groups


def pre_spawn(ids, object_logger, batch_number):
    threads_number = int(len(ids) / batch_number) + 1
    random.shuffle(ids)
    user_ids_groups = utils.split_into_groups(ids, threads_number)
    object_logger.info(f'Number of credentials is: {len(ids)}, starting {len(user_ids_groups)} threads')
    del ids
    return user_ids_groups


def fin(gt):
    res = gt.wait()


def create_entity_loger(entity_id=None):
    object_logger = None
    if entity_id:
        entity = crud.scraper_entity.get(id=entity_id)
        if entity:
            object_logger = CustomLogger(name="Scraper_name Scraper", path=entity.log_file)

    if not object_logger:
        object_logger = CustomLogger(name="Scraper_name Scraper")
    return object_logger
