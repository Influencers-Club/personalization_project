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


# Tasks

# This task is just an example, the task is scheduled to be started 2 minutes after starting the scraper
# If you see the 'Initial task' printed in your logs that means the beat container and the celery worker container are
# connected okay and your other scheduled task will be started on time
def main():
    logger = get_logger('initial')
    logger.info('Initial task')


def scrape_credentials_from_db(entity_id, **kwargs):
    object_logger = pre_task(entity_id)
    # Feel free to change the following function to get the credentials that you need as an input to scraping tasks
    credentials = find_credentials_for_no_parameters_scrape()
    spawn_threads(credentials=credentials, object_logger=object_logger, scrape_tag='', mode="usernames",
                  entity_id=entity_id, do_export=False, pool_size=30)


def scrape_credentials_from_file(mode, update, file_path, scrape_tag, entity_id=None, do_export=False, **kwargs):
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


# Implement this task based on the logic of scraping new users for your project
def scrape_new_users(entity_id, **kwargs):
    object_logger = pre_task(entity_id)
    # ToDo implement function into the ScraperClass for scraping new users, call it with or without threads depending
    #  on the use_case
    # ToDo after scraping user credentials you can call scrape_users function to scrape the full info for those users


# Additional functions


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


def spawn_threads(object_logger, credentials, scrape_tag, mode, entity_id, do_export, batch_number=50, pool_size=30):
    """
    :param object_logger: logger
    :param credentials: input that you need for scraping example: usernames/ user_ids
    :param scrape_tag: str that will be added to all documents from that task in the db
    :param mode: if you can scrape by different type of credentials example: usernames/ user_ids
    :param entity_id: the id of the entity obj(this obj contains statistic info for that task)
    :param do_export: do we want the results to be exported into csv file
    :param batch_number: number of credentials to be scraped by a single thread
    :param pool_size: number of threads to be working at the same time
    """

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
