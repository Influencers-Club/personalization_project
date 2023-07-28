import datetime
import random
import threading

import app.crud as crud
from app.proxy_management.proxy_manager import ProxyManager
from app.schemas import ScraperEntityStatus

from app.utils import get_static_proxies


class Scraper:
    def __init__(self, logger, scrape_tag=None, entity_id=None, kafka_api=None, export=False):
        self.entity_type = None
        self.logger = logger
        self.scrape_tag = scrape_tag
        self.entity_id = entity_id
        self.client = None
        self.saved_settings = None
        self.proxy = random.choice(get_static_proxies())
        self.original_proxy = None
        self.proxy_manager = ProxyManager(proxy_str=self.proxy, logger=self.logger)
        self.kafka_api = kafka_api
        self.unauthorized_errors = 0
        self.connection_errors = 0
        self.scrape_counter = 0
        self.error_counter = 0
        self.max_retries_errors = 0
        self.status = ScraperEntityStatus.running
        self.totals = 0
        self.dt_end = None
        self.inserted = 0
        self.updated = 0
        self.already_in_db_counter = 0
        self.daily_calls = 0
        self.daily_success_calls = 0
        self.lock = threading.Lock()
        self.users_dict = {}
        self.export = export
        self.cursor = None

    def update_entity(self):
        if self.entity_id:
            status = crud.scraper_entity.update_entity_stats(
                entity_id=self.entity_id,
                scrape_counter=self.scrape_counter,
                error_counter=self.error_counter,
                status=self.status,
                totals=self.totals,
                dt_end=self.dt_end,
                inserted=self.inserted,
                updated=self.updated,
                in_db=self.already_in_db_counter,
                logger=self.logger
            )
            self.scrape_counter = 0
            self.error_counter = 0
            self.inserted = 0
            self.updated = 0
            self.totals = 0
            self.already_in_db_counter = 0
            return status
        else:
            self.logger.error('No entity id')

    def finish_thread(self):
        # ToDo update_proxy_stats
        # ToDo update_daily_calls_stats
        # ToDo update accounts stats if accounts are used
        # ToDo update_entity
        if self.entity_id:
            self.dt_end = datetime.datetime.now()
            self.status = ScraperEntityStatus.complete
            self.proxy_manager.insert_proxy_calls_in_daily_statistic()
            if not self.update_entity():
                self.logger.info("Entity was not updated")
        self.logger.info("Thread finished")

    def scrape_users(self, credentials, mode):
        self.entity_type = "user_ids"
        self.users_dict = {}
        self.totals = len(credentials)
        if mode == "user_ids":
            self.scrape_users_by_user_ids(credentials)
        elif mode == "usernames":
            self.scrape_users_by_usernames(credentials)
        self.process_users()
        self.finish_thread()

    def scrape_users_by_user_ids(self, user_ids):
        # ToDo scrape users by their user_ids
        pass

    def scrape_users_by_usernames(self, usernames):
        # ToDo scrape users by their usernames
        pass

    def process_users(self):
        # ToDo process users
        # ToDo upsert users into db or/and send users to kafka
        pass
