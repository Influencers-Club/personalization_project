import datetime
import os

from app import crud, schemas
from app.core.celery_app import celery_app
from app.core.config import settings
from app.scraper_engine import scraper_api


@celery_app.task()
def main() -> str:
    return scraper_api.main()


# Modify this task to create the rescrape task
@celery_app.task()
def scrape_credentials_from_db(**kwargs) -> str:
    return scraper_api.scrape_credentials_from_db(**kwargs)


@celery_app.task()
def scrape_credentials_from_file(**kwargs) -> str:
    return scraper_api.scrape_credentials_from_file(**kwargs)


# Implement this task based on the logic of scraping new users for your project
@celery_app.task()
def scrape_new_users(**kwargs) -> str:
    return scraper_api.scrape_new_users(**kwargs)


@celery_app.task()
def scrape_cross(**kwargs) -> str:
    if not kwargs.get('entity_id'):
        kwargs = create_entity("app.celery_worker.scrape_cross", kwargs)
    return scraper_api.scrape_cross_scraping(**kwargs)


def create_entity(func, kwargs):
    part_name = func.split('worker.')[1]
    name = f"{part_name}-{datetime.datetime.now():%Y-%m-%d_%H:%M}"
    file_name = str(name).replace(" ", "_")
    output_file = os.path.join(settings.EXPORT_DIR, f"{file_name}.csv")
    entity_id = crud.scraper_entity.create(
        obj_in=schemas.scraper_entity.ScraperEntityCreate(name=name,
                                                          function_kwargs=kwargs,
                                                          scrape_function=func,
                                                          status=1,
                                                          mode=3,
                                                          stats={"total": 0, "inserted": 0, "updated": 0,
                                                                 "not_exist": 0, "in_db_not_updated": 0,
                                                                 'scrape_rate': 0.0},
                                                          output_file=output_file,
                                                          do_export=False)
    )
    if entity_id:
        kwargs["entity_id"] = entity_id
    return kwargs
