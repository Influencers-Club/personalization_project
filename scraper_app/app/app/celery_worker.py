from app.core.celery_app import celery_app
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
