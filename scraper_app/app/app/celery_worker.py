from app.core.celery_app import celery_app
from app.scraper_engine import scraper_api


@celery_app.task()
def main() -> str:
    return scraper_api.main()


@celery_app.task()
def no_parameters_task(**kwargs) -> str:
    return scraper_api.no_parameters_task(**kwargs)


@celery_app.task()
def multiple_parameters_task(**kwargs) -> str:
    return scraper_api.multiple_parameters_task(**kwargs)



