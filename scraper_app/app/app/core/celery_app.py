from celery.schedules import crontab
from celery.signals import task_revoked, worker_ready
from app import crud, schemas
from app.core.config import settings
from celery import Celery, Task
from app.db.postgres_api import get_db
from app.utils import get_logger, get_current_hour, get_x_minute_after_now, get_day_of_month, get_month_of_year

celery_app = Celery("celery_worker",
                    broker=settings.CELERY_BROKER_URL,
                    backend=settings.CELERY_RESULT_BACKEND,
                    broker_connection_retry_on_startup=True)

celery_app.conf.timezone = 'Europe/Skopje'
celery_app.conf.update(task_track_started=True)
logger = get_logger("celery logger")


@worker_ready.connect
def at_start(sender, **kwargs):
    insp = celery_app.control.inspect()

    with sender.app.connection() as conn:
        pass
    pass


@task_revoked.connect
def on_task_revoked(request, terminated, signum, expired, *args, **kwargs):
    insp = celery_app.control.inspect()
    if hasattr(request, "parent_id") and not request.parent_id:

        with get_db() as db:
            entity_obj = crud.scraper_entity.get_by_redis_task_id(db=db, task_id=request.id)

            if entity_obj:
                logger.info(f"terminated task with id {entity_obj.redis_id}.")
                crud.scraper_entity.update(db=db,
                                           db_obj=entity_obj,
                                           obj_in=schemas.ScraperEntityUpdate(
                                               status=schemas.ScraperEntityStatus.interrupt
                                           ))

    celery_app.control.terminate(request.id)
    pass


# Empty Celery cache
celery_app.control.purge()

celery_app.autodiscover_tasks()

celery_app.conf.beat_schedule = {
    'auto-rescrape': {
        'task': 'app.celery_worker.main',
        'schedule': crontab(hour=get_current_hour(),
                            minute=get_x_minute_after_now(2),
                            day_of_month=get_day_of_month(),
                            month_of_year=get_month_of_year())
    }
}
