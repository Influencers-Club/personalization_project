import datetime
import os
import shutil
from typing import Optional, Union
from fastapi import APIRouter, File, Query, UploadFile, Depends
from fastapi.responses import FileResponse
from starlette.background import BackgroundTasks
from sqlalchemy.orm import Session
from app import schemas
from app.api import deps
from app.core.config import settings

if settings.CELERY_BROKER_URL:
    from app.core.celery_app import celery_app
import app.crud as crud

router = APIRouter()


@router.post("/main/")
def main():
    celery_app.send_task("app.celery_worker.main")
    return {"Result": "Sent as background task"}


@router.post("/scrape-credentials-from-db/")
def scrape_credentials_from_db(db: Session = Depends(deps.get_db)):
    kwargs = {}
    name = f"no--parameters--{datetime.datetime.now():%Y-%m-%d_%H:%M}"
    file_name = str(name).replace(" ", "_")
    output_file = os.path.join(settings.EXPORT_DIR, f"{file_name}.csv")
    func = "app.celery_worker.scrape_credentials_from_db"
    entity = crud.scraper_entity.create(
        db=db,
        obj_in=schemas.ScraperEntityCreate(
            name=name,
            function_kwargs=kwargs,
            scrape_function=func,
            status=schemas.ScraperEntityStatus.pending,
            mode=schemas.ScraperEntityMode.rescrape,
            stats={"total": 0, "inserted": 0,
                   "updated": 0,
                   "not_exist": 0,
                   "in_db_not_updated": 0,
                   'scrape_rate': 0.0},
            output_file=output_file,
            do_export=False)
    )
    if entity:
        kwargs["entity_id"] = entity.id
        task = celery_app.send_task(func, kwargs=kwargs)
        log_file = os.path.join(settings.LOG_DIR, f"{entity.id}.log")
        if task:
            crud.scraper_entity.update_entity(
                db=db,
                entity_id=entity.id,
                obj_in=schemas.ScraperEntityUpdate(
                    redis_id=task.id,
                    log_file=log_file
                )
            )
    return {"task": f"{task}"}


@router.post("/scrape-credentials-from-file/")
def scrape_credentials_from_file(
        background_tasks: BackgroundTasks,
        db: Session = Depends(deps.get_db),
        in_file: UploadFile = File(...),
        name: Optional[str] = "",
        update: Union[bool, None] = Query(default=False),
        mode: str = Query(default='user_ids', enum=('user_ids', 'usernames')),
        scrape_tag: str = Query(default=''),
        do_export: bool = Query(default=False, title="Export csv file with scraped data.")):
    entity = crud.scraper_entity.get_by_name(name=name)
    if entity:
        result = f"Task with name: {name} already exists. Please send the task with another name."
        return {"Result": result}
    if in_file:
        out_file_path = os.path.join(settings.INPUT_DIR, os.path.basename(in_file.filename))
        background_tasks.add_task(write_file_and_send_to_redis,
                                  db=db,
                                  out_file_path=out_file_path,
                                  in_file=in_file,
                                  update=update,
                                  mode=mode,
                                  scrape_tag=scrape_tag,
                                  name=name,
                                  do_export=do_export,
                                  scrape_func="app.celery_worker.scrape_credentials_from_file"
                                  )

    return {"Result": "Sent as background task"}


@router.post("/export-csv")
def export_task_data(name: Optional[str] = ""):
    file_name = name
    if len(name) >= 4 and name[-4:] == '.csv':
        file_name = name[:-4]
    file_name = f"{file_name}.csv"
    if name:
        file_path = os.path.join(settings.EXPORT_DIR, file_name)
        if os.path.exists(file_path):
            return FileResponse(file_path, filename=file_name)
        else:
            return {"Message": f"File {file_name} does not exist"}
    else:
        return {"Message": "Invalid name"}


def write_file_and_send_to_redis(db=None, in_file: Optional[File] = None, out_file_path: Optional[str] = "", do_export=False,
                                 name="", update=False, mode="", scrape_tag="", scrape_func="app.celery_worker.main"):
    if out_file_path:
        try:
            with open(out_file_path, "wb") as buffer:
                shutil.copyfileobj(in_file.file, buffer)
        finally:
            in_file.file.close()

    kwargs = {
        "mode": mode,
        "update": update,
        "scrape_tag": scrape_tag,
        "do_export": do_export
    }

    if out_file_path:
        kwargs["file_path"] = out_file_path

    if not name:
        name = f"scrape_{mode}-{datetime.datetime.now():%Y-%m-%d_%H:%M}"
    file_name = str(name).replace(" ", "_")
    output_file = os.path.join(settings.EXPORT_DIR, f"{file_name}.csv")
    if mode == 'user_ids':
        mode_number = schemas.ScraperEntityMode.user_ids
    elif mode == 'usernames':
        mode_number = schemas.ScraperEntityMode.usernames
    else:
        mode_number = schemas.ScraperEntityMode.unknown

    entity = crud.scraper_entity.create(
        db=db,
        obj_in=schemas.ScraperEntityCreate(
            name=name,
            function_kwargs=kwargs,
            scrape_function=scrape_func,
            status=1,
            mode=mode_number,
            stats={"total": 0, "inserted": 0,
                   "updated": 0, "not_exist": 0,
                   "in_db_not_updated": 0,
                   'scrape_rate': 0.0},
            output_file=output_file,
            do_export=do_export)
    )
    entity_id = entity.id
    if entity_id:
        kwargs["entity_id"] = entity_id
    task = celery_app.send_task(scrape_func, kwargs=kwargs)
    log_file = os.path.join(settings.LOG_DIR, f"{entity_id}.log")
    if task:
        crud.scraper_entity.update_entity(entity_id=entity_id,
                                          obj_in=schemas.ScraperEntityUpdate(redis_id=task.id,
                                                                             log_file=log_file))
    return {"task": f"{task}"}


@router.post("/scrape-cross-matched/")
def scrape_cross_matched_task():
    kwargs = {}
    name = f"cross--matched--{datetime.datetime.now():%Y-%m-%d_%H:%M}"
    file_name = str(name).replace(" ", "_")
    output_file = os.path.join(settings.EXPORT_DIR, f"{file_name}.csv")
    func = "app.celery_worker.scrape_cross"
    entity_id = crud.scraper_entity.create(
        obj_in=schemas.ScraperEntityCreate(name=name,
                                           function_kwargs=kwargs,
                                           scrape_function=func,
                                           stats={"total": 0, "inserted": 0, "updated": 0, "not_exist": 0,
                                                  "in_db_not_updated": 0, 'scrape_rate': 0.0},
                                           output_file=output_file,
                                           do_export=False,
                                           status=schemas.ScraperEntityStatus.pending,
                                           mode=schemas.ScraperEntityMode.rescrape
                                           )
    )
    if entity_id:
        kwargs["entity_id"] = entity_id
    task = celery_app.send_task(func, kwargs=kwargs)
    log_file = os.path.join(settings.LOG_DIR, f"{entity_id}.log")
    if task:
        crud.scraper_entity.update_entity(entity_id=entity_id,
                                          obj_in=schemas.ScraperEntityUpdate(redis_id=task.id, log_file=log_file))
    return {"task": f"{task}"}
