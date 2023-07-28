import json
import random
import time
from typing import Any

from psycopg2 import OperationalError
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.db.postgres_api import get_db
from app.logger_manager import CustomLogger
from app.models.scraper_entity import ScraperEntity
from app.schemas.scraper_entity import ScraperEntityCreate, ScraperEntityUpdate


class CRUDScraperEntity(CRUDBase[ScraperEntity, ScraperEntityCreate, ScraperEntityUpdate]):

    def get_by_name(self,
                    db: Session = None,
                    name: str = None) -> ScraperEntity:
        while True:
            try:
                if not db:
                    with get_db() as db:
                        return db.query(self.model).filter(ScraperEntity.name == name).first()
                return db.query(self.model).filter(ScraperEntity.name == name).first()
            except OperationalError:
                time.sleep(random.uniform(1, 3))

    def get_by_redis_task_id(self, db: Session, task_id: Any = None) -> ScraperEntity:
        while True:
            try:
                return db.query(self.model).filter(ScraperEntity.redis_id == task_id).first()
            except OperationalError:
                time.sleep(random.uniform(1, 3))

    def update_entity(self, entity_id, obj_in, db: Session = None):
        if db:
            entity = self.get(id=entity_id, db=db)
            return True if self.update(db=db, db_obj=entity, obj_in=obj_in) else False
        else:
            with get_db() as db:
                entity = self.get(id=entity_id, db=db)
                return True if self.update(db=db, db_obj=entity, obj_in=obj_in) else False

    def update_entity_stats(self, entity_id, scrape_counter=0, phase=None, error_counter=0, status=None,
                            redis_id=None, totals=None, dt_start=None, dt_end=None, inserted=0, updated=0, in_db=0,
                            log_file=None, logger=None):
        if not logger:
            logger = CustomLogger("crud_entity")
        try:
            with get_db() as session:
                db_obj = self.get(db=session, id=entity_id)
                if db_obj:
                    time_spent = None
                    time_spent_serializable = None
                    db_obj.scrape_counter += scrape_counter
                    db_obj.error_counter += error_counter
                    if status:
                        db_obj.status = status
                    if phase:
                        # and phase > db_obj.phase:
                        db_obj.phase = phase
                    if redis_id:
                        db_obj.redis_id = redis_id
                    if totals:
                        db_obj.totals = db_obj.totals + totals
                    if dt_end:
                        db_obj.dt_end = dt_end
                        time_spent = dt_end.replace(tzinfo=None) - db_obj.dt_start.replace(tzinfo=None)
                        time_spent_serializable = json.dumps(time_spent, default=str)
                    if dt_start:
                        db_obj.dt_start = dt_start
                    if log_file:
                        db_obj.log_file = log_file
                    try:
                        stats = {
                            'total': db_obj.totals,
                            'inserted': db_obj.stats['inserted'] + inserted,
                            'updated': db_obj.stats['updated'] + updated,
                            'not_exist': db_obj.error_counter,
                            'in_db_not_updated': db_obj.stats['in_db_not_updated'] + in_db,
                            'time_spent': time_spent_serializable if time_spent_serializable else None,
                            'scrape_rate': round(
                                (db_obj.scrape_counter / (time_spent.days * 84000 + time_spent.seconds)),
                                2)
                            if time_spent and db_obj.scrape_counter else db_obj.stats['scrape_rate']
                        }
                        db_obj.stats = stats
                    except Exception as e:
                        logger.error(e)
                    session.add(db_obj)
                    session.commit()
                    return True
                else:
                    logger.error("Entity not found")
        except Exception as e:
            logger.error(e)
            time.sleep(10)
        return False


scraper_entity = CRUDScraperEntity(ScraperEntity)
