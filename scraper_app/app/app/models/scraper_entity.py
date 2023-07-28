from sqlalchemy import Boolean, Column, Integer, String, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID
from app.db.base_class import Base
import datetime
import uuid


class ScraperEntity(Base):
    __tablename__ = 'scraper_entity'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String)
    scrape_counter = Column(Integer, default=0, nullable=True)
    error_counter = Column(Integer, default=0, nullable=True)
    totals = Column(Integer, default=0, nullable=True)
    function_kwargs = Column(JSON, nullable=True)
    scrape_function = Column(String, default="", nullable=True)
    mode = Column(Integer, default=0, nullable=True, index=True)
    status = Column(Integer, default=0, nullable=True, index=True)
    stats = Column(JSON, nullable=True)
    created_on = Column(DateTime, default=datetime.datetime.now)
    do_export = Column(Boolean, default=False)
    dt_start = Column(DateTime, default=None, nullable=True)
    dt_end = Column(DateTime, default=None, nullable=True)
    output_file = Column(String, default="", nullable=True)
    log_file = Column(String, default="", nullable=True)
    redis_id = Column(String, default="", nullable=True)
    redis_subtasks_id_str = Column(String, default="", nullable=True)

    @property
    def progress(self):
        return ((self.scrape_counter + self.error_counter + self.stats.get('in_db_not_updated', 0)) / self.totals) * 100 if self.totals else 0.0

    @property
    def redis_subtasks_id(self):
        return f"{self.redis_subtasks_id_str}".split(",")