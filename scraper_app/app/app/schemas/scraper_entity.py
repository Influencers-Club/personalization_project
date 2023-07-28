import datetime
import uuid
from typing import Optional, Dict
from pydantic import BaseModel


# 0=Unknown, 1=Pending, 2=Waiting, 3=Running, 4=Error, 5=Interrupt, 6=Complete
class ScraperEntityStatus:
    unknown = 0
    pending = 1
    waiting = 2
    running = 3
    error = 4
    interrupt = 5
    complete = 6


class ScraperEntityMode:
    unknown = 0
    user_ids = 1
    usernames = 2
    rescrape = 3


# Shared properties
class ScraperEntityBase(BaseModel):
    name: Optional[str] = ""
    do_export: Optional[bool] = False
    mode: Optional[int] = 0
    status: Optional[int] = 0
    scrape_function: Optional[str] = ""
    function_kwargs: Optional[Dict] = {}
    scrape_counter: Optional[int] = 0
    error_counter: Optional[int] = 0
    totals: Optional[int] = 0
    stats: Optional[Dict] = {}
    dt_start: Optional[datetime.datetime] = None
    dt_end: Optional[datetime.datetime] = None
    output_file: Optional[str] = ""
    log_file: Optional[str] = ""
    redis_id: Optional[str] = ""


# Properties to receive via API on creation
class ScraperEntityCreate(ScraperEntityBase):
    name: str


# Properties to receive via API on update
class ScraperEntityUpdate(ScraperEntityBase):
    redis_subtasks_id_str: Optional[str] = ""
    pass


class ScraperEntityInDBBase(ScraperEntityBase):
    id: Optional[uuid.UUID] = None

    class Config:
        orm_mode = True


# Additional properties to return via API
class ScraperEntity(ScraperEntityInDBBase):
    progress: Optional[float] = 0.0
    redis_subtasks_id: Optional[list] = []
    pass


# Additional properties stored in DB
class ScraperEntityInDB(ScraperEntityInDBBase):
    pass
