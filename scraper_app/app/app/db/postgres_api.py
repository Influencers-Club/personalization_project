import contextlib
from sqlalchemy import create_engine, NullPool
from sqlalchemy.orm import scoped_session, sessionmaker
from typing import Generator

from app.core.config import settings

connection_string = settings.SQLALCHEMY_DATABASE_URI
engine = create_engine(connection_string, poolclass=NullPool)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


@contextlib.contextmanager
def get_db() -> Generator:
    try:
        yield SessionLocal()
    finally:
        SessionLocal.remove()

