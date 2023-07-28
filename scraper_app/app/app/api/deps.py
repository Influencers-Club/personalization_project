from typing import Generator
from app.db.postgres_api import SessionLocal


def get_db() -> Generator:
    try:
        db = SessionLocal()
        yield db
    finally:
        SessionLocal.remove()
