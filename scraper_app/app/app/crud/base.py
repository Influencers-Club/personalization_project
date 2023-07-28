import random
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union
from fastapi.encoders import jsonable_encoder
from psycopg2 import OperationalError
from pydantic import BaseModel
from sqlalchemy.orm import Session

import time as t
from app.db.base_class import Base
from app.db.postgres_api import get_db

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):

    def __init__(self, model: Type[ModelType]):
        """
        CRUD object with default methods to Create, Read, Update, Delete (CRUD).

        **Parameters**

        * `model`: A SQLAlchemy model class
        * `schema`: A Pydantic model (schema) class
        """
        self.model = model

    def get(self, id: Any, db: Optional[Session] = None) -> Optional[ModelType]:
        while True:
            try:
                if db:
                    return db.query(self.model).filter(self.model.id == id).first()
                with get_db() as db:
                    return db.query(self.model).filter(self.model.id == id).first()
            except OperationalError:
                t.sleep(random.uniform(1, 3))

    def get_multi(
            self, db: Session, *, skip: Optional[int] = None, limit: Optional[int] = None
    ) -> List[ModelType]:
        while True:
            try:
                return db.query(self.model).offset(skip).limit(limit).all()
            except OperationalError:
                t.sleep(random.uniform(1, 3))

    def create(self, db: Session, *, obj_in: CreateSchemaType) -> ModelType:
        while True:
            try:
                obj_in_data = obj_in.dict()
                db_obj = self.model(**obj_in_data)  # type: ignore
                db.add(db_obj)
                db.commit()
                db.refresh(db_obj)
                return db_obj
            except OperationalError:
                t.sleep(random.uniform(1, 3))

    def update(
            self,
            db: Session,
            *,
            db_obj: ModelType,
            obj_in: Union[UpdateSchemaType, Dict[str, Any]]
    ) -> ModelType:
        while True:
            try:
        # obj_data = obj_in.dict()
                obj_data = jsonable_encoder(db_obj)
                if isinstance(obj_in, dict):
                    update_data = obj_in
                else:
                    update_data = obj_in.dict(exclude_unset=True)

                for field in obj_data:
                    if field in update_data:
                        setattr(db_obj, field, update_data[field])
                db.add(db_obj)
                db.commit()
                db.refresh(db_obj)
                return db_obj
            except OperationalError:
                t.sleep(random.uniform(1, 3))

    def remove(self, db: Session, *, id: Any) -> ModelType:
        while True:
            try:
                obj = db.query(self.model).get(id)
                db.delete(obj)
                db.commit()
                return obj
            except OperationalError:
                t.sleep(random.uniform(1, 3))

