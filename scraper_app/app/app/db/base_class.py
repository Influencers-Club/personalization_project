from typing import Any
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy import inspect


@as_declarative()
class Base:
    id: Any
    __name__: str

    def _as_dict(self):
        return {c.key: getattr(self, c.key)
                for c in inspect(self).mapper.column_attrs}