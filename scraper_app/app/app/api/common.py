import shutil
from typing import Any, Optional
from fastapi import File


def upload_file(in_file: Optional[File] = None,
                file_path: Optional[str] = None) -> Any:

    if file_path:
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(in_file.file, buffer)
        finally:
            in_file.file.close()

    return file_path


