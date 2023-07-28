from fastapi import APIRouter

from app.api.api_v1.endpoints import scraper

api_router = APIRouter()
api_router.include_router(scraper.router, prefix="/scraper", tags=["scraper"])
