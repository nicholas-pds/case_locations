from fastapi import APIRouter
from dashboard.data.cache import cache
from dashboard.data.refresh import refresh_all_queries

router = APIRouter()


@router.get("/api/status")
async def status():
    return await cache.get_metadata()


@router.post("/api/refresh")
async def manual_refresh():
    await refresh_all_queries()
    meta = await cache.get_metadata()
    return {"ok": True, "datasets": meta["datasets"]}
