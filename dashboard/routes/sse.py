"""Server-Sent Events endpoint for real-time updates."""
import asyncio
from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse
from dashboard.data.refresh import subscribe, unsubscribe

router = APIRouter()


@router.get("/sse/stream")
async def sse_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue()

    async def on_refresh():
        await queue.put("data-refreshed")

    subscribe(on_refresh)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    yield {"event": event, "data": "refresh"}
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield {"event": "ping", "data": "keepalive"}
        finally:
            unsubscribe(on_refresh)

    return EventSourceResponse(event_generator())
