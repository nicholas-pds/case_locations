import asyncio
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class CacheEntry:
    df: pd.DataFrame
    timestamp: datetime
    row_count: int = 0

    def __post_init__(self):
        self.row_count = len(self.df)


class DataCache:
    """Thread-safe in-memory cache for query results."""

    def __init__(self):
        self._store: dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._last_refresh: Optional[datetime] = None
        self._refresh_error: Optional[str] = None
        self._is_paused: bool = False

    async def get(self, key: str) -> Optional[pd.DataFrame]:
        async with self._lock:
            entry = self._store.get(key)
            return entry.df.copy() if entry else None

    async def set(self, key: str, df: pd.DataFrame) -> None:
        async with self._lock:
            self._store[key] = CacheEntry(df=df, timestamp=datetime.now())

    async def get_metadata(self) -> dict:
        async with self._lock:
            return {
                "last_refresh": self._last_refresh,
                "is_paused": self._is_paused,
                "refresh_error": self._refresh_error,
                "datasets": {
                    key: {
                        "row_count": entry.row_count,
                        "timestamp": entry.timestamp,
                    }
                    for key, entry in self._store.items()
                },
            }

    async def set_last_refresh(self, timestamp: datetime) -> None:
        async with self._lock:
            self._last_refresh = timestamp
            self._refresh_error = None

    async def set_error(self, error: str) -> None:
        async with self._lock:
            self._refresh_error = error

    async def set_paused(self, paused: bool) -> None:
        async with self._lock:
            self._is_paused = paused

    def get_sync(self, key: str) -> Optional[pd.DataFrame]:
        """Synchronous get for use in Jinja2 template context."""
        entry = self._store.get(key)
        return entry.df.copy() if entry else None

    @property
    def last_refresh(self) -> Optional[datetime]:
        return self._last_refresh

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    @property
    def refresh_error(self) -> Optional[str]:
        return self._refresh_error


# Global singleton
cache = DataCache()
