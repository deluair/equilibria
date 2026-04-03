import asyncio
import logging
from abc import ABC, abstractmethod

import httpx


class BaseCollector(ABC):
    name: str = "base"
    max_retries: int = 3
    timeout: int = 30

    def __init__(self):
        self.logger = logging.getLogger(f"collector.{self.name}")

    @abstractmethod
    async def collect(self) -> dict | list[dict]: ...

    async def validate(self, data: list[dict]) -> list[dict]:
        return data

    async def store(self, data: list[dict]) -> int:
        return len(data)

    async def run(self) -> dict:
        """Execute collect -> validate -> store pipeline."""
        try:
            self.logger.info(f"[{self.name}] collecting...")
            result = await self.collect()
            if isinstance(result, dict):
                self.logger.info(f"[{self.name}] done: {result}")
                return {"status": "success", **result}
            raw = result
            valid = await self.validate(raw)
            stored = await self.store(valid)
            self.logger.info(
                f"[{self.name}] collected={len(raw)} valid={len(valid)} stored={stored}"
            )
            return {
                "status": "success",
                "collected": len(raw),
                "valid": len(valid),
                "stored": stored,
            }
        except Exception as e:
            self.logger.error(f"[{self.name}] failed: {e}")
            return {"status": "failed", "error": str(e)}

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc = None
        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    resp = await client.request(method, url, **kwargs)
                    resp.raise_for_status()
                    return resp
            except (httpx.HTTPError, ConnectionError, TimeoutError) as e:
                last_exc = e
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2**attempt)
        raise last_exc
