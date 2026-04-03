import logging
from abc import ABC, abstractmethod

from app.config import SIGNAL_LEVELS


class LayerBase(ABC):
    layer_id: str = ""
    name: str = ""
    weight: float = 0.20

    def __init__(self):
        self.logger = logging.getLogger(f"equilibria.layer.{self.layer_id}")

    @abstractmethod
    async def compute(self, db, **kwargs) -> dict:
        """Return dict with results. Must include 'score' (0-100) key."""
        ...

    async def run(self, db, **kwargs) -> dict:
        try:
            result = await self.compute(db, **kwargs)
            result.setdefault("layer_id", self.layer_id)
            result.setdefault("name", self.name)
            if "signal" not in result and result.get("score") is not None:
                result["signal"] = self.classify_signal(result["score"])
            return result
        except Exception as e:
            self.logger.exception("Error in layer %s", self.layer_id)
            return {
                "layer_id": self.layer_id,
                "name": self.name,
                "score": None,
                "signal": "UNAVAILABLE",
                "error": str(e),
            }

    @staticmethod
    def classify_signal(score: float) -> str:
        for (low, high), level in SIGNAL_LEVELS.items():
            if low <= score < high:
                return level
        return "CRISIS"
