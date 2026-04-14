import asyncio
import logging
from typing import Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FreeLayerResult:
    score: float
    data: dict[str, Any]
    sources: list[str]
    errors: list[str]
EOFPY # Create free_layer.py part 1
