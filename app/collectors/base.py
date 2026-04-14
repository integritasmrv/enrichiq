from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class RawEvidence:
    entity_id: str
    collector_name: str
    content_type: str
    raw_content: str
    source_url: Optional[str] = None
    source_weight: float = 0.5
    round_number: int = 1
    scrapiq_request_id: Optional[int] = None
    scrapiq_result_id: Optional[int] = None
    business_key: str = "integritasmrv"


class BaseCollector(ABC):
    name: str = ""
    entity_types: list[str] = []
    required_fields: list[str] = []
    source_weight: float = 0.5
    uses_scrapiq: bool = False

    async def can_run(self, entity: dict, trusted_attrs: dict) -> bool:
        if entity.get("entity_type") not in self.entity_types:
            return False
        return all(
            k in trusted_attrs and trusted_attrs.get(k) for k in self.required_fields
        )

    @abstractmethod
    async def collect(self, entity: dict, context: dict) -> list[RawEvidence]:
        raise NotImplementedError
