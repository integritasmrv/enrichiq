import httpx

def main(entity_id: str, business_key: str = "integritasmrv"):
    r = httpx.post(f"http://localhost:8088/api/enrichment/trigger/{entity_id}", json={"business_key": business_key}, timeout=60)
    r.raise_for_status()
    return {"status": "triggered", "entity_id": entity_id}
