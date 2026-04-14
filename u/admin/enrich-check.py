import httpx

def main(entity_id: str):
    r = httpx.get(f"http://localhost:8088/api/enrichment/status/{entity_id}", timeout=30)
    r.raise_for_status()
    return r.json()
