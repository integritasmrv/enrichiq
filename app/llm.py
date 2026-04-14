import os

import httpx

LITELLM_PROXY_URL = os.environ.get("LITELLM_PROXY_URL", "http://127.0.0.1:4000")
LITELLM_PROXY_KEY = os.environ.get("LITELLM_PROXY_KEY", "")
LIGHTRAG_BASE_URL = os.environ.get("LIGHTRAG_BASE_URL", "http://127.0.0.1:9621")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://127.0.0.1:6333")


async def llm_complete(
    alias: str,
    prompt: str,
    json_mode: bool = False,
    max_tokens: int = 2048,
    temperature: float = 0.1,
) -> str:
    payload = {
        "model": alias,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    headers = {"Content-Type": "application/json"}
    if LITELLM_PROXY_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_PROXY_KEY}"
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{LITELLM_PROXY_URL}/chat/completions", json=payload, headers=headers
        )
        r.raise_for_status()
        data = r.json()
    return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")


async def llm_embed(text: str) -> list[float]:
    headers = {"Content-Type": "application/json"}
    if LITELLM_PROXY_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_PROXY_KEY}"
    payload = {"model": "embedder", "input": text}
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{LITELLM_PROXY_URL}/embeddings", json=payload, headers=headers
        )
        r.raise_for_status()
        data = r.json()
    return (data.get("data") or [{}])[0].get("embedding") or []


async def lightrag_insert(text: str, business_key: str = "integritasmrv") -> dict:
    headers = {"Content-Type": "application/json"}
    if LITELLM_PROXY_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_PROXY_KEY}"
    payload = {"text": text, "workspace": business_key}
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{LIGHTRAG_BASE_URL}/insert", json=payload, headers=headers
        )
        r.raise_for_status()
        return r.json()


async def lightrag_query(query: str, business_key: str = "integritasmrv", mode: str = "hybrid") -> dict:
    headers = {"Content-Type": "application/json"}
    if LITELLM_PROXY_KEY:
        headers["Authorization"] = f"Bearer {LITELLM_PROXY_KEY}"
    payload = {"query": query, "workspace": business_key, "mode": mode}
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.post(
            f"{LIGHTRAG_BASE_URL}/query", json=payload, headers=headers
        )
        r.raise_for_status()
        return r.json()


async def qdrant_upsert(
    collection: str,
    point_id: str,
    vector: list[float],
    payload: dict,
    business_key: str = "integritasmrv",
) -> dict:
    payload["business_key"] = business_key
    headers = {"Content-Type": "application/json"}
    body = {
        "points": [
            {"id": point_id, "vector": vector, "payload": payload}
        ]
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.put(
            f"{QDRANT_URL}/collections/{collection}/points",
            json=body,
            headers=headers,
        )
        r.raise_for_status()
        return r.json()


async def qdrant_search(
    collection: str,
    vector: list[float],
    business_key: str = "integritasmrv",
    limit: int = 10,
) -> dict:
    headers = {"Content-Type": "application/json"}
    body = {
        "vector": vector,
        "filter": {
            "must": [
                {"key": "business_key", "match": {"value": business_key}}
            ]
        },
        "limit": limit,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{QDRANT_URL}/collections/{collection}/points/search",
            json=body,
            headers=headers,
        )
        r.raise_for_status()
        return r.json()
