import asyncio
import base64
import html
import hashlib
import json
import os
import re
import time
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import asyncpg
import httpx
from temporalio import activity

from app.llm import llm_complete

SCRAPIQ_DB_URL = os.environ.get(
    "SCRAPIQ_DATABASE_URL", os.environ.get("SCRAPIQ_DB_DSN", "")
)
SEARXNG_URL = os.environ.get("SEARXNG_URL", "http://127.0.0.1:3010")
WHOOGLE_SEARCH_URL = os.environ.get(
    "WHOOGLE_SEARCH_URL", "http://217.76.59.199:5000/search"
)
STRACT_SEARCH_URL = os.environ.get(
    "STRACT_SEARCH_URL", "https://stract.com/beta/api/search"
)
DDGS_REGION = os.environ.get("DDGS_REGION", "wt-wt")
DDGS_SAFESEARCH = os.environ.get("DDGS_SAFESEARCH", "off")
CRAWL_USER_AGENT = os.environ.get(
    "CRAWL_USER_AGENT",
    "IntegritasMRV-IntelligenceBot/1.0 (+https://integritasmrv.com)",
)
CRAWL_SSL_VERIFY = os.environ.get("CRAWL_SSL_VERIFY", "true").lower() == "true"
CRAWL_ALLOW_INSECURE_FALLBACK = (
    os.environ.get("CRAWL_ALLOW_INSECURE_FALLBACK", "true").lower() == "true"
)
CRAWL_MIN_LINK_FOLLOW_SCORE = int(os.environ.get("CRAWL_MIN_LINK_FOLLOW_SCORE", "45"))
CRAWL_MIN_SUMMARY_SCORE = int(os.environ.get("CRAWL_MIN_SUMMARY_SCORE", "50"))
CRAWL_MAX_SAME_DOMAIN_PAGES = int(os.environ.get("CRAWL_MAX_SAME_DOMAIN_PAGES", "15"))
CRAWL_FETCHING_STALE_SEC = int(os.environ.get("CRAWL_FETCHING_STALE_SEC", "150"))
SEED_QUERY_FANOUT = int(os.environ.get("SEED_QUERY_FANOUT", "6"))
SEED_EXCLUDED_DOMAINS = {"searx.space"}
TRUSTED_DISCOVERY_DOMAINS = {
    "linkedin.com",
    "crunchbase.com",
    "pitchbook.com",
    "bloomberg.com",
    "wikipedia.org",
    "craft.co",
}
NOISY_DISCOVERY_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
}

PROFILE_DIRECTORY_DOMAINS = {
    "linkedin.com",
    "crunchbase.com",
    "pitchbook.com",
    "wikipedia.org",
    "facebook.com",
    "x.com",
    "twitter.com",
    "instagram.com",
    "youtube.com",
    "github.com",
    "google.com",
}

NON_AUTHORITATIVE_STORE_DOMAINS = {
    "crunchbase.com",
    "pitchbook.com",
    "github.com",
    "issuu.com",
}

DOCUMENT_EXT_RE = re.compile(
    r"\.(pdf|doc|docx|xls|xlsx|ppt|pptx|csv|txt|rtf)(?:$|\?)",
    re.IGNORECASE,
)


async def _conn() -> asyncpg.Connection:
    if not SCRAPIQ_DB_URL:
        raise RuntimeError("SCRAPIQ_DATABASE_URL is not set")
    return await asyncpg.connect(SCRAPIQ_DB_URL)


async def _resolve_paperless_instance(
    conn: asyncpg.Connection, request_id: int
) -> dict | None:
    row = await conn.fetchrow(
        """
        SELECT i.id, i.base_url, i.api_token
        FROM nb_scrapiq_requests r
        LEFT JOIN nb_scrapiq_paperless_instances i ON i.id = r.paperless_instance_id
        WHERE r.id = $1
        """,
        request_id,
    )
    if row and row.get("id") and row.get("base_url") and row.get("api_token"):
        return dict(row)

    row = await conn.fetchrow(
        """
        SELECT id, base_url, api_token
        FROM nb_scrapiq_paperless_instances
        WHERE is_active = true
        ORDER BY is_default DESC, id ASC
        LIMIT 1
        """
    )
    return dict(row) if row else None


async def _upload_document_to_paperless(
    *,
    base_url: str,
    api_token: str,
    filename: str,
    content: bytes,
    content_type: str,
) -> tuple[int | None, str | None, str | None]:
    if not base_url or not api_token or not content:
        return None, None, None

    endpoint = base_url.rstrip("/") + "/api/documents/post_document/"
    headers = {"Authorization": f"Token {api_token}"}
    files = {
        "document": (
            filename or "document.bin",
            content,
            content_type or "application/octet-stream",
        )
    }
    data = {"title": filename or "document"}

    try:
        async with httpx.AsyncClient(
            timeout=40, follow_redirects=True, verify=False
        ) as c:
            r = await c.post(endpoint, headers=headers, files=files, data=data)
        if r.status_code not in (200, 201, 202):
            return None, None, (r.text or "")[:800]
        raw = (r.text or "").strip().strip('"')
        try:
            payload = r.json()
        except Exception:
            if raw:
                try:
                    return int(raw), raw, None
                except Exception:
                    return None, raw, None
            return None, None, None
        if isinstance(payload, dict):
            for key in ["document", "document_id", "id", "task_id", "task"]:
                val = payload.get(key)
                if val is None:
                    continue
                try:
                    return int(val), str(val), None
                except Exception:
                    return None, str(val), None
        if raw:
            try:
                return int(raw), raw, None
            except Exception:
                return None, raw, None
        return None, None, None
    except Exception as exc:
        return None, None, str(exc)[:800]


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8", errors="ignore")).hexdigest()


def _extract_links(html: str, base_url: str) -> list[str]:
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html or "", flags=re.IGNORECASE)
    out: list[str] = []
    for href in hrefs:
        h = (href or "").strip()
        if not h or h.startswith(("#", "mailto:", "javascript:", "tel:")):
            continue
        link = urljoin(base_url, h)
        if not link.startswith(("http://", "https://")):
            continue
        if link not in out:
            out.append(link)
        if len(out) >= 80:
            break
    return out


def _is_document_url(url: str) -> bool:
    return bool(DOCUMENT_EXT_RE.search(url or ""))


def _guess_document_type(url: str, content_type: str) -> str:
    ct = (content_type or "").lower()
    if "pdf" in ct:
        return "pdf"
    if "word" in ct or "doc" in ct:
        return "docx"
    if "excel" in ct or "spreadsheet" in ct or "xls" in ct:
        return "xlsx"
    if "powerpoint" in ct or "presentation" in ct or "ppt" in ct:
        return "pptx"
    m = DOCUMENT_EXT_RE.search(url or "")
    if m:
        return m.group(1).lower()
    return "document"


def _compact_summary(text: str, max_chars: int = 1000) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[:max_chars].rstrip()


def _page_summary_prompt(
    *, entity_type: str, entity_name: str, objective: str, url: str, html_content: str
) -> str:
    return (
        f"Create a factual summary for one {entity_type} evidence page. "
        "Return plain text only, maximum 1000 characters, no bullet list, no markdown. "
        "Focus on identity, offerings, location, leadership, contact signals, and evidence-backed facts only.\n\n"
        f"Entity name: {entity_name}\n"
        f"Objective: {objective}\n"
        f"Page URL: {url}\n"
        f"Page content:\n{html_content[:7000]}"
    )


def _same_domain(url: str, seed_domain: str) -> bool:
    d = (urlparse(url).netloc or "").lower().replace("www.", "")
    s = (seed_domain or "").lower().replace("www.", "")
    return bool(d and s and (d == s or d.endswith("." + s)))


def _root_domain(url: str) -> str:
    domain = (urlparse(url).netloc or "").lower().replace("www.", "")
    return domain


def _entity_tokens(entity_name: str) -> list[str]:
    return [
        t for t in re.findall(r"[a-z0-9]+", (entity_name or "").lower()) if len(t) >= 3
    ]


def _entity_slug(entity_name: str) -> str:
    tokens = _entity_tokens(entity_name)
    return "-".join(tokens[:6])


def _domain_has_entity_token(entity_name: str, url: str) -> bool:
    domain = _root_domain(url)
    if not domain:
        return False
    for tok in _entity_tokens(entity_name):
        if tok in domain:
            return True
    return False


def _normalize_linkedin_company_url(url: str) -> str | None:
    raw = (url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    host = (parsed.netloc or "").lower().replace("www.", "")
    if "linkedin.com" not in host:
        return None
    parts = [p for p in (parsed.path or "").split("/") if p]
    if len(parts) < 2 or parts[0].lower() != "company":
        return None
    slug = parts[1].strip()
    if not slug:
        return None
    return f"https://www.linkedin.com/company/{slug}"


def _is_directory_domain(domain: str) -> bool:
    d = (domain or "").lower().replace("www.", "")
    for item in PROFILE_DIRECTORY_DOMAINS:
        if d == item or d.endswith("." + item):
            return True
    return False


def _is_noise_domain(domain: str) -> bool:
    d = (domain or "").lower().replace("www.", "")
    for item in NOISY_DISCOVERY_DOMAINS:
        if d == item or d.endswith("." + item):
            return True
    return False


def _is_non_authoritative_store_domain(domain: str) -> bool:
    d = (domain or "").lower().replace("www.", "")
    for item in NON_AUTHORITATIVE_STORE_DOMAINS:
        if d == item or d.endswith("." + item):
            return True
    return False


async def _resolve_primary_profiles(entity_name: str) -> tuple[str, str]:
    if not (entity_name or "").strip():
        return "", ""
    query = f'"{entity_name}" official website linkedin company'
    provider_urls = await _seed_search_parallel(query, limit=10)
    merged: list[str] = []
    for provider in ["searxng", "whoogle", "ddgs", "stract"]:
        for u in provider_urls.get(provider, []):
            if u not in merged:
                merged.append(u)

    tokens = _entity_tokens(entity_name)
    website_candidates: list[tuple[float, str]] = []
    linkedin_candidates: list[tuple[float, str]] = []
    for u in merged:
        domain = _root_domain(u)
        if not domain:
            continue
        norm_li = _normalize_linkedin_company_url(u)
        if norm_li:
            score = 1.0
            lower = norm_li.lower()
            if any(tok in lower for tok in tokens):
                score += 1.2
            if "/life" in lower or "/jobs" in lower:
                score -= 0.4
            linkedin_candidates.append((score, norm_li))
            continue
        if _is_directory_domain(domain):
            continue
        lowered_url = (u or "").lower()
        if not any(tok in domain or tok in lowered_url for tok in tokens):
            continue
        score = 1.0
        if any(tok in domain for tok in tokens):
            score += 1.5
        if domain.endswith(".com"):
            score += 0.3
        website_candidates.append((score, f"https://{domain}"))

    website = ""
    linkedin = ""
    if website_candidates:
        website_candidates.sort(key=lambda x: x[0], reverse=True)
        website = website_candidates[0][1]
    if linkedin_candidates:
        linkedin_candidates.sort(key=lambda x: x[0], reverse=True)
        linkedin = linkedin_candidates[0][1]
    return website, linkedin


def _is_excluded_seed_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower().replace("www.", "")
        path = (parsed.path or "").lower()
        if domain in SEED_EXCLUDED_DOMAINS:
            return True
        if domain == "github.com" and path.startswith("/searxng/"):
            return True
    except Exception:
        return False
    return False


def _dedupe_urls(urls: list[str], max_urls: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        url = (u or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= max_urls:
            break
    return out


def _decode_bing_redirect(url: str) -> str:
    try:
        parsed = urlparse(html.unescape(url))
        if "bing.com" not in (parsed.netloc or ""):
            return html.unescape(url)
        params = parse_qs(parsed.query)
        token = (params.get("u") or [""])[0]
        if not token:
            return html.unescape(url)
        payload = token[2:] if token.startswith("a1") else token
        padding = "=" * ((4 - len(payload) % 4) % 4)
        decoded = base64.urlsafe_b64decode((payload + padding).encode("utf-8")).decode(
            "utf-8", "ignore"
        )
        if decoded.startswith(("http://", "https://")):
            return decoded
    except Exception:
        return html.unescape(url)
    return html.unescape(url)


def _score_domain_quality(url: str, seed_domain: str) -> int:
    domain = _root_domain(url)
    if not domain:
        return -20
    if _same_domain(url, seed_domain):
        return 18
    if domain in TRUSTED_DISCOVERY_DOMAINS:
        return 14
    if domain in NOISY_DISCOVERY_DOMAINS:
        return -12
    if domain.endswith(".gov") or domain.endswith(".edu"):
        return 6
    return 0


def _score_entity_match(entity_name: str, url: str, html: str) -> int:
    tokens = _entity_tokens(entity_name)
    if not tokens:
        return 0
    body = (html or "")[:5000].lower()
    url_l = (url or "").lower()
    phrase = " ".join(tokens)
    token_hits = sum(1 for t in tokens if t in body)

    score = 0
    if phrase and phrase in body:
        score += 18
    if phrase and phrase in url_l:
        score += 12
    score += min(16, token_hits * 4)

    if token_hits == 0 and phrase and phrase not in url_l:
        score -= 18
    elif token_hits == 1:
        score -= 4

    return score


def _extract_query_lines(prompt: str, entity_name: str, seed_domain: str) -> list[str]:
    raw_lines = [ln.strip() for ln in (prompt or "").splitlines() if ln.strip()]
    lines: list[str] = []
    for ln in raw_lines:
        if ln not in lines:
            lines.append(ln)
    if not lines:
        root = seed_domain or entity_name or "company"
        lines = [f"{root} company profile", f"{root} leadership", f"{root} funding"]
    if seed_domain and seed_domain not in lines:
        lines.insert(0, seed_domain)
    return lines[: max(3, SEED_QUERY_FANOUT)]


def _expand_query_variants(query: str, entity_name: str, seed_domain: str) -> list[str]:
    variants = [query.strip()]
    name = (entity_name or "").strip()
    if name and name.lower() not in query.lower():
        variants.append(f'"{name}" {query}')
    if name:
        variants.extend(
            [
                f'"{name}" linkedin',
                f'"{name}" crunchbase',
                f'"{name}" leadership team',
                f'"{name}" company profile',
            ]
        )
    if seed_domain:
        variants.append(f"site:{seed_domain} about")
    out: list[str] = []
    for v in variants:
        t = v.strip()
        if t and t not in out:
            out.append(t)
    return out[:6]


async def _seed_search_searx(query: str, limit: int) -> list[str]:
    urls: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                f"{SEARXNG_URL.rstrip('/')}/search",
                params={
                    "q": query,
                    "format": "json",
                    "language": "en",
                    "safesearch": 0,
                },
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code == 200:
                data = r.json()
                for item in (data.get("results") or [])[: max(1, limit)]:
                    u = (item.get("url") or "").strip()
                    if u.startswith(("http://", "https://")):
                        urls.append(u)
                if urls:
                    return _dedupe_urls(urls, max_urls=max(1, limit * 3))
    except Exception:
        pass

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                f"{SEARXNG_URL.rstrip('/')}/search",
                params={"q": query, "language": "en", "safesearch": 0},
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code == 200:
                html = r.text or ""
                hrefs = re.findall(
                    r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE
                )
                searx_host = (urlparse(SEARXNG_URL).netloc or "").lower()
                for href in hrefs:
                    u = (href or "").strip()
                    if not u:
                        continue
                    if u.startswith("/url?"):
                        qv = parse_qs(urlparse(u).query).get("q")
                        if qv:
                            u = unquote(qv[0]).strip()
                    if not u.startswith(("http://", "https://")):
                        continue
                    host = (urlparse(u).netloc or "").lower()
                    parsed = urlparse(u)
                    if searx_host and host.endswith(searx_host):
                        continue
                    if parsed.path.startswith("/search"):
                        continue
                    if u.startswith(("http://", "https://")):
                        urls.append(u)
                    if len(urls) >= max(1, limit * 3):
                        break
    except Exception:
        pass

    urls = _dedupe_urls(urls, max_urls=max(1, limit * 3))
    if urls:
        return urls

    # Stract endpoint can be regionally unavailable; keep provider slot alive with free fallback.
    return await _seed_search_ddgs(query, limit=max(1, limit))


async def _seed_search_whoogle(query: str, limit: int) -> list[str]:
    params = {
        "q": query,
        "output": "json",
        "output_format": "json",
    }
    urls: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                WHOOGLE_SEARCH_URL,
                params=params,
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code == 200:
                payload = r.json()
                rows = payload.get("results") or payload.get("items") or []
                for row in rows[: max(1, limit)]:
                    u = str(row.get("url") or row.get("link") or "").strip()
                    if u.startswith(("http://", "https://")):
                        urls.append(u)
                if urls:
                    return _dedupe_urls(urls, max_urls=max(1, limit * 3))
    except Exception:
        pass

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                WHOOGLE_SEARCH_URL,
                params={"q": query, "lang_search": "en"},
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code != 200:
                return []
            html = r.text or ""
            hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
            whoogle_host = (urlparse(WHOOGLE_SEARCH_URL).netloc or "").lower()
            for href in hrefs:
                link = (href or "").strip()
                if not link:
                    continue
                if link.startswith("/url?"):
                    qv = parse_qs(urlparse(link).query).get("q")
                    if qv:
                        link = unquote(qv[0]).strip()
                if not link.startswith(("http://", "https://")):
                    continue
                host = (urlparse(link).netloc or "").lower()
                if whoogle_host and host.endswith(whoogle_host):
                    continue
                if host.startswith("maps.google."):
                    continue
                urls.append(link)
                if len(urls) >= max(1, limit * 3):
                    break
    except Exception:
        return []
    return _dedupe_urls(urls, max_urls=max(1, limit * 3))


async def _seed_search_ddgs(query: str, limit: int) -> list[str]:
    try:
        from ddgs import DDGS
    except Exception:
        try:
            from duckduckgo_search import DDGS
        except Exception:
            return []

    def _run_sync() -> list[str]:
        found: list[str] = []
        with DDGS() as ddgs:
            rows = list(
                ddgs.text(
                    query,
                    region=DDGS_REGION,
                    safesearch=DDGS_SAFESEARCH,
                    max_results=max(1, limit),
                )
            )
        for row in rows:
            u = str(row.get("href") or row.get("url") or "").strip()
            if u.startswith(("http://", "https://")):
                found.append(u)
        return found

    try:
        urls = await asyncio.to_thread(_run_sync)
        return _dedupe_urls(urls, max_urls=max(1, limit * 3))
    except Exception:
        return []


async def _seed_search_stract(query: str, limit: int) -> list[str]:
    urls: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.post(
                STRACT_SEARCH_URL,
                json={
                    "query": query,
                    "offset": 0,
                    "limit": max(1, min(limit, 20)),
                },
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code == 200:
                data = r.json()
                rows = data.get("webpages") or data.get("results") or []
                if isinstance(rows, dict):
                    rows = rows.get("results") or rows.get("items") or []
                for row in rows[: max(1, limit)]:
                    u = str(row.get("url") or row.get("link") or "").strip()
                    if u.startswith(("http://", "https://")):
                        urls.append(u)
    except Exception:
        pass

    if urls:
        return _dedupe_urls(urls, max_urls=max(1, limit * 3))

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                STRACT_SEARCH_URL,
                params={"q": query, "count": max(1, min(limit, 20))},
                headers={"User-Agent": CRAWL_USER_AGENT},
            )
            if r.status_code != 200:
                raise RuntimeError("stract_get_unavailable")
            data = r.json()
            rows = data.get("webpages") or data.get("results") or []
            if isinstance(rows, dict):
                rows = rows.get("results") or rows.get("items") or []
            for row in rows[: max(1, limit)]:
                u = str(row.get("url") or row.get("link") or "").strip()
                if u.startswith(("http://", "https://")):
                    urls.append(u)
    except Exception:
        pass

    if urls:
        return _dedupe_urls(urls, max_urls=max(1, limit * 3))

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(
                "https://www.bing.com/search",
                params={"q": query},
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
                },
            )
            if r.status_code != 200:
                raise RuntimeError("bing_fallback_unavailable")
            html = r.text or ""
            hrefs = re.findall(
                r'<h2[^>]*>\s*<a[^>]+href="([^"]+)"',
                html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            for href in hrefs[: max(1, limit * 3)]:
                u = _decode_bing_redirect((href or "").strip())
                if u.startswith(("http://", "https://")):
                    host = (urlparse(u).netloc or "").lower()
                    if "bing.com" in host:
                        continue
                    urls.append(u)
    except Exception:
        pass

    urls = _dedupe_urls(urls, max_urls=max(1, limit * 3))
    if urls:
        return urls

    # Keep 4-way fanout resilient when Stract is unavailable from this region.
    return await _seed_search_ddgs(query, limit=max(1, limit))


async def _seed_search_parallel(query: str, limit: int) -> dict[str, list[str]]:
    provider_names = ["searxng", "whoogle", "ddgs", "stract"]
    tasks = [
        _seed_search_searx(query, limit=max(1, limit)),
        _seed_search_whoogle(query, limit=max(1, limit)),
        _seed_search_ddgs(query, limit=max(1, limit)),
        _seed_search_stract(query, limit=max(1, limit)),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out: dict[str, list[str]] = {}
    for name, result in zip(provider_names, results):
        if isinstance(result, Exception):
            out[name] = []
            continue
        out[name] = _dedupe_urls(list(result), max_urls=max(1, limit * 3))
    return out


@activity.defn
async def init_crawl_session(request_id: int) -> None:
    conn = await _conn()
    try:
        req = await conn.fetchrow(
            """
            SELECT id, max_results, extra
            FROM nb_scrapiq_requests
            WHERE id=$1
            """,
            request_id,
        )
        if not req:
            raise RuntimeError(f"request_id {request_id} not found")
        max_pages = int(req.get("max_results") or 200)
        await conn.execute(
            """
            INSERT INTO nb_scrapiq_crawl_sessions
              (request_id, max_pages, started_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (request_id)
            DO UPDATE SET max_pages=EXCLUDED.max_pages, started_at=COALESCE(nb_scrapiq_crawl_sessions.started_at, NOW())
            """,
            request_id,
            max_pages,
        )
        await conn.execute(
            "UPDATE nb_scrapiq_requests SET status='ongoing', started_at=COALESCE(started_at,NOW()) WHERE id=$1",
            request_id,
        )
    finally:
        await conn.close()


@activity.defn
async def seed_frontier_from_searxng(request_id: int) -> int:
    conn = await _conn()
    try:
        req = await conn.fetchrow(
            "SELECT prompt, starting_url, max_results, context_entity_name FROM nb_scrapiq_requests WHERE id=$1",
            request_id,
        )
        if not req:
            return 0
        query = (str(req["prompt"] or "")).strip() or "company research"
        starting_url = (str(req["starting_url"] or "")).strip()
        max_results = int(req["max_results"] or 20)
        entity_name = str(req.get("context_entity_name") or "").strip()

        resolved_main_website = ""
        resolved_linkedin = ""
        if not starting_url and entity_name:
            resolved_main_website, resolved_linkedin = await _resolve_primary_profiles(
                entity_name
            )
            if resolved_main_website:
                starting_url = resolved_main_website
                await conn.execute(
                    "UPDATE nb_scrapiq_requests SET starting_url=$2 WHERE id=$1 AND (starting_url IS NULL OR starting_url='')",
                    request_id,
                    starting_url,
                )

        seed_domain = (urlparse(starting_url).netloc or "").replace("www.", "")

        discovered: list[str] = []
        discovered_scores: dict[str, float] = {}
        inserted = 0

        if starting_url:
            try:
                r0 = await conn.execute(
                    """
                    INSERT INTO nb_scrapiq_crawl_frontier
                      (request_id, url, url_hash, crawl_depth, final_priority, domain, is_same_domain, status)
                    VALUES ($1,$2,$3,0,$5,$4,true,'queued')
                    ON CONFLICT (request_id, url_hash) DO NOTHING
                    """,
                    request_id,
                    starting_url,
                    _url_hash(starting_url),
                    (urlparse(starting_url).netloc or "").lower(),
                    2.8 if _is_document_url(starting_url) else 1.6,
                )
                if str(r0).endswith("1"):
                    inserted = 1
                discovered.append(starting_url)
                discovered_scores[starting_url] = (
                    discovered_scores.get(starting_url, 0.0) + 2.5
                )
            except Exception:
                pass

        if resolved_linkedin:
            discovered_scores[resolved_linkedin] = (
                discovered_scores.get(resolved_linkedin, 0.0) + 1.4
            )

        base_queries = _extract_query_lines(query, entity_name, seed_domain)
        query_variants: list[str] = []
        for q in base_queries:
            for variant in _expand_query_variants(q, entity_name, seed_domain):
                if variant not in query_variants:
                    query_variants.append(variant)
                if len(query_variants) >= max(6, SEED_QUERY_FANOUT * 2):
                    break
                if len(query_variants) >= max(6, SEED_QUERY_FANOUT * 2):
                    break

        slug = _entity_slug(entity_name)
        if slug:
            for seeded in [
                f"https://www.linkedin.com/company/{slug}",
                f"https://www.crunchbase.com/organization/{slug}",
                f"https://www.pitchbook.com/profiles/company/{slug}",
            ]:
                discovered_scores[seeded] = discovered_scores.get(seeded, 0.0) + 0.24

        for q in query_variants:
            provider_urls = await _seed_search_parallel(q, limit=max(5, max_results))
            provider_weights = {
                "searxng": 1.00,
                "whoogle": 1.00,
                "ddgs": 0.95,
                "stract": 0.90,
            }
            merged_scores: dict[str, float] = {}
            for provider, urls in provider_urls.items():
                weight = float(provider_weights.get(provider, 1.0))
                for rank, u in enumerate(urls[: max_results * 3], start=1):
                    if not u.startswith(("http://", "https://")):
                        continue
                    if _is_excluded_seed_url(u):
                        continue
                    merged_scores[u] = merged_scores.get(u, 0.0) + (
                        weight / float(rank + 1)
                    )

            merged = [
                u
                for u, _ in sorted(
                    merged_scores.items(), key=lambda item: item[1], reverse=True
                )
            ]

            activity.logger.info(
                "crawl_seed_provider_counts",
                request_id=request_id,
                query=q,
                searxng_count=len(provider_urls.get("searxng") or []),
                whoogle_count=len(provider_urls.get("whoogle") or []),
                ddgs_count=len(provider_urls.get("ddgs") or []),
                stract_count=len(provider_urls.get("stract") or []),
                merged_count=len(merged),
            )

            for rank, u in enumerate(merged[: max_results * 3], start=1):
                if not u.startswith(("http://", "https://")):
                    continue
                if _is_excluded_seed_url(u):
                    continue
                rank_score = 1.0 / float(rank + 1)
                fusion = rank_score + (_score_domain_quality(u, seed_domain) / 100.0)
                if entity_name:
                    lowered = u.lower()
                    for tok in _entity_tokens(entity_name):
                        if tok in lowered:
                            fusion += 0.08
                            break
                discovered_scores[u] = discovered_scores.get(u, 0.0) + fusion

        discovered = [
            u
            for u, _ in sorted(
                discovered_scores.items(), key=lambda item: item[1], reverse=True
            )
        ]

        if starting_url and starting_url not in discovered:
            discovered.insert(0, starting_url)
        activity.logger.info(
            "crawl_seed_candidates",
            request_id=request_id,
            starting_url=starting_url,
            discovered_count=len(discovered),
        )
        if not discovered:
            return 0

        for i, url in enumerate(discovered):
            if url == starting_url:
                continue
            same = _same_domain(url, seed_domain)
            fusion = float(discovered_scores.get(url) or 0.0)
            pri = max(0.2, 1.15 - (i * 0.03))
            if same:
                pri += 0.25
            else:
                pri -= 0.05
            domain_bonus = _score_domain_quality(url, seed_domain)
            if entity_name:
                lowered = (url or "").lower()
                for tok in _entity_tokens(entity_name):
                    if tok in lowered:
                        domain_bonus += 3
                        break
            pri += domain_bonus / 100.0
            pri += min(0.35, fusion)
            if _is_document_url(url):
                pri += 0.9
            pri = max(0.1, min(2.8, pri))
            domain = (urlparse(url).netloc or "").lower()
            try:
                result = await conn.execute(
                    """
                    INSERT INTO nb_scrapiq_crawl_frontier
                      (request_id, url, url_hash, crawl_depth, final_priority, domain, is_same_domain, status)
                    VALUES ($1,$2,$3,0,$4,$5,$6,'queued')
                    ON CONFLICT (request_id, url_hash) DO NOTHING
                    """,
                    request_id,
                    url,
                    _url_hash(url),
                    pri,
                    domain,
                    same,
                )
                if str(result).endswith("1"):
                    inserted += 1
            except Exception:
                continue

        await conn.execute(
            "UPDATE nb_scrapiq_crawl_sessions SET pages_queued=$2 WHERE request_id=$1",
            request_id,
            inserted,
        )
        activity.logger.info(
            "crawl_seed_inserted",
            request_id=request_id,
            inserted=inserted,
        )
        return inserted
    finally:
        await conn.close()


@activity.defn
async def check_stop_conditions(request_id: int) -> bool:
    conn = await _conn()
    try:
        await conn.execute(
            """
            UPDATE nb_scrapiq_crawl_frontier
            SET status='queued', error_message=COALESCE(error_message, 'requeued_stale_fetch')
            WHERE request_id=$1
              AND status='fetching'
              AND fetched_at IS NOT NULL
              AND fetched_at < NOW() - (($2::text || ' seconds')::interval)
            """,
            request_id,
            str(CRAWL_FETCHING_STALE_SEC),
        )

        session = await conn.fetchrow(
            "SELECT max_pages, max_duration_minutes, started_at FROM nb_scrapiq_crawl_sessions WHERE request_id=$1",
            request_id,
        )
        if not session:
            return True

        processed = await conn.fetchval(
            "SELECT COUNT(*) FROM nb_scrapiq_result_pages WHERE request_id=$1",
            request_id,
        )
        stored = await conn.fetchval(
            "SELECT COUNT(*) FROM nb_scrapiq_result_pages WHERE request_id=$1 AND status='stored'",
            request_id,
        )
        queued = await conn.fetchval(
            "SELECT COUNT(*) FROM nb_scrapiq_crawl_frontier WHERE request_id=$1 AND status='queued'",
            request_id,
        )

        await conn.execute(
            "UPDATE nb_scrapiq_crawl_sessions SET pages_crawled=$2, pages_queued=$3 WHERE request_id=$1",
            request_id,
            int(processed or 0),
            int(queued or 0),
        )

        activity.logger.info(
            "crawl_stop_probe",
            request_id=request_id,
            processed=int(processed or 0),
            stored=int(stored or 0),
            queued=int(queued or 0),
            max_pages=int(session.get("max_pages") or 200),
        )

        if int(processed or 0) >= int(session.get("max_pages") or 200):
            return True
        if int(queued or 0) <= 0:
            return True

        started_at = session.get("started_at")
        if started_at:
            elapsed_min = (time.time() - started_at.timestamp()) / 60.0
            if elapsed_min >= float(session.get("max_duration_minutes") or 30):
                return True
        return False
    finally:
        await conn.close()


@activity.defn
async def pop_frontier_url(request_id: int) -> dict | None:
    conn = await _conn()
    try:
        row = await conn.fetchrow(
            """
            WITH stats AS (
              SELECT COUNT(*)::int AS done_same
              FROM nb_scrapiq_crawl_frontier
              WHERE request_id=$1 AND status='done' AND is_same_domain=true
            ),
            nxt AS (
              SELECT id
              FROM nb_scrapiq_crawl_frontier f, stats s
              WHERE f.request_id=$1 AND f.status='queued'
                AND (f.is_same_domain=false OR s.done_same < $2)
              ORDER BY final_priority DESC, id ASC
              LIMIT 1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE nb_scrapiq_crawl_frontier f
            SET status='fetching', fetched_at=NOW()
            FROM nxt
            WHERE f.id=nxt.id
            RETURNING f.id, f.url, f.crawl_depth, f.domain
            """,
            request_id,
            CRAWL_MAX_SAME_DOMAIN_PAGES,
        )
        if row:
            return dict(row)

        row = await conn.fetchrow(
            """
            WITH nxt AS (
              SELECT id
              FROM nb_scrapiq_crawl_frontier
              WHERE request_id=$1 AND status='queued'
              ORDER BY final_priority DESC, id ASC
              LIMIT 1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE nb_scrapiq_crawl_frontier f
            SET status='fetching', fetched_at=NOW()
            FROM nxt
            WHERE f.id=nxt.id
            RETURNING f.id, f.url, f.crawl_depth, f.domain
            """,
            request_id,
        )
        return dict(row) if row else None
    finally:
        await conn.close()


@activity.defn
async def fetch_and_process_url(request_id: int, url_item: dict) -> dict:
    conn = await _conn()
    frontier_id = int(url_item["id"])
    url = str(url_item["url"])
    crawl_depth = int(url_item.get("crawl_depth") or 0)
    t0 = time.time()
    try:
        req = await conn.fetchrow(
            "SELECT context_entity_name, context_entity_type, prompt, min_relevance_score, starting_url, paperless_instance_id FROM nb_scrapiq_requests WHERE id=$1",
            request_id,
        )
        entity_name = (req.get("context_entity_name") if req else "") or "entity"
        entity_type = (req.get("context_entity_type") if req else "") or "company"
        objective = (req.get("prompt") if req else "") or "Research objective"
        min_req_score = int(
            float((req.get("min_relevance_score") if req else 0.5) or 0.5) * 100
        )
        seed_domain = _root_domain((req.get("starting_url") if req else "") or "")

        headers = {
            "User-Agent": CRAWL_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        if _is_document_url(url):
            async with httpx.AsyncClient(
                timeout=25, follow_redirects=True, verify=False
            ) as client:
                resp = await client.get(url, headers=headers)
                http_code = resp.status_code
                content_type = str(resp.headers.get("content-type") or "")
                binary_content = resp.content or b""
                file_size = len(binary_content)
                text_hint = ""
                try:
                    text_hint = binary_content.decode("utf-8", errors="ignore")[:5000]
                except Exception:
                    text_hint = ""
                text_hint = text_hint.replace("\x00", " ")

            if not text_hint:
                text_hint = f"Document URL: {url}"

            try:
                summary = await llm_complete(
                    "summarizer",
                    (
                        "Summarize this document evidence for enrichment. "
                        "Return plain text only, maximum 1000 characters.\n\n"
                        f"Entity: {entity_name} ({entity_type})\n"
                        f"Objective: {objective}\n"
                        f"Document URL: {url}\n"
                        f"Extracted text preview:\n{text_hint[:4000]}"
                    ),
                    max_tokens=450,
                )
                summary = _compact_summary(summary, max_chars=1000)
            except Exception:
                summary = _compact_summary(text_hint, max_chars=1000)

            score = max(65, min(95, min_req_score + 20))
            doc_status = "stored"
            paperless_instance_id = None
            paperless_doc_id = None
            paperless_task_ref = None
            paperless_error = None
            instance = await _resolve_paperless_instance(conn, request_id)
            if instance:
                paperless_instance_id = int(instance["id"])
                (
                    paperless_doc_id,
                    paperless_task_ref,
                    paperless_error,
                ) = await _upload_document_to_paperless(
                    base_url=str(instance.get("base_url") or ""),
                    api_token=str(instance.get("api_token") or ""),
                    filename=(
                        urlparse(url).path.rsplit("/", 1)[-1] or "document"
                    ).strip()[:500],
                    content=binary_content,
                    content_type=content_type,
                )

            doc_id = await conn.fetchval(
                """
                INSERT INTO nb_scrapiq_result_documents
                  (request_id, document_name, document_url, source_page_url, file_type,
                   file_size_bytes, relevance_score, relevance_reasoning, ai_summary,
                   ai_summary_alias, total_processing_ms, status, paperless_doc_id,
                   paperless_instance_id, storage_path, error_message, stored_at)
                VALUES
                  ($1, $2, $3, $4, $5,
                   $6, $7, $8, $9,
                   'summarizer', $10, $11, $12, $13, $14, $15, NOW())
                RETURNING id
                """,
                request_id,
                (urlparse(url).path.rsplit("/", 1)[-1] or "document").strip()[:500],
                url,
                None,
                _guess_document_type(url, content_type),
                int(file_size),
                float(score),
                "document_discovery",
                summary,
                int((time.time() - t0) * 1000),
                doc_status,
                paperless_doc_id,
                paperless_instance_id,
                (
                    f"paperless-task:{paperless_task_ref}"
                    if paperless_task_ref
                    else None
                ),
                paperless_error,
            )

            await conn.execute(
                """
                UPDATE nb_scrapiq_crawl_frontier
                SET status='done', http_status_code=$2, fetch_duration_ms=$3,
                    page_relevance_score=$4, result_doc_id=$5
                WHERE id=$1
                """,
                frontier_id,
                http_code,
                int((time.time() - t0) * 1000),
                float(score),
                int(doc_id),
            )
            return {"links": [], "relevance": score, "status": doc_status}

        async with httpx.AsyncClient(
            timeout=20, follow_redirects=True, verify=False
        ) as client:
            resp = await client.get(url, headers=headers)
            http_code = resp.status_code
            html = resp.text or ""
        relevance_prompt = (
            "Score relevance 0-100 and provide one-sentence reasoning. "
            'Return JSON: {"score":75,"reasoning":"..."}.\n'
            f"Entity: {entity_name} ({entity_type})\n"
            f"Research objective: {objective}\n"
            f"Page content (first 1000 chars): {html[:1000]}"
        )
        score = 45
        reasoning = "default"
        try:
            score_raw = await llm_complete(
                "relevance-scorer", relevance_prompt, json_mode=True, max_tokens=220
            )
            parsed = json.loads(score_raw)
            if isinstance(parsed, dict):
                score = int(float(parsed.get("score") or 50))
                reasoning = str(parsed.get("reasoning") or "default")[:600]
        except Exception:
            sample = html[:3000].lower()
            url_l = url.lower()
            tokens = _entity_tokens(entity_name)
            token_hits = sum(1 for t in tokens if t in sample)
            if token_hits >= 2:
                score = max(score, 72)
                reasoning = "heuristic_entity_token_match"
            elif token_hits == 1:
                score = max(score, 60)
                reasoning = "heuristic_partial_entity_match"
            elif any(t in url_l for t in tokens):
                score = max(score, 58)
                reasoning = "heuristic_url_entity_match"
            else:
                score = 35
                reasoning = "heuristic_low_entity_match"

        try:
            summary = await llm_complete(
                "summarizer",
                _page_summary_prompt(
                    entity_type=entity_type,
                    entity_name=entity_name,
                    objective=objective,
                    url=url,
                    html_content=html,
                ),
                max_tokens=450,
            )
            summary = _compact_summary(summary, max_chars=1000)
        except Exception:
            summary = _compact_summary(re.sub(r"<[^>]+>", " ", html), max_chars=1000)

        domain_quality = _score_domain_quality(url, seed_domain)
        entity_match = _score_entity_match(entity_name, url, html)
        score += domain_quality
        score += entity_match

        is_same = _same_domain(url, seed_domain)
        domain = _root_domain(url)
        if (not is_same) and _is_noise_domain(domain):
            score = min(score, 18)
            reasoning = "discard_noise_domain"
        if (not is_same) and _is_non_authoritative_store_domain(domain):
            score = min(score, 22)
            reasoning = "discard_non_authoritative_domain"
        if not is_same and entity_match < 10:
            score -= 25
        if not is_same and not _domain_has_entity_token(entity_name, url):
            if domain not in TRUSTED_DISCOVERY_DOMAINS:
                score -= 12
        score = max(0, min(100, score))

        page_status = "stored" if score >= min_req_score else "discarded"
        existing_page_id = await conn.fetchval(
            "SELECT id FROM nb_scrapiq_result_pages WHERE request_id=$1 AND found_page_url=$2 LIMIT 1",
            request_id,
            url,
        )
        if existing_page_id:
            page_id = int(existing_page_id)
        else:
            page_id = await conn.fetchval(
                """
                INSERT INTO nb_scrapiq_result_pages
                  (request_id, base_url, found_page_url, page_title, relevance_score,
                    relevance_reasoning, ai_summary, ai_summary_alias, http_status_code,
                    crawl_depth, discovered_via, total_processing_ms, status, stored_at)
                VALUES
                  ($1,$2,$3,$4,$5,$6,$7,'summarizer',$8,$9,'scrapling_crawl',$10,$11,NOW())
                RETURNING id
                """,
                request_id,
                str(
                    urlparse(url)
                    ._replace(path="", params="", query="", fragment="")
                    .geturl()
                ),
                url,
                "",
                float(score),
                reasoning,
                summary,
                http_code,
                crawl_depth,
                int((time.time() - t0) * 1000),
                page_status,
            )

        await conn.execute(
            """
            UPDATE nb_scrapiq_crawl_frontier
            SET status='done', http_status_code=$2, fetch_duration_ms=$3,
                page_relevance_score=$4, result_page_id=$5
            WHERE id=$1
            """,
            frontier_id,
            http_code,
            int((time.time() - t0) * 1000),
            float(score),
            page_id,
        )

        links = (
            _extract_links(html, url) if score >= CRAWL_MIN_LINK_FOLLOW_SCORE else []
        )
        return {"links": links, "relevance": score, "status": page_status}
    except Exception as exc:
        await conn.execute(
            "UPDATE nb_scrapiq_crawl_frontier SET status='failed', error_message=$2 WHERE id=$1",
            frontier_id,
            str(exc)[:900],
        )
        return {"links": [], "relevance": 0}
    finally:
        await conn.close()


@activity.defn
async def score_and_enqueue_links(
    request_id: int, links: list[str], crawl_depth: int
) -> int:
    conn = await _conn()
    try:
        req = await conn.fetchrow(
            "SELECT starting_url FROM nb_scrapiq_requests WHERE id=$1", request_id
        )
        seed_domain = (
            urlparse((req.get("starting_url") if req else "") or "").netloc or ""
        ).replace("www.", "")
        inserted = 0
        for i, link in enumerate(links[:50]):
            if not link.startswith(("http://", "https://")):
                continue
            if re.search(
                r"\.(png|jpg|jpeg|gif|svg|webp|css|js|ico|woff2?)($|\?)",
                link,
                re.IGNORECASE,
            ):
                continue
            same = _same_domain(link, seed_domain)
            domain = _root_domain(link)
            if (not same) and _is_noise_domain(domain):
                continue
            if (not same) and _is_non_authoritative_store_domain(domain):
                continue
            priority = 0.8 if same else 0.4
            priority -= min(0.25, i * 0.01)
            try:
                await conn.execute(
                    """
                    INSERT INTO nb_scrapiq_crawl_frontier
                      (request_id, url, url_hash, crawl_depth, final_priority, domain, is_same_domain, status)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,'queued')
                    ON CONFLICT (request_id, url_hash) DO NOTHING
                    """,
                    request_id,
                    link,
                    _url_hash(link),
                    int(crawl_depth),
                    float(max(0.1, priority)),
                    (urlparse(link).netloc or "").lower(),
                    same,
                )
                inserted += 1
            except Exception:
                continue
        return inserted
    finally:
        await conn.close()


@activity.defn
async def finalize_crawl(request_id: int) -> dict:
    conn = await _conn()
    try:
        stored_pages = int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM nb_scrapiq_result_pages WHERE request_id=$1 AND status='stored'",
                request_id,
            )
            or 0
        )
        discarded_pages = int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM nb_scrapiq_result_pages WHERE request_id=$1 AND status='discarded'",
                request_id,
            )
            or 0
        )
        processed_pages = stored_pages + discarded_pages
        failed = int(
            await conn.fetchval(
                "SELECT COUNT(*) FROM nb_scrapiq_crawl_frontier WHERE request_id=$1 AND status='failed'",
                request_id,
            )
            or 0
        )
        started = await conn.fetchval(
            "SELECT started_at FROM nb_scrapiq_requests WHERE id=$1", request_id
        )
        duration = 0
        if started:
            duration = int(max(0, time.time() - started.timestamp()))

        await conn.execute(
            """
            UPDATE nb_scrapiq_requests
            SET status='finished', finished_at=NOW(), duration_seconds=$2,
                results_found=$3, results_processed=$4, results_failed=$5
            WHERE id=$1
            """,
            request_id,
            duration,
            stored_pages,
            processed_pages,
            failed,
        )
        await conn.execute(
            "UPDATE nb_scrapiq_crawl_sessions SET stopped_at=NOW(), pages_crawled=$2 WHERE request_id=$1",
            request_id,
            processed_pages,
        )
        await conn.execute(
            """
            UPDATE nb_scrapiq_crawl_frontier
            SET status='stopped', error_message=COALESCE(error_message, 'stopped_by_limit')
            WHERE request_id=$1 AND status IN ('queued', 'fetching')
            """,
            request_id,
        )
        return {
            "request_id": request_id,
            "stored": stored_pages,
            "processed": processed_pages,
            "discarded": discarded_pages,
            "failed": failed,
        }
    finally:
        await conn.close()
