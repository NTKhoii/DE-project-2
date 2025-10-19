import asyncio
import aiohttp
from typing import List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pathlib import Path
from datetime import datetime
import aiofiles

from config import API_TEMPLATE, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF, USER_AGENT, CONCURRENCY
from utils.text_cleaner import clean_description

class FetchError(Exception):
    pass

def _make_headers():
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
    }

async def log_error(product_id: str, error: str, batch_index: int, logs_dir: str = "logs"):
    """
    Ghi log lỗi bất đồng bộ. Tạo folder logs nếu chưa có.
    """
    log_dir = Path(logs_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "errors.log"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiofiles.open(log_path, "a", encoding="utf-8") as f:
        await f.write(f"[{ts}] Batch {batch_index} | ID={product_id} | Error={error}\n")

async def _extract_fields(raw_json: dict) -> dict:
    product = {
        "id": raw_json.get("id"),
        "name": raw_json.get("name"),
        "url_key": raw_json.get("url_key"),
        "price": raw_json.get("price"),
        "description": clean_description(raw_json.get("description")),
        "images": [img.get("large_url") or img.get("base_url")
                   for img in (raw_json.get("images") or []) if img],
    }
    return product

# Retry only on network/timeouts/server errors (we will NOT raise for 404)
@retry(stop=stop_after_attempt(MAX_RETRIES),
       wait=wait_exponential(multiplier=RETRY_BACKOFF, max=30),
       retry=retry_if_exception_type((asyncio.TimeoutError, aiohttp.ClientError, FetchError)))
async def fetch_one(session: aiohttp.ClientSession, product_id: str) -> Optional[dict]:
    url = API_TEMPLATE.format(product_id=product_id)
    try:
        async with session.get(url, headers=_make_headers(), timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status == 200:
                try:
                    data = await resp.json()
                except Exception as e:
                    raise FetchError(f"Invalid JSON: {e}")
                return await _extract_fields(data)
            elif resp.status == 404:
                # product not found -> do NOT retry, just return None
                return None
            elif 400 <= resp.status < 500:
                # other client errors -> do not retry
                return None
            else:
                # server errors (5xx) -> raise to trigger retry
                raise FetchError(f"Server {resp.status}")
    except asyncio.TimeoutError as e:
        # Let tenacity handle retry for timeouts
        raise
    except aiohttp.ClientError as e:
        # network-level errors -> retry
        raise
    except Exception as e:
        # wrap unexpected exceptions as FetchError so tenacity can retry if configured
        raise FetchError(str(e))

async def fetch_batch(product_ids: List[str], concurrency: int = CONCURRENCY, batch_index: int = 0) -> List[dict]:
    """
    Fetch product_ids concurrently. Returns list of successful product dicts.
    Logs errors into logs/errors.log (async).
    """
    results: List[dict] = []
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit_per_host=concurrency, ssl=False)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        async def bound_fetch(pid: str):
            async with sem:
                try:
                    res = await fetch_one(session, pid)
                    if res is None:
                        # treat None as not-found or client error; log it (optional)
                        await log_error(pid, "Not found or client error", batch_index)
                    return res
                except Exception as e:
                    # tenacity can raise RetryError wrapping the final exception,
                    # but here we catch any exception and log it (so single failure won't crash)
                    await log_error(pid, str(e), batch_index)
                    return None

        tasks = [asyncio.create_task(bound_fetch(pid)) for pid in product_ids]

        # iterate completed tasks safely, but also protect await fut with try/except
        for i, fut in enumerate(asyncio.as_completed(tasks), start=1):
            try:
                res = await fut  # this will not raise (we catch inside bound_fetch), but keep try anyway
                if res:
                    results.append(res)
            except Exception as e:
                # If anything bubbles up unexpectedly, log it and continue
                await log_error("unknown", f"Unexpected task error: {e}", batch_index)
            if i % 100 == 0:
                print(f"  Progress: {i}/{len(tasks)} fetched...")

    print(f"Batch done → {len(results)}/{len(product_ids)} success.")
    return results
