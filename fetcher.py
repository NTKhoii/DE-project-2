#hàm fetch dữ liệu sản phẩm từ API 
# fetcher.py
# Gửi hàng nghìn request song song (concurrently) đến API Tiki để lấy dữ liệu sản phẩm, xử lý lỗi (retry khi cần)
import asyncio
import aiohttp
from typing import List, Dict, Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import API_TEMPLATE, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_BACKOFF, USER_AGENT, CONCURRENCY
from utils.text_cleaner import clean_description

# Exceptions to retry on: network/timeouts and 5xx statuses
class FetchError(Exception):
    pass

async def _extract_fields(raw_json: dict) -> dict:
    """
    Extract only necessary fields: id, name, url_key, price, cleaned description, images urls.
    """
    product = {}
    product["id"] = raw_json.get("id")
    product["name"] = raw_json.get("name")
    product["url_key"] = raw_json.get("url_key")
    product["price"] = raw_json.get("price")
    # description cleaned
    product["description"] = clean_description(raw_json.get("description"))
    # images: collect list of image urls (large_url if present else base_url)
    images = []
    for img in raw_json.get("images") or []:
        url = img.get("large_url") or img.get("base_url") or img.get("thumbnail_url")
        if url:
            images.append(url)
    product["images"] = images
    return product

def _make_headers():
# Tạo headers chuẩn cho mỗi request (User-Agent, Accept, …).
# Giúp tránh bị chặn 403 từ API (do “bot detection”).
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
    }

def _raise_for_status(status: int):
    if status >= 500:
        raise FetchError(f"Server error {status}")
    # for 404 or 4xx we won't retry — return None later

# Sử dụng decorator @retry(...):
    # Retry tối đa MAX_RETRIES lần.
    # Dùng exponential backoff (tăng dần delay sau mỗi lần lỗi).
    # Chỉ retry nếu lỗi là Timeout, ClientError hoặc FetchError.


@retry(stop=stop_after_attempt(MAX_RETRIES),
       wait=wait_exponential(multiplier=RETRY_BACKOFF, max=30),
       retry=retry_if_exception_type((asyncio.TimeoutError, aiohttp.ClientError, FetchError)))
async def fetch_one(session: aiohttp.ClientSession, product_id: str) -> Optional[dict]:
    url = API_TEMPLATE.format(product_id=product_id)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    headers = _make_headers()
    async with session.get(url, headers=headers, timeout=timeout) as resp:
        status = resp.status
        if status == 200:
            try:
                data = await resp.json()
            except Exception:
                text = await resp.text()
                raise FetchError(f"Invalid JSON for {product_id}: {text[:200]}")
            return await _extract_fields(data)
        elif status == 404:
            # product not found — return minimal object or None
            return None
        else:
            # 4xx other -> treat as non-retriable except 429 maybe; 5xx throw for retry
            _raise_for_status(status)
            return None

async def fetch_batch(product_ids: List[str], concurrency: int = CONCURRENCY) -> List[dict]:
    """
    Fetch a list of product_ids concurrently and return list of extracted product dicts.
    Skips None results (404 or missing).
    """
    connector = aiohttp.TCPConnector(limit_per_host=concurrency, limit=0, ssl=False)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    semaphore = asyncio.Semaphore(concurrency)

    results: List[dict] = []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def bound_fetch(pid: str):
            async with semaphore:
                try:
                    res = await fetch_one(session, pid)
                    return res
                except Exception as e:
                    # log minimal info (print; main can use logging)
                    print(f"[fetch error] id={pid} error={e}")
                    return None

        tasks = [asyncio.create_task(bound_fetch(pid)) for pid in product_ids]
        for fut in asyncio.as_completed(tasks):
            item = await fut
            if item:
                results.append(item)

    return results
