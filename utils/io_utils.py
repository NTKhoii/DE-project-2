# hàm độc ghi file json
# utils/io_utils.py
from pathlib import Path
from typing import List, Iterable
import orjson
import aiofiles
import asyncio

def read_ids_from_file(path: str | Path) -> List[str]:
# Đọc toàn bộ file chứa danh sách product_id (mỗi dòng 1 id) và trả về List[str].
    path = Path(path)
    ids = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ids.append(line)
    return ids

def chunk_list(iterable: Iterable, chunk_size: int):
# Chia danh sách lớn (ví dụ 200k product_id) thành nhiều batch nhỏ, mỗi batch có tối đa chunk_size phần tử.
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

async def save_batch_async(data_batch: List[dict], batch_index: int, output_dir: str | Path):
    """
    Save a batch list of dicts to a .json file as binary (orjson) asynchronously.
    Filename: products_{batch_index:04}.json
    Ghi một batch kết quả (danh sách các dict sản phẩm) vào file .json, bằng I/O bất đồng bộ (aiofiles)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = output_dir / f"products_{batch_index:04}.json"
    # orjson.dumps returns bytes
    content = orjson.dumps(data_batch, option=orjson.OPT_NON_STR_KEYS)
    # Use aiofiles to write bytes
    async with aiofiles.open(filename, "wb") as f:
        await f.write(content)

# # synchronous convenience wrapper
# def save_batch(data_batch: List[dict], batch_index: int, output_dir: str | Path):
#     output_dir = Path(output_dir)
#     output_dir.mkdir(parents=True, exist_ok=True)
#     filename = output_dir / f"products_{batch_index:04}.json"
#     with open(filename, "wb") as f:
#         f.write(orjson.dumps(data_batch, option=orjson.OPT_NON_STR_KEYS))
