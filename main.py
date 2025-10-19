import asyncio
import time
import sys
import json
from tqdm.asyncio import tqdm
from pathlib import Path
import aiofiles
from utils.io_utils import read_ids_from_file, chunk_list
from fetcher import fetch_batch
from config import IDS_FILE, OUTPUT_DIR, BATCH_SIZE, CONCURRENCY

def get_completed_batches(output_dir: Path) -> set[int]:
    """
    Chỉ đánh dấu batch đã hoàn thành nếu file json có dung lượng > 1KB.
    """
    completed = set()
    if not output_dir.exists():
        return completed

    for f in output_dir.glob("products_*.json"):
        if f.stat().st_size < 1024:  # file trống -> không coi là done
            continue
        try:
            idx = int(f.stem.split("_")[1])
            completed.add(idx)
        except Exception:
            continue
    return completed

async def save_batch(products, batch_index, output_dir):
    if not products:
        print(f"⚠️ Batch {batch_index} không có dữ liệu hợp lệ.")
        return

    out_file = output_dir / f"products_{batch_index:04d}.json"

    # 🧠 Ghi file bất đồng bộ bằng aiofiles
    async with aiofiles.open(out_file, "w", encoding="utf-8") as f:
        await f.write(json.dumps(products, ensure_ascii=False, indent=2))

    print(f"✅ Saved {len(products)} products to {out_file.name}")


async def process_all():
    ids = read_ids_from_file(IDS_FILE)
    total = len(ids)
    batch_iter = list(chunk_list(ids, BATCH_SIZE))
    total_batches = len(batch_iter)

    print(f"Total ids: {total}")
    print(f"Total batches: {total_batches} (batch_size={BATCH_SIZE})")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    completed_batches = get_completed_batches(OUTPUT_DIR)
    print(f"Found {len(completed_batches)} completed batches → skipping them.\n")

    start_time = time.time()
    done_count = 0

    with tqdm(total=total, desc="Crawling", unit="ids") as pbar:
        for batch_index, batch_ids in enumerate(batch_iter, start=1):
            if batch_index in completed_batches:
                pbar.update(len(batch_ids))
                continue

            print(f"\n▶️ Processing batch {batch_index}/{total_batches} ...")

            products = await fetch_batch(batch_ids, concurrency=CONCURRENCY, batch_index=batch_index)

            if products:
                await save_batch(products, batch_index, OUTPUT_DIR)
            else:
                print(f"❌ Batch {batch_index} returned no data.")

            pbar.update(len(batch_ids))
            done_count += len(products)

            await asyncio.sleep(0.5)

    total_time = time.time() - start_time
    print(f"\n✅ Done. {done_count} products crawled in {total_time/60:.2f} minutes.")

if __name__ == "__main__":
    try:
        asyncio.run(process_all())
    except KeyboardInterrupt:
        print("⛔ Interrupted by user", file=sys.stderr)
