# Điểm khởi động chương trình 
# main.py
# main.py
import asyncio
import time
import sys
from tqdm.asyncio import tqdm
from pathlib import Path

from utils.io_utils import read_ids_from_file, chunk_list, save_batch_async
from fetcher import fetch_batch
from config import IDS_FILE, OUTPUT_DIR, BATCH_SIZE, CONCURRENCY


def get_completed_batches(output_dir: Path) -> set[int]:
    """
    Kiểm tra thư mục output, xác định batch nào đã tồn tại file .json
    Trả về set chứa index các batch đã hoàn thành
    """
    completed = set()
    if not output_dir.exists():
        return completed

    for f in output_dir.glob("products_*.json"):
        # file name dạng: products_0001.json
        name = f.stem
        try:
            idx = int(name.split("_")[1])
            completed.add(idx)
        except Exception:
            continue
    return completed


async def process_all():
    ids = read_ids_from_file(IDS_FILE)
    total = len(ids)
    batch_iter = list(chunk_list(ids, BATCH_SIZE))
    total_batches = len(batch_iter)
    print(f"Total ids: {total}")
    print(f"Total batches: {total_batches} (batch_size={BATCH_SIZE})")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    completed_batches = get_completed_batches(OUTPUT_DIR)
    print(f"Found {len(completed_batches)} completed batches → will skip them.\n")

    start_time = time.time()

    # Progress bar cho toàn bộ tiến trình
    with tqdm(total=total, desc="Crawling", unit="ids") as pbar:
        # Nếu có batch đã xong, cập nhật progress bar tương ứng
        pbar.update(len(completed_batches) * BATCH_SIZE)

        for batch_index, batch_ids in enumerate(batch_iter, start=1):
            if batch_index in completed_batches:
                continue  # bỏ qua batch đã có file

            print(f"\n▶️ Processing batch {batch_index}/{total_batches} (ids: {len(batch_ids)})")

            # fetch song song
            products = await fetch_batch(batch_ids, concurrency=CONCURRENCY)

            # lưu dữ liệu async
            await save_batch_async(products, batch_index, OUTPUT_DIR)

            # cập nhật progress bar
            pbar.update(len(batch_ids))

            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_index - len(completed_batches))
            pbar.set_postfix({
                "batch": f"{batch_index}/{total_batches}",
                "avg_per_batch": f"{avg_time:.1f}s"
            })

            await asyncio.sleep(0.2)  # tránh gửi quá nhanh

    total_time = time.time() - start_time
    print(f"\n✅ Done. Processed {total} ids in {total_time/60:.2f} minutes.")


if __name__ == "__main__":
    try:
        asyncio.run(process_all())
    except KeyboardInterrupt:
        print("⛔ Interrupted by user", file=sys.stderr)
