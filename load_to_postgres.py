import os
import json
import psycopg2
from psycopg2.extras import Json

# ===============================
# ⚙️ Cấu hình kết nối PostgreSQL
# ===============================
# cấu hình của để client kết nối tới PostgreSQL trong Docker 
DB_CONFIG = {
    "host": "localhost", # nếu chạy ngoài Docker, giữ nguyên "localhost"
    "port": 5432,       # port mapping trong docker-compose
    "database": "tiki_db",
    "user": "myuser",
    "password": "mypassword"
}

# Nếu script này CHẠY TRONG container khác cùng network Docker (vd: container ETL)
# thì đổi host = "postgres" (tên service trong docker-compose)

DATA_DIR = "./data"

# ===============================
# 🧩 Các hàm xử lý
# ===============================
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Kết nối PostgreSQL thành công!")
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối PostgreSQL: {e}")
        raise

def load_json_files(data_dir):
    all_records = []
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".json"):
            file_path = os.path.join(data_dir, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    all_records.extend(data)
                except json.JSONDecodeError:
                    print(f"⚠️ Lỗi đọc file: {file_name}")
    return all_records

def insert_records(conn, records):
    cur = conn.cursor()
    for r in records:
        cur.execute("""
            INSERT INTO tiki_products (id, name, url_key, price, description, images)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            r.get("id"),
            r.get("name"),
            r.get("url_key"),
            r.get("price"),
            r.get("description"),
            Json(r.get("images", []))
        ))
    conn.commit()
    cur.close()
    print(f"✅ Đã ghi {len(records)} bản ghi vào PostgreSQL")

def create_table_if_not_exists(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tiki_products (
            id BIGINT PRIMARY KEY,
            name TEXT,
            url_key TEXT,
            price NUMERIC,
            description TEXT,
            images JSONB
        );
    """)
    conn.commit()
    cur.close()
    print("✅ Đảm bảo bảng tiki_products đã tồn tại.")


if __name__ == "__main__":
    conn = connect_db()
    create_table_if_not_exists(conn)
    records = load_json_files(DATA_DIR)
    print(f"📦 Tổng số bản ghi đọc được: {len(records)}")
    insert_records(conn, records)
    conn.close()
