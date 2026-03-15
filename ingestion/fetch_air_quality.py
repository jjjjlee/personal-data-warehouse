import requests
import json
import psycopg2
from psycopg2 import extras
from datetime import datetime

# --- 配置區 ---
API_KEY = '147dcb7d-3db7-44bd-bfbc-512862928bdf'
DATASET_ID = 'aqx_p_432' # 空氣品質指標歷史資料
API_URL = f'https://data.moenv.gov.tw/api/v2/{DATASET_ID}'

DB_CONFIG = {
    "host": "localhost",
    "database": "personal_data_warehouse",
    "user": "jameslee",
    "password": "leewi123"
}

def get_last_creation_date():
    """從資料庫獲取最新一筆資料的產製時間，用於增量抓取"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # 從 JSONB 欄位 raw_data 中提取 datacreationdate
        cur.execute('SELECT MAX(raw_data->>\'datacreationdate\') FROM "bronze-air-quality"')
        result = cur.fetchone()
        return result[0] if result[0] else "1970-01-01 00:00:00"
    finally:
        cur.close()
        conn.close()

def fetch_incremental_data(limit=1000):
    """使用 filters 參數實現冪等增量抓取"""
    last_date = get_last_creation_date()
    
    # 使用 GT (大於) 篩選器，只抓取比資料庫中更新的資料
    params = {
        'format': 'json',
        'limit': limit,
        'api_key': API_KEY,
        'filters': f'datacreationdate,GT,{last_date}',
        'sort': 'datacreationdate asc' # 依時間由遠至近排序，方便接續
    }
    
    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        records = response.json().get('records', [])
        return records
    except Exception as e:
        print(f"API 抓取失敗: {e}")
        return []

def batch_upsert_to_db(records):
    """批量寫入並處理衝突 (Idempotent Upsert)"""
    if not records:
        print("沒有新資料需要攝取。")
        return

    fetch_at = datetime.now()
    source_name = "moenv_api_v2"
    
    # 準備寫入 raw_data, fetchat, source 欄位
    data_to_insert = [
        (json.dumps(rec), fetch_at, source_name) 
        for rec in records
    ]

    # 使用 ON CONFLICT DO NOTHING 確保重複執行時不會產生重複列
    insert_query = """
        INSERT INTO "bronze-air-quality" (raw_data, fetchat, source)
        VALUES %s
        ON CONFLICT ((raw_data->>'sitename'), (raw_data->>'publishtime')) 
        DO NOTHING;
    """

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        extras.execute_values(cur, insert_query, data_to_insert)
        conn.commit()
        print(f"成功完成攝取。本次處理 {len(records)} 筆紀錄。")
    except Exception as e:
        print(f"資料庫批量寫入出錯: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    # 流程：1. 檢查 DB 時間 -> 2. API 增量抓取 -> 3. 批量 Upsert
    new_records = fetch_incremental_data()
    batch_upsert_to_db(new_records)