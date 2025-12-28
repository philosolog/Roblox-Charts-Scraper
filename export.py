
import sqlite3
import json
import logging

DB_FILE = "scraper_data.db"
OUTPUT_JSON = "roblox_data_final.json"

def export_db_to_json():
    print(f"Exporting {DB_FILE} to {OUTPUT_JSON}...")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT raw_data, description, scraped_at FROM games WHERE status = 'COMPLETED'")
        rows = cursor.fetchall()
        
        output_data = []
        for raw_json, description, scraped_at in rows:
            try:
                data = json.loads(raw_json)
                data['Description'] = description
                data['ScrapedAt'] = scraped_at
                output_data.append(data)
            except Exception as e:
                print(f"Error processing row: {e}")

        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
            
        print(f"Successfully exported {len(output_data)} records to {OUTPUT_JSON}")
        
    except Exception as e:
        print(f"Error exporting database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    export_db_to_json()
