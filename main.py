
import threading
import queue
import time
import json
import os
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# --- CONFIGURATION ---
BASE_URL = "https://rotrends.com"
ROTRENDS_URL_TEMPLATE = f"{BASE_URL}/experiences?page={{}}&page_size=10&sort=-playing"
DB_FILE = "scraper_data.db"
OUTPUT_JSON = "roblox_data_final.json"

# Paths - Updated to point to the discovered location
CHROME_PATH = os.path.abspath(r"C:/Users/philo/Desktop/chrome-win64/chrome.exe")
CHROMEDRIVER_PATH = os.path.abspath(r"chromedriver.exe")

# Threading Config
NUM_WORKERS = 5  # Number of parallel browsers for description scraping

# Column Visibility Settings (from old scraper)
COLUMN_VISIBILITY = {
    "gameName": "show", "rank": "show", "rankChangeDay": "show", "rankChangeWeek": "show",
    "rankChangeMonth": "show", "rap1Day": "show", "rap7Day": "show", "rap14Day": "show",
    "rankRap1Day": "show", "rankRap7Day": "show", "rankRap14Day": "show",
    "rankRapChange1Day": "show", "rankRapChange7Day": "show", "rankRapChange14Day": "show",
    "earningRank": "show", "primaryGenre": "show", "subGenre": "show", "visits": "show",
    "activePlayers": "show", "platformShare": "show", "avgSessionTime": "show",
    "favorites": "show", "likeRatio": "show", "upVotes": "show", "downVotes": "show",
    "created": "show", "actions": "show",
}

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- DATABASE HANDLER ---
class Database:
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.lock = threading.Lock()
        self.create_tables()

    def create_tables(self):
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS games (
                    link TEXT PRIMARY KEY,
                    name TEXT,
                    raw_data TEXT,
                    description TEXT,
                    scraped_at TIMESTAMP,
                    status TEXT
                )
            ''')
            self.conn.commit()

    def game_exists(self, link):
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1 FROM games WHERE link = ?", (link,))
            return cursor.fetchone() is not None

    def save_game(self, data, description):
        with self.lock:
            cursor = self.conn.cursor()
            # Find the Roblox link from the data to use as ID
            roblox_link = data.get('roblox_link', '')
            if not roblox_link:
                logging.warning(f"No Roblox link found for game: {data.get('Name', 'Unknown')}")
                return # Can't save without ID

            # Remove roblox_link from raw_data if you want, or keep it. Keeping it.
            cursor.execute('''
                INSERT OR REPLACE INTO games (link, name, raw_data, description, scraped_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                roblox_link,
                data.get('Name', 'Unknown'),
                json.dumps(data, ensure_ascii=False),
                description,
                datetime.now(),
                "COMPLETED"
            ))
            self.conn.commit()

    def get_completed_count(self):
         with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM games WHERE status = 'COMPLETED'")
            return cursor.fetchone()[0]

    def close(self):
        self.conn.close()

# Paths
CHROME_PATH = os.path.abspath(r"C:/Users/philo/Desktop/Utility/chrome-win64/chrome.exe")
CHROMEDRIVER_PATH = os.path.abspath("chromedriver.exe")

# --- BROWSER SETUP ---
def create_driver(headless=True, driver_path=None):
    options = Options()
    # if os.path.exists(CHROME_PATH):
    #     options.binary_location = CHROME_PATH
    
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Block images/css for speed
    options.add_experimental_option(
        "prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_settings.popups": 0,
        }
    )
    
    if headless:
        options.add_argument("--headless=new")

    # Use specific driver path if provided, else global constant
    actual_driver_path = driver_path if driver_path else CHROMEDRIVER_PATH
    
    if not os.path.exists(actual_driver_path):
        raise FileNotFoundError(f"Chromedriver not found at {actual_driver_path}")

    service = Service(executable_path=actual_driver_path, log_output=os.devnull)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# --- WORKER THREAD ---
class DescriptionWorker(threading.Thread):
    def __init__(self, task_queue, db, driver_path=None):
        super().__init__()
        self.task_queue = task_queue
        self.db = db
        self.driver_path = driver_path or CHROMEDRIVER_PATH
        self.daemon = True
        self.driver = None
        self.running = True

    def run(self):
        logging.info("Worker starting... initializing driver")
        try:
            self.driver = create_driver(headless=True, driver_path=self.driver_path)
        except Exception as e:
            logging.error(f"Failed to initialize driver: {e}")
            return

        while self.running:
            try:
                task = self.task_queue.get(timeout=5)
            except queue.Empty:
                continue

            if task is None: # Sentinel to stop
                break

            game_data = task
            roblox_link = game_data.get('roblox_link')

            if not roblox_link:
                self.task_queue.task_done()
                continue
            
            if self.db.game_exists(roblox_link):
                logging.info(f"Skipping already scraped: {game_data.get('Name')}")
                self.task_queue.task_done()
                continue

            try:
                description = self.scrape_description(roblox_link)
                self.db.save_game(game_data, description)
                logging.info(f"Scraped & Saved: {game_data.get('Name')}")
            except Exception as e:
                logging.error(f"Error scraping {roblox_link}: {e}")
            finally:
                self.task_queue.task_done()

        if self.driver:
            self.driver.quit()
        logging.info("Worker stopped")

    def scrape_description(self, url):
        self.driver.get(url)
        try:
            # Wait briefly for description
            time.sleep(1) # Simple wait, or use WebDriverWait
            desc_elem = self.driver.find_element(By.CSS_SELECTOR, ".text.game-description")
            return desc_elem.get_attribute("innerHTML")
        except NoSuchElementException:
            return ""
        except Exception as e:
            logging.warning(f"Failed to get description for {url}: {e}")
            return ""

    def stop(self):
        self.running = False


# --- MAIN LIST SCRAPER ---
class RoTrendsScraper:
    def __init__(self, driver_path):
        self.driver = create_driver(headless=False, driver_path=driver_path) # Headless=False to see progress if needed, or True
        # Setup Column Visibility
        self.driver.get(BASE_URL)
        time.sleep(2)
        self.driver.execute_script(
            f"window.localStorage.setItem('exploreGamesUserColumnVisibility', '{json.dumps(COLUMN_VISIBILITY)}');"
        )
        self.display_names = ["Games"] # Initial required column
        
    def wait_for_headers(self, timeout=30):
        # Simplification of the old wait_for_columns logic
        start = time.time()
        while time.time() - start < timeout:
            try:
                header_elements = self.driver.find_elements(By.CSS_SELECTOR, ".ant-table-thead th")
                headers = [th.text.strip() for th in header_elements]
                if headers and "Games" in headers: 
                    return headers
            except Exception:
                pass
            time.sleep(1)
        return []

    def scrape_page(self, page_num):
        url = ROTRENDS_URL_TEMPLATE.format(page_num)
        self.driver.get(url)
        
        headers = self.wait_for_headers()
        if not headers:
            logging.error(f"Failed to load headers on page {page_num}")
            return []

        # Ensure we have our custom headers if they are missing from DOM (sometimes they load late)
        # But for new logic, we map by index mostly or try to be robust
        
        try:
            time.sleep(3) # Wait for table
            tbody = self.driver.find_element(By.CSS_SELECTOR, ".ant-table-tbody")
            rows = tbody.find_elements(By.CSS_SELECTOR, "tr")
        except Exception as e:
            logging.error(f"Failed to find table on page {page_num}: {e}")
            return []

        page_data = []
        for tr in rows:
            try:
                tds = tr.find_elements(By.CSS_SELECTOR, "td")
                if not tds: continue
                
                row_dict = {}
                roblox_link = None
                
                # Extract Name and Creator
                try:
                    name_el = tds[0].find_element(By.CSS_SELECTOR, "a")
                    name = name_el.text.strip()
                except:
                    name = tds[0].text.strip()
                
                # Extract Roblox Link
                try:
                    links = tr.find_elements(By.CSS_SELECTOR, "a")
                    for a in links:
                        href = a.get_attribute("href")
                        if href and "roblox.com/games/" in href:
                            roblox_link = href
                            break
                except:
                    pass

                row_dict['Name'] = name
                row_dict['roblox_link'] = roblox_link
                
                # Extract other columns based on headers
                # We skip complex mapping for now and just dump all text by header if possible
                # Or just iterate tds
                for i, td in enumerate(tds):
                    if i < len(headers):
                        header_name = headers[i]
                        # Don't overwrite Name/Link custom logic, but add others
                        if header_name not in row_dict:
                             row_dict[header_name] = td.text.strip()

                if roblox_link: # Only add if we have a link to scrape
                    page_data.append(row_dict)
                    
            except Exception as e:
                logging.error(f"Error parsing row: {e}")
                continue
                
        return page_data

    def close(self):
        self.driver.quit()

# --- MAIN EXECUTION ---
def main():
    # if not os.path.exists(CHROMEDRIVER_PATH):
    #    print(f"ERROR: Chromedriver not found at {CHROMEDRIVER_PATH}")
    #    return

    db = Database(DB_FILE)
    task_queue = queue.Queue()
    
    # Start Workers
    workers = []
    print(f"Starting {NUM_WORKERS} worker threads using driver: {CHROMEDRIVER_PATH}")
    for _ in range(NUM_WORKERS):
        w = DescriptionWorker(task_queue, db, CHROMEDRIVER_PATH)
        w.start()
        workers.append(w)

    scraper = RoTrendsScraper(CHROMEDRIVER_PATH)
    
    try:
        page = 1
        max_empty_pages = 3
        empty_pages_count = 0

        while True:
            logging.info(f"--- Processing Page {page} ---")
            games = scraper.scrape_page(page)
            
            if not games:
                empty_pages_count += 1
                logging.warning(f"Page {page} empty. ({empty_pages_count}/{max_empty_pages})")
                if empty_pages_count >= max_empty_pages:
                    logging.info("Max empty pages reached. Stopping.")
                    break
            else:
                empty_pages_count = 0
                for game in games:
                    task_queue.put(game)
                
                logging.info(f"Queued {len(games)} games from page {page}")

            # Optional: Check if queue is getting too big to backpressure
            while task_queue.qsize() > 50:
                time.sleep(1)

            page += 1
            
            # Simple resume logic check? 
            # If we want to skip pages, we need to check if all games on page are in DB.
            # But since we queue them, the worker checks DB. 
            # Optimization: Check DB before queueing to avoid queue fillup on resume?
            # Yes, let's do that for speed.
            
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    except Exception as e:
        logging.error(f"Critical error: {e}")
    finally:
        logging.info("Shutting down...")
        
        # Stop workers
        for _ in workers:
            task_queue.put(None)
        
        for w in workers:
            w.join()
            
        scraper.close()
        db.close()
        logging.info("Done.")

if __name__ == "__main__":
    main()
