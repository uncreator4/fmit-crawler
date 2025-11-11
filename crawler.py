import os
import time
import json
import logging
import subprocess
import re
import glob
import requests
import zipfile
from pathlib import Path
from typing import Tuple, List, Dict, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth

DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

PARQUET_FILE = os.path.join(DATA_DIR, "fmit_data.parquet")
PAGE_CHECKPOINT = os.path.join(DATA_DIR, "page_checkpoint.json")
OUTPUT_JSON_PATTERN = os.path.join(DATA_DIR, "fmit_data_*.json")  # Pattern for multiple JSON files
OUTPUT_JSON_PREFIX = os.path.join(DATA_DIR, "fmit_data")  # Prefix for JSON files
MAX_JSON_FILE_SIZE_MB = 95  # Maximum file size in MB (safety margin below GitHub's 100 MB limit)

BASE_URL = "https://fmit.vn/en/glossary"
START_PAGE = 6019  # Account 5: Pages 6019-7185
MAX_PAGES = 7185

CLOUDFLARE_KEYWORDS = [
    "just a moment",
    "checking your browser",
    "please enable cookies",
    "attention required",
    "verify you are human",
    "enable javascript"
]


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_chrome_version() -> str:
    """Get Chrome version from binary."""
    chrome_bin = os.getenv("CHROME_BIN")
    if not chrome_bin:
        chrome_bin = "google-chrome"
        if os.path.exists("/opt/hostedtoolcache/setup-chrome/chromium"):
            # GitHub Actions Chrome
            chrome_bin_pattern = "/opt/hostedtoolcache/setup-chrome/chromium/*/x64/chrome"
            matches = glob.glob(chrome_bin_pattern)
            if matches:
                chrome_bin = matches[0]
    
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        version_output = result.stdout.strip()
        logging.info(f"Chrome version output: {version_output}")
        
        # Extract version number (e.g., "Chromium 144.0.7508.0" -> "144.0.7508.0")
        match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_output)
        if match:
            full_version = match.group(1)
            logging.info(f"Detected full Chrome version: {full_version}")
            # Get major version (e.g., "144.0.7508.0" -> "144")
            major_version = full_version.split('.')[0]
            logging.info(f"Detected Chrome version: {major_version}")
            return major_version
        return None
    except Exception as e:
        logging.warning(f"Could not detect Chrome version: {e}")
        return None


def download_chromedriver_for_version(chrome_version: str) -> str:
    """Download ChromeDriver for a specific Chrome version from Chrome for Testing."""
    import platform
    
    try:
        # Detect platform
        system = platform.system().lower()
        if system == "darwin":
            # macOS
            if platform.machine().lower() in ["arm64", "aarch64"]:
                platform_name = "mac-arm64"
            else:
                platform_name = "mac-x64"
        elif system == "linux":
            platform_name = "linux64"
        elif system == "windows":
            platform_name = "win64"
        else:
            # Default to linux64 for GitHub Actions
            platform_name = "linux64"
        
        logging.info(f"Detected platform: {platform_name}")
        
        # Get available ChromeDriver versions
        versions_url = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
        response = requests.get(versions_url, timeout=30)
        response.raise_for_status()
        versions_data = response.json()
        
        # Find matching ChromeDriver version
        target_version = None
        for version_info in reversed(versions_data["versions"]):
            version_str = version_info["version"]
            if version_str.startswith(f"{chrome_version}."):
                target_version = version_str
                break
        
        if not target_version:
            logging.warning(f"No ChromeDriver found for Chrome {chrome_version}, trying latest")
            # Try to get the latest version for this major version
            for version_info in reversed(versions_data["versions"]):
                version_str = version_info["version"]
                if version_str.split('.')[0] == chrome_version:
                    target_version = version_str
                    break
        
        if not target_version:
            raise Exception(f"No ChromeDriver found for Chrome version {chrome_version}")
        
        logging.info(f"Found ChromeDriver version: {target_version}")
        
        # Get download URL for the detected platform
        download_url = None
        for version_info in versions_data["versions"]:
            if version_info["version"] == target_version:
                downloads = version_info.get("downloads", {})
                chromedriver = downloads.get("chromedriver", [])
                for item in chromedriver:
                    if item["platform"] == platform_name:
                        download_url = item["url"]
                        break
                break
        
        if not download_url:
            raise Exception(f"No {platform_name} ChromeDriver download found for version {target_version}")
        
        # Download and extract
        logging.info(f"Downloading ChromeDriver from {download_url}")
        cache_dir = Path.home() / ".wdm" / "drivers" / "chromedriver" / platform_name / target_version
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine zip filename based on platform
        zip_filename = {
            "linux64": "chromedriver-linux64.zip",
            "mac-x64": "chromedriver-mac-x64.zip",
            "mac-arm64": "chromedriver-mac-arm64.zip",
            "win64": "chromedriver-win64.zip"
        }.get(platform_name, "chromedriver.zip")
        
        zip_path = cache_dir / zip_filename
        response = requests.get(download_url, timeout=120)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        
        # Extract
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(cache_dir)
        
        # Find chromedriver executable (it might be in a subdirectory)
        chromedriver_path = None
        executable_name = "chromedriver.exe" if system == "windows" else "chromedriver"
        for root, dirs, files in os.walk(cache_dir):
            if executable_name in files:
                chromedriver_path = Path(root) / executable_name
                break
        
        if not chromedriver_path or not chromedriver_path.exists():
            raise Exception(f"ChromeDriver executable not found after extraction in {cache_dir}")
        
        # Make executable (not needed on Windows)
        if system != "windows":
            os.chmod(chromedriver_path, 0o755)
        
        logging.info(f"ChromeDriver installed at: {chromedriver_path}")
        return str(chromedriver_path)
        
    except Exception as e:
        logging.error(f"Failed to download ChromeDriver for version {chrome_version}: {e}")
        raise


def create_driver() -> webdriver.Chrome:
    logging.info("Creating Chrome driver...")
    
    # Get Chrome binary path - prioritize CHROME_BIN env var
    chrome_bin = os.getenv("CHROME_BIN")
    if not chrome_bin:
        # Fallback: try to find Chrome in GitHub Actions location
        if os.path.exists("/opt/hostedtoolcache/setup-chrome/chromium"):
            chrome_bin_pattern = "/opt/hostedtoolcache/setup-chrome/chromium/*/x64/chrome"
            matches = glob.glob(chrome_bin_pattern)
            if matches:
                chrome_bin = matches[0]
        else:
            chrome_bin = "google-chrome"
    
    # Verify Chrome binary exists
    if not os.path.exists(chrome_bin):
        raise FileNotFoundError(f"Chrome binary not found at: {chrome_bin}")
    
    # Verify it's executable
    if not os.access(chrome_bin, os.X_OK):
        raise PermissionError(f"Chrome binary is not executable: {chrome_bin}")
    
    logging.info(f"Using Chrome binary: {chrome_bin}")
    
    # Verify the version of this binary
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        logging.info(f"Chrome binary version check: {result.stdout.strip()}")
    except Exception as e:
        logging.warning(f"Could not verify Chrome binary version: {e}")
    
    # Set Chrome binary location BEFORE getting version (so we use the same binary)
    chrome_options = Options()
    chrome_options.binary_location = chrome_bin
    
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    # Add user agent to avoid bot detection
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")
    
    # Get Chrome version using the same binary we'll use for Selenium
    chromedriver_path = None
    try:
        # Temporarily set CHROME_BIN so get_chrome_version uses the correct binary
        original_chrome_bin = os.getenv("CHROME_BIN")
        os.environ["CHROME_BIN"] = chrome_bin
        
        chrome_version = get_chrome_version()
        if chrome_version:
            logging.info(f"Installing ChromeDriver for Chrome {chrome_version}...")
            chromedriver_path = download_chromedriver_for_version(chrome_version)
        
        # Restore original env var
        if original_chrome_bin:
            os.environ["CHROME_BIN"] = original_chrome_bin
        elif "CHROME_BIN" in os.environ:
            del os.environ["CHROME_BIN"]
    except Exception as e:
        logging.warning(f"Failed to get ChromeDriver for specific version: {e}")
        logging.info("Falling back to webdriver-manager...")
    
    # Fallback to webdriver-manager if Chrome for Testing fails
    if not chromedriver_path:
        try:
            logging.info("Installing ChromeDriver via webdriver-manager...")
            chromedriver_path = ChromeDriverManager().install()
        except Exception as e:
            logging.error(f"Failed to install ChromeDriver: {e}")
            raise
    
    service = Service(chromedriver_path)
    logging.info("Starting Chrome browser...")
    logging.info(f"ChromeDriver path: {chromedriver_path}")
    logging.info(f"Chrome binary path: {chrome_bin}")
    
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Apply selenium-stealth to avoid bot detection
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Linux",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
        )
        
        logging.info("Chrome driver created successfully with stealth mode enabled")
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome driver: {e}")
        logging.error(f"ChromeDriver path: {chromedriver_path}")
        logging.error(f"Chrome binary path: {chrome_bin}")
        raise


def load_page_checkpoint() -> int:
    if not os.path.exists(PAGE_CHECKPOINT):
        return START_PAGE - 1  # Will be incremented to START_PAGE
    try:
        with open(PAGE_CHECKPOINT, "r", encoding="utf-8") as f:
            last_page = int(json.load(f).get("last_page", START_PAGE - 1))
            # If checkpoint is before our START_PAGE, start from START_PAGE
            return max(last_page, START_PAGE - 1)
    except Exception:
        return START_PAGE - 1


def read_parquet_df() -> pd.DataFrame:
    if not os.path.exists(PARQUET_FILE):
        return pd.DataFrame(columns=["url", "h1", "h2", "content"])
    try:
        return pd.read_parquet(PARQUET_FILE)
    except Exception:
        return pd.DataFrame(columns=["url", "h1", "h2", "content"])


def write_parquet_df(df: pd.DataFrame) -> None:
    for col in ["url", "h1", "h2", "content"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["url", "h1", "h2", "content"]]
    df.to_parquet(PARQUET_FILE, index=False)


def rebuild_parquet_from_json() -> None:
    """Rebuild parquet file from all existing JSON files for fast duplicate checking."""
    all_json_files = sorted(glob.glob(OUTPUT_JSON_PATTERN))
    if not all_json_files:
        logging.debug("No JSON files found to rebuild parquet from")
        return
    
    logging.info(f"🔄 Rebuilding parquet file from {len(all_json_files)} JSON file(s)...")
    all_records = []
    
    for json_file in all_json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_records.extend(data)
        except Exception as e:
            logging.warning(f"Could not read JSON file {json_file}: {e}")
    
    if all_records:
        try:
            df = pd.DataFrame(all_records)
            for col in ["url", "h1", "h2", "content"]:
                if col not in df.columns:
                    df[col] = ""
            df = df[["url", "h1", "h2", "content"]]
            write_parquet_df(df)
            logging.info(f"✅ Rebuilt parquet file with {len(df)} records from JSON files")
        except Exception as e:
            logging.error(f"❌ Failed to rebuild parquet file: {e}")
            import traceback
            logging.error(traceback.format_exc())
    else:
        logging.warning("No records found in JSON files to rebuild parquet")


def load_processed_urls() -> Set[str]:
    """Load processed URLs from parquet file. If parquet is empty, rebuild it from JSON files."""
    df = read_parquet_df()
    
    # If parquet is empty or very small, try to rebuild from JSON files
    if df.empty or len(df) < 100:
        rebuild_parquet_from_json()
        df = read_parquet_df()
    
    if "url" in df.columns:
        return set(df["url"].dropna().astype(str))
    return set()


def wait_for_cloudflare_clear(driver: webdriver.Chrome, url: str, timeout: int = 45) -> bool:
    """Detect and wait for Cloudflare challenge pages to clear."""
    start_time = time.time()
    already_refreshed = False
    while time.time() - start_time < timeout:
        try:
            title = (driver.title or "").lower()
        except Exception:
            title = ""
        try:
            page_source = driver.page_source.lower() if driver.page_source else ""
        except Exception:
            page_source = ""
        
        if any(keyword in title or keyword in page_source for keyword in CLOUDFLARE_KEYWORDS):
            logging.warning(f"☁️  Cloudflare challenge detected on {url}. Waiting for clearance...")
            time.sleep(5)
            # Refresh once if still stuck after first wait
            if not already_refreshed:
                try:
                    driver.refresh()
                    already_refreshed = True
                except Exception:
                    pass
            continue
        return True

    logging.error(f"❌ Cloudflare challenge did not clear for {url} within {timeout}s")
    return False


def get_current_json_file() -> str:
    """Get the current JSON file to write to. Returns the latest file or creates a new one."""
    # Find all existing JSON files matching the pattern
    existing_files = sorted(glob.glob(OUTPUT_JSON_PATTERN))
    
    # If no files exist, create the first one
    if not existing_files:
        return os.path.join(DATA_DIR, "fmit_data_001.json")
    
    # Get the latest file
    latest_file = existing_files[-1]
    
    # Check if the latest file is approaching the size limit
    file_size_mb = os.path.getsize(latest_file) / 1024 / 1024
    if file_size_mb >= MAX_JSON_FILE_SIZE_MB:
        # Create a new file with incremented number
        # Extract number from latest file (e.g., "fmit_data_001.json" -> 1)
        latest_basename = os.path.basename(latest_file)
        match = re.search(r'_(\d+)\.json$', latest_basename)
        if match:
            next_num = int(match.group(1)) + 1
        else:
            next_num = len(existing_files) + 1
        return os.path.join(DATA_DIR, f"fmit_data_{next_num:03d}.json")
    
    return latest_file


def migrate_old_json_file() -> None:
    """Migrate old single fmit_data.json to new numbered format if it exists."""
    old_file = os.path.join(DATA_DIR, "fmit_data.json")
    if os.path.exists(old_file):
        logging.info(f"🔄 Migrating old JSON file to new format...")
        try:
            with open(old_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if isinstance(data, list) and len(data) > 0:
                # Save to first numbered file
                new_file = os.path.join(DATA_DIR, "fmit_data_001.json")
                with open(new_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logging.info(f"✅ Migrated {len(data)} records to {new_file}")
                
                # Remove old file
                os.remove(old_file)
                logging.info(f"✅ Removed old file: {old_file}")
        except Exception as e:
            logging.warning(f"Could not migrate old JSON file: {e}")


def save_page_checkpoint(page: int) -> None:
    """Save checkpoint to file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PAGE_CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump({"last_page": page}, f, ensure_ascii=False)


def initialize_output_files() -> None:
    """Initialize output files at the start of crawling."""
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Migrate old single JSON file to new format if it exists
    migrate_old_json_file()
    
    # Get or create current JSON file
    current_json_file = get_current_json_file()
    if not os.path.exists(current_json_file):
        with open(current_json_file, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        logging.info(f"✅ Created new JSON file: {current_json_file}")
    else:
        # Count records in current JSON file
        try:
            with open(current_json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                record_count = len(data) if isinstance(data, list) else 0
        except:
            record_count = 0
        file_size = os.path.getsize(current_json_file) / 1024 / 1024
        logging.info(f"✅ Current JSON file: {current_json_file} ({record_count} records, {file_size:.2f} MB)")
    
    # Count all JSON files
    all_json_files = sorted(glob.glob(OUTPUT_JSON_PATTERN))
    total_records = 0
    total_size = 0
    for json_file in all_json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    total_records += len(data)
            total_size += os.path.getsize(json_file) / 1024 / 1024
        except:
            pass
    logging.info(f"📊 Total JSON files: {len(all_json_files)}, Total records: {total_records}, Total size: {total_size:.2f} MB")
    
    # Create or rebuild parquet file (for internal use - fast duplicate checking)
    if not os.path.exists(PARQUET_FILE):
        # Try to rebuild from JSON files first
        rebuild_parquet_from_json()
        df = read_parquet_df()
        if df.empty:
            # If no JSON files exist, create empty parquet
            empty_df = pd.DataFrame(columns=["url", "h1", "h2", "content"])
            write_parquet_df(empty_df)
            logging.info(f"✅ Created empty parquet file: {PARQUET_FILE}")
        else:
            file_size = os.path.getsize(PARQUET_FILE) / 1024 / 1024
            logging.info(f"✅ Rebuilt parquet file: {PARQUET_FILE} ({len(df)} records, {file_size:.2f} MB)")
    else:
        file_size = os.path.getsize(PARQUET_FILE) / 1024 / 1024
        df = read_parquet_df()
        # If parquet seems too small compared to JSON files, rebuild it
        # Only rebuild if we have JSON records and parquet is significantly smaller
        if total_records > 0 and len(df) < total_records * 0.9:  # If parquet has <90% of JSON records, rebuild
            logging.info(f"⚠️  Parquet file seems incomplete ({len(df)} vs {total_records} JSON records), rebuilding...")
            rebuild_parquet_from_json()
            df = read_parquet_df()
            file_size = os.path.getsize(PARQUET_FILE) / 1024 / 1024
        logging.info(f"✅ Parquet file exists: {PARQUET_FILE} ({len(df)} records, {file_size:.2f} MB)")
    
    # Initialize checkpoint file if it doesn't exist
    if not os.path.exists(PAGE_CHECKPOINT):
        save_page_checkpoint(0)
        logging.info(f"✅ Created checkpoint file: {PAGE_CHECKPOINT}")
    else:
        last_page = load_page_checkpoint()
        logging.info(f"✅ Checkpoint file exists: {PAGE_CHECKPOINT} (last page: {last_page})")


def append_to_files(rows: List[Dict[str, str]]) -> None:
    """Append new rows to JSON file (and update parquet for internal use)."""
    if not rows:
        return
    
    # Ensure output files exist
    all_json_files = sorted(glob.glob(OUTPUT_JSON_PATTERN))
    if not all_json_files:
        initialize_output_files()
    
    # Load all existing URLs from all JSON files for duplicate checking
    existing_urls = set()
    for json_file in all_json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    existing_urls.update({item.get("url") for item in data if item.get("url")})
        except Exception as e:
            logging.warning(f"Could not read JSON file {json_file}: {e}")
    
    # Filter out duplicates and prepare new rows
    new_rows = []
    for row in rows:
        if row.get("url") and row["url"] not in existing_urls:
            record = {
                "url": row.get("url", ""),
                "h1": row.get("h1", ""),
                "h2": row.get("h2", ""),
                "content": row.get("content", "")
            }
            new_rows.append(record)
            existing_urls.add(record["url"])
    
    if not new_rows:
        logging.debug("All URLs already exist in JSON files, skipping append")
        return
    
    # Get current JSON file (may create a new one if current is too large)
    current_json_file = get_current_json_file()
    
    # Read current file data
    existing_data = []
    if os.path.exists(current_json_file):
        try:
            with open(current_json_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    existing_data = []
        except Exception as e:
            logging.warning(f"Could not read current JSON file: {e}")
            existing_data = []
    
    # Check if adding new rows would exceed the size limit
    # Estimate size by creating a temporary merged array
    test_data = existing_data + new_rows
    test_json = json.dumps(test_data, ensure_ascii=False, indent=2)
    estimated_size_mb = len(test_json.encode('utf-8')) / 1024 / 1024
    
    if estimated_size_mb >= MAX_JSON_FILE_SIZE_MB:
        # Current file is full, save what we have and create a new file
        if existing_data:
            with open(current_json_file, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
            file_size = os.path.getsize(current_json_file) / 1024 / 1024
            logging.info(f"💾 Saved {len(existing_data)} records to {current_json_file} ({file_size:.2f} MB)")
        
        # Create new file for remaining rows
        current_json_file = get_current_json_file()
        existing_data = []
        logging.info(f"📁 Created new JSON file: {current_json_file}")
    
    # Append new rows to current file
    updated_data = existing_data + new_rows
    with open(current_json_file, "w", encoding="utf-8") as f:
        json.dump(updated_data, f, ensure_ascii=False, indent=2)
    
    file_size = os.path.getsize(current_json_file) / 1024 / 1024
    logging.info(f"💾 Appended {len(new_rows)} records to {current_json_file} (total in file: {len(updated_data)}, size: {file_size:.2f} MB)")
    
    # Also update parquet for internal use (fast duplicate checking)
    new_df = pd.DataFrame(new_rows)
    for col in ["url", "h1", "h2", "content"]:
        if col not in new_df.columns:
            new_df[col] = ""
    
    old_df = read_parquet_df()
    if not old_df.empty and "url" in old_df.columns and "url" in new_df.columns:
        new_df = new_df[~new_df["url"].isin(old_df["url"])]
        if not new_df.empty:
            df = pd.concat([old_df, new_df], ignore_index=True)
            write_parquet_df(df)


def click_next_page(driver: webdriver.Chrome) -> bool:
    """Click the 'Next page' button to navigate. Returns True if successful."""
    try:
        # Find and click the next page link
        next_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//a[@title="Next page" or contains(text(), "›")]'))
        )
        next_link.click()
        time.sleep(3)  # Wait for page transition
        return True
    except Exception as e:
        logging.debug(f"Could not click next page button: {e}")
        return False


def extract_page_links(driver: webdriver.Chrome, url: str, use_click: bool = False, max_retries: int = 3) -> Tuple[List[str], webdriver.Chrome]:
    """Extract links from a glossary page. If use_click=True, assumes already on page and just extracts."""
    for attempt in range(max_retries):
        try:
            # Only navigate if not using click navigation
            if not use_click:
                driver.get(url)
                
                # Wait for page to load
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Wait for Cloudflare challenge to clear if present
                if not wait_for_cloudflare_clear(driver, url):
                    raise TimeoutException("Cloudflare challenge did not clear in time")
                
                # Ensure body is still present after Cloudflare clears
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            
            # Wait for the dictionary-items element to appear
            try:
                items = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "dictionary-items"))
                )
            except TimeoutException:
                # If element not found, log page source snippet for debugging
                page_source_preview = driver.page_source[:500] if driver.page_source else "No page source"
                logging.warning(f"Element '.dictionary-items' not found. Page source preview: {page_source_preview}...")
                raise
            
            links = items.find_elements(By.XPATH, './/li[@class="item"]/a[@href]')
            hrefs: List[str] = []
            for link in links:
                href = link.get_attribute("href")
                if href and "fmit.vn" in href and ("/glossary/" in href or "/tu-dien-quan-ly/" in href):
                    hrefs.append(href)
            
            current_url = driver.current_url
            if hrefs:
                logging.info(f"Found {len(hrefs)} links on {current_url}")
            else:
                logging.warning(f"No links found in dictionary-items on {current_url}")
            
            return list(set(hrefs)), driver
        except TimeoutException as e:
            logging.warning(f"Page timeout (attempt {attempt + 1}/{max_retries}): {e}. Retry in 10s...")
            time.sleep(10)
        except Exception as e:
            logging.warning(f"Page error (attempt {attempt + 1}/{max_retries}): {e}. Retry in 10s...")
            time.sleep(10)

    logging.error(f"Failed to extract links after {max_retries} attempts")
    return [], driver


def extract_url_data(driver: webdriver.Chrome, url: str, max_retries: int = 5) -> Tuple[Dict[str, str], webdriver.Chrome]:
    for attempt in range(max_retries):
        try:
            driver.get(url)
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            if not wait_for_cloudflare_clear(driver, url):
                raise TimeoutException("Cloudflare challenge did not clear in time")
            
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            h1 = h2 = content = ""
            try:
                h1_el = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1.dictionary-detail-title"))
                )
                h1 = h1_el.text.strip()
            except TimeoutException:
                pass
            try:
                h2_el = driver.find_element(By.CSS_SELECTOR, "h2.dictionary-detail-title")
                h2 = h2_el.text.strip()
            except NoSuchElementException:
                pass
            try:
                content_el = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.dictionary-details"))
                )
                content = content_el.text.strip()
            except TimeoutException:
                pass
            return {"url": url, "h1": h1, "h2": h2, "content": content}, driver
        except Exception as e:
            logging.warning(f"URL error {url} (attempt {attempt + 1}/{max_retries}): {e}. Retry in 10s...")
            time.sleep(10)
    return {"url": url, "h1": "", "h2": "", "content": ""}, driver


def run_once() -> None:
    setup_logging()
    logging.info("=" * 60)
    logging.info("START CRAWL (GitHub Actions, Parquet)")
    logging.info("=" * 60)
    
    # Initialize output files first (create if they don't exist)
    initialize_output_files()
    
    # Set maximum runtime (5.5 hours to stay under 6-hour limit)
    # But we'll process only 10 pages per run to complete faster
    MAX_RUNTIME_SECONDS = 5.5 * 60 * 60  # 5.5 hours (safety limit)
    PAGES_PER_RUN = 10  # Process 10 pages per run (~2-3 hours)
    start_time = time.time()
    
    processed = load_processed_urls()
    logging.info(f"Loaded {len(processed)} already processed URLs")
    
    driver = create_driver()

    all_urls_collected: Set[str] = set()
    total_successful_extractions = 0
    total_failed_extractions = 0
    total_pages_processed = 0
    
    # Process exactly PAGES_PER_RUN pages (or until timeout)
    start_page = load_page_checkpoint() + 1
    current_page = max(start_page, 1)
    
    if current_page > MAX_PAGES:
        logging.info("✅ All pages processed! Crawling complete.")
        try:
            driver.quit()
        except Exception:
            pass
        return
    
    target_page = min(current_page + PAGES_PER_RUN - 1, MAX_PAGES)
    logging.info(f"📄 Processing pages {current_page} to {target_page} ({target_page - current_page + 1} pages)")
    
    # Phase 1: Collect URLs from PAGES_PER_RUN pages
    batch_urls: Set[str] = set()
    pages_processed = 0
    
    # Use fresh browser for each page to avoid Cloudflare detection
    while current_page <= target_page and current_page <= MAX_PAGES:
        # Safety check: don't exceed 5.5 hours
        elapsed_time = time.time() - start_time
        if elapsed_time > MAX_RUNTIME_SECONDS - 600:
            logging.warning("⏰ Approaching time limit, stopping URL collection")
            break
        
        # Close current browser and create fresh one for each page
        try:
            driver.quit()
            logging.info(f"🔄 Closed browser for page {current_page}")
        except Exception:
            pass
        
        # Small delay between browser sessions
        time.sleep(3)
        
        # Create fresh browser instance with stealth mode
        logging.info(f"🆕 Creating fresh browser for page {current_page}...")
        driver = create_driver()
        
        # Navigate directly to the page
        url = BASE_URL if current_page == 1 else f"{BASE_URL}?page={current_page}"
        logging.info(f"🔗 Navigating to {url}")
        
        # Extract links with fresh browser
        hrefs, driver = extract_page_links(driver, url, use_click=False)
        
        # If extraction failed, try one more time with longer wait
        if not hrefs:
            logging.warning(f"⚠️  Page {current_page} returned no links. Retrying once with fresh browser...")
            try:
                driver.quit()
            except Exception:
                pass
            
            time.sleep(10)  # Longer wait
            driver = create_driver()
            logging.info(f"🔁 Retry: Navigating to {url}")
            hrefs, driver = extract_page_links(driver, url, use_click=False)
            
            if not hrefs:
                logging.error(f"❌ Page {current_page} failed after retry. Skipping...")
                current_page += 1
                pages_processed += 1
                total_pages_processed += 1
                continue
        
        new_hrefs = [h for h in hrefs if h not in processed and h not in batch_urls]
        batch_urls.update(new_hrefs)
        
        # Always save checkpoint after processing a page
        save_page_checkpoint(current_page)
        pages_processed += 1
        total_pages_processed += 1
        
        logging.info(f"Page {current_page}: Found {len(hrefs)} links, {len(new_hrefs)} new (batch: {len(batch_urls)})")
        
        current_page += 1
    
    logging.info(f"Phase 1 Complete: Collected {len(batch_urls)} new URLs from {pages_processed} pages")
    
    if not batch_urls:
        logging.info("No new URLs to process. Next run will continue from page checkpoint.")
        try:
            driver.quit()
        except Exception:
            pass
        return

    # Phase 2: Extract content from URLs in small batches and save incrementally
    logging.info("=" * 60)
    logging.info(f"Phase 2: Extracting content from {len(batch_urls)} URLs")
    logging.info("=" * 60)
    
    batch: List[Dict[str, str]] = []
    successful_extractions = 0
    failed_extractions = 0
    batch_size = 20  # Save every 20 successful extractions
    
    for idx, url in enumerate(batch_urls, 1):
        # Safety check: don't exceed 5.5 hours
        elapsed_time = time.time() - start_time
        if elapsed_time > MAX_RUNTIME_SECONDS - 300:  # 5 min buffer
            logging.warning("⏰ Approaching time limit, stopping content extraction")
            break
        
        # Close current browser and create fresh one for each URL
        try:
            driver.quit()
        except Exception:
            pass
        
        # Small delay between browser sessions
        time.sleep(2)
        
        # Create fresh browser instance
        driver = create_driver()
            
        try:
            data, driver = extract_url_data(driver, url)
            
            # Only append if we got actual content (not empty)
            if data.get("h1") or data.get("h2") or data.get("content"):
                batch.append(data)
                successful_extractions += 1
                logging.info(f"[{idx}/{len(batch_urls)}] ✅ Extracted: {url}")
            else:
                failed_extractions += 1
                logging.warning(f"[{idx}/{len(batch_urls)}] ⚠️  Empty content: {url}")
            
            # Save batch incrementally to avoid data loss
            if len(batch) >= batch_size:
                append_to_files(batch)
                logging.info(f"💾 Saved batch of {len(batch)} URLs to output files")
                batch = []
                
        except Exception as e:
            failed_extractions += 1
            logging.error(f"[{idx}/{len(batch_urls)}] ❌ Failed to extract {url}: {e}")
    
    # Save remaining batch
    if batch:
        append_to_files(batch)
        logging.info(f"💾 Saved final batch of {len(batch)} URLs to output files")
    
    total_successful_extractions = successful_extractions
    total_failed_extractions = failed_extractions
    
    logging.info("=" * 60)
    logging.info(f"RUN COMPLETE:")
    logging.info(f"  📄 Pages processed: {total_pages_processed}")
    logging.info(f"  🔗 URLs collected: {len(batch_urls)}")
    logging.info(f"  ✅ Successful extractions: {total_successful_extractions}")
    logging.info(f"  ❌ Failed extractions: {total_failed_extractions}")
    logging.info(f"  ⏱️  Runtime: {(time.time() - start_time)/3600:.2f} hours")
    logging.info("=" * 60)
    
    # Next page info for auto-trigger
    next_page = load_page_checkpoint() + 1
    if next_page <= MAX_PAGES:
        logging.info(f"📌 Next run will start from page: {next_page}")
    else:
        logging.info("🎉 All pages will be complete after this run!")

    try:
        driver.quit()
    except Exception:
        pass
    
    logging.info("DONE. Data saved in Parquet. Next run will continue from page checkpoint.")


if __name__ == "__main__":
    run_once()


