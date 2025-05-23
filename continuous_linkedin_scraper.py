#!/usr/bin/env python3
"""
LinkedIn Profile Continuous Scraper

This script continuously monitors a Google Sheet for rows with blank values in the
LI Ads?, 30 days, and Overall columns. When it finds blank rows, it scrapes LinkedIn
profile data and updates the sheet with ad counts.

Usage:
  python continuous_linkedin_scraper.py --sheet-id YOUR_SHEET_ID --apify-token YOUR_APIFY_TOKEN [options]

Options:
  --sheet-id              Google Sheet ID containing profile URLs (required)
  --apify-token           Apify API token for LinkedIn profile scraping (required)
  --linkedin-username     LinkedIn username for login (optional, uses env var if not provided)
  --linkedin-password     LinkedIn password for login (optional, uses env var if not provided)
  --visible               Show the browser window during execution (default: headless)
  --wait                  Additional wait time between page loads in seconds (default: 2)
  --debug                 Enable debug logging
"""

import csv
import json
import requests
import time
import os
import datetime
import re
import argparse
import logging
from typing import List, Dict, Union, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from apify_client import ApifyClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import sys
from webdriver_manager.core.os_manager import ChromeType

# Load environment variables
load_dotenv()

# Bright Data API settings
BRIGHT_DATA_API_TOKEN = "aa5ba209-9e62-458f-95db-9401ccf87617"
COMPANY_DATASET_ID = "gd_l1vikfnt1wgvvqz95w"
BRIGHT_DATA_BASE_URL = "https://api.brightdata.com/datasets/v3"

# LinkedIn credentials (default values)
DEFAULT_LINKEDIN_USERNAME = os.getenv('LINKEDIN_USERNAME')
DEFAULT_LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD')

# Google API settings
RENDER_ENVIRONMENT = os.getenv('RENDER', 'false').lower() == 'true'
if RENDER_ENVIRONMENT:
    SERVICE_ACCOUNT_FILE = '/etc/secrets/gen-lang-client-0669898182-f88dce7f97c7.json'
else:
    SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', "gen-lang-client-0669898182-f88dce7f97c7.json")

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def setup_logging():
    """Configure basic logging for the script"""
    logs_dir = create_directory("logs")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"linkedin_scraper_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger("linkedin_scraper")
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

def create_directory(dir_name):
    """Create a directory if it doesn't exist"""
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
            logging.info(f"Created directory: {dir_name}")
        except Exception as e:
            logging.warning(f"Could not create directory {dir_name}: {e}")
    return dir_name

def get_google_sheets_service():
    """Initialize and return Google Sheets service."""
    try:
        logging.info(f"Initializing Google Sheets service using service account file: {SERVICE_ACCOUNT_FILE}")
        
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logging.error(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")
            if RENDER_ENVIRONMENT:
                logging.error("Running in Render but credentials file not found in /etc/secrets")
            else:
                logging.error("Running locally but credentials file not found in current directory")
            return None
            
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        logging.info("Successfully loaded credentials from service account file")
        
        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
        logging.info("Successfully built Google Sheets service")
        
        # Test the service
        try:
            service.spreadsheets()
            logging.info("Successfully verified spreadsheets() method availability")
        except Exception as e:
            logging.error(f"Service initialization succeeded but spreadsheets() method not available: {e}")
            return None
            
        return service
    except Exception as e:
        logging.error(f"Error initializing Google Sheets service: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error(f"Error details: {str(e)}")
        return None

def find_blank_rows(sheet_id: str, service, logger=None) -> List[Dict[str, Union[str, int]]]:
    """Find rows where LI Ads?, 30 days, and Overall columns are blank.
    
    Args:
        sheet_id: Google Sheet ID
        service: Google Sheets service
        logger: Optional logger
        
    Returns:
        List of dictionaries with profile URLs and row indices
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        
    try:
        # Get the sheet metadata
        sheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheet_title = sheet['sheets'][0]['properties']['title']
        
        # Get all values from the sheet
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{sheet_title}!A1:Z"  # Get all columns
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:  # Need at least header row and one data row
            logger.error('No data found in the sheet')
            return []
            
        # Find column indices
        headers = values[0]
        try:
            profile_url_col = headers.index('profileUrl')
            li_ads_col = headers.index('LI Ads?')
            days_30_col = headers.index('30 days')
            overall_col = headers.index('Overall')
            
            logger.info(f'Found required columns: profileUrl={profile_url_col}, LI Ads?={li_ads_col}, 30 days={days_30_col}, Overall={overall_col}')
        except ValueError as e:
            logger.error(f'Could not find one of the required columns: {e}')
            return []
            
        # Find rows with blank values in the specified columns
        blank_rows = []
        
        for i, row in enumerate(values[1:], start=2):  # Skip header row, 1-indexed for Google Sheets
            # Skip rows without a profile URL
            if len(row) <= profile_url_col or not row[profile_url_col].strip() or 'linkedin.com/in/' not in row[profile_url_col]:
                continue
                
            # Check if any of the specified columns are blank
            li_ads_blank = len(row) <= li_ads_col or not row[li_ads_col].strip()
            days_30_blank = len(row) <= days_30_col or not row[days_30_col].strip()
            overall_blank = len(row) <= overall_col or not row[overall_col].strip()
            
            if li_ads_blank or days_30_blank or overall_blank:
                blank_rows.append({
                    'profile_url': row[profile_url_col].strip(),
                    'row_index': i,
                    'li_ads_col': li_ads_col,  # Store column indices for updating later
                    'days_30_col': days_30_col,
                    'overall_col': overall_col
                })
        
        logger.info(f'Found {len(blank_rows)} rows with blank values in the specified columns')
        return blank_rows
        
    except HttpError as err:
        logger.error(f'Error reading from Google Sheet: {err}')
        return []

def scrape_linkedin_profiles(profile_urls, apify_token, logger):
    """Scrape LinkedIn profiles using Apify"""
    logger.info(f"Starting Apify scraper for {len(profile_urls)} profiles")
        
    try:
        # Prepare the Apify input
        input_data = {
            "profileUrls": profile_urls,
            "includeContactInfo": True,
            "includeActivityData": True,
            "includeEducationData": True,
            "includeExperienceData": True
        }
        
        # Create Apify client
        client = ApifyClient(apify_token)
        
        # Start the actor and wait for it to finish
        run = client.actor("2SyF0bVxmgGr8IVCZ").call(run_input=input_data)
        
        # Fetch results
        items = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            items.append(item)
            
        logger.info(f"Successfully scraped {len(items)} profiles with Apify")
        return items
        
    except Exception as e:
        logger.error(f"Error in Apify scraping: {str(e)}")
        return None

def format_company_url(url: str) -> str:
    """Format company URL to be compatible with the Bright Data API requirements."""
    if not url or url == 'N/A':
        return url
        
    # Remove tracking parameters and clean up the URL
    url = url.split('?')[0].strip('/')
    
    # Extract company name/id from URL
    if '/company/' in url:
        company_id = url.split('/company/')[-1].split('/')[0]
        # Format URL to match the working pattern
        return f"https://www.linkedin.com/company/{company_id}"
    
    # Handle cases where URL might be in a different format
    if 'linkedin.com' in url and not url.startswith('http'):
        return f"https://www.{url}"
    elif not url.startswith('http'):
        return f"https://www.linkedin.com/company/{url}"
    
    return url

def trigger_collection(inputs: List[Dict[str, str]]) -> Optional[str]:
    """Trigger a new data collection and return the snapshot ID."""
    url = f"{BRIGHT_DATA_BASE_URL}/trigger"
    params = {
        "dataset_id": COMPANY_DATASET_ID,
        "include_errors": "true"
    }
    headers = {
        "Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    logging.info(f"Triggering new data collection for {len(inputs)} companies...")
    response = requests.post(url, headers=headers, params=params, json=inputs)
    
    if response.status_code != 200:
        logging.error(f"Error: Collection trigger failed with status code {response.status_code}")
        logging.error(f"Response: {response.text}")
        return None
        
    try:
        result = response.json()
        snapshot_id = result.get('snapshot_id')
        if not snapshot_id:
            logging.error("Error: No snapshot_id in response")
            return None
        logging.info(f"Collection triggered successfully. Snapshot ID: {snapshot_id}")
        return snapshot_id
    except Exception as e:
        logging.error(f"Error parsing response: {e}")
        return None

def check_progress(snapshot_id: str) -> str:
    """Check the progress of a collection."""
    url = f"{BRIGHT_DATA_BASE_URL}/progress/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.error(f"Error checking progress: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return 'failed'
            
        result = response.json()
        return result.get('status', 'failed')
    except Exception as e:
        logging.error(f"Error checking progress: {e}")
        return 'failed'

def wait_for_completion(snapshot_id: str, timeout: int = 300, check_interval: int = 5) -> bool:
    """Wait for the collection to complete."""
    start_time = time.time()
    logging.info("\nWaiting for data collection to complete...")
    
    while time.time() - start_time < timeout:
        status = check_progress(snapshot_id)
        
        if status == 'ready':
            logging.info("Data collection completed successfully!")
            return True
        elif status == 'failed':
            logging.error("Data collection failed!")
            return False
        elif status == 'running':
            logging.info(f"Status: {status}...")
            time.sleep(check_interval)
        else:
            logging.error(f"Unknown status: {status}")
            return False
    
    logging.error("Timeout reached while waiting for completion")
    return False

def fetch_results(snapshot_id: str) -> Optional[List[Dict]]:
    """Fetch the results of a completed collection."""
    url = f"{BRIGHT_DATA_BASE_URL}/snapshot/{snapshot_id}"
    headers = {"Authorization": f"Bearer {BRIGHT_DATA_API_TOKEN}"}
    params = {"format": "json"}
    
    try:
        logging.info("Fetching results...")
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            logging.error(f"Error fetching results: {response.status_code}")
            logging.error(f"Response: {response.text}")
            return None
            
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return None

def capture_screenshot(browser, filename):
    """Capture a screenshot of the current page"""
    try:
        screenshots_dir = create_directory("screenshots")
        filepath = os.path.join(screenshots_dir, filename)
        browser.save_screenshot(filepath)
        logging.info(f"Screenshot saved to {filepath}")
        return filepath
    except Exception as e:
        logging.error(f"Error capturing screenshot: {e}")
        return None

def login_to_linkedin(browser, username=None, password=None, logger=None):
    """Login to LinkedIn with the provided credentials"""
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # Use default credentials if none provided
    if username is None or password is None:
        username = DEFAULT_LINKEDIN_USERNAME
        password = DEFAULT_LINKEDIN_PASSWORD
        logger.info("Using default LinkedIn credentials")
    
    if not username or not password:
        logger.error("No LinkedIn credentials provided and no valid defaults found")
        return False
    
    try:
        logger.info("Logging in to LinkedIn...")
        browser.get("https://www.linkedin.com/login")
        logger.info("Login page loaded successfully")

        # Enter username
        username_elem = browser.find_element(By.ID, "username")
        username_elem.send_keys(username)
        logger.info("Username entered")

        # Enter password
        password_elem = browser.find_element(By.ID, "password")
        password_elem.send_keys(password)
        logger.info("Password entered")

        # Click login button
        login_button = browser.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_button.click()
        logger.info("Login form submitted")
        
        # Short delay before proceeding
        time.sleep(2)
        return True

    except Exception as e:
        logger.error(f"Error during login: {str(e)}")
        capture_screenshot(browser, "login_error.png")
        return False

def get_ads_count(browser, url, logger):
    """Extract the number of ads from a LinkedIn Ad Library page"""
    logger.info(f"Navigating to {url}")
    
    try:
        browser.get(url)
        time.sleep(8)  # Increased wait time for page load
        
        # Scroll to load all content
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        browser.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        # Take screenshot for debugging
        screenshot_file = f"ad_library_{int(time.time())}.png"
        screenshot_path = capture_screenshot(browser, screenshot_file)
        logger.info(f"Screenshot saved to {screenshot_path}")
        
        # Check various patterns to find ad count
        page_source = browser.page_source
        
        # Look for various patterns in the page
        patterns = [
            r'([\d,]+)\s+ads?\s+match',
            r'([\d,]+)\s+ads?\s+found',
            r'showing\s+([\d,]+)\s+ads?',
            r'found\s+([\d,]+)\s+ads?'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            if matches:
                counts = [int(m.replace(',', '')) for m in matches]
                count = max(counts)
                logger.info(f"Found {count} ads (largest match)")
                return count
        
        if re.search(r'No ads to show|No results found|No ads match', page_source, re.IGNORECASE):
            logger.info("No ads found on this page")
            return 0
        
        logger.warning("Could not find ad count on the page")
        return None
            
    except Exception as e:
        logger.error(f"Error loading {url}: {e}")
        return None

def continuously_monitor_sheet(args, logger):
    """Continuously monitor the Google Sheet for blank rows and process them.
    
    Args:
        args: Command-line arguments
        logger: Logger instance
    """
    logger.info("Starting continuous monitoring of Google Sheet")
    
    # Get Google Sheets service
    service = get_google_sheets_service()
    if service is None:
        logger.error("Failed to initialize Google Sheets service - exiting monitoring")
        return
    
    # Test the service
    try:
        logger.info("Testing Google Sheets API connection...")
        sheet_metadata = service.spreadsheets().get(spreadsheetId=args.sheet_id).execute()
        sheet_title = sheet_metadata['sheets'][0]['properties']['title']
        logger.info(f"Successfully connected to sheet: {sheet_title}")
    except Exception as e:
        logger.error(f"Error testing Google Sheets connection: {e}")
        logger.error("Exiting monitoring due to Google Sheets API error")
        return
        
    while True:
        try:
            logger.info("Checking for blank rows in Google Sheet...")
            
            # Find rows with blank values in the specified columns
            blank_rows = find_blank_rows(args.sheet_id, service, logger)
            
            if not blank_rows:
                logger.info("No blank rows found. Waiting for 30 seconds before checking again.")
                time.sleep(30)
                continue
            
            logger.info(f"Found {len(blank_rows)} rows with blank values")
            
            # Get profile URLs from blank rows
            profile_urls = [row['profile_url'] for row in blank_rows]
            
            # Process in batches of 10 (or whatever is available)
            batch_size = 10
            for i in range(0, len(profile_urls), batch_size):
                batch = profile_urls[i:i+batch_size]
                logger.info(f"Processing batch of {len(batch)} profiles")
                
                # Extract rows for this batch
                batch_rows = blank_rows[i:i+batch_size]
                
                # Scrape profiles
                profile_data = scrape_linkedin_profiles(batch, args.apify_token, logger)
                if not profile_data:
                    logger.error("Failed to scrape profiles with Apify")
                    continue
                
                # Extract company data
                logger.info("Extracting company details from profiles")
                company_urls = []
                for profile in profile_data:
                    experiences = profile.get('experiences', [])
                    if experiences and len(experiences) > 0:
                        company_url = experiences[0].get('companyLink1', 'N/A')
                        if company_url != 'N/A' and 'linkedin.com/company/' in company_url:
                            formatted_url = format_company_url(company_url)
                            company_urls.append({"url": formatted_url})
                
                # Scrape company data using Bright Data if company URLs exist
                if company_urls:
                    logger.info(f"Scraping data for {len(company_urls)} companies")
                    
                    # Trigger collection
                    snapshot_id = trigger_collection(company_urls)
                    if not snapshot_id:
                        logger.error("Failed to create snapshot for company data")
                        continue
                    
                    # Wait for completion
                    if not wait_for_completion(snapshot_id):
                        logger.error("Snapshot failed or timed out")
                        continue
                    
                    # Fetch results
                    company_data = fetch_results(snapshot_id)
                    if not company_data:
                        logger.error("Failed to fetch company data from snapshot")
                        continue
                    
                    logger.info(f"Successfully scraped {len(company_data)} companies")
                else:
                    logger.warning("No valid company URLs found in profiles")
                    company_data = []
                
                # Set up Chrome for ad scraping
                chrome_options = Options()
                if not args.visible:
                    chrome_options.add_argument('--headless=new')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--disable-software-rasterizer')
                chrome_options.add_argument("--remote-debugging-port=0")
                chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                chrome_options.add_argument('--disable-extensions')
                chrome_options.add_argument('--log-level=0')
                chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
                
                try:
                    # Initialize ChromeDriver using webdriver_manager's auto-detect feature
                    from webdriver_manager.chrome import ChromeDriverManager
                    from webdriver_manager.core.os_manager import ChromeType
                    
                    logger.info("Initializing ChromeDriver...")
                    
                    # On macOS, look for the driver executable directly
                    if sys.platform == 'darwin':  # macOS
                        try:
                            # First try the direct approach with ChromeDriverManager
                            driver_path = ChromeDriverManager().install()
                            logger.info(f"ChromeDriver path from manager: {driver_path}")
                            
                            # If the path contains THIRD_PARTY_NOTICES or similar non-executable file
                            if "NOTICES" in driver_path or not os.path.isfile(driver_path) or not os.access(driver_path, os.X_OK):
                                # Look in the parent directory for the actual executable
                                parent_dir = os.path.dirname(os.path.dirname(driver_path))
                                logger.info(f"Looking for chromedriver in parent directory: {parent_dir}")
                                
                                # Search for chromedriver-mac-arm64 or similar directories
                                for root, dirs, files in os.walk(parent_dir):
                                    for directory in dirs:
                                        if "chromedriver" in directory.lower():
                                            chrome_dir = os.path.join(root, directory)
                                            logger.info(f"Found potential chromedriver directory: {chrome_dir}")
                                            
                                            # Look for the executable in this directory
                                            for chrome_root, chrome_dirs, chrome_files in os.walk(chrome_dir):
                                                for file in chrome_files:
                                                    if file == "chromedriver" or file == "chromedriver.exe":
                                                        driver_path = os.path.join(chrome_root, file)
                                                        logger.info(f"Found chromedriver executable: {driver_path}")
                                                        if os.path.isfile(driver_path):
                                                            os.chmod(driver_path, 0o755)  # Make executable
                                                            break
                        except Exception as chrome_ex:
                            logger.error(f"Error setting up ChromeDriver on macOS: {chrome_ex}")
                            raise
                    else:
                        # For other platforms, use the standard approach
                        driver_path = ChromeDriverManager().install()
                        logger.info(f"ChromeDriver installed at: {driver_path}")
                        
                    # Create browser instance
                    driver_service = Service(executable_path=driver_path)
                    browser = webdriver.Chrome(service=driver_service, options=chrome_options)
                    browser.set_window_size(1920, 1080)
                    
                    try:
                        # Login to LinkedIn
                        if not login_to_linkedin(browser, args.linkedin_username, args.linkedin_password, logger=logger):
                            logger.error("Failed to login to LinkedIn")
                            continue
                        
                        # Scrape ad counts
                        if company_data:
                            logger.info("Scraping ad counts for companies")
                            for company in company_data:
                                company_id = company.get('company_id')
                                company_name = company.get('name', 'Unknown')
                                
                                if not company_id:
                                    logger.warning(f"No company ID for {company_name}, skipping...")
                                    continue
                                
                                logger.info(f"Processing {company_name} (ID: {company_id})")
                                
                                # Get all-time ads count
                                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
                                all_time_count = get_ads_count(browser, url, logger)
                                
                                # Get last 30 days count
                                url = f"https://www.linkedin.com/ad-library/search?companyIds={company_id}&dateOption=last-30-days"
                                last_30_count = get_ads_count(browser, url, logger)
                                
                                # Update company data
                                company['all_time_ads'] = all_time_count if all_time_count is not None else 0
                                company['last_30_days_ads'] = last_30_count if last_30_count is not None else 0
                                
                                logger.info(f"Ad counts for {company_name}:")
                                logger.info(f"  All time: {company['all_time_ads']}")
                                logger.info(f"  Last 30 days: {company['last_30_days_ads']}")
                                
                                # Small delay between companies
                                time.sleep(args.wait)
                        
                    except Exception as e:
                        logger.error(f"Error during LinkedIn ad scraping: {str(e)}")
                    finally:
                        browser.quit()
                    
                except Exception as e:
                    logger.error(f"Error in Chrome setup: {str(e)}")
                
                # Create final combined data
                combined_data = []
                def extract_company_id_from_url(url):
                    match = re.search(r"/company/(\d+)", str(url))
                    return match.group(1) if match else None
                    
                for idx, profile in enumerate(profile_data):
                    profile_company_id = None
                    if profile.get('experiences') and len(profile['experiences']) > 0:
                        raw_company_link = profile['experiences'][0].get('companyLink1', 'N/A')
                        profile_company_id = extract_company_id_from_url(raw_company_link)
                        
                    company_info_for_profile = {}  # Default to empty if no match
                    if profile_company_id and profile_company_id != 'N/A':  # Only attempt match if profile_company_id is valid
                        for company_entry in company_data:
                            company_data_id = str(company_entry.get('company_id', ''))
                            if company_data_id == str(profile_company_id):
                                company_info_for_profile = company_entry
                                break
                                
                    company_info = company_info_for_profile  # Use the matched company_info or the default empty {}
                    combined_record = {
                        'profile_url': profile.get('linkedinUrl', 'N/A'),
                        'all_time_ads': company_info.get('all_time_ads', 0),
                        'last_30_days_ads': company_info.get('last_30_days_ads', 0),
                        'linkedin_ads': 'y' if (company_info.get('all_time_ads', 0) > 0 or company_info.get('last_30_days_ads', 0) > 0) else 'n',
                        'row_index': batch_rows[idx]['row_index'],
                        'li_ads_col': batch_rows[idx]['li_ads_col'],
                        'days_30_col': batch_rows[idx]['days_30_col'],
                        'overall_col': batch_rows[idx]['overall_col']
                    }
                    combined_data.append(combined_record)
                
                # Update Google Sheet with the results
                logger.info("Updating Google Sheet with results")
                
                # Get sheet metadata
                sheet = service.spreadsheets().get(spreadsheetId=args.sheet_id).execute()
                sheet_title = sheet['sheets'][0]['properties']['title']
                
                # Prepare batch update
                data = []
                for item in combined_data:
                    row_idx = item['row_index']
                    
                    # Add LI Ads? column update
                    li_ads = item.get('linkedin_ads', 'n')
                    data.append({
                        'range': f"{sheet_title}!{chr(65+item['li_ads_col'])}{row_idx}",
                        'values': [[li_ads]]
                    })
                    
                    # Add 30 days column update
                    days_30 = item.get('last_30_days_ads', 0)
                    data.append({
                        'range': f"{sheet_title}!{chr(65+item['days_30_col'])}{row_idx}",
                        'values': [[days_30]]
                    })
                    
                    # Add Overall column update
                    overall = item.get('all_time_ads', 0)
                    data.append({
                        'range': f"{sheet_title}!{chr(65+item['overall_col'])}{row_idx}",
                        'values': [[overall]]
                    })
                
                if data:
                    body = {
                        'valueInputOption': 'RAW',
                        'data': data
                    }
                    service.spreadsheets().values().batchUpdate(
                        spreadsheetId=args.sheet_id,
                        body=body
                    ).execute()
                    logger.info(f"Updated {len(data)//3} rows with ad data")
                else:
                    logger.warning("No updates to perform in this batch")
                
                logger.info(f"Batch processing complete, immediately checking for more rows")
            
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {str(e)}")
            logger.info("Waiting for 30 seconds before trying again")
            time.sleep(30)

def main():
    parser = argparse.ArgumentParser(description="LinkedIn Profile and Ad Count Scraper with Continuous Monitoring")
    parser.add_argument('--sheet-id', required=True, help='Google Sheet ID containing profile URLs')
    parser.add_argument('--apify-token', required=True, help='Apify API token')
    parser.add_argument('--linkedin-username', help='LinkedIn username')
    parser.add_argument('--linkedin-password', help='LinkedIn password')
    parser.add_argument('--visible', action='store_true', help='Show the browser window during execution')
    parser.add_argument('--wait', type=int, default=2, help='Additional wait time after page load in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    # Setup logging
    logger = setup_logging()
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    logger.info("Starting LinkedIn Profile and Ad Count Scraper with Continuous Monitoring")
    
    # Start continuous monitoring
    continuously_monitor_sheet(args, logger)

if __name__ == "__main__":
    main() 