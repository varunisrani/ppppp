from flask import Flask, jsonify, request
from continuous_linkedin_scraper import (
    setup_logging,
    get_google_sheets_service,
    find_blank_rows,
    scrape_linkedin_profiles,
    trigger_collection,
    wait_for_completion,
    fetch_results,
    format_company_url,
    get_ads_count,
    continuously_monitor_sheet
)
import os
from threading import Thread
import logging
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
import platform

# Load environment variables
load_dotenv()

# Initialize Flask app and logger first
app = Flask(__name__)
logger = setup_logging()

# Setup Google Credentials
RENDER_ENVIRONMENT = os.getenv('RENDER', 'false').lower() == 'true'
if RENDER_ENVIRONMENT:
    GOOGLE_CREDS_PATH = '/etc/secrets/gen-lang-client-0669898182-f88dce7f97c7.json'
else:
    # For local development, use credentials from current directory
    GOOGLE_CREDS_PATH = os.path.join(os.getcwd(), 'gen-lang-client-0669898182-f88dce7f97c7.json')

if os.path.exists(GOOGLE_CREDS_PATH):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_CREDS_PATH
    logger.info(f"Using Google credentials from {GOOGLE_CREDS_PATH}")
else:
    logger.warning(f"Google credentials file not found at {GOOGLE_CREDS_PATH}")
    if not RENDER_ENVIRONMENT:
        logger.info("In local environment - please place the credentials file in the project root directory")

monitoring_thread = None
should_stop = False

def start_continuous_monitoring():
    """Function to start the continuous monitoring process"""
    sheet_id = os.getenv('SHEET_ID')
    apify_token = os.getenv('APIFY_TOKEN')
    linkedin_username = os.getenv('LINKEDIN_USERNAME')
    linkedin_password = os.getenv('LINKEDIN_PASSWORD')

    if not sheet_id or not apify_token:
        logger.error("Missing required environment variables: SHEET_ID and APIFY_TOKEN")
        return

    logger.info("Starting automatic continuous monitoring...")
    logger.info(f"Using Sheet ID: {sheet_id}")
    logger.info("LinkedIn credentials configured: %s", "Yes" if linkedin_username and linkedin_password else "No")

    class Args:
        def __init__(self):
            self.sheet_id = sheet_id
            self.apify_token = apify_token
            self.linkedin_username = linkedin_username
            self.linkedin_password = linkedin_password
            self.visible = False
            self.wait = 2
            self.debug = False

    args = Args()

    while not should_stop:
        try:
            # Get Google Sheets service
            service = get_google_sheets_service()
            if service is None:
                logger.error("Failed to initialize Google Sheets service")
                time.sleep(30)
                continue

            logger.info("Checking for blank rows in Google Sheet...")
            blank_rows = find_blank_rows(sheet_id, service, logger)
            
            if not blank_rows:
                logger.info("No blank rows found. Waiting for 30 seconds before checking again.")
                time.sleep(30)
                continue
            
            logger.info(f"Found {len(blank_rows)} rows with blank values")
            
            # Process the rows using the existing logic
            continuously_monitor_sheet(args, logger)
            
        except Exception as e:
            logger.error(f"Error in continuous monitoring: {str(e)}")
            logger.info("Waiting 30 seconds before retrying...")
            time.sleep(30)

@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "message": "LinkedIn Scraper API is active",
        "monitoring_active": monitoring_thread is not None and monitoring_thread.is_alive(),
        "timestamp": datetime.now().isoformat()
    })

@app.route('/status')
def status():
    if monitoring_thread and monitoring_thread.is_alive():
        status_msg = "Monitoring is active and running"
    else:
        status_msg = "Monitoring thread is not running"

    return jsonify({
        "status": "healthy",
        "monitoring_status": status_msg,
        "monitoring_active": monitoring_thread is not None and monitoring_thread.is_alive(),
        "sheet_id": os.getenv('SHEET_ID'),
        "google_creds_configured": os.path.exists('/etc/secrets/google-credentials.json'),
        "timestamp": datetime.now().isoformat()
    })

def start_monitoring_thread():
    """Start the monitoring thread if not already running"""
    global monitoring_thread, should_stop
    
    if monitoring_thread and monitoring_thread.is_alive():
        logger.info("Monitoring thread is already running")
        return
    
    should_stop = False
    monitoring_thread = Thread(target=start_continuous_monitoring)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    logger.info("Started new monitoring thread")

def setup_chrome_driver():
    """Setup Chrome driver with undetected-chromedriver"""
    try:
        # Use context manager to handle version mismatch
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--proxy-server='direct://'")
        chrome_options.add_argument("--proxy-bypass-list=*")
        chrome_options.add_argument("--start-maximized")
        
        if RENDER_ENVIRONMENT:
            # Set specific Chrome binary path for Render
            chrome_binary = "/usr/bin/chromium"
            logger.info(f"Setting Chrome binary location to: {chrome_binary}")
            chrome_options.binary_location = chrome_binary
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-dev-tools')
            chrome_options.add_argument('--no-zygote')
            chrome_options.add_argument('--single-process')
            
            # Driver configuration for Render environment
            chromedriver_path = "/usr/bin/chromedriver"
            logger.info(f"Using ChromeDriver at: {chromedriver_path}")
            
            # Set explicit version to match Chrome 136 as reported in the error
            chrome_version = 136
            logger.info(f"Setting Chrome version to: {chrome_version}")
            
            driver = uc.Chrome(
                options=chrome_options,
                driver_executable_path=chromedriver_path,
                version_main=chrome_version
            )
        else:
            # For non-Render environments, use automatic version detection
            logger.info("Using automatic ChromeDriver version detection")
            driver = uc.Chrome(options=chrome_options)
            
        logger.info("Chrome driver initialized successfully!")
        return driver
    except Exception as e:
        logger.error(f"Error initializing Chrome driver: {e}")
        
        # Add more diagnostic information
        if RENDER_ENVIRONMENT:
            try:
                import subprocess
                chrome_version_output = subprocess.check_output(["/usr/bin/chromium", "--version"], stderr=subprocess.STDOUT).decode('utf-8').strip()
                logger.error(f"Installed Chrome version: {chrome_version_output}")
                
                chromedriver_version_output = subprocess.check_output(["/usr/bin/chromedriver", "--version"], stderr=subprocess.STDOUT).decode('utf-8').strip()
                logger.error(f"Installed ChromeDriver version: {chromedriver_version_output}")
            except Exception as diag_error:
                logger.error(f"Error getting diagnostic info: {diag_error}")
                
        raise

# Initialize the driver
try:
    driver = setup_chrome_driver()
    print("Chrome driver initialized successfully!")
    # Your code here...
    
except Exception as e:
    print(f"Error initializing Chrome driver: {e}")
finally:
    if 'driver' in locals():
        driver.quit()

if __name__ == '__main__':
    # Start monitoring thread before running the Flask app
    start_monitoring_thread()
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False) 