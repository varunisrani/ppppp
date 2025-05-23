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

# Load environment variables
load_dotenv()

# Initialize logger first
logger = setup_logging()

# Setup Google Credentials - Check both development and Render paths
GOOGLE_CREDS_PATHS = [
    '/etc/secrets/gen-lang-client-0669898182-f88dce7f97c7.json',  # Render path
    'gen-lang-client-0669898182-f88dce7f97c7.json'  # Local development path
]

creds_found = False
for creds_path in GOOGLE_CREDS_PATHS:
    if os.path.exists(creds_path):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_path
        logger.info(f"Using Google credentials from {creds_path}")
        creds_found = True
        break

if not creds_found:
    logger.error("Google credentials file not found in any of the expected locations")

app = Flask(__name__)
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
                logger.error("Failed to initialize Google Sheets service. Check credentials.")
                time.sleep(30)
                continue

            logger.info("Successfully connected to Google Sheets service")
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
            logger.error("Stack trace:", exc_info=True)  # Add stack trace for better debugging
            logger.info("Waiting 30 seconds before retrying...")
            time.sleep(30)

@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "message": "LinkedIn Scraper API is active",
        "monitoring_active": monitoring_thread is not None and monitoring_thread.is_alive(),
        "google_creds_configured": os.path.exists(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')),
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
        "google_creds_configured": os.path.exists(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')),
        "google_creds_path": os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'Not set'),
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

if __name__ == '__main__':
    # Start monitoring thread before running the Flask app
    start_monitoring_thread()
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False) 