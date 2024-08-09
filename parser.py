import os
import yfinance as yf
import pymongo
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from dotenv import load_dotenv
import time
import signal

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection details
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME", "stock_data")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "tesla_stock")

# Global flag for graceful shutdown
running = True

def get_mongodb_client():
    return pymongo.MongoClient(MONGO_URL)

def fetch_and_store_data(start_date=None):
    client = get_mongodb_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # If no start_date is provided, use January 1, 2024
    if not start_date:
        start_date = datetime(2024, 1, 1)

    # Fetch data
    tesla = yf.Ticker("TSLA")
    data = tesla.history(start=start_date)

    # Prepare data for MongoDB
    records = []
    for date, row in data.iterrows():
        year = date.year
        record = {
            "date": date.strftime("%Y-%m-%d"),
            "open": row['Open'],
            "high": row['High'],
            "low": row['Low'],
            "close": row['Close'],
            "volume": row['Volume'],
            "dividends": row['Dividends'],
            "stock_splits": row['Stock Splits']
        }
        records.append((year, record))

    # Store data in MongoDB
    for year, record in records:
        collection.update_one(
            {"year": year},
            {"$set": {f"data.{record['date']}": record}},
            upsert=True
        )

    logging.info(f"Stored {len(records)} records in MongoDB")
    client.close()


def daily_update():
    logging.info("daily_update")
    yesterday = datetime.now() - timedelta(days=1)
    fetch_and_store_data(yesterday)

def signal_handler(signum, frame):
    global running
    running = False
    logging.info("Received shutdown signal. Stopping gracefully...")

if __name__ == "__main__":
    if not MONGO_URL:
        logging.error("MONGO_URL not found in .env file. Please set it and try again.")
        exit(1)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initial data fetch
    #fetch_and_store_data()

    # Set up scheduler for daily updates
    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_update, 'cron', hour=0, minute=5)  # Run at 00:05 every day
    scheduler.start()

    logging.info("Daemon started. Press Ctrl+C to exit.")

    try:
        # Use a more efficient loop that sleeps
        while running:
            time.sleep(3600)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        scheduler.shutdown()
        logging.info("Daemon stopped.")