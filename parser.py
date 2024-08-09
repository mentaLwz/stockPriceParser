import os
import yfinance as yf
import pymongo
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection details
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "test")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "tesla_stock")

def get_mongodb_client():
    return pymongo.MongoClient(MONGO_URI)

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
    yesterday = datetime.now() - timedelta(days=1)
    fetch_and_store_data(yesterday)

if __name__ == "__main__":
    if not MONGO_URI:
        logging.error("MONGO_URI not found in .env file. Please set it and try again.")
        exit(1)

    # Initial data fetch
    fetch_and_store_data()

    # Set up scheduler for daily updates
    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_update, 'cron', hour=0, minute=5)  # Run at 00:05 every day
    scheduler.start()

    logging.info("Daemon started. Press Ctrl+C to exit.")

    try:
        # Keep the script running
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Daemon stopped.")