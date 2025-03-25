from cryptopanic_scraper import CryptoPanicScraper
import json
from datetime import datetime, timedelta, timezone
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_info(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data['key'], data['coins']

def parse_args():
    parser = argparse.ArgumentParser(description="CryptoPanic Scraper")
    parser.add_argument('--coins_file', type=str, default='coins.json', help='Path to the JSON file containing the list of coins and API key')
    parser.add_argument('--start_date', type=str, help='Start date for scraping (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, help='End date for scraping (YYYY-MM-DD)')
    parser.add_argument('--timedelta', type=int, default=1, help='Time delta for grouping messages in days')
    parser.add_argument('--bucket_name', type=str, default="cryptopanic-scraper", help='S3 bucket name')
    parser.add_argument('--lambda_name', type=str, default="cryptopanic_scraper", help='Name of the Lambda function')

    return parser.parse_args()

def main(): 
    args = parse_args()
    API_KEY, coins = load_info(args.coins_file)
    timedif = timedelta(days=args.timedelta)
    bucket_name = args.bucket_name
    lambda_name = args.lambda_name

    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        start_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"Starting CryptoPanic Scraper for {start_date} to {end_date}")
    start_time = datetime.now(timezone.utc)

    cryptopanic_scraper = CryptoPanicScraper(coins, API_KEY, start_date, end_date, timedif, bucket_name=bucket_name, lambda_name=lambda_name)
    cryptopanic_scraper.run()

    logger.info("CryptoPanic Scraper finished")
    end_time = datetime.now(timezone.utc)
    total_time = end_time - start_time
    logger.info(f"Total time taken: {total_time}")

if __name__ == '__main__':
    main()
