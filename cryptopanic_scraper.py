import requests
import json
from datetime import datetime, timedelta, timezone
import boto3
from queue import Queue
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class CryptoPanicScraper:
    def __init__(
        self,
        currencies, # List of cryptocurrency symbols, e.g. ["BTC", "ETH", "XRP"]
        API_KEY,
        start_date: datetime,
        end_date: datetime,
        delta: timedelta,
        bucket_name: str = None,
        lambda_name: str = "cryptopanic_scraper_lambda"
    ):
        self.currencies = currencies
        self.start_date = start_date
        self.end_date = end_date
        self.delta = delta
        self.bucket_name = bucket_name
        self.lambda_name = lambda_name
        self.bucket_tmp_dir = "cryptopanic_tmp"
        self.s3_client = boto3.client('s3')
        self.lambda_client = boto3.client('lambda', region_name='eu-north-1')
        self.batch_size = 500
        self.files_contents_queue = Queue()
        self.api_key = API_KEY

    def fetch_news(self):
        """
        Fetch news posts from CryptoPanic for the specified currencies.
        """

        page = 1
        posts = []
        url = "https://cryptopanic.com/api/v1/posts/"

        while True:
            finished = False

            params = {
                "auth_token": self.api_key,
                "currencies": ",".join(self.currencies),
                "page": page
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                logger.error(f"Failed to fetch news: {response.status_code}")
                return []
            data = response.json()

            for post in data.get("results", []):
                published_at = post.get("published_at")

                try:
                    published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.error(f"Error parsing date {published_at}: {e}")
                    continue
            
                if self.start_date <= published_at <= self.end_date:
                    posts.append({
                        'date': published_at.isoformat(),
                        'text': f"{post.get('title')}\n"
                    })

                # The news are too old or the page count above magic constant
                # after which they start to self - repeat
                if published_at < self.start_date or page > 11:  
                    finished = True

            if finished:
                break
            page += 1
        
        return posts

    def save_to_files(self, news_list):
        """
        Save news to a JSON file.
        If an S3 bucket name is provided, upload the file; otherwise, save locally.
        """

        file_name = f"cryptopanic_{self.start_date.strftime('%Y-%m-%d')}_{(self.end_date - timedelta(days=1)).strftime('%Y-%m-%d')}.json"
        file_content = json.dumps(news_list, indent=4, ensure_ascii=False)
        if self.bucket_name:
            s3_key = f"{self.start_date.strftime('%Y/%m/%d')}/{file_name}"
            try:
                self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=file_content)
                logger.info(f"Uploaded file to S3: {s3_key}")
            except ClientError as e:
                logger.error(f"Error uploading file to S3: {e}")
        else:
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(file_content)
            logger.info(f"Saved file locally: {file_name}")

    def run(self):
        logger.info("Scraping news started")
        all_posts = self.fetch_news()
        logger.info(f"Scraping completed. Retrieved {len(all_posts)} records.")
        self.save_to_files(all_posts)
