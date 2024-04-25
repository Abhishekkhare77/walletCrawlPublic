import requests
from bs4 import BeautifulSoup
import json
import time
import random
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime


MONGODB_URI = "mongodb://user:pass@164.52.205.97:27018/"

user_agents = [
    # Common desktop browsers
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/76.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/83.0.478.45 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/77.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    # Common mobile browsers
    "Mozilla/5.0 (Linux; Android 10; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    # Search engine bots
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    # Miscellaneous
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
]

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# Function to extract data from a single page
def extract_data_from_page(url, page):
    try:
        headers = {"User-Agent": random.choice(user_agents)}
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        data = []
        if table:
            for row in table.find_all('tr')[1:]:  # Skip the header row
                columns = row.find_all('td')
                address = columns[0].text.strip()
                balance = columns[1].text.strip()
                incoming_txs = columns[2].text.strip()
                last_used_in_block = columns[3].text.strip()
                data.append({
                    'wallet': url.split('/')[4],  # Extract exchange name from URL
                    'address': address,
                    'balance': balance,
                    'incoming_txs': incoming_txs,
                    'last_used_in_block': last_used_in_block,
                    'page': page
                })
        return data
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return []


def main():
    with open('exchange.json') as f:
        exchange_info_list = json.load(f)

    for exchange_info in exchange_info_list:
        if not exchange_info['isCompleted']:
            base_url = exchange_info['url_link']
            max_page = exchange_info['max_page']
            crawled = exchange_info['crawled']
            exchange_info['start_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for page in range(crawled, max_page + 1):
                url = f"{base_url}?page={page}"
                data = extract_data_from_page(url, page)

                client = MongoClient(MONGODB_URI)
                db = client.get_database("WalletData")
                collection = db.get_collection(exchange_info['exchange'])

                for item in data:
                    # Check if data already exists in the database
                    existing_data = collection.find_one({'address': item['address']})
                    if existing_data:
                        # If data exists, update it
                        collection.update_one({'_id': existing_data['_id']}, {'$set': item})
                        print(f"Data updated in MongoDB for exchange: {exchange_info['exchange']}")
                    else:
                        # If data doesn't exist, insert it
                        collection.insert_one(item)
                        print(f"Data inserted in MongoDB for exchange: {exchange_info['exchange']}")

                exchange_info['crawled'] = crawled
                crawled += 1
                time.sleep(random.uniform(5, 7))
                exchange_info['completed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Update exchange.json with new start_at value
                with open('exchange.json', 'w') as f:
                    json.dump(exchange_info_list, f, indent=4)

            # Check if all pages have been crawled
            if crawled >= max_page:
                exchange_info['isCompleted'] = True

        else:
            print(f"All pages crawled for exchange: {exchange_info['exchange']}")

    with open('exchange.json', 'w') as f:
        json.dump(exchange_info_list, f, indent=4)

if __name__ == "__main__":
    main()