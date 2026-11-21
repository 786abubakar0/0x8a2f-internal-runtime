import time
import json
import random
import csv
from curl_cffi import requests
from datetime import datetime


# Base headers shared across all profiles
base_header = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.metro-online.pk/",
    "Origin": "https://www.metro-online.pk",
}

# Your custom browser profiles
PROFILES = [
    {"impersonate": "chrome110", "headers": {**base_header, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36", "Accept-Language": "en-PK,en;q=0.9"}},
    {"impersonate": "chrome120", "headers": {**base_header, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.8"}},
    {"impersonate": "firefox", "headers": {**base_header, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0", "Accept-Language": "en-US,en;q=0.5"}},
    {"impersonate": "safari17_0", "headers": {**base_header, "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15", "Accept-Language": "en-GB,en;q=0.9"}}
]

def fetch_url_data(url, max_retries=3):
    """Fetches and cleans data from a single URL with retry logic."""
    for attempt in range(1, max_retries + 1):
        profile = random.choice(PROFILES)
        try:
            print(f"Requesting: {url[:60]}... (Attempt {attempt})")
            response = requests.get(url, headers=profile["headers"], impersonate=profile["impersonate"], timeout=20)
            
            if response.status_code == 200:
                products = response.json().get("data", [])
                return [{
                    "name": p.get("product_name"),
                    "original_price": p.get("price"),
                    "sale": p.get("sale"), #if sale is true then price=sale_price otherwise price=original_price/price 
                    "sale_price": p.get("sale_price"),
                    "stock": p.get("available_stock"),
                    "max_per_order" : p.get("product_order_limit"),
                    "cat1": p.get("teir1Name"),
                    "cat2": p.get("tier2Name"),
                    "cat3": p.get("tier3Name"),
                    "cat4": p.get("tier4Name"),
                    "product_code":p.get("product_code_app"), # url is made using https://www.metro-online.pk/detail/cat1/cat2/cat3/cat4/name/product_code
                    "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "available" : p.get("active"), # if active=true only then product is available
                    "url" : p.get("deep_link"),
                    
                } for p in products]
            
            print(f"Status {response.status_code} on attempt {attempt}")
        except Exception as e:
            print(f"Error on attempt {attempt}: {e}")
        
        time.sleep(random.uniform(3, 6)) # Delay between retries
    return []

# --- Main Execution ---

# List of your 6 URLs (Replace these with your actual target URLs)
urls = [
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8191",
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8355",
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8107",
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8017",
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8268",
    "https://admin.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=11&filter=tier1Id&filterValue=8507",
]

all_merged_products = []

for target_url in urls:
    data = fetch_url_data(target_url)
    if data:
        all_merged_products.extend(data)
        print(f"Added {len(data)} products. Total so far: {len(all_merged_products)}")
    
    # Polite delay between different URL requests
    time.sleep(random.uniform(2, 4))

# --- Save to CSV ---
if all_merged_products:
    keys = all_merged_products[0].keys() # Get headers from the first dictionary
    with open('metro_products.csv', 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_merged_products)
    print("\nDone! Data merged and saved to 'metro_products.csv'.")
else:
    print("No data collected.")
