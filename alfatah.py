from curl_cffi import requests
import json
import time
import random
import csv
from datetime import datetime
import os
import argparse

##this import and load_dotenv() line is for local run on pc. not needed on github
from dotenv import load_dotenv 
# 2. Load .env file (GitHub will ignore this because it doesn't have a .env file)
load_dotenv()

# --- CONFIGURATION ---
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

PROXIES = json.loads(os.getenv("PROXIES_JSON"))

BRANCH_CONFIG = {
    "bahria": {"lat": 33.52479241926591, "lng": 73.09750395229818},
    "centaurus": {"lat": 33.710789, "lng": 73.0497076},

}

# --- ADVANCED HEADER CONFIGURATION ---

# Base headers that every real browser sends to Shopify
base_headers = {
    "X-Shopify-Storefront-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://alfatah.pk",
    "Referer": "https://alfatah.pk/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin", # Vital: Tells Shopify the request is from its own site
}

PROFILES = [
    {
        "impersonate": "chrome110",
        "headers": {
            **base_headers,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
    },
    {
        "impersonate": "chrome120",
        "headers": {
            **base_headers,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
    },
    {
        "impersonate": "firefox",
        "headers": {
            **base_headers,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
            "TE": "trailers", # Specific to Firefox connection handling
        }
    },
    {
        "impersonate": "safari17_0",
        "headers": {
            **base_headers,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        }
    }
]
def get_args():
    parser = argparse.ArgumentParser(description="Al-Fatah Master Scraper")
    parser.add_argument("--branch", required=True, choices=BRANCH_CONFIG.keys())
    return parser.parse_args()

args = get_args()
LAT, LNG = BRANCH_CONFIG[args.branch]["lat"], BRANCH_CONFIG[args.branch]["lng"]
OUTPUT_FILE = f"alfatah_master_{args.branch}.csv"

CATEGORIES = [
  "fresh-fruits-online",
     "buy-fresh-vegetables-online", "chicken-price-in-pakistan",
    "meat-price-in-pakistan", "mutton", "seafood", "bakery", 
    "cooking-ingredients", "dairy", "food-beverages", "tobacco-shop",
    "snacks-confectioneries", "household-cleaning-products",
    "personal-care-beauty-products", "cookware-baking", "baby-food-in-pakistan",
    "baby-care-products", "maternity-care-products", "baby-toiletry",
    "diapers-wipes", "skin-care-products-in-pakistan", "shampoo-and-conditioner-online"

]

def scrape():
    master_data = {}

    # 1. PRESERVE OLD DATA: Load existing CSV so we don't lose items if a category fails
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f): 
                    master_data[row['id']] = row
            print(f"📂 Loaded {len(master_data)} existing records to ensure persistence.")
        except Exception as e:
            print(f"⚠️ Could not load existing file: {e}")

    for handle in CATEGORIES:
        print(f"🚀 Processing: {handle}...")
        has_next_page = True
        cursor = None

        while has_next_page:
            success = False
            
            # 2. HIGH RELIABILITY: 5 Retries per fetch request
            for attempt in range(1, 6):
                try:
                    profile = random.choice(PROFILES)
                    proxy = random.choice(PROXIES)
                    # print(profile)
                    print(proxy)
                    
                    payload = {
                        "query": """
                        query getCategory($handle: String!, $cursor: String, $lat: Float!, $lng: Float!) {
                          collection(handle: $handle) {
                            products(first: 250, after: $cursor) {
                              pageInfo { hasNextPage endCursor }
                              nodes {
                                id title handle
                                variants(first: 1) {
                                  nodes {
                                    sku price { amount } compareAtPrice { amount }
                                    max_limit: metafield(key: "techandaz_variant_max_limit", namespace: "custom") { value }
                                    storeAvailability(near: {latitude: $lat, longitude: $lng}, first: 1) {
                                      nodes { available quantityAvailable }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }""",
                        "variables": {"handle": handle, "cursor": cursor, "lat": LAT, "lng": LNG}
                    }

                    resp = requests.post(
                        "https://alfatah.pk/api/2026-01/graphql.json",
                        json=payload,
                        headers=profile['headers'],
                        impersonate=profile['impersonate'],
                        proxies=proxy,
                        timeout=40
                    )

                    if resp.status_code == 429:
                        wait = attempt * 25 # Increased backoff for rate limits
                        print(f"  ⚠️ Rate Limited. Retry {attempt}/5 in {wait}s...")
                        time.sleep(wait)
                        continue
                    
                    resp.raise_for_status()
                    res_json = resp.json()
                    
                    # Handle internal Shopify errors
                    if 'errors' in res_json:
                        print(f"  [!] Shopify Error: {res_json['errors'][0]['message']}")
                        break

                    p_data = res_json.get('data', {}).get('collection', {}).get('products', {})
                    nodes = p_data.get('nodes', [])

                    for p in nodes:
                        # 1. Safely get variants
                        variants = p.get('variants', {}).get('nodes', [])
                        if not variants:
                            continue
                        
                        v = variants[0]
                        
                        # 2. Safely get storeAvailability
                        avail_nodes = v.get('storeAvailability', {}).get('nodes', [])
                        if avail_nodes:
                            avail = avail_nodes[0]
                            stock = avail.get('quantityAvailable', 0)
                            is_instock = "Yes" if avail.get('available') else "No"
                        else:
                            stock = 0
                            is_instock = "No"

                        pid = p['id'].split('/')[-1]

                        # 3. Restore your original field mapping
                        master_data[pid] = {
                            "id": pid,
                            "title": p['title'],
                            "sku": v.get('sku', 'N/A'),
                            "url": f"https://alfatah.pk/products/{p['handle']}",
                            # If compareAtPrice exists, use it as original_price, otherwise use sale_price
                            "original_price": v['compareAtPrice']['amount'] if v.get('compareAtPrice') else v['price']['amount'],
                            "sale_price": v['price']['amount'],
                            "total_stock": stock,
                            "is_available": is_instock,
                            "max_allow_per_order": v.get('max_limit', {}).get('value') if v.get('max_limit') else "No Limit",
                            # Keep these for your own tracking (optional)
                            "category": handle,
                            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
                    has_next_page = p_data.get('pageInfo', {}).get('hasNextPage', False)
                    cursor = p_data.get('pageInfo', {}).get('endCursor')
                    
                    success = True
                    print(f"  [+] {handle}: Captured {len(nodes)} items.")
                    time.sleep(random.uniform(4, 8)) # Random delay
                    break 

                except Exception as e:
                    wait = attempt * 15
                    print(f"  ❗ Attempt {attempt}/5 failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)

            if not success:
                print(f"  ❌ Skipping page after 5 retries. Moving on to preserve existing data.")
                has_next_page = False

        # Random delay between categories
        time.sleep(random.uniform(5, 15))

    # 3. SINGLE FILE WRITE: Only happens once at the end
    if master_data:
        print(f"\n💾 Writing {len(master_data)} items to {OUTPUT_FILE}...")
        fieldnames = master_data[next(iter(master_data))].keys()
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(master_data.values())
        print("🎉 All done!")
    else:
        print("❌ No data collected and no existing file found.")

if __name__ == "__main__":
    scrape()
