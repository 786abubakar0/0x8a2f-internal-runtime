
from curl_cffi import requests
import json
import time
import random
import csv
from datetime import datetime
import os
import argparse


PROXIES = json.loads(os.getenv("PROXIES_JSON", "[]"))
CUSTOMER_ID = os.getenv("CUSTOMER_ID")
# 1. Configuration & CLI
BRANCH_CONFIG = {
    "saddar": "khdw",
    "westridge": "qguh",
    "satellitetown": "clnz"
}

def get_args():
    parser = argparse.ArgumentParser(description="Pandamart Category Scraper")
    parser.add_argument("--branch", required=True, choices=BRANCH_CONFIG.keys(), help="Branch to scrape")
    return parser.parse_args()

args = get_args()
VENDOR_ID = BRANCH_CONFIG[args.branch]
# Default filename if none provided
OUTPUT_FILE = f"extracted_{args.branch}.csv"

# 2. GraphQL Query
QUERY = """
query getProductsByCategoryList($categoryId: String!, $globalEntityId: String!, $isDarkstore: Boolean!, $locale: String!, $vendorID: String!) {
    categoryProductList(input: { categoryID: $categoryId, globalEntityID: $globalEntityId, isDarkstore: $isDarkstore, locale: $locale, platform: "web", vendorID: $vendorID }) {
        categoryProducts {
            items {
                productID
                name
                price
                originalPrice
                stockAmount
                isAvailable
                attributes { key value }
            }
        }
    }
}
"""

# Base headers - keeping your original structure
header = {
    "accept": "application/json",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://www.foodpanda.pk",
    "referer": "https://www.foodpanda.pk/",
    "x-pd-language-id": "1",
    "platform": "web",
    "apollographql-client-name": "web",
    "apollographql-client-version": "GROCERIES-MENU-MICROFRONTEND.26.10.0007",
    "cust-code": CUSTOMER_ID,
}


PROFILES = [
    {
        "impersonate": "chrome110", # Very stable
        "headers": {
            **header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept-Language": "en-PK,en;q=0.9",
        }
    },
    {
        "impersonate": "chrome120", # Current standard
        "headers": {
             **header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.8",
        }
    },
    {
        "impersonate": "firefox", # Use the generic 'firefox' tag instead of 'firefox120'
        "headers": {
             **header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
            "Accept-Language": "en-US,en;q=0.5",
        }
    },
    
    {
        "impersonate": "safari17_0",
        "headers": {
             **header,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Accept-Language": "en-GB,en;q=0.9",
        }
    }
]






def scrape_mart():
    
# # 4. Variables for the specific Beverages category
    categories = [
    "0d7a99a8-3b47-4970-be58-67cda1e600c0", #meat and seafood
    "35fd2d7c-7838-4b98-b86f-6a86944f896d", #dairy products
    "c579905b-8afa-48c1-9741-72b274c4184e", #bakery and breakfast
    "79b8dcac-28f2-4f3d-8c32-455c6126dc3d", #everyday grocery
    "143024ef-02a5-4e7a-bfa7-82e00a70e49c", #beverages
    "7d31bcec-06db-4a02-a63d-754c3035dcf5", #oil and ghee
    "3cb1867c-37b7-4b83-b179-fd4d8377e21d", # noodles and pasta
    "6a4c5e9a-fb13-4ee5-99e9-24f6ec3a302e", # spices and dressings
    "9ee8eeb7-e637-4f87-bc45-b9463a087d7a", # tea and coffee
    "d0983207-17e8-437a-9167-eb0578ee92e7", # snacks and confectionary
    "876b9353-6d6c-4f5b-a06b-767b284c549e", # chocolate and deserts
    "3f834f4f-1ba9-445b-9f4b-c1a88c0c5bbd", # ice cream and frozen
    "3aea2a54-69f0-420f-a578-4d6b6955a4a3", # ready to cook and eat
    "ac0a5ee2-682d-4397-8af1-cf728a96e3da", # nicotine hub
    "9fe85ec9-3d72-40f6-b159-44d448d830f2", # personal care
    "a0599fa9-67a9-4e07-adec-89e19106765a", # cosmetics and fragrances
    "aeda9f94-ccd9-4189-b2b8-5f1fef6af286", # mother and baby
    "3e10c754-7108-4794-99df-fa29a3db2029", # cleaning and laundary
    "aca4934f-099a-4a50-9cec-da76fd72a9f2", # stationary and party supplies
    "5e1452a3-8c58-41f6-abf2-a81bb49b0eb2", # pet needs
    "2068febd-a38f-46f4-8f5f-59c090be384a", # household essentials
    "aa20e9c9-5c36-4a39-b9f2-513a291c677d", # fruits and vegetables
    "ca0029b5-6983-41eb-bace-15e8f2a0fe1f", # pharmacy
]

    # Data starts empty for every branch run
    branch_data = {}
    # --- ADD THIS BLOCK HERE ---
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # This ensures we keep old data if the new scrape fails for an item
                    branch_data[row['product_id']] = row
            print(f"📂 Loaded {len(branch_data)} existing records from {OUTPUT_FILE}")
        except Exception as e:
            print(f"⚠️ Could not read existing file (starting fresh): {e}")
    # --- END OF ADDED BLOCK ---

    for cat_id in categories:
        print(f"🚀 Scraping Category {cat_id} for {args.branch}...")
        
        success = False
        for attempt in range(5): # 5 Retries with random delays
            try:
                profile = random.choice(PROFILES)
                proxy = random.choice(PROXIES) if PROXIES else None
                
                payload = {
                    "query": QUERY,
                    "variables": {
                        "categoryId": cat_id,
                        "globalEntityId": "FP_PK",
                        "isDarkstore": True,
                        "locale": "en_PK",
                        "vendorID": VENDOR_ID
                    }
                }

                resp = requests.post(
                    "https://pk.fd-api.com/api/v5/graphql",
                    json=payload,
                    headers=profile["headers"],
                    impersonate=profile["impersonate"],
                    proxies= proxy if proxy else None,
                    timeout=30
                )

                if resp.status_code == 200:
                    data = resp.json()
                    # Safe get to handle empty/null categories
                    results = data.get('data', {}).get('categoryProductList', {}).get('categoryProducts', [])
                    
                    if not results:
                        print(f"  ⚠️ No products found in this category.")
                        break

                    for wrap in results:
                        items = wrap.get('items') or []
                        for item in items:
                            pid = item.get('productID')
                            if not pid: continue
                            
                            # Flatten attributes for easy access
                            attrs = {a['key']: a['value'] for a in item.get('attributes', []) if 'key' in a}
                            
                            # Add to dictionary (automatically handles nulls via .get)
                            branch_data[pid] = {
                                "product_id": pid,
                                "sku": attrs.get("sku", "N/A"),
                                "name": item.get("name", "N/A"),
                                "price": item.get("price", 0),
                                "original_price": item.get("originalPrice") or item.get("price"),
                                "stock": item.get("stockAmount", 0),
                                "max_per_order": attrs.get("maximumSalesQuantity", "N/A"),
                                "available": item.get("isAvailable", False),
                                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                    
                    print(f"  ✅ Category complete. Current total: {len(branch_data)}")
                    success = True
                    break 
                else:
                    wait_time = random.uniform(5, 15)
                    print(f"  ❌ Status {resp.status_code}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)

            except Exception as e:
                wait_time = random.uniform(10, 20)
                print(f"  ❗ Error: {e}. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)

        # Random delay between categories to avoid rate limits
        time.sleep(random.uniform(15, 25))

    # 3. Final Save (Overwrite the file once per branch)
    if branch_data:
        # Get headers from the first item
        headers = branch_data[next(iter(branch_data))].keys()
        
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(branch_data.values())
        
        print(f"\n🎉 Finished! Overwrote {OUTPUT_FILE} with {len(branch_data)} unique items.")
    else:
        print("\n❌ No data was collected. File not updated.")

if __name__ == "__main__":
    scrape_mart()



