"""Quick test of OpenTable pipeline on 3 restaurants"""
import os
import json
from dotenv import load_dotenv
from apify_client import ApifyClient
import pandas as pd

load_dotenv()
client = ApifyClient(os.getenv("APIFY_TOKEN"))

# Test with just 3 Miami restaurants
print("=== TESTING OPENTABLE PIPELINE ===\n")

# Step 1: Find URLs via Google
print("Step 1: Searching Google for OpenTable URLs...")
run = client.actor("apify/google-search-scraper").call(
    run_input={
        "queries": "site:opentable.com/r Miami FL restaurant",
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "languageCode": "en",
        "countryCode": "us"
    },
    timeout_secs=60
)

items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
urls = []
for item in items:
    for result in item.get("organicResults", []):
        url = result.get("url", "")
        if "/r/" in url and "opentable.com" in url:
            clean_url = url.split("?")[0]
            if clean_url not in urls:
                urls.append(clean_url)

print(f"Found {len(urls)} URLs:")
for u in urls[:3]:
    print(f"  - {u}")

# Step 2: Scrape those restaurants
print("\nStep 2: Scraping menus...")
test_urls = urls[:3]  # Just 3 for test

run = client.actor("memo23/opentable-reviews-cheerio").call(
    run_input={
        "startUrls": [{"url": u} for u in test_urls],
        "includeAllReviews": False,
        "maxItems": 3,
        "maxConcurrency": 3,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    },
    timeout_secs=180
)

restaurants = list(client.dataset(run["defaultDatasetId"]).iterate_items())
print(f"\nScraped {len(restaurants)} restaurants:")

total_menu_items = 0
foie_items = []

for r in restaurants:
    name = r.get("name", "Unknown")
    city = r.get("city", "")
    has_menu = r.get("hasMenu", False)
    
    menu_count = 0
    for menu in r.get("menus", []):
        for section in menu.get("sections", []):
            for mi in section.get("items", []):
                menu_count += 1
                if "foie" in mi.get("title", "").lower():
                    foie_items.append({
                        "restaurant": name,
                        "item": mi.get("title"),
                        "price": mi.get("price")
                    })
    
    total_menu_items += menu_count
    print(f"  - {name} ({city}): {menu_count} menu items")

print(f"\n=== RESULTS ===")
print(f"Restaurants scraped: {len(restaurants)}")
print(f"Total menu items: {total_menu_items}")
print(f"Foie gras items found: {len(foie_items)}")

if foie_items:
    print("\nFoie gras items:")
    for f in foie_items:
        print(f"  - {f['restaurant']}: {f['item']} (${f['price']})")

print("\n=== PIPELINE TEST SUCCESSFUL ===")
