"""Test OpenTable actor"""
import os
import json
from dotenv import load_dotenv
from apify_client import ApifyClient

load_dotenv()
client = ApifyClient(os.getenv("APIFY_TOKEN"))

print("Testing OpenTable menu actor with sample restaurant...")

run = client.actor("memo23/opentable-reviews-cheerio").call(
    run_input={
        "startUrls": [
            {"url": "https://www.opentable.com/r/jays-fort-lauderdale"}
        ],
        "includeAllReviews": False,
        "maxItems": 1,
        "maxConcurrency": 1,
        "maxRequestRetries": 10,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    },
    timeout_secs=180
)

items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
print(f"Items returned: {len(items)}")

if items:
    item = items[0]
    name = item.get("name")
    city = item.get("city")
    state = item.get("state")
    has_menu = item.get("hasMenu")
    
    print(f"\nRestaurant: {name}")
    print(f"City: {city}, {state}")
    print(f"Has Menu: {has_menu}")
    
    menus = item.get("menus", [])
    if menus:
        total_items = 0
        for menu in menus:
            for section in menu.get("sections", []):
                total_items += len(section.get("items", []))
        print(f"Total menu items: {total_items}")
        
        # Check for foie gras
        for menu in menus:
            for section in menu.get("sections", []):
                for mi in section.get("items", []):
                    title = mi.get("title", "")
                    if "foie" in title.lower():
                        price = mi.get("price", "N/A")
                        print(f"FOIE GRAS FOUND: {title} - ${price}")
else:
    print("No results")
