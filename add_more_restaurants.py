"""Add more restaurants for cities with gaps"""
import os
import json
import pandas as pd
from dotenv import load_dotenv
from apify_client import ApifyClient

load_dotenv()
client = ApifyClient(os.getenv("APIFY_TOKEN"))

# Cities that need more restaurants
gaps = {
    ("Miami", "FL"): 10,        # Need 7, get 10 extra
    ("Orlando", "FL"): 3,       # Need 1, get 3 extra
    ("Philadelphia", "PA"): 5,  # Need 2, get 5 extra
    ("Cleveland", "OH"): 3,     # Need 1, get 3 extra
    ("Cincinnati", "OH"): 3,    # Need 1, get 3 extra
    ("Bethesda", "MD"): 5,      # Need 2, get 5 extra
    ("Portland", "OR"): 15,     # Need 9, get 15 extra
    ("Eugene", "OR"): 6,        # Need 3, get 6 extra
    ("Washington", "DC"): 5,    # Need 1, get 5 extra
}

# Load existing data
existing_restaurants = pd.read_csv("restaurants_opentable.csv")
existing_menu_items = pd.read_csv("menu_items_opentable.csv")
existing_urls = set(existing_restaurants["source_url"].dropna().tolist())

print(f"Starting with {len(existing_restaurants)} restaurants, {len(existing_menu_items)} menu items")
print(f"Existing URLs tracked: {len(existing_urls)}")
print("=" * 60)

all_new_restaurants = []
all_new_menu_items = []

for (city, state), target in gaps.items():
    print(f"\n[{city}, {state}] Searching for {target} more restaurants...")
    
    # Search for URLs
    query = f"site:opentable.com/r {city} {state} restaurant fine dining"
    pages = (target // 10) + 2
    
    run = client.actor("apify/google-search-scraper").call(
        run_input={
            "queries": query,
            "maxPagesPerQuery": pages,
            "resultsPerPage": 10,
            "languageCode": "en",
            "countryCode": "us"
        },
        timeout_secs=120
    )
    
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    
    # Extract unique NEW URLs
    new_urls = []
    for item in items:
        for result in item.get("organicResults", []):
            url = result.get("url", "").split("?")[0]
            if "/r/" in url and "opentable.com" in url:
                if url not in existing_urls and url not in new_urls:
                    new_urls.append(url)
    
    new_urls = new_urls[:target]
    print(f"  Found {len(new_urls)} NEW URLs")
    
    if not new_urls:
        continue
    
    # Scrape the new restaurants
    print(f"  Scraping menus...")
    run = client.actor("memo23/opentable-reviews-cheerio").call(
        run_input={
            "startUrls": [{"url": u} for u in new_urls],
            "includeAllReviews": False,
            "maxItems": len(new_urls),
            "maxConcurrency": 5,
            "proxy": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        },
        timeout_secs=300
    )
    
    restaurants = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    print(f"  Scraped {len(restaurants)} restaurants")
    
    for item in restaurants:
        restaurant_id = item.get("id", "")
        
        restaurant = {
            "restaurant_id": restaurant_id,
            "name": item.get("name", ""),
            "city": item.get("city", city),
            "state": item.get("state", state),
            "cuisine_type": ", ".join([c.get("name", "") for c in item.get("cuisines", [])]),
            "price_band": item.get("priceBand", {}).get("label", ""),
            "rating": item.get("reviews", {}).get("overallRating", 0),
            "review_count": item.get("reviews", {}).get("count", 0),
            "address": item.get("line1", ""),
            "postal_code": item.get("postalCode", ""),
            "source_url": item.get("nonNaturalUrl", ""),
            "has_menu": item.get("hasMenu", False),
            "date_accessed": pd.Timestamp.now().isoformat(),
        }
        all_new_restaurants.append(restaurant)
        existing_urls.add(restaurant.get("source_url", ""))
        
        # Extract menu items
        for menu in item.get("menus", []):
            menu_title = menu.get("title", "Unknown Menu")
            currency = menu.get("currency", "USD")
            
            for section in menu.get("sections", []):
                section_title = section.get("title", "")
                
                for mi in section.get("items", []):
                    menu_item = {
                        "menu_item_id": f"{restaurant_id}_{mi.get('title', '')[:30].replace(' ', '_')}",
                        "restaurant_id": restaurant_id,
                        "title": mi.get("title", ""),
                        "description": mi.get("desc", ""),
                        "section": section_title,
                        "menu_type": menu_title,
                        "price": mi.get("price", ""),
                        "currency": currency,
                        "date_accessed": pd.Timestamp.now().isoformat(),
                    }
                    all_new_menu_items.append(menu_item)
    
    # Check for foie gras
    foie_count = sum(1 for mi in all_new_menu_items if "foie" in mi.get("title", "").lower())
    if foie_count > 0:
        print(f"  🦆 Found {foie_count} foie gras items so far!")

# Append to existing data
if all_new_restaurants:
    new_restaurants_df = pd.DataFrame(all_new_restaurants)
    combined_restaurants = pd.concat([existing_restaurants, new_restaurants_df], ignore_index=True)
    combined_restaurants.to_csv("restaurants_opentable.csv", index=False)
    
    new_menu_items_df = pd.DataFrame(all_new_menu_items)
    combined_menu_items = pd.concat([existing_menu_items, new_menu_items_df], ignore_index=True)
    combined_menu_items.to_csv("menu_items_opentable.csv", index=False)
    
    print("\n" + "=" * 60)
    print("ADDITIONAL SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Added: {len(all_new_restaurants)} restaurants, {len(all_new_menu_items)} menu items")
    print(f"New Total: {len(combined_restaurants)} restaurants, {len(combined_menu_items)} menu items")
    
    # Count foie gras
    foie_items = [mi for mi in all_new_menu_items if "foie" in mi.get("title", "").lower()]
    print(f"🦆 Foie gras in new items: {len(foie_items)}")
else:
    print("\nNo new restaurants added")
