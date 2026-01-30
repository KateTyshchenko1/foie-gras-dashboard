"""
DoorDash Menu Scraper

Scrapes restaurant menus from DoorDash for the research study.
Searches by city and extracts restaurants with complete menu data.

Uses the Apify DoorDash scraper (aDkjUfdP8dVpC4AKU).
"""

import os
import json
import time
from typing import Optional
import pandas as pd
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load environment variables
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# Target distribution from overview.txt
TARGET_COUNTS = {
    # Florida - 107 restaurants
    ("Miami", "FL"): 43,
    ("Orlando", "FL"): 32,
    ("Tampa", "FL"): 32,
    # Pennsylvania - 70 restaurants
    ("Philadelphia", "PA"): 40,
    ("Pittsburgh", "PA"): 30,
    # Ohio - 60 restaurants
    ("Columbus", "OH"): 20,
    ("Cleveland", "OH"): 20,
    ("Cincinnati", "OH"): 20,
    # North Carolina - 60 restaurants
    ("Charlotte", "NC"): 25,
    ("Raleigh", "NC"): 25,
    ("Asheville", "NC"): 10,
    # New Jersey - 35 restaurants
    ("Jersey City", "NJ"): 15,
    ("Newark", "NJ"): 10,
    ("Princeton", "NJ"): 10,
    # Washington - 35 restaurants
    ("Seattle", "WA"): 25,
    ("Tacoma", "WA"): 5,
    ("Spokane", "WA"): 5,
    # Massachusetts - 30 restaurants
    ("Boston", "MA"): 20,
    ("Cambridge", "MA"): 5,
    ("Worcester", "MA"): 5,
    # Maryland - 30 restaurants
    ("Baltimore", "MD"): 15,
    ("Bethesda", "MD"): 10,
    ("Annapolis", "MD"): 5,
    # Oregon - 28 restaurants
    ("Portland", "OR"): 20,
    ("Eugene", "OR"): 4,
    ("Bend", "OR"): 4,
    # Colorado - 25 restaurants
    ("Denver", "CO"): 18,
    ("Boulder", "CO"): 4,
    ("Colorado Springs", "CO"): 3,
    # DC - 20 restaurants
    ("Washington", "DC"): 20,
}

# Output files
OUTPUT_RESTAURANTS = "restaurants_doordash.csv"
OUTPUT_MENUS = "menus_doordash.json"
PROGRESS_FILE = "doordash_progress.json"


def load_progress() -> dict:
    """Load progress from file to enable resume."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"completed_cities": [], "restaurants": [], "menus": []}


def save_progress(progress: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def scrape_city(client: ApifyClient, city: str, state: str, target_count: int) -> list:
    """
    Scrape restaurants from DoorDash for a given city.
    
    Args:
        client: ApifyClient instance
        city: City name
        state: State abbreviation
        target_count: Number of restaurants to fetch
    
    Returns:
        List of restaurant data with menus
    """
    location = f"{city}, {state}"
    
    # Search for restaurants in this city
    # Use generic terms to get variety
    search_terms = ["restaurant", "american", "italian", "asian", "seafood", "steakhouse"]
    
    all_restaurants = []
    seen_ids = set()
    
    for search_term in search_terms:
        if len(all_restaurants) >= target_count:
            break
            
        print(f"    Searching '{search_term}'...")
        
        try:
            run = client.actor('aDkjUfdP8dVpC4AKU').call(
                run_input={
                    'scrapeType': 'search',
                    'searchTerm': search_term,
                    'location': location,
                    'storeType': 'restaurant',
                    'includeMenu': True,
                    'maxResults': min(target_count - len(all_restaurants) + 5, 20),
                    'demoMode': False
                },
                timeout_secs=300
            )
            
            items = list(client.dataset(run['defaultDatasetId']).iterate_items())
            
            for item in items:
                store_id = item.get('storeId')
                if store_id and store_id not in seen_ids:
                    # Only include if it has menu data
                    menu = item.get('menu', [])
                    if menu and len(menu) >= 3:  # At least 3 menu items
                        seen_ids.add(store_id)
                        item['city'] = city
                        item['state'] = state
                        all_restaurants.append(item)
                        
                        if len(all_restaurants) >= target_count:
                            break
            
            time.sleep(2)  # Rate limiting between searches
            
        except Exception as e:
            print(f"    Error searching '{search_term}': {e}")
            continue
    
    return all_restaurants[:target_count]


def main():
    """Main scraping workflow."""
    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not found in .env file")
        return
    
    client = ApifyClient(APIFY_TOKEN)
    progress = load_progress()
    
    completed_cities = set(tuple(c) for c in progress.get("completed_cities", []))
    all_restaurants = progress.get("restaurants", [])
    all_menus = progress.get("menus", [])
    
    total_target = sum(TARGET_COUNTS.values())
    print(f"Target: {total_target} restaurants across {len(TARGET_COUNTS)} cities")
    print(f"Already collected: {len(all_restaurants)} restaurants")
    print("=" * 60)
    
    for (city, state), target in TARGET_COUNTS.items():
        if (city, state) in completed_cities:
            print(f"[{city}, {state}] Already completed, skipping...")
            continue
        
        print(f"\n[{city}, {state}] Target: {target} restaurants")
        
        restaurants = scrape_city(client, city, state, target)
        
        print(f"  ✓ Found {len(restaurants)} restaurants with menus")
        
        # Process and store results
        for r in restaurants:
            # Extract restaurant info
            restaurant_record = {
                "restaurant_id": r.get("storeId"),
                "name": r.get("name"),
                "city": city,
                "state": state,
                "source_url": r.get("url"),
                "cuisine_type": ", ".join(r.get("cuisineTypes", [])),
                "rating": r.get("rating"),
                "review_count": r.get("reviewCount"),
                "delivery_fee": r.get("deliveryFee"),
                "date_accessed": r.get("scrapedAt", ""),
            }
            all_restaurants.append(restaurant_record)
            
            # Extract menu items
            for menu_item in r.get("menu", []):
                menu_record = {
                    "menu_item_id": f"{r.get('storeId')}_{menu_item.get('name', '').replace(' ', '_')[:30]}",
                    "restaurant_id": r.get("storeId"),
                    "title": menu_item.get("name"),
                    "description": menu_item.get("description", ""),
                    "section": menu_item.get("category", ""),
                    "price": menu_item.get("price"),
                    "price_string": menu_item.get("priceString"),
                    "currency": "USD",
                    "date_accessed": r.get("scrapedAt", ""),
                }
                all_menus.append(menu_record)
        
        # Mark city as complete and save progress
        completed_cities.add((city, state))
        progress["completed_cities"] = [list(c) for c in completed_cities]
        progress["restaurants"] = all_restaurants
        progress["menus"] = all_menus
        save_progress(progress)
        
        # Save intermediate outputs
        pd.DataFrame(all_restaurants).to_csv(OUTPUT_RESTAURANTS, index=False)
        with open(OUTPUT_MENUS, 'w') as f:
            json.dump(all_menus, f, indent=2)
        
        print(f"  [Saved: {len(all_restaurants)} restaurants, {len(all_menus)} menu items]")
    
    # Final summary
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total restaurants: {len(all_restaurants)}")
    print(f"Total menu items: {len(all_menus)}")
    print(f"Output files:")
    print(f"  - {OUTPUT_RESTAURANTS}")
    print(f"  - {OUTPUT_MENUS}")
    
    # City breakdown
    print("\nCity breakdown:")
    df = pd.DataFrame(all_restaurants)
    if not df.empty:
        summary = df.groupby(['city', 'state']).size().reset_index(name='count')
        for _, row in summary.iterrows():
            target = TARGET_COUNTS.get((row['city'], row['state']), 0)
            diff = row['count'] - target
            status = "✓" if diff >= 0 else "⚠"
            print(f"  {status} {row['city']}, {row['state']}: {row['count']}/{target}")


if __name__ == "__main__":
    main()
