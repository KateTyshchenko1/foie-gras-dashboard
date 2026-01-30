"""
OpenTable Menu Scraper Pipeline

Step 1: Use Google Search to find OpenTable restaurant URLs for each city
Step 2: Use OpenTable actor to scrape full menus with prices

Target: 500 restaurants across 30 cities as per overview.txt
"""

import os
import json
import time
from typing import List, Dict
import pandas as pd
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load environment variables
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# Target distribution from overview.txt (500 total restaurants)
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
OUTPUT_RESTAURANTS = "restaurants_opentable.csv"
OUTPUT_MENU_ITEMS = "menu_items_opentable.csv"
PROGRESS_FILE = "opentable_progress.json"


def load_progress() -> dict:
    """Load progress from file to enable resume."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"completed_cities": [], "restaurant_urls": {}, "restaurants": [], "menu_items": []}


def save_progress(progress: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def search_opentable_urls(client: ApifyClient, city: str, state: str, target_count: int) -> List[str]:
    """
    Use Google Search to find OpenTable restaurant URLs for a city.
    
    Args:
        client: ApifyClient instance
        city: City name
        state: State abbreviation  
        target_count: Number of restaurants to find
    
    Returns:
        List of OpenTable restaurant URLs
    """
    # Calculate number of Google pages needed (10 results per page)
    pages_needed = (target_count // 10) + 2  # Extra pages for duplicates
    
    print(f"    Searching Google for OpenTable URLs...")
    
    query = f"site:opentable.com/r {city} {state} restaurant"
    
    try:
        run = client.actor("apify/google-search-scraper").call(
            run_input={
                "queries": query,
                "maxPagesPerQuery": pages_needed,
                "resultsPerPage": 10,
                "languageCode": "en",
                "countryCode": "us"
            },
            timeout_secs=120
        )
        
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        # Extract unique OpenTable URLs
        urls = []
        seen = set()
        for item in items:
            for result in item.get("organicResults", []):
                url = result.get("url", "")
                if "/r/" in url and "opentable.com" in url and url not in seen:
                    # Clean URL (remove query params)
                    clean_url = url.split("?")[0]
                    if clean_url not in seen:
                        seen.add(clean_url)
                        urls.append(clean_url)
        
        print(f"    Found {len(urls)} unique OpenTable URLs")
        return urls[:target_count]  # Return only what we need
        
    except Exception as e:
        print(f"    Error searching: {e}")
        return []


def scrape_restaurant_menus(client: ApifyClient, urls: List[str], city: str, state: str) -> tuple:
    """
    Scrape restaurant details and menus from OpenTable URLs.
    
    Args:
        client: ApifyClient instance
        urls: List of OpenTable restaurant URLs
        city: City name (for reference)
        state: State abbreviation
    
    Returns:
        Tuple of (restaurants list, menu_items list)
    """
    if not urls:
        return [], []
    
    print(f"    Scraping {len(urls)} restaurants...")
    
    try:
        run = client.actor("memo23/opentable-reviews-cheerio").call(
            run_input={
                "startUrls": [{"url": url} for url in urls],
                "includeAllReviews": False,
                "maxItems": len(urls),
                "maxConcurrency": 5,  # Parallel scraping
                "minConcurrency": 1,
                "maxRequestRetries": 10,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                }
            },
            timeout_secs=600  # 10 minutes for batch
        )
        
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        restaurants = []
        menu_items = []
        
        for item in items:
            restaurant_id = item.get("id", "")
            
            # Extract restaurant info
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
            restaurants.append(restaurant)
            
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
                        menu_items.append(menu_item)
        
        print(f"    Scraped {len(restaurants)} restaurants with {len(menu_items)} menu items")
        return restaurants, menu_items
        
    except Exception as e:
        print(f"    Error scraping: {e}")
        return [], []


def main():
    """Main scraping workflow."""
    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not found in .env file")
        return
    
    client = ApifyClient(APIFY_TOKEN)
    progress = load_progress()
    
    completed_cities = set(tuple(c) for c in progress.get("completed_cities", []))
    all_restaurants = progress.get("restaurants", [])
    all_menu_items = progress.get("menu_items", [])
    
    total_target = sum(TARGET_COUNTS.values())
    print(f"Target: {total_target} restaurants across {len(TARGET_COUNTS)} cities")
    print(f"Already collected: {len(all_restaurants)} restaurants, {len(all_menu_items)} menu items")
    print("=" * 70)
    
    for (city, state), target in TARGET_COUNTS.items():
        if (city, state) in completed_cities:
            print(f"\n[{city}, {state}] Already completed, skipping...")
            continue
        
        print(f"\n[{city}, {state}] Target: {target} restaurants")
        
        # Step 1: Find OpenTable URLs via Google Search
        urls = search_opentable_urls(client, city, state, target)
        
        if not urls:
            print(f"  ⚠ No URLs found, skipping city")
            continue
        
        # Step 2: Scrape restaurant menus
        restaurants, menu_items = scrape_restaurant_menus(client, urls, city, state)
        
        if restaurants:
            all_restaurants.extend(restaurants)
            all_menu_items.extend(menu_items)
            
            # Check for foie gras
            foie_count = sum(1 for mi in menu_items if "foie" in mi.get("title", "").lower())
            if foie_count > 0:
                print(f"    🦆 Found {foie_count} foie gras items!")
            
            # Mark city as complete and save progress
            completed_cities.add((city, state))
            progress["completed_cities"] = [list(c) for c in completed_cities]
            progress["restaurants"] = all_restaurants
            progress["menu_items"] = all_menu_items
            save_progress(progress)
            
            # Save CSV outputs
            pd.DataFrame(all_restaurants).to_csv(OUTPUT_RESTAURANTS, index=False)
            pd.DataFrame(all_menu_items).to_csv(OUTPUT_MENU_ITEMS, index=False)
            
            print(f"  ✓ Saved: {len(all_restaurants)} restaurants, {len(all_menu_items)} menu items")
        
        # Rate limiting between cities
        time.sleep(2)
    
    # Final summary
    print("\n" + "=" * 70)
    print("SCRAPING COMPLETE")
    print("=" * 70)
    print(f"Total restaurants: {len(all_restaurants)}")
    print(f"Total menu items: {len(all_menu_items)}")
    
    # Check for foie gras across all menu items
    foie_items = [mi for mi in all_menu_items if "foie" in mi.get("title", "").lower()]
    print(f"🦆 Foie gras items found: {len(foie_items)}")
    
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
    
    print(f"\nOutput files:")
    print(f"  - {OUTPUT_RESTAURANTS}")
    print(f"  - {OUTPUT_MENU_ITEMS}")


if __name__ == "__main__":
    main()
