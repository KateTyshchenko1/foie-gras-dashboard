"""
Restaurant Menu Scraper using Yelp via Apify

This script bridges the gap between the restaurant dataset (with name/city/state)
and the Apify Yelp scraper (which needs a Yelp URL).

Workflow:
1. Read restaurant_sample_500.csv
2. For each restaurant, search Google with googlesearch-python to find its Yelp URL
3. Call Apify "endspec/yelp-full-scraper" to extract menu data
4. Save results to final_menus.json and errors to errors.csv
"""

import os
import json
import time
import csv
import re
from typing import Optional
import pandas as pd
from dotenv import load_dotenv
from apify_client import ApifyClient
from googlesearch import search

# Load environment variables
load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# Configuration
INPUT_CSV = "restaurant_sample_500.csv"
OUTPUT_JSON = "final_menus.json"
ERRORS_CSV = "errors.csv"
PROGRESS_FILE = "scrape_progress.json"

# Rate limiting (to avoid Google blocking)
GOOGLE_SEARCH_DELAY = 2.0  # seconds between Google searches
APIFY_CALL_DELAY = 1.0     # seconds between Apify calls


def find_yelp_url_via_google(restaurant_name: str, city: str, state: str) -> Optional[str]:
    """
    Search Google to find the Yelp URL for a restaurant.
    """
    # Try multiple query formats
    queries = [
        f'{restaurant_name} {city} {state} site:yelp.com',
        f'"{restaurant_name}" {city} site:yelp.com/biz',
        f'{restaurant_name} restaurant {city} yelp'
    ]
    
    for query in queries:
        try:
            results = list(search(query, num_results=5, sleep_interval=1))
            
            for url in results:
                if "yelp.com/biz/" in url:
                    clean_url = url.split("?")[0]
                    return clean_url
            time.sleep(1)  # Rate limiting between queries
            
        except Exception as e:
            print(f"    Google search error: {e}")
            time.sleep(2)
            continue
    
    return None


def find_yelp_url_via_apify(client: ApifyClient, restaurant_name: str, city: str, state: str) -> Optional[str]:
    """
    Use Apify Yelp scraper's search functionality to find a business.
    """
    input_params = {
        "searchType": "search",
        "location": f"{city}, {state}",
        "searchTerm": restaurant_name,
        "limit": 5,
        "businessDetailsType": "basic",
        "includeReviews": False,
        "includeMenu": False,
        "includePopularDishes": False,
        "maxRequests": 10
    }
    
    try:
        run = client.actor("endspec/yelp-full-scraper").call(run_input=input_params)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            # Find best match by name similarity
            name_lower = restaurant_name.lower()
            for item in items:
                item_name = item.get("name", "").lower()
                if name_lower in item_name or item_name in name_lower:
                    return item.get("url")
            # If no exact match, return first result
            return items[0].get("url")
        
        return None
        
    except Exception as e:
        print(f"    Apify search error: {e}")
        return None


def find_yelp_url(client: ApifyClient, restaurant_name: str, city: str, state: str) -> Optional[str]:
    """
    Find Yelp URL using multiple strategies.
    
    Args:
        client: ApifyClient instance
        restaurant_name: Name of the restaurant
        city: City name
        state: State abbreviation
    
    Returns:
        Yelp URL if found, None otherwise
    """
    # Strategy 1: Try Google search first (faster, free)
    print("    Trying Google search...")
    url = find_yelp_url_via_google(restaurant_name, city, state)
    if url:
        return url
    
    # Strategy 2: Fall back to Apify Yelp search (more reliable, uses credits)
    print("    Falling back to Apify Yelp search...")
    url = find_yelp_url_via_apify(client, restaurant_name, city, state)
    return url


def scrape_menu_from_yelp(client: ApifyClient, yelp_url: str) -> Optional[dict]:
    """
    Call Apify Yelp scraper to extract menu data.
    
    Args:
        client: ApifyClient instance
        yelp_url: Yelp business URL
    
    Returns:
        Menu data dictionary if successful, None otherwise
    """
    input_params = {
        "searchType": "full_profile",
        "businessUrl": yelp_url,
        "includeMenu": True,
        "includeReviews": False,
        "includePopularDishes": False,
        "maxRequests": 5
    }
    
    try:
        # Run the Apify actor
        run = client.actor("endspec/yelp-full-scraper").call(run_input=input_params)
        
        # Fetch results from dataset
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        
        if items:
            return items[0]  # Return first result
        return None
        
    except Exception as e:
        print(f"    Apify error: {e}")
        return None


def load_progress() -> dict:
    """Load progress from file to enable resume."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"processed": [], "results": []}


def save_progress(progress: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def save_errors(errors: list):
    """Save error log to CSV."""
    with open(ERRORS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["restaurant_id", "name", "city", "state", "error_type", "error_message"])
        writer.writeheader()
        writer.writerows(errors)


def main():
    """Main scraping workflow."""
    if not APIFY_TOKEN:
        print("ERROR: APIFY_TOKEN not found in .env file")
        return
    
    # Initialize Apify client
    client = ApifyClient(APIFY_TOKEN)
    
    # Load restaurant data
    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} restaurants from {INPUT_CSV}")
    
    # Load progress (for resume capability)
    progress = load_progress()
    processed_ids = set(progress["processed"])
    results = progress["results"]
    errors = []
    
    print(f"Resuming from {len(processed_ids)} already processed restaurants")
    print("=" * 60)
    
    for idx, row in df.iterrows():
        restaurant_id = row["restaurant_id"]
        name = row["name"]
        city = row["city"]
        state = row["state"]
        
        # Skip already processed
        if restaurant_id in processed_ids:
            continue
        
        print(f"\n[{idx + 1}/{len(df)}] {name} ({city}, {state})")
        
        # Step 1: Find Yelp URL
        print("  Searching for Yelp URL...")
        yelp_url = find_yelp_url(client, name, city, state)
        time.sleep(GOOGLE_SEARCH_DELAY)
        
        if not yelp_url:
            print("  ❌ No Yelp URL found")
            errors.append({
                "restaurant_id": restaurant_id,
                "name": name,
                "city": city,
                "state": state,
                "error_type": "NO_YELP_URL",
                "error_message": "Could not find Yelp business page via Google search"
            })
            processed_ids.add(restaurant_id)
            progress["processed"] = list(processed_ids)
            save_progress(progress)
            save_errors(errors)
            continue
        
        print(f"  ✓ Found: {yelp_url}")
        
        # Step 2: Scrape menu from Yelp via Apify
        print("  Scraping menu via Apify...")
        menu_data = scrape_menu_from_yelp(client, yelp_url)
        time.sleep(APIFY_CALL_DELAY)
        
        if not menu_data:
            print("  ❌ Failed to scrape menu")
            errors.append({
                "restaurant_id": restaurant_id,
                "name": name,
                "city": city,
                "state": state,
                "error_type": "SCRAPE_FAILED",
                "error_message": f"Apify scraper failed for {yelp_url}"
            })
            processed_ids.add(restaurant_id)
            progress["processed"] = list(processed_ids)
            save_progress(progress)
            save_errors(errors)
            continue
        
        # Check if menu data exists
        menu = menu_data.get("menu", [])
        if not menu:
            print("  ⚠ No menu data available on Yelp")
            errors.append({
                "restaurant_id": restaurant_id,
                "name": name,
                "city": city,
                "state": state,
                "error_type": "NO_MENU",
                "error_message": "Restaurant has no menu listed on Yelp"
            })
        else:
            print(f"  ✓ Found {len(menu)} menu sections")
        
        # Store result
        result = {
            "restaurant_id": restaurant_id,
            "name": name,
            "city": city,
            "state": state,
            "yelp_url": yelp_url,
            "yelp_data": menu_data
        }
        results.append(result)
        
        # Update progress
        processed_ids.add(restaurant_id)
        progress["processed"] = list(processed_ids)
        progress["results"] = results
        save_progress(progress)
        save_errors(errors)
        
        # Save final output periodically
        if len(results) % 10 == 0:
            with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)
            print(f"  [Saved {len(results)} results to {OUTPUT_JSON}]")
    
    # Final save
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    save_errors(errors)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total restaurants:     {len(df)}")
    print(f"Successfully scraped:  {len(results)}")
    print(f"Errors:                {len(errors)}")
    print(f"Results saved to:      {OUTPUT_JSON}")
    print(f"Errors saved to:       {ERRORS_CSV}")
    
    # Breakdown of errors
    if errors:
        print("\nError breakdown:")
        error_types = {}
        for err in errors:
            error_types[err["error_type"]] = error_types.get(err["error_type"], 0) + 1
        for error_type, count in error_types.items():
            print(f"  {error_type}: {count}")


if __name__ == "__main__":
    main()
