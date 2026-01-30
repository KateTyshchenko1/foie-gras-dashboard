"""
Restaurant Dataset Generator using Google Places API (New)

Generates a stratified random sample of 500 U.S. restaurants across
specified cities for a foie gras menu study.
"""

import os
import random
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("Maps_API_KEY")

# Google Places API (New) endpoint
PLACES_API_URL = "https://places.googleapis.com/v1/places:searchText"

# Target counts per city (500 total)
TARGET_COUNTS = {
    "Miami, FL": 43, "Orlando, FL": 32, "Tampa, FL": 32,
    "Philadelphia, PA": 40, "Pittsburgh, PA": 30,
    "Columbus, OH": 20, "Cleveland, OH": 20, "Cincinnati, OH": 20,
    "Charlotte, NC": 25, "Raleigh, NC": 25, "Asheville, NC": 10,
    "Jersey City, NJ": 15, "Newark, NJ": 10, "Princeton, NJ": 10,
    "Seattle, WA": 25, "Tacoma, WA": 5, "Spokane, WA": 5,
    "Boston, MA": 20, "Cambridge, MA": 5, "Worcester, MA": 5,
    "Baltimore, MD": 15, "Bethesda, MD": 10, "Annapolis, MD": 5,
    "Portland, OR": 20, "Eugene, OR": 4, "Bend, OR": 4,
    "Denver, CO": 18, "Boulder, CO": 4, "Colorado Springs, CO": 3,
    "Washington, DC": 20
}

# Primary types to exclude (not actual restaurants)
EXCLUDED_TYPES = {
    "delivery_service", "food_delivery", "meal_delivery", 
    "grocery_store", "supermarket", "convenience_store",
    "gas_station", "catering_service"
}

# Common chain names to optionally filter out
CHAIN_KEYWORDS = {
    "mcdonald", "burger king", "wendy", "taco bell", "subway",
    "chick-fil-a", "chipotle", "panda express", "kfc", "domino",
    "pizza hut", "papa john", "dunkin", "starbucks", "ihop",
    "denny", "applebee", "chili", "olive garden", "red lobster",
    "outback", "buffalo wild wings", "five guys", "in-n-out",
    "shake shack", "panera", "wingstop", "popeyes", "sonic",
    "arby", "jack in the box", "carl's jr", "hardee", "whataburger"
}


def is_chain_restaurant(name: str) -> bool:
    """Check if restaurant name matches known chain keywords."""
    name_lower = name.lower()
    return any(chain in name_lower for chain in CHAIN_KEYWORDS)


def fetch_restaurants_for_city(city: str, max_pages: int = 3) -> list[dict]:
    """
    Fetch restaurants for a given city using Google Places API.
    
    Args:
        city: City name with state (e.g., "Miami, FL")
        max_pages: Maximum number of pages to fetch (20 results per page)
    
    Returns:
        List of restaurant dictionaries
    """
    all_restaurants = []
    query = f"restaurant in {city}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.websiteUri,places.priceLevel,places.rating,places.userRatingCount,places.primaryType,places.googleMapsUri,nextPageToken"
    }
    
    payload = {
        "textQuery": query,
        "languageCode": "en",
        "regionCode": "US",
        "pageSize": 20
    }
    
    page_count = 0
    next_page_token = None
    
    while page_count < max_pages:
        if next_page_token:
            payload["pageToken"] = next_page_token
        
        try:
            response = requests.post(PLACES_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            places = data.get("places", [])
            if not places:
                print(f"  No more results for {city} (page {page_count + 1})")
                break
            
            for place in places:
                # Extract primary type and check exclusions
                primary_type = place.get("primaryType", "")
                if primary_type in EXCLUDED_TYPES:
                    continue
                
                # Extract restaurant data
                restaurant = {
                    "restaurant_id": place.get("id", ""),
                    "name": place.get("displayName", {}).get("text", ""),
                    "address": place.get("formattedAddress", ""),
                    "city": city.split(",")[0].strip(),
                    "state": city.split(",")[1].strip() if "," in city else "",
                    "website_url": place.get("websiteUri", ""),
                    "price_level": place.get("priceLevel", ""),
                    "rating": place.get("rating", ""),
                    "review_count": place.get("userRatingCount", ""),
                    "primary_type": primary_type,
                    "Maps_link": place.get("googleMapsUri", "")
                }
                all_restaurants.append(restaurant)
            
            print(f"  Page {page_count + 1}: Fetched {len(places)} places ({len(all_restaurants)} total)")
            
            # Check for next page
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            
            page_count += 1
            
            # Small delay between pages (API best practice)
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {page_count + 1} for {city}: {e}")
            # Print response body for debugging
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    print(f"  API Error Details: {error_detail}")
                except:
                    print(f"  Response text: {e.response.text[:500]}")
            break
    
    return all_restaurants


def random_sample_restaurants(
    restaurants: list[dict], 
    target_count: int, 
    exclude_chains: bool = True
) -> list[dict]:
    """
    Randomly sample restaurants from the fetched list.
    
    Args:
        restaurants: List of restaurant dictionaries
        target_count: Number of restaurants to sample
        exclude_chains: Whether to filter out chain restaurants
    
    Returns:
        Randomly sampled list of restaurants
    """
    # Optionally filter out chains
    if exclude_chains:
        filtered = [r for r in restaurants if not is_chain_restaurant(r["name"])]
        # If filtering removes too many, fall back to including chains
        if len(filtered) < target_count:
            print(f"    Warning: Only {len(filtered)} non-chain restaurants found, including some chains")
            filtered = restaurants
    else:
        filtered = restaurants
    
    # Random sample (or take all if not enough)
    if len(filtered) <= target_count:
        return filtered
    
    return random.sample(filtered, target_count)


def generate_dataset(exclude_chains: bool = True, pages_per_city: int = 3) -> pd.DataFrame:
    """
    Generate the full restaurant dataset.
    
    Args:
        exclude_chains: Whether to exclude chain restaurants
        pages_per_city: Number of API pages to fetch per city (20 results each)
    
    Returns:
        DataFrame with sampled restaurants
    """
    all_sampled = []
    results_summary = {}
    
    print("=" * 60)
    print("RESTAURANT DATASET GENERATOR")
    print(f"Target: {sum(TARGET_COUNTS.values())} restaurants across {len(TARGET_COUNTS)} cities")
    print(f"Exclude chains: {exclude_chains}")
    print("=" * 60)
    print()
    
    for city, target in TARGET_COUNTS.items():
        print(f"\n[{city}] Target: {target} restaurants")
        
        # Fetch restaurants (oversampling)
        restaurants = fetch_restaurants_for_city(city, max_pages=pages_per_city)
        print(f"  Fetched {len(restaurants)} restaurants before filtering")
        
        # Random sample to target
        sampled = random_sample_restaurants(restaurants, target, exclude_chains)
        print(f"  Sampled {len(sampled)} restaurants")
        
        all_sampled.extend(sampled)
        results_summary[city] = {"target": target, "actual": len(sampled)}
        
        # Small delay between cities to avoid rate limiting
        time.sleep(0.3)
    
    # Create DataFrame
    df = pd.DataFrame(all_sampled)
    
    # Reorder columns for output
    column_order = [
        "restaurant_id", "name", "address", "city", "state", 
        "website_url", "price_level", "rating", "review_count", "Maps_link"
    ]
    # Only include columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]
    
    return df, results_summary


def print_summary(results: dict):
    """Print a summary of target vs actual counts."""
    print("\n" + "=" * 60)
    print("SAMPLING SUMMARY")
    print("=" * 60)
    print(f"{'City':<25} {'Target':>8} {'Actual':>8} {'Diff':>8}")
    print("-" * 60)
    
    total_target = 0
    total_actual = 0
    
    for city, counts in results.items():
        target = counts["target"]
        actual = counts["actual"]
        diff = actual - target
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        
        print(f"{city:<25} {target:>8} {actual:>8} {diff_str:>8}")
        
        total_target += target
        total_actual += actual
    
    print("-" * 60)
    diff = total_actual - total_target
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"{'TOTAL':<25} {total_target:>8} {total_actual:>8} {diff_str:>8}")
    print("=" * 60)


def main():
    """Main entry point."""
    if not API_KEY:
        print("ERROR: Maps_API_KEY not found in .env file")
        return
    
    print(f"Using API Key: {API_KEY[:10]}...{API_KEY[-4:]}")
    print()
    
    # Generate dataset
    df, results = generate_dataset(exclude_chains=True, pages_per_city=3)
    
    # Save to CSV
    output_file = "restaurant_sample_500.csv"
    df.to_csv(output_file, index=False)
    print(f"\nDataset saved to: {output_file}")
    print(f"Total restaurants: {len(df)}")
    
    # Print summary
    print_summary(results)
    
    # Show sample of data
    print("\n" + "=" * 60)
    print("SAMPLE DATA (first 5 rows)")
    print("=" * 60)
    print(df.head().to_string())


if __name__ == "__main__":
    main()
