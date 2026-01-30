"""
Foie Gras Dashboard - Data Processor
Pre-compute aggregated statistics for the dashboard (no raw data exposed)
"""
import pandas as pd
import json
from pathlib import Path

# City coordinates for mapping
CITY_COORDS = {
    # Florida
    "Miami": (25.7617, -80.1918),
    "Orlando": (28.5383, -81.3792),
    "Tampa": (27.9506, -82.4572),
    # Pennsylvania
    "Philadelphia": (39.9526, -75.1652),
    "Pittsburgh": (40.4406, -79.9959),
    # Ohio
    "Columbus": (39.9612, -82.9988),
    "Cleveland": (41.4993, -81.6944),
    "Cincinnati": (39.1031, -84.5120),
    # North Carolina
    "Charlotte": (35.2271, -80.8431),
    "Raleigh": (35.7796, -78.6382),
    "Asheville": (35.5951, -82.5515),
    # New Jersey
    "Jersey City": (40.7178, -74.0431),
    "Newark": (40.7357, -74.1724),
    "Princeton": (40.3573, -74.6672),
    # Washington State
    "Seattle": (47.6062, -122.3321),
    "Tacoma": (47.2529, -122.4443),
    "Spokane": (47.6588, -117.4260),
    # Massachusetts
    "Boston": (42.3601, -71.0589),
    "Cambridge": (42.3736, -71.1097),
    "Worcester": (42.2626, -71.8023),
    # Maryland
    "Baltimore": (39.2904, -76.6122),
    "Bethesda": (38.9847, -77.0947),
    "Annapolis": (38.9784, -76.4922),
    # Oregon
    "Portland": (45.5152, -122.6784),
    "Eugene": (44.0521, -123.0868),
    "Bend": (44.0582, -121.3153),
    # Colorado
    "Denver": (39.7392, -104.9903),
    "Boulder": (40.0150, -105.2705),
    "Colorado Springs": (38.8339, -104.8214),
    # DC
    "Washington": (38.9072, -77.0369),
}

STATE_NAMES = {
    "FL": "Florida",
    "PA": "Pennsylvania", 
    "OH": "Ohio",
    "NC": "North Carolina",
    "NJ": "New Jersey",
    "WA": "Washington",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "OR": "Oregon",
    "CO": "Colorado",
    "DC": "District of Columbia",
}


def process_data():
    """Process restaurant and menu data into dashboard-ready aggregates."""
    
    base_path = Path(__file__).parent.parent
    
    # Load data
    restaurants_df = pd.read_csv(base_path / "restaurants_opentable.csv")
    menu_df = pd.read_csv(base_path / "menu_items_opentable.csv")
    
    # Find foie gras items
    foie_mask = menu_df['title'].str.lower().str.contains('foie', na=False)
    foie_items = menu_df[foie_mask].copy()
    
    # Get restaurant IDs that have foie gras
    foie_restaurant_ids = set(foie_items['restaurant_id'].unique())
    
    # === AGGREGATE STATS ===
    stats = {
        "total_restaurants": len(restaurants_df),
        "total_menu_items": len(menu_df),
        "total_foie_gras_items": len(foie_items),
        "restaurants_with_foie": len(foie_restaurant_ids),
    }
    
    # Foie gras price stats (only items with valid prices)
    foie_prices = pd.to_numeric(foie_items['price'], errors='coerce').dropna()
    if len(foie_prices) > 0:
        stats["avg_foie_price"] = round(foie_prices.mean(), 2)
        stats["min_foie_price"] = round(foie_prices.min(), 2)
        stats["max_foie_price"] = round(foie_prices.max(), 2)
    else:
        stats["avg_foie_price"] = None
        stats["min_foie_price"] = None
        stats["max_foie_price"] = None
    
    # === FOIE GRAS PRICE PREMIUM ANALYSIS ===
    # Compare foie gras price to median food price (excluding wines/drinks which skew average)
    # Filter out likely non-food items (wines, spirits, bottles)
    drink_keywords = ['wine', 'champagne', 'whisky', 'whiskey', 'bourbon', 'cognac', 
                      'scotch', 'vodka', 'gin', 'rum', 'tequila', 'bottle', 'glass',
                      'beer', 'cocktail', 'martini', 'sangria', 'vermouth', 'port']
    
    food_mask = ~menu_df['title'].str.lower().str.contains('|'.join(drink_keywords), na=False)
    food_prices = pd.to_numeric(menu_df[food_mask]['price'], errors='coerce').dropna()
    # Also filter out extreme outliers (likely errors or special items) - cap at $500
    food_prices = food_prices[(food_prices > 0) & (food_prices <= 500)]
    
    if len(food_prices) > 0 and len(foie_prices) > 0:
        median_food_price = food_prices.median()
        median_foie_price = foie_prices.median()
        price_premium_pct = ((median_foie_price - median_food_price) / median_food_price) * 100
        stats["median_food_price"] = round(median_food_price, 2)
        stats["median_foie_price"] = round(median_foie_price, 2)
        stats["foie_price_premium_pct"] = round(price_premium_pct, 1)
    else:
        stats["median_food_price"] = None
        stats["median_foie_price"] = None
        stats["foie_price_premium_pct"] = None
    
    # === FOIE GRAS MENU SECTION CATEGORIZATION ===
    # Categorize foie gras items by menu section based on section field
    section_categories = {
        "Appetizers/Starters": ["appetizer", "starter", "small plate", "first course", "hors d'oeuvre", "antipasti", "antipasto", "entree", "entrée"],
        "Main Courses": ["main", "entrees", "entrées", "second course", "plat principal", "secondi"],
        "Specials/Features": ["special", "feature", "chef", "tasting", "prix fixe", "omakase"],
        "Other/Uncategorized": []
    }
    
    foie_by_section = {}
    if 'section' in foie_items.columns:
        for _, item in foie_items.iterrows():
            section = str(item.get('section', '')).lower().strip()
            categorized = False
            for category, keywords in section_categories.items():
                if category == "Other/Uncategorized":
                    continue
                for kw in keywords:
                    if kw in section:
                        foie_by_section[category] = foie_by_section.get(category, 0) + 1
                        categorized = True
                        break
                if categorized:
                    break
            if not categorized:
                foie_by_section["Other/Uncategorized"] = foie_by_section.get("Other/Uncategorized", 0) + 1
    
    # === STATE AGGREGATES ===
    state_data = []
    for state_abbr, state_name in STATE_NAMES.items():
        state_restaurants = restaurants_df[restaurants_df['state'] == state_abbr]
        state_rest_ids = set(state_restaurants['restaurant_id'].unique())
        state_foie_rest = state_rest_ids.intersection(foie_restaurant_ids)
        
        state_data.append({
            "state": state_abbr,
            "state_name": state_name,
            "restaurant_count": len(state_restaurants),
            "restaurants_with_foie": len(state_foie_rest),
        })
    
    # Sort by restaurant count
    state_data = sorted(state_data, key=lambda x: x['restaurant_count'], reverse=True)
    
    # === CITY MAP DATA ===
    city_data = []
    for city, coords in CITY_COORDS.items():
        # Match city (case insensitive)
        city_restaurants = restaurants_df[
            restaurants_df['city'].str.lower() == city.lower()
        ]
        city_rest_ids = set(city_restaurants['restaurant_id'].unique())
        city_foie_rest = city_rest_ids.intersection(foie_restaurant_ids)
        
        # Count foie gras items in this city
        city_foie_items = foie_items[foie_items['restaurant_id'].isin(city_rest_ids)]
        
        if len(city_restaurants) > 0:
            city_data.append({
                "city": city,
                "lat": coords[0],
                "lng": coords[1],
                "restaurant_count": len(city_restaurants),
                "foie_gras_items": len(city_foie_items),
                "restaurants_with_foie": len(city_foie_rest),
            })
    
    # === CUISINE DISTRIBUTION ===
    cuisine_counts = restaurants_df['cuisine_type'].value_counts().head(10).to_dict()
    
    # === PRICE BAND DISTRIBUTION ===
    price_band_counts = restaurants_df['price_band'].value_counts().to_dict()
    
    # === PRICE TIER FOIE GRAS PREVALENCE ===
    price_tier_foie = {}
    tier_order = ['$30 and under', '$31 to $50', '$50 and over']
    for band in tier_order:
        band_rests = restaurants_df[restaurants_df['price_band'] == band]
        band_ids = set(band_rests['restaurant_id'].unique())
        band_foie = band_ids.intersection(foie_restaurant_ids)
        if len(band_ids) > 0:
            price_tier_foie[band] = {
                "total": len(band_ids),
                "with_foie": len(band_foie),
                "rate": round(len(band_foie) / len(band_ids) * 100, 1)
            }
    
    # === CUISINE OF FOIE GRAS RESTAURANTS ===
    foie_restaurants = restaurants_df[restaurants_df['restaurant_id'].isin(foie_restaurant_ids)]
    foie_cuisines = {}
    for cuisine in foie_restaurants['cuisine_type'].dropna():
        # Simplify cuisine to primary type
        primary = cuisine.split(',')[0].strip()
        foie_cuisines[primary] = foie_cuisines.get(primary, 0) + 1
    # Sort by count
    foie_cuisines = dict(sorted(foie_cuisines.items(), key=lambda x: x[1], reverse=True))
    
    # === ORIGIN MENTIONS ANALYSIS ===
    origin_data = {"Hudson Valley": 0, "No origin specified": 0}
    for _, item in foie_items.iterrows():
        title_desc = str(item.get('title', '')).lower() + ' ' + str(item.get('description', '')).lower()
        if 'hudson' in title_desc or 'hudson valley' in title_desc:
            origin_data["Hudson Valley"] += 1
        else:
            origin_data["No origin specified"] += 1
    
    # === PRICE COMPARISON DATA ===
    # Filter out drinks for fair comparison
    drink_keywords = ['wine', 'champagne', 'whisky', 'bourbon', 'cognac', 'scotch', 
                      'vodka', 'gin', 'rum', 'tequila', 'bottle', 'beer', 'cocktail']
    food_mask = ~menu_df['title'].str.lower().str.contains('|'.join(drink_keywords), na=False)
    food_prices = pd.to_numeric(menu_df[food_mask]['price'], errors='coerce').dropna()
    food_prices = food_prices[(food_prices > 0) & (food_prices <= 500)]
    
    price_comparison = {
        "foie_gras_median": round(foie_prices.median(), 2) if len(foie_prices) > 0 else None,
        "foie_gras_mean": round(foie_prices.mean(), 2) if len(foie_prices) > 0 else None,
        "all_food_median": round(food_prices.median(), 2) if len(food_prices) > 0 else None,
        "premium_pct": round(((foie_prices.median() - food_prices.median()) / food_prices.median()) * 100, 1) if len(foie_prices) > 0 and len(food_prices) > 0 else None
    }
    
    # === FOIE GRAS PRICE DISTRIBUTION ===
    foie_price_dist = {
        "under_15": int(len(foie_prices[foie_prices < 15])),
        "15_to_25": int(len(foie_prices[(foie_prices >= 15) & (foie_prices < 25)])),
        "25_to_35": int(len(foie_prices[(foie_prices >= 25) & (foie_prices < 35)])),
        "35_plus": int(len(foie_prices[foie_prices >= 35]))
    }
    
    # === SAMPLE FOIE GRAS MENU ITEMS ===
    # Get 10 sample items with restaurant info for display
    sample_foie_items = []
    foie_with_details = foie_items.merge(
        restaurants_df[['restaurant_id', 'name', 'city', 'state', 'price_band']],
        on='restaurant_id',
        how='left'
    )
    # Get items with prices for better display
    foie_with_prices = foie_with_details[foie_with_details['price'].notna()].copy()
    
    # Deduplicate by title + restaurant_id (same item on different menus)
    foie_with_prices['dedup_key'] = foie_with_prices['title'].str.lower() + '_' + foie_with_prices['restaurant_id'].astype(str)
    foie_with_prices = foie_with_prices.drop_duplicates(subset=['dedup_key'])
    
    # Prioritize items with descriptions for more interesting display
    foie_with_desc = foie_with_prices[foie_with_prices['description'].notna() & (foie_with_prices['description'] != '')]
    foie_without_desc = foie_with_prices[foie_with_prices['description'].isna() | (foie_with_prices['description'] == '')]
    foie_sorted = pd.concat([foie_with_desc, foie_without_desc])
    
    for _, item in foie_sorted.head(10).iterrows():
        sample_foie_items.append({
            "title": item.get('title', ''),
            "description": str(item.get('description', ''))[:200] if pd.notna(item.get('description')) else '',
            "price": float(item['price']) if pd.notna(item.get('price')) else None,
            "restaurant": item.get('name', ''),
            "city": item.get('city', ''),
            "state": item.get('state', ''),
            "section": item.get('section', '')
        })
    
    # === MULTIPLE OFFERINGS STATS ===
    # Count restaurants with more than 1 foie gras item
    foie_per_restaurant = foie_items.groupby('restaurant_id').size()
    restaurants_with_multiple = len(foie_per_restaurant[foie_per_restaurant > 1])
    multi_offering_pct = round(restaurants_with_multiple / len(foie_restaurant_ids) * 100, 1) if len(foie_restaurant_ids) > 0 else 0
    avg_foie_per_restaurant = round(foie_per_restaurant.mean(), 1) if len(foie_per_restaurant) > 0 else 0
    
    # === EUROPEAN RESTAURANT INSIGHT ===
    # Calculate European cuisine prevalence
    european_keywords = ['french', 'italian', 'european', 'mediterranean', 'spanish', 'greek']
    foie_restaurants_df = restaurants_df[restaurants_df['restaurant_id'].isin(foie_restaurant_ids)]
    
    # Count European foie restaurants
    european_foie_count = 0
    for cuisine in foie_restaurants_df['cuisine_type'].dropna():
        if any(kw in cuisine.lower() for kw in european_keywords):
            european_foie_count += 1
    
    # Count European restaurants overall
    european_total_count = 0
    for cuisine in restaurants_df['cuisine_type'].dropna():
        if any(kw in cuisine.lower() for kw in european_keywords):
            european_total_count += 1
    
    european_foie_pct = round(european_foie_count / len(foie_restaurant_ids) * 100, 1) if len(foie_restaurant_ids) > 0 else 0
    european_total_pct = round(european_total_count / len(restaurants_df) * 100, 1) if len(restaurants_df) > 0 else 0
    
    european_insight = {
        "foie_count": european_foie_count,
        "foie_pct": european_foie_pct,
        "total_count": european_total_count,
        "total_pct": european_total_pct,
    }
    
    # === STATE DATA WITH FOIE RATE ===
    # Add foie gras rate to state data for layered visualization
    for state in state_data:
        if state['restaurant_count'] > 0:
            state['foie_rate'] = round(state['restaurants_with_foie'] / state['restaurant_count'] * 100, 1)
        else:
            state['foie_rate'] = 0
    
    # Add to stats
    stats['multi_offering_pct'] = multi_offering_pct
    stats['avg_foie_per_restaurant'] = avg_foie_per_restaurant
    stats['restaurants_with_multiple_foie'] = restaurants_with_multiple
    
    return {
        "stats": stats,
        "states": state_data,
        "cities": city_data,
        "cuisines": cuisine_counts,
        "price_bands": price_band_counts,
        "foie_sections": foie_by_section,
        "price_tier_foie": price_tier_foie,
        "foie_cuisines": foie_cuisines,
        "origin_data": origin_data,
        "price_comparison": price_comparison,
        "foie_price_dist": foie_price_dist,
        "sample_foie_items": sample_foie_items,
        "european_insight": european_insight,
    }


if __name__ == "__main__":
    data = process_data()
    print(json.dumps(data, indent=2))
