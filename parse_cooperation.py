import requests
import json
from typing import Dict, List, Any
from datetime import datetime

BASE_URL = "https://new.cooperation.uz/ocelot/api-client/Client"

def fetch_categories() -> List[Dict[str, Any]]:
    """Fetch all categories from the API"""
    url = f"{BASE_URL}/GetAllTnVedCategory"
    params = {"take": 100, "skip": 0}
    
    print(f"Fetching categories...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    if data.get("statusCode") == 200 and "result" in data:
        categories = data["result"].get("data", [])
        print(f"✓ Found {len(categories)} categories")
        return categories
    return []

def fetch_offers(category_id: int, skip: int = 0, take: int = 12) -> Dict[str, Any]:
    """Fetch offers for a specific category"""
    url = f"{BASE_URL}/GetAllOffer"
    params = {
        "OfferType": 1,
        "skip": skip,
        "take": take,
        "productName": "",
        "firstTnvedCategoryId": category_id
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    if data.get("statusCode") == 200 and "result" in data:
        return data["result"]
    return {"data": [], "total": 0}

def format_price(price: int) -> str:
    """Format price in UZS"""
    return f"{price:,} UZS"

def format_date(date_str: str) -> str:
    """Format date string"""
    try:
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M")
    except:
        return date_str

def display_categories(categories: List[Dict[str, Any]]):
    """Display categories in a readable format"""
    print("\n" + "=" * 80)
    print("CATEGORIES")
    print("=" * 80)
    
    for i, cat in enumerate(categories, 1):
        name_ru = cat.get("name", {}).get("ru", "N/A")
        cat_id = cat.get("id")
        count = cat.get("count", 0)
        print(f"{i:2d}. [{cat_id:2d}] {name_ru}")
        print(f"    Products: {count:,}")

def display_offers(offers: List[Dict[str, Any]], category_name: str = ""):
    """Display offers in a readable format"""
    if category_name:
        print(f"\n{'=' * 80}")
        print(f"OFFERS - {category_name}")
        print("=" * 80)
    
    for i, offer in enumerate(offers, 1):
        product_name = offer.get("productName", {}).get("ru") or offer.get("productNameUz", "N/A")
        offer_number = offer.get("offerNumber", "N/A")
        unit_price = offer.get("unitPrice", 0)
        quantity = offer.get("productQuantity", 0)
        min_part = offer.get("minPart", 0)
        max_part = offer.get("maxPart", 0)
        measure = offer.get("measureName", "")
        end_date = offer.get("publicEndDate", "")
        code = offer.get("code", "")
        
        print(f"\n{i}. {product_name}")
        print(f"   Offer #: {offer_number}")
        print(f"   Price: {format_price(unit_price)} per {measure}")
        print(f"   Quantity: {quantity} {measure} (min: {min_part}, max: {max_part})")
        print(f"   End Date: {format_date(end_date)}")
        print(f"   Code: {code}")
        if offer.get("isCertificate"):
            print(f"   ✓ Certificate required")

def parse_multiple_pages(category_ids: List[int], pages_per_category: int = 3, items_per_page: int = 12):
    """Parse multiple pages for multiple categories"""
    print("=" * 80)
    print("PARSING MULTIPLE PAGES")
    print("=" * 80)
    
    # First, get all categories
    all_categories = fetch_categories()
    display_categories(all_categories)
    
    # Create a mapping of category ID to name
    category_map = {cat["id"]: cat["name"].get("ru", "Unknown") for cat in all_categories}
    
    # Parse offers for specified categories
    all_offers_data = {}
    
    for cat_id in category_ids:
        if cat_id not in category_map:
            print(f"\n⚠ Category ID {cat_id} not found, skipping...")
            continue
        
        category_name = category_map[cat_id]
        print(f"\n{'=' * 80}")
        print(f"PARSING CATEGORY: {category_name} (ID: {cat_id})")
        print("=" * 80)
        
        # Fetch first page to get total count
        first_page = fetch_offers(cat_id, skip=0, take=items_per_page)
        total_offers = first_page.get("total", 0)
        print(f"Total offers in this category: {total_offers:,}")
        
        all_offers = first_page.get("data", [])
        
        # Fetch additional pages
        for page in range(1, pages_per_category):
            skip = page * items_per_page
            if skip >= total_offers:
                break
            
            print(f"  Fetching page {page + 1} (skip={skip})...")
            page_data = fetch_offers(cat_id, skip=skip, take=items_per_page)
            all_offers.extend(page_data.get("data", []))
        
        all_offers_data[cat_id] = {
            "category_name": category_name,
            "total": total_offers,
            "offers": all_offers
        }
        
        print(f"\n✓ Collected {len(all_offers)} offers from {min(pages_per_category, (total_offers // items_per_page) + 1)} pages")
        display_offers(all_offers[:5], category_name)  # Show first 5 as sample
        if len(all_offers) > 5:
            print(f"\n   ... and {len(all_offers) - 5} more offers")
    
    return all_offers_data

if __name__ == "__main__":
    # Parse several categories to see the structure
    # You can modify these category IDs based on what you want to parse
    category_ids_to_parse = [10, 22, 21]  # First 3 categories with most products
    
    parsed_data = parse_multiple_pages(
        category_ids=category_ids_to_parse,
        pages_per_category=3,  # Parse 3 pages per category
        items_per_page=12
    )
    
    # Save parsed data
    output_file = "parsed_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(parsed_data, f, indent=2, ensure_ascii=False)
    
    print("\n" + "=" * 80)
    print(f"✓ Parsed data saved to: {output_file}")
    print("=" * 80)

