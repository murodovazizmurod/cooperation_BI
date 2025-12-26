import requests
import sqlite3
import json
from typing import Dict, List, Any, Set
from datetime import datetime, timedelta
import time

BASE_URL = "https://new.cooperation.uz/ocelot/api-client/Client"
DB_NAME = "cooperation_data.db"

def safe_int(value, default=0):
    """Safely convert value to int, handling None"""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_bool(value, default=False):
    """Safely convert value to int (0/1) for boolean, handling None"""
    if value is None:
        return int(default)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(bool(value))
    return int(default)

def get_existing_offer_ids(conn, category_id: int) -> Set[int]:
    """Get set of existing offer IDs for a category"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM offers WHERE category_id = ?', (category_id,))
    return {row[0] for row in cursor.fetchall()}

def get_last_update_time(conn, category_id: int) -> datetime:
    """Get the last update time for a category"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT MAX(created_at) FROM offers WHERE category_id = ?
    ''', (category_id,))
    result = cursor.fetchone()[0]
    if result:
        return datetime.fromisoformat(result.replace('Z', '+00:00'))
    return datetime.min

def fetch_categories() -> List[Dict[str, Any]]:
    """Fetch all categories from the API"""
    url = f"{BASE_URL}/GetAllTnVedCategory"
    params = {"take": 100, "skip": 0}
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    if data.get("statusCode") == 200 and "result" in data:
        return data["result"].get("data", [])
    return []

def update_category(conn, category: Dict[str, Any]):
    """Update category information in the database"""
    cursor = conn.cursor()
    name = category.get("name", {})
    
    cursor.execute('''
        INSERT OR REPLACE INTO categories 
        (id, name_uz, name_cyrl, name_ru, name_en, photo, count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        category.get("id"),
        name.get("uz"),
        name.get("cyrl"),
        name.get("ru"),
        name.get("en"),
        category.get("photo"),
        category.get("count", 0)
    ))
    conn.commit()

def fetch_all_offers_for_category(category_id: int) -> List[Dict[str, Any]]:
    """Fetch ALL offers for a category"""
    url = f"{BASE_URL}/GetAllOffer"
    
    try:
        # First, get total count
        params = {
            "OfferType": 1,
            "skip": 0,
            "take": 1,
            "productName": "",
            "firstTnvedCategoryId": category_id
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("statusCode") != 200 or "result" not in data:
            return []
        
        result = data["result"]
        total_offers = result.get("total", 0)
        
        if total_offers == 0:
            return []
        
        # Fetch all offers
        params["take"] = total_offers
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        if data.get("statusCode") == 200 and "result" in data:
            return data["result"].get("data", [])
        
        return []
        
    except Exception as e:
        print(f"    ⚠ Error fetching offers: {e}")
        return []

def insert_or_update_offers(conn, offers: List[Dict[str, Any]], category_id: int, existing_ids: Set[int]) -> Dict[str, int]:
    """Insert new offers or update existing ones. Returns stats."""
    cursor = conn.cursor()
    stats = {"new": 0, "updated": 0, "unchanged": 0}
    
    for offer in offers:
        product_name = offer.get("productName", {})
        offer_id = offer.get("id")
        
        try:
            # Check if offer exists
            is_new = offer_id not in existing_ids
            
            cursor.execute('''
                INSERT OR REPLACE INTO offers (
                    id, category_id, product_name_uz, product_name_cyrl, 
                    product_name_ru, product_name_en, product_name_raw_uz,
                    product_name_raw_cyrl, photos, min_part, max_part,
                    is_sertificate, measure_name, product_quantity,
                    public_end_date, unit_price, offer_number, status_date,
                    company_tnved_name, company_enkt_name, code,
                    is_certificate, first_tnved_category_id,
                    first_company_enkt_code, has_disability, type_offer,
                    is_innovative
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                offer_id,
                category_id,
                product_name.get("uz"),
                product_name.get("cyrl"),
                product_name.get("ru"),
                product_name.get("en"),
                offer.get("productNameUz"),
                offer.get("productNameCyrl"),
                offer.get("photos"),
                safe_int(offer.get("minPart")),
                safe_int(offer.get("maxPart")),
                safe_bool(offer.get("isSertificate")),
                offer.get("measureName"),
                safe_int(offer.get("productQuantity")),
                offer.get("publicEndDate"),
                safe_int(offer.get("unitPrice")),
                offer.get("offerNumber"),
                offer.get("statusDate"),
                offer.get("companyTnvedName"),
                offer.get("companyEnktName"),
                offer.get("code"),
                safe_bool(offer.get("isCertificate")),
                safe_int(offer.get("firstTnvedCategoryId")),
                offer.get("firstCompanyEnktCode"),
                safe_bool(offer.get("hasDisability")),
                safe_int(offer.get("typeOffer")),
                safe_bool(offer.get("isInnovative"))
            ))
            
            if is_new:
                stats["new"] += 1
            else:
                stats["updated"] += 1
                
        except sqlite3.Error as e:
            print(f"    ⚠ Database error: {e}")
            continue
    
    conn.commit()
    return stats

def remove_deleted_offers(conn, category_id: int, current_offer_ids: Set[int]):
    """Remove offers that no longer exist in the API"""
    cursor = conn.cursor()
    
    # Get all existing IDs for this category
    existing_ids = get_existing_offer_ids(conn, category_id)
    
    # Find IDs that exist in DB but not in current API response
    deleted_ids = existing_ids - current_offer_ids
    
    if deleted_ids:
        placeholders = ','.join('?' * len(deleted_ids))
        cursor.execute(f'''
            DELETE FROM offers 
            WHERE category_id = ? AND id IN ({placeholders})
        ''', (category_id,) + tuple(deleted_ids))
        conn.commit()
        return len(deleted_ids)
    
    return 0

def update_category_data(conn, category_id: int, category_name: str, full_update: bool = False) -> Dict[str, Any]:
    """Update offers for a single category"""
    print(f"\n  Processing category {category_id} ({category_name[:50]}...)")
    
    # Get existing offer IDs
    existing_ids = get_existing_offer_ids(conn, category_id)
    existing_count = len(existing_ids)
    
    # Fetch all offers from API
    offers = fetch_all_offers_for_category(category_id)
    
    if not offers:
        print(f"    ✓ No offers in this category")
        return {"new": 0, "updated": 0, "deleted": 0, "total": 0}
    
    current_offer_ids = {offer.get("id") for offer in offers if offer.get("id")}
    
    print(f"    Existing in DB: {existing_count:,} | API: {len(offers):,}")
    
    # Insert/update offers
    stats = insert_or_update_offers(conn, offers, category_id, existing_ids)
    
    # Remove deleted offers (only if we're doing a full update)
    deleted_count = 0
    if full_update:
        deleted_count = remove_deleted_offers(conn, category_id, current_offer_ids)
        if deleted_count > 0:
            print(f"    Removed {deleted_count} deleted offers")
    
    stats["deleted"] = deleted_count
    stats["total"] = len(offers)
    
    print(f"    ✓ New: {stats['new']:,} | Updated: {stats['updated']:,} | Total: {stats['total']:,}")
    
    return stats

def get_database_statistics(conn):
    """Get current database statistics"""
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM categories')
    category_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers')
    offer_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT category_id) FROM offers')
    categories_with_offers = cursor.fetchone()[0]
    
    return {
        "categories": category_count,
        "offers": offer_count,
        "categories_with_offers": categories_with_offers
    }

def main(full_update: bool = False):
    """Main update function"""
    print("=" * 80)
    print("UPDATING COOPERATION.UZ DATABASE")
    print("=" * 80)
    print(f"Mode: {'Full Update' if full_update else 'Incremental Update'}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = sqlite3.connect(DB_NAME)
    
    # Get current stats
    print("\nCurrent database status:")
    stats_before = get_database_statistics(conn)
    print(f"  Categories: {stats_before['categories']}")
    print(f"  Offers: {stats_before['offers']:,}")
    
    # Update categories
    print("\nUpdating categories...")
    categories = fetch_categories()
    for category in categories:
        update_category(conn, category)
    print(f"✓ Updated {len(categories)} categories")
    
    # Update offers for each category
    print("\n" + "=" * 80)
    print("UPDATING OFFERS")
    print("=" * 80)
    
    total_stats = {"new": 0, "updated": 0, "deleted": 0, "total": 0}
    start_time = time.time()
    
    for i, category in enumerate(categories, 1):
        category_id = category.get("id")
        category_name = category.get("name", {}).get("ru", "Unknown")
        
        stats = update_category_data(conn, category_id, category_name, full_update)
        
        total_stats["new"] += stats["new"]
        total_stats["updated"] += stats["updated"]
        total_stats["deleted"] += stats["deleted"]
        total_stats["total"] += stats["total"]
        
        # Progress
        elapsed = time.time() - start_time
        if i > 0:
            avg_time = elapsed / i
            remaining = avg_time * (len(categories) - i)
            print(f"  Progress: {i}/{len(categories)} | "
                  f"New: {total_stats['new']:,} | "
                  f"Updated: {total_stats['updated']:,} | "
                  f"ETA: {remaining/60:.1f}min")
    
    # Final statistics
    print("\n" + "=" * 80)
    print("UPDATE SUMMARY")
    print("=" * 80)
    
    stats_after = get_database_statistics(conn)
    
    print(f"\nBefore: {stats_before['offers']:,} offers")
    print(f"After:  {stats_after['offers']:,} offers")
    print(f"\nChanges:")
    print(f"  New offers:     {total_stats['new']:,}")
    print(f"  Updated offers: {total_stats['updated']:,}")
    if full_update:
        print(f"  Deleted offers:  {total_stats['deleted']:,}")
    
    elapsed_total = time.time() - start_time
    print(f"\n✓ Update completed in {elapsed_total:.1f} seconds ({elapsed_total/60:.1f} minutes)")
    print(f"✓ Database saved to: {DB_NAME}")
    
    conn.close()

if __name__ == "__main__":
    import sys
    
    # Check if full update is requested
    full_update = "--full" in sys.argv or "-f" in sys.argv
    
    try:
        main(full_update=full_update)
    except KeyboardInterrupt:
        print("\n\n⚠ Update interrupted by user")
        print("Data saved up to this point is in the database")
    except Exception as e:
        print(f"\n\n⚠ Error: {e}")
        import traceback
        traceback.print_exc()




