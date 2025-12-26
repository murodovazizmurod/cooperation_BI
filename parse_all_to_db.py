import requests
import sqlite3
import json
from typing import Dict, List, Any
from datetime import datetime
import time

BASE_URL = "https://new.cooperation.uz/ocelot/api-client/Client"
DB_NAME = "cooperation_data.db"

def create_database():
    """Create SQLite database with tables for categories and offers"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Categories table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            name_uz TEXT,
            name_cyrl TEXT,
            name_ru TEXT,
            name_en TEXT,
            photo TEXT,
            count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Offers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY,
            category_id INTEGER,
            product_name_uz TEXT,
            product_name_cyrl TEXT,
            product_name_ru TEXT,
            product_name_en TEXT,
            product_name_raw_uz TEXT,
            product_name_raw_cyrl TEXT,
            photos TEXT,
            min_part INTEGER,
            max_part INTEGER,
            is_sertificate INTEGER,
            measure_name TEXT,
            product_quantity INTEGER,
            public_end_date TEXT,
            unit_price INTEGER,
            offer_number TEXT,
            status_date TEXT,
            company_tnved_name TEXT,
            company_enkt_name TEXT,
            code TEXT,
            is_certificate INTEGER,
            first_tnved_category_id INTEGER,
            first_company_enkt_code TEXT,
            has_disability INTEGER,
            type_offer INTEGER,
            is_innovative INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        )
    ''')
    
    # Create indexes for better query performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_category ON offers(category_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_code ON offers(code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_offer_number ON offers(offer_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_offers_price ON offers(unit_price)')
    
    conn.commit()
    return conn

def fetch_categories() -> List[Dict[str, Any]]:
    """Fetch all categories from the API"""
    url = f"{BASE_URL}/GetAllTnVedCategory"
    params = {"take": 100, "skip": 0}
    
    print("Fetching all categories...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    if data.get("statusCode") == 200 and "result" in data:
        categories = data["result"].get("data", [])
        print(f"✓ Found {len(categories)} categories")
        return categories
    return []

def insert_category(conn, category: Dict[str, Any]):
    """Insert or update a category in the database"""
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

def fetch_all_offers_for_category(conn, category_id: int, category_name: str) -> int:
    """Fetch ALL offers for a category in one request"""
    url = f"{BASE_URL}/GetAllOffer"
    
    print(f"\n  Fetching offers for category {category_id} ({category_name[:50]}...)")
    
    try:
        # First, get a small sample to know the total count
        params = {
            "OfferType": 1,
            "skip": 0,
            "take": 1,  # Just get 1 item to know the total
            "productName": "",
            "firstTnvedCategoryId": category_id
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("statusCode") != 200 or "result" not in data:
            print(f"    ⚠ Error: Invalid response")
            return 0
        
        result = data["result"]
        total_offers = result.get("total", 0)
        
        if total_offers == 0:
            print(f"    ✓ No offers in this category")
            return 0
        
        print(f"    Total offers available: {total_offers:,}")
        print(f"    Fetching all {total_offers:,} offers in one request...", end="")
        
        # Now fetch ALL offers in one request
        params = {
            "OfferType": 1,
            "skip": 0,
            "take": total_offers,  # Get all offers at once
            "productName": "",
            "firstTnvedCategoryId": category_id
        }
        
        response = requests.get(url, params=params, timeout=120)  # Longer timeout for large requests
        response.raise_for_status()
        data = response.json()
        
        if data.get("statusCode") != 200 or "result" not in data:
            print(f"\n    ⚠ Error: Invalid response")
            return 0
        
        result = data["result"]
        offers = result.get("data", [])
        
        if not offers:
            print(f"\n    ⚠ No offers returned")
            return 0
        
        print(f" ✓ Got {len(offers):,} offers")
        print(f"    Inserting into database...", end="")
        
        # Insert all offers
        inserted = insert_offers_batch(conn, offers, category_id)
        
        print(f" ✓ Inserted {inserted:,} offers")
        return inserted
        
    except requests.exceptions.Timeout:
        print(f"\n    ⚠ Error: Request timeout (category might be too large)")
        return 0
    except requests.exceptions.RequestException as e:
        print(f"\n    ⚠ Error: {e}")
        return 0

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

def insert_offers_batch(conn, offers: List[Dict[str, Any]], category_id: int) -> int:
    """Insert a batch of offers into the database"""
    cursor = conn.cursor()
    inserted = 0
    
    for offer in offers:
        product_name = offer.get("productName", {})
        
        try:
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
                offer.get("id"),
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
            inserted += 1
        except sqlite3.Error as e:
            print(f"\n    ⚠ Database error inserting offer {offer.get('id')}: {e}")
            continue
        except Exception as e:
            print(f"\n    ⚠ Error inserting offer {offer.get('id')}: {e}")
            continue
    
    conn.commit()
    return inserted

def get_statistics(conn):
    """Get statistics from the database"""
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM categories')
    category_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM offers')
    offer_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT SUM(count) FROM categories')
    total_available = cursor.fetchone()[0] or 0
    
    cursor.execute('''
        SELECT c.name_ru, COUNT(o.id) as offer_count
        FROM categories c
        LEFT JOIN offers o ON c.id = o.category_id
        GROUP BY c.id, c.name_ru
        ORDER BY offer_count DESC
    ''')
    category_stats = cursor.fetchall()
    
    return {
        "categories": category_count,
        "offers": offer_count,
        "total_available": total_available,
        "category_stats": category_stats
    }

def main():
    print("=" * 80)
    print("PARSING ALL DATA FROM COOPERATION.UZ")
    print("=" * 80)
    
    # Create database
    print("\nCreating database...")
    conn = create_database()
    print(f"✓ Database created: {DB_NAME}")
    
    # Fetch all categories
    categories = fetch_categories()
    
    if not categories:
        print("⚠ No categories found!")
        return
    
    # Insert categories
    print("\nInserting categories into database...")
    for category in categories:
        insert_category(conn, category)
    print(f"✓ Inserted {len(categories)} categories")
    
    # Fetch all offers for each category
    print("\n" + "=" * 80)
    print("FETCHING ALL OFFERS")
    print("=" * 80)
    
    total_offers_inserted = 0
    start_time = time.time()
    
    for i, category in enumerate(categories, 1):
        category_id = category.get("id")
        category_name = category.get("name", {}).get("ru", "Unknown")
        
        print(f"\n[{i}/{len(categories)}] Processing category {category_id}...")
        
        inserted = fetch_all_offers_for_category(conn, category_id, category_name)
        total_offers_inserted += inserted
        
        # Show progress
        elapsed = time.time() - start_time
        avg_time = elapsed / i
        remaining = avg_time * (len(categories) - i)
        print(f"  Progress: {i}/{len(categories)} categories, {total_offers_inserted:,} offers inserted")
        print(f"  Estimated time remaining: {remaining/60:.1f} minutes")
    
    # Show final statistics
    print("\n" + "=" * 80)
    print("FINAL STATISTICS")
    print("=" * 80)
    
    stats = get_statistics(conn)
    print(f"\nCategories in database: {stats['categories']}")
    print(f"Offers in database: {stats['offers']:,}")
    print(f"Total offers available (from API): {stats['total_available']:,}")
    print(f"Coverage: {stats['offers']/stats['total_available']*100:.2f}%")
    
    print("\nTop 10 categories by offer count:")
    for name, count in stats['category_stats'][:10]:
        print(f"  {name[:60]:60s} {count:>6,} offers")
    
    elapsed_total = time.time() - start_time
    print(f"\n✓ Total time: {elapsed_total/60:.1f} minutes")
    print(f"✓ Database saved to: {DB_NAME}")
    
    conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Parsing interrupted by user")
        print("Data saved up to this point is in the database")
    except Exception as e:
        print(f"\n\n⚠ Error: {e}")
        import traceback
        traceback.print_exc()

