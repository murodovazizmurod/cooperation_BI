"""Add indexes to optimize query performance"""
import sqlite3

DB_NAME = "cooperation_data.db"

def add_indexes():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print("Adding indexes for better performance...")
    
    try:
        # Index on status_date for date-based queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_status_date 
            ON offers(status_date)
        ''')
        print("✓ Index on status_date created")
        
        # Index on product_name_ru for product queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_product_name 
            ON offers(product_name_ru)
        ''')
        print("✓ Index on product_name_ru created")
        
        # Composite index for date + product queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_status_product 
            ON offers(status_date, product_name_ru)
        ''')
        print("✓ Composite index on status_date + product_name_ru created")
        
        # Index on category_id for joins
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_category 
            ON offers(category_id)
        ''')
        print("✓ Index on category_id created")
        
        # Composite index for product + category queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_product_category 
            ON offers(product_name_ru, category_id)
        ''')
        print("✓ Composite index on product_name_ru + category_id created")
        
        # Index on unit_price for price queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_unit_price 
            ON offers(unit_price)
        ''')
        print("✓ Index on unit_price created")
        
        # Composite index for product + category + measure
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_product_category_measure 
            ON offers(product_name_ru, category_id, measure_name)
        ''')
        print("✓ Composite index on product + category + measure created")
        
        # Index on measure_name
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_measure 
            ON offers(measure_name)
        ''')
        print("✓ Index on measure_name created")
        
        # Index on is_certificate
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_offers_certificate 
            ON offers(is_certificate)
        ''')
        print("✓ Index on is_certificate created")
        
        # Analyze tables for query optimization
        cursor.execute('ANALYZE offers')
        cursor.execute('ANALYZE categories')
        print("✓ Table statistics updated")
        
        conn.commit()
        print("\n✓ All indexes created successfully!")
        print("\nPerformance optimization complete. Queries should be much faster now.")
        
    except Exception as e:
        print(f"Error creating indexes: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    add_indexes()

