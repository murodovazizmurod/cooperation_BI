import sqlite3
from typing import Dict, List, Any
from datetime import datetime, timedelta
from collections import Counter

DB_NAME = "cooperation_data.db"

def get_connection():
    """Get database connection"""
    return sqlite3.connect(DB_NAME)

def get_category_statistics(conn) -> List[Dict[str, Any]]:
    """Get statistics for each category"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 
            c.id,
            c.name_ru,
            COUNT(o.id) as offer_count,
            AVG(o.unit_price) as avg_price,
            MIN(o.unit_price) as min_price,
            MAX(o.unit_price) as max_price,
            SUM(o.product_quantity) as total_quantity
        FROM categories c
        LEFT JOIN offers o ON c.id = o.category_id
        GROUP BY c.id, c.name_ru
        HAVING offer_count > 0
        ORDER BY offer_count DESC
    ''')
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "category_id": row[0],
            "category_name": row[1],
            "offer_count": row[2],
            "avg_price": round(row[3] or 0, 2),
            "min_price": row[4] or 0,
            "max_price": row[5] or 0,
            "total_quantity": row[6] or 0
        })
    return results

def get_top_products(conn, limit: int = 20, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
    """Get most frequently posted products with detailed statistics, accounting for measurement units
    
    Args:
        conn: Database connection
        limit: Maximum number of products to return
        start_date: Optional start date filter (YYYY-MM-DD format)
        end_date: Optional end date filter (YYYY-MM-DD format)
    """
    cursor = conn.cursor()
    
    # Build WHERE clause with optional date filtering
    where_clauses = ["product_name_ru IS NOT NULL", "product_name_ru != ''"]
    params = []
    
    if start_date:
        where_clauses.append("status_date >= ?")
        params.append(start_date)
    
    if end_date:
        where_clauses.append("status_date <= ?")
        params.append(end_date)
    
    where_clause = " AND ".join(where_clauses)
    
    # Get basic statistics grouped by product name, category, AND measure_name
    # This ensures we don't mix prices from different units (e.g., kg vs tonnes)
    cursor.execute(f'''
        SELECT 
            product_name_ru,
            category_id,
            measure_name,
            COUNT(*) as post_count,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price,
            SUM(product_quantity) as total_quantity,
            AVG(product_quantity) as avg_quantity,
            MIN(product_quantity) as min_quantity,
            MAX(product_quantity) as max_quantity,
            SUM(unit_price * product_quantity) as total_value,
            SUM(CASE WHEN is_certificate = 1 THEN 1 ELSE 0 END) as with_certificate
        FROM offers
        WHERE {where_clause}
        GROUP BY product_name_ru, category_id, measure_name
        ORDER BY post_count DESC
    ''', params)
    
    # Group by product name and category to aggregate across different measurement units
    products_dict = {}
    
    for row in cursor.fetchall():
        product_name = row[0]
        category_id = row[1]
        measure_name = row[2] or "Unknown"
        post_count = row[3]
        avg_price = row[4] or 0
        min_price = row[5] or 0
        max_price = row[6] or 0
        total_quantity = row[7] or 0
        avg_quantity = row[8] or 0
        min_quantity = row[9] or 0
        max_quantity = row[10] or 0
        total_value = row[11] or 0
        with_certificate = row[12] or 0
        
        key = (product_name, category_id)
        
        if key not in products_dict:
            products_dict[key] = {
                "product_name": product_name,
                "category_id": category_id,
                "category_name": None,  # Will be filled in batch later
                "post_count": 0,
                "measure_units": {},
                "all_prices": [],
                "all_quantities": [],
                "total_value": 0,
                "with_certificate": 0,
                "earliest_post": None,
                "latest_post": None
            }
        
        # Aggregate data
        products_dict[key]["post_count"] += post_count
        products_dict[key]["total_value"] += total_value
        products_dict[key]["with_certificate"] += with_certificate
        
        # Store measurement unit specific data
        products_dict[key]["measure_units"][measure_name] = {
            "post_count": post_count,
            "avg_price": avg_price,
            "min_price": min_price,
            "max_price": max_price,
            "total_quantity": total_quantity,
            "avg_quantity": avg_quantity,
            "min_quantity": min_quantity,
            "max_quantity": max_quantity
        }
        
        # Collect all prices and quantities for overall stats
        products_dict[key]["all_prices"].extend([avg_price] * post_count)
        products_dict[key]["all_quantities"].extend([total_quantity])
    
    # Process aggregated data
    results = []
    for key, product_data in products_dict.items():
        measure_units = product_data["measure_units"]
        all_prices = product_data["all_prices"]
        post_count = product_data["post_count"]
        
        # Calculate overall statistics accounting for different units
        if all_prices:
            # Weighted average price (weighted by post count per unit)
            weighted_avg = sum(p for p in all_prices) / len(all_prices) if all_prices else 0
            overall_min = min(p for unit_data in measure_units.values() for p in [unit_data["min_price"]] if p > 0)
            overall_max = max(p for unit_data in measure_units.values() for p in [unit_data["max_price"]])
        else:
            weighted_avg = 0
            overall_min = 0
            overall_max = 0
        
        # Calculate total quantity across all units
        total_qty = sum(unit_data["total_quantity"] for unit_data in measure_units.values())
        avg_qty = total_qty / post_count if post_count > 0 else 0
        min_qty = min(unit_data["min_quantity"] for unit_data in measure_units.values())
        max_qty = max(unit_data["max_quantity"] for unit_data in measure_units.values())
        
        price_range = overall_max - overall_min if overall_max > overall_min else 0
        price_spread_percentage = ((overall_max - overall_min) / weighted_avg * 100) if weighted_avg > 0 else 0
        
        # Get date range and price distribution in one query per product
        date_where_clauses = ["product_name_ru = ?", "category_id = ?"]
        date_params = [product_data["product_name"], product_data["category_id"]]
        
        if start_date:
            date_where_clauses.append("date(status_date) >= date(?)")
            date_params.append(start_date)
        
        if end_date:
            date_where_clauses.append("date(status_date) <= date(?)")
            date_params.append(end_date)
        
        date_where_clause = " AND ".join(date_where_clauses)
        
        cursor.execute(f'''
            SELECT 
                MIN(status_date) as earliest,
                MAX(status_date) as latest,
                COUNT(CASE WHEN unit_price < ? AND unit_price > 0 THEN 1 END) as low_price,
                COUNT(CASE WHEN unit_price >= ? AND unit_price < ? AND unit_price > 0 THEN 1 END) as mid_price,
                COUNT(CASE WHEN unit_price >= ? AND unit_price > 0 THEN 1 END) as high_price
            FROM offers
            WHERE {date_where_clause}
        ''', (
            weighted_avg * 0.7,
            weighted_avg * 0.7,
            weighted_avg * 1.3,
            weighted_avg * 1.3,
            *date_params
        ))
        date_price_row = cursor.fetchone()
        date_range = (date_price_row[0], date_price_row[1]) if date_price_row else (None, None)
        price_dist = (date_price_row[2] or 0, date_price_row[3] or 0, date_price_row[4] or 0) if date_price_row else (0, 0, 0)
        
        results.append({
            "product_name": product_data["product_name"],
            "category_id": product_data["category_id"],
            "category_name": None,  # Will be filled in batch
            "post_count": post_count,
            "avg_price": round(weighted_avg, 2),
            "min_price": overall_min,
            "max_price": overall_max,
            "price_range": round(price_range, 2),
            "price_spread_percentage": round(price_spread_percentage, 2),
            "total_quantity": total_qty,
            "avg_quantity": round(avg_qty, 2),
            "min_quantity": min_qty,
            "max_quantity": max_qty,
            "total_value": round(product_data["total_value"], 2),
            "with_certificate": product_data["with_certificate"],
            "certificate_percentage": round((product_data["with_certificate"] / post_count * 100) if post_count > 0 else 0, 2),
            "unique_measures": len(measure_units),
            "measure_units": measure_units,  # Detailed breakdown by unit
            "has_multiple_units": len(measure_units) > 1,
            "price_distribution": {
                "low": price_dist[0] or 0,
                "mid": price_dist[1] or 0,
                "high": price_dist[2] or 0
            },
            "earliest_post": date_range[0] if date_range[0] else None,
            "latest_post": date_range[1] if date_range[1] else None
        })
    
    # Sort by post count and limit BEFORE expensive queries
    results.sort(key=lambda x: x["post_count"], reverse=True)
    results = results[:limit]
    
    # Batch fetch date ranges and price distributions for all top products
    if results:
        # Build query to get all date ranges in one go
        product_keys = [(r["product_name"], r["category_id"]) for r in results]
        
        # Get date ranges for all products in fewer queries
        for result in results:
            date_where_clauses = ["product_name_ru = ?", "category_id = ?"]
            date_params = [result["product_name"], result["category_id"]]
            
            if start_date:
                date_where_clauses.append("status_date >= ?")
                date_params.append(start_date)
            
            if end_date:
                date_where_clauses.append("status_date <= ?")
                date_params.append(end_date)
            
            date_where_clause = " AND ".join(date_where_clauses)
            
            # Combine date range and price distribution into single query
            avg_price = result["avg_price"]
            cursor.execute(f'''
                SELECT 
                    MIN(status_date) as earliest,
                    MAX(status_date) as latest,
                    COUNT(CASE WHEN unit_price < ? AND unit_price > 0 THEN 1 END) as low_price,
                    COUNT(CASE WHEN unit_price >= ? AND unit_price < ? AND unit_price > 0 THEN 1 END) as mid_price,
                    COUNT(CASE WHEN unit_price >= ? AND unit_price > 0 THEN 1 END) as high_price
                FROM offers
                WHERE {date_where_clause}
            ''', (
                avg_price * 0.7,
                avg_price * 0.7,
                avg_price * 1.3,
                avg_price * 1.3,
                *date_params
            ))
            row = cursor.fetchone()
            if row:
                result["earliest_post"] = row[0]
                result["latest_post"] = row[1]
                result["price_distribution"] = {
                    "low": row[2] or 0,
                    "mid": row[3] or 0,
                    "high": row[4] or 0
                }
            else:
                result["earliest_post"] = None
                result["latest_post"] = None
                result["price_distribution"] = {"low": 0, "mid": 0, "high": 0}
    
    # Batch fetch category names to avoid N+1 queries
    if results:
        category_ids = list(set(r["category_id"] for r in results))
        placeholders = ','.join('?' * len(category_ids))
        cursor.execute(f'''
            SELECT id, name_ru FROM categories WHERE id IN ({placeholders})
        ''', category_ids)
        category_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Fill in category names
        for result in results:
            result["category_name"] = category_map.get(result["category_id"], "Unknown")
    
    return results

def get_price_distribution(conn) -> Dict[str, Any]:
    """Get price distribution statistics"""
    cursor = conn.cursor()
    
    # Get all prices for median calculation
    cursor.execute('''
        SELECT unit_price
        FROM offers
        WHERE unit_price > 0
        ORDER BY unit_price
    ''')
    prices = [row[0] for row in cursor.fetchall()]
    
    median = 0
    if prices:
        n = len(prices)
        if n % 2 == 0:
            median = (prices[n//2 - 1] + prices[n//2]) / 2
        else:
            median = prices[n//2]
    
    # Overall statistics
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            AVG(unit_price) as avg_price,
            MIN(unit_price) as min_price,
            MAX(unit_price) as max_price
        FROM offers
        WHERE unit_price > 0
    ''')
    
    row = cursor.fetchone()
    
    # Price ranges
    cursor.execute('''
        SELECT 
            COUNT(CASE WHEN unit_price < 100000 THEN 1 END),
            COUNT(CASE WHEN unit_price >= 100000 AND unit_price < 500000 THEN 1 END),
            COUNT(CASE WHEN unit_price >= 500000 AND unit_price < 1000000 THEN 1 END),
            COUNT(CASE WHEN unit_price >= 1000000 AND unit_price < 5000000 THEN 1 END),
            COUNT(CASE WHEN unit_price >= 5000000 AND unit_price < 10000000 THEN 1 END),
            COUNT(CASE WHEN unit_price >= 10000000 THEN 1 END)
        FROM offers
        WHERE unit_price > 0
    ''')
    
    price_ranges = cursor.fetchone()
    
    return {
        "total_offers": row[0],
        "avg_price": round(row[1] or 0, 2),
        "min_price": row[2] or 0,
        "max_price": row[3] or 0,
        "median_price": round(median, 2),
        "price_ranges": {
            "under_100k": price_ranges[0],
            "100k_500k": price_ranges[1],
            "500k_1m": price_ranges[2],
            "1m_5m": price_ranges[3],
            "5m_10m": price_ranges[4],
            "over_10m": price_ranges[5]
        }
    }

def get_recent_activity(conn, days: int = 7) -> Dict[str, Any]:
    """Get recent activity statistics based on offer posting date (status_date)"""
    cursor = conn.cursor()
    
    # Parse dates and count by day - using status_date
    cursor.execute('''
        SELECT 
            date(status_date) as date,
            COUNT(*) as count
        FROM offers
        WHERE status_date >= datetime('now', '-' || ? || ' days')
        AND status_date IS NOT NULL
        GROUP BY date(status_date)
        ORDER BY date DESC
    ''', (days,))
    
    daily_activity = []
    for row in cursor.fetchall():
        daily_activity.append({
            "date": row[0],
            "count": row[1]
        })
    
    # Total new offers in period - using status_date
    cursor.execute('''
        SELECT COUNT(*) 
        FROM offers
        WHERE status_date >= datetime('now', '-' || ? || ' days')
        AND status_date IS NOT NULL
    ''', (days,))
    
    total_recent = cursor.fetchone()[0]
    
    # By category - using status_date
    cursor.execute('''
        SELECT 
            c.name_ru,
            COUNT(o.id) as count
        FROM offers o
        JOIN categories c ON o.category_id = c.id
        WHERE o.status_date >= datetime('now', '-' || ? || ' days')
        AND o.status_date IS NOT NULL
        GROUP BY c.id, c.name_ru
        ORDER BY count DESC
        LIMIT 10
    ''', (days,))
    
    category_activity = []
    for row in cursor.fetchall():
        category_activity.append({
            "category": row[0],
            "count": row[1]
        })
    
    return {
        "period_days": days,
        "total_new_offers": total_recent,
        "daily_activity": daily_activity,
        "top_categories": category_activity
    }

def get_certificate_statistics(conn) -> Dict[str, Any]:
    """Get certificate requirement statistics"""
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN is_certificate = 1 THEN 1 ELSE 0 END) as with_certificate,
            SUM(CASE WHEN is_certificate = 0 THEN 1 ELSE 0 END) as without_certificate
        FROM offers
    ''')
    
    row = cursor.fetchone()
    
    return {
        "total": row[0],
        "with_certificate": row[1],
        "without_certificate": row[2],
        "certificate_percentage": round((row[1] / row[0] * 100) if row[0] > 0 else 0, 2)
    }

def get_quantity_statistics(conn) -> Dict[str, Any]:
    """Get product quantity statistics"""
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            AVG(product_quantity) as avg_quantity,
            MIN(product_quantity) as min_quantity,
            MAX(product_quantity) as max_quantity,
            SUM(product_quantity) as total_quantity
        FROM offers
        WHERE product_quantity > 0
    ''')
    
    row = cursor.fetchone()
    
    return {
        "avg_quantity": round(row[0] or 0, 2),
        "min_quantity": row[1] or 0,
        "max_quantity": row[2] or 0,
        "total_quantity": row[3] or 0
    }

def get_measure_statistics(conn) -> Dict[str, Any]:
    """Get measurement unit (measureName) statistics"""
    cursor = conn.cursor()
    
    # Get distribution of measurement units
    cursor.execute('''
        SELECT 
            measure_name,
            COUNT(*) as count,
            AVG(unit_price) as avg_price,
            SUM(product_quantity) as total_quantity,
            AVG(product_quantity) as avg_quantity
        FROM offers
        WHERE measure_name IS NOT NULL AND measure_name != ''
        GROUP BY measure_name
        ORDER BY count DESC
    ''')
    
    measures = []
    for row in cursor.fetchall():
        measures.append({
            "measure_name": row[0] or "Unknown",
            "count": row[1],
            "avg_price": round(row[2] or 0, 2),
            "total_quantity": row[3] or 0,
            "avg_quantity": round(row[4] or 0, 2)
        })
    
    # Get top measures by category
    cursor.execute('''
        SELECT 
            c.name_ru as category_name,
            o.measure_name,
            COUNT(*) as count
        FROM offers o
        JOIN categories c ON o.category_id = c.id
        WHERE o.measure_name IS NOT NULL AND o.measure_name != ''
        GROUP BY c.id, c.name_ru, o.measure_name
        ORDER BY count DESC
        LIMIT 30
    ''')
    
    category_measures = []
    for row in cursor.fetchall():
        category_measures.append({
            "category": row[0],
            "measure_name": row[1],
            "count": row[2]
        })
    
    # Overall statistics
    cursor.execute('''
        SELECT 
            COUNT(DISTINCT measure_name) as unique_measures,
            COUNT(*) as total_with_measure
        FROM offers
        WHERE measure_name IS NOT NULL AND measure_name != ''
    ''')
    
    stats_row = cursor.fetchone()
    
    return {
        "unique_measures": stats_row[0],
        "total_with_measure": stats_row[1],
        "top_measures": measures[:20],  # Top 20
        "category_measures": category_measures
    }

def get_daily_posting_trends(conn, days: int = 30) -> Dict[str, Any]:
    """Get daily posting trends with product breakdown based on offer posting date (status_date)"""
    cursor = conn.cursor()
    
    # Get daily posting counts - using status_date (when offer was posted)
    cursor.execute('''
        SELECT 
            date(status_date) as date,
            COUNT(*) as count
        FROM offers
        WHERE status_date >= datetime('now', '-' || ? || ' days')
        AND status_date IS NOT NULL
        GROUP BY date(status_date)
        ORDER BY date DESC
    ''', (days,))
    
    daily_counts = []
    for row in cursor.fetchall():
        daily_counts.append({
            "date": row[0],
            "count": row[1]
        })
    
    # Get yesterday's data - using status_date
    cursor.execute('''
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT category_id) as categories,
            COUNT(DISTINCT product_name_ru) as unique_products
        FROM offers
        WHERE date(status_date) = date('now', '-1 day')
        AND status_date IS NOT NULL
    ''')
    yesterday_row = cursor.fetchone()
    yesterday_stats = {
        "total_offers": yesterday_row[0] if yesterday_row else 0,
        "categories": yesterday_row[1] if yesterday_row else 0,
        "unique_products": yesterday_row[2] if yesterday_row else 0
    }
    
    # Get yesterday's top products - using status_date
    cursor.execute('''
        SELECT 
            product_name_ru,
            COUNT(*) as count,
            AVG(unit_price) as avg_price
        FROM offers
        WHERE date(status_date) = date('now', '-1 day')
        AND status_date IS NOT NULL
        AND product_name_ru IS NOT NULL AND product_name_ru != ''
        GROUP BY product_name_ru
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    yesterday_products = []
    for row in cursor.fetchall():
        yesterday_products.append({
            "product_name": row[0],
            "count": row[1],
            "avg_price": round(row[2] or 0, 2)
        })
    
    # Get yesterday's top categories - using status_date
    cursor.execute('''
        SELECT 
            c.name_ru,
            COUNT(o.id) as count
        FROM offers o
        JOIN categories c ON o.category_id = c.id
        WHERE date(o.status_date) = date('now', '-1 day')
        AND o.status_date IS NOT NULL
        GROUP BY c.id, c.name_ru
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    yesterday_categories = []
    for row in cursor.fetchall():
        yesterday_categories.append({
            "category": row[0],
            "count": row[1]
        })
    
    return {
        "period_days": days,
        "daily_counts": daily_counts,
        "yesterday": yesterday_stats,
        "yesterday_products": yesterday_products,
        "yesterday_categories": yesterday_categories
    }

def get_trending_products(conn, days: int = 7) -> Dict[str, Any]:
    """Get trending products - most posted in recent period based on offer posting date (status_date)"""
    cursor = conn.cursor()
    
    # Get products posted in the period - using status_date
    cursor.execute('''
        SELECT 
            product_name_ru,
            category_id,
            COUNT(*) as recent_count,
            AVG(unit_price) as avg_price,
            MIN(status_date) as first_seen,
            MAX(status_date) as last_seen
        FROM offers
        WHERE status_date >= datetime('now', '-' || ? || ' days')
        AND status_date IS NOT NULL
        AND product_name_ru IS NOT NULL AND product_name_ru != ''
        GROUP BY product_name_ru, category_id
        ORDER BY recent_count DESC
        LIMIT 30
    ''', (days,))
    
    trending = []
    category_ids = []
    for row in cursor.fetchall():
        category_ids.append(row[1])
        trending.append({
            "product_name": row[0],
            "category_id": row[1],
            "category_name": None,  # Will be filled in batch
            "recent_count": row[2],
            "avg_price": round(row[3] or 0, 2),
            "first_seen": row[4],
            "last_seen": row[5]
        })
    
    # Batch fetch category names
    if category_ids:
        unique_category_ids = list(set(category_ids))
        placeholders = ','.join('?' * len(unique_category_ids))
        cursor.execute(f'''
            SELECT id, name_ru FROM categories WHERE id IN ({placeholders})
        ''', unique_category_ids)
        category_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Fill in category names
        for item in trending:
            item["category_name"] = category_map.get(item["category_id"], "Unknown")
    
    return {
        "period_days": days,
        "trending_products": trending
    }

def get_weekly_monthly_trends(conn) -> Dict[str, Any]:
    """Get weekly and monthly trends for most demanded products based on offer posting date (status_date)"""
    cursor = conn.cursor()
    
    # Use a single query with UNION to get both weekly and monthly data efficiently
    # Weekly trends (last 7 days) - using status_date (when offer was posted)
    cursor.execute('''
        SELECT 
            product_name_ru,
            COUNT(*) as count,
            AVG(unit_price) as avg_price,
            SUM(product_quantity) as total_quantity
        FROM offers
        WHERE status_date >= datetime('now', '-7 days')
        AND status_date IS NOT NULL
        AND product_name_ru IS NOT NULL AND product_name_ru != ''
        GROUP BY product_name_ru
        ORDER BY count DESC
        LIMIT 20
    ''')
    
    weekly_products = []
    for row in cursor.fetchall():
        weekly_products.append({
            "product_name": row[0],
            "count": row[1],
            "avg_price": round(row[2] or 0, 2),
            "total_quantity": row[3] or 0
        })
    
    # Monthly trends (last 30 days) - using status_date
    cursor.execute('''
        SELECT 
            product_name_ru,
            COUNT(*) as count,
            AVG(unit_price) as avg_price,
            SUM(product_quantity) as total_quantity
        FROM offers
        WHERE status_date >= datetime('now', '-30 days')
        AND status_date IS NOT NULL
        AND product_name_ru IS NOT NULL AND product_name_ru != ''
        GROUP BY product_name_ru
        ORDER BY count DESC
        LIMIT 20
    ''')
    
    monthly_products = []
    for row in cursor.fetchall():
        monthly_products.append({
            "product_name": row[0],
            "count": row[1],
            "avg_price": round(row[2] or 0, 2),
            "total_quantity": row[3] or 0
        })
    
    # Weekly category trends - using status_date (optimized with JOIN)
    cursor.execute('''
        SELECT 
            c.name_ru,
            COUNT(o.id) as count,
            AVG(o.unit_price) as avg_price
        FROM offers o
        INNER JOIN categories c ON o.category_id = c.id
        WHERE o.status_date >= datetime('now', '-7 days')
        AND o.status_date IS NOT NULL
        GROUP BY c.id, c.name_ru
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    weekly_categories = []
    for row in cursor.fetchall():
        weekly_categories.append({
            "category": row[0],
            "count": row[1],
            "avg_price": round(row[2] or 0, 2)
        })
    
    # Monthly category trends - using status_date (optimized with JOIN)
    cursor.execute('''
        SELECT 
            c.name_ru,
            COUNT(o.id) as count,
            AVG(o.unit_price) as avg_price
        FROM offers o
        INNER JOIN categories c ON o.category_id = c.id
        WHERE o.status_date >= datetime('now', '-30 days')
        AND o.status_date IS NOT NULL
        GROUP BY c.id, c.name_ru
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    monthly_categories = []
    for row in cursor.fetchall():
        monthly_categories.append({
            "category": row[0],
            "count": row[1],
            "avg_price": round(row[2] or 0, 2)
        })
    
    return {
        "weekly": {
            "products": weekly_products,
            "categories": weekly_categories
        },
        "monthly": {
            "products": monthly_products,
            "categories": monthly_categories
        }
    }

def get_all_insights(conn) -> Dict[str, Any]:
    """Get all insights - optimized to reduce redundant queries"""
    print("Generating insights...")
    
    # Get price distribution first (used in summary)
    print("  - Price distribution...")
    price_dist = get_price_distribution(conn)
    
    # Get category statistics (used in summary)
    print("  - Category statistics...")
    category_stats = get_category_statistics(conn)
    
    # Get other statistics in parallel where possible
    print("  - Recent activity...")
    recent_activity = get_recent_activity(conn, 7)
    
    print("  - Certificate statistics...")
    cert_stats = get_certificate_statistics(conn)
    
    print("  - Quantity statistics...")
    qty_stats = get_quantity_statistics(conn)
    
    print("  - Measure statistics...")
    measure_stats = get_measure_statistics(conn)
    
    print("  - Top products...")
    top_products = get_top_products(conn, 30)
    
    print("  - Daily trends...")
    daily_trends = get_daily_posting_trends(conn, 30)
    
    print("  - Trending products...")
    trending = get_trending_products(conn, 7)
    
    print("  - Weekly/Monthly trends...")
    weekly_monthly = get_weekly_monthly_trends(conn)
    
    print("✓ All insights generated")
    
    return {
        "timestamp": datetime.now().isoformat(),
        "category_statistics": category_stats,
        "top_products": top_products,
        "price_distribution": price_dist,
        "recent_activity": recent_activity,
        "certificate_statistics": cert_stats,
        "quantity_statistics": qty_stats,
        "measure_statistics": measure_stats,
        "daily_trends": daily_trends,
        "trending_products": trending,
        "weekly_monthly_trends": weekly_monthly,
        "summary": {
            "total_categories": len(category_stats),
            "total_offers": price_dist["total_offers"]
        }
    }

if __name__ == "__main__":
    conn = get_connection()
    insights = get_all_insights(conn)
    
    print("=" * 80)
    print("MARKET INSIGHTS - COOPERATION.UZ")
    print("=" * 80)
    print(f"\nGenerated at: {insights['timestamp']}")
    print(f"\nTotal Categories: {insights['summary']['total_categories']}")
    print(f"Total Offers: {insights['summary']['total_offers']:,}")
    
    print("\n" + "=" * 80)
    print("TOP 10 CATEGORIES BY OFFER COUNT")
    print("=" * 80)
    for i, cat in enumerate(insights['category_statistics'][:10], 1):
        print(f"{i:2d}. {cat['category_name'][:60]:60s} {cat['offer_count']:>6,} offers | "
              f"Avg Price: {cat['avg_price']:>12,.0f} UZS")
    
    print("\n" + "=" * 80)
    print("TOP 10 MOST POSTED PRODUCTS")
    print("=" * 80)
    for i, product in enumerate(insights['top_products'][:10], 1):
        print(f"{i:2d}. {product['product_name'][:50]:50s} {product['post_count']:>4} posts | "
              f"Avg Price: {product['avg_price']:>12,.0f} UZS")
    
    print("\n" + "=" * 80)
    print("PRICE STATISTICS")
    print("=" * 80)
    price_stats = insights['price_distribution']
    print(f"Average Price: {price_stats['avg_price']:,.0f} UZS")
    print(f"Median Price: {price_stats['median_price']:,.0f} UZS")
    print(f"Min Price: {price_stats['min_price']:,.0f} UZS")
    print(f"Max Price: {price_stats['max_price']:,.0f} UZS")
    
    print("\nPrice Distribution:")
    ranges = price_stats['price_ranges']
    print(f"  Under 100K:     {ranges['under_100k']:>6,}")
    print(f"  100K - 500K:    {ranges['100k_500k']:>6,}")
    print(f"  500K - 1M:      {ranges['500k_1m']:>6,}")
    print(f"  1M - 5M:        {ranges['1m_5m']:>6,}")
    print(f"  5M - 10M:       {ranges['5m_10m']:>6,}")
    print(f"  Over 10M:       {ranges['over_10m']:>6,}")
    
    print("\n" + "=" * 80)
    print("RECENT ACTIVITY (Last 7 Days)")
    print("=" * 80)
    recent = insights['recent_activity']
    print(f"New Offers: {recent['total_new_offers']:,}")
    print("\nTop Categories (Last 7 Days):")
    for i, cat in enumerate(recent['top_categories'][:5], 1):
        print(f"  {i}. {cat['category'][:60]:60s} {cat['count']:>4} new offers")
    
    print("\n" + "=" * 80)
    print("MEASUREMENT UNITS (MEASURENAME) STATISTICS")
    print("=" * 80)
    measure_stats = insights['measure_statistics']
    print(f"Unique Measurement Units: {measure_stats['unique_measures']}")
    print(f"Offers with Measurement: {measure_stats['total_with_measure']:,}")
    print("\nTop 10 Measurement Units:")
    for i, measure in enumerate(measure_stats['top_measures'][:10], 1):
        print(f"  {i:2d}. {measure['measure_name']:20s} {measure['count']:>6,} offers | "
              f"Avg Price: {measure['avg_price']:>12,.0f} UZS | "
              f"Total Qty: {measure['total_quantity']:>12,.0f}")
    
    conn.close()

