from flask import Flask, render_template, jsonify, request
from flask_caching import Cache
import sqlite3
from analyze_data import get_all_insights, get_connection
from datetime import datetime
import json

app = Flask(__name__)
DB_NAME = "cooperation_data.db"

# Configure caching
cache_config = {
    "CACHE_TYPE": "SimpleCache",  # In-memory cache
    "CACHE_DEFAULT_TIMEOUT": 300  # 5 minutes
}
app.config.from_mapping(cache_config)
cache = Cache(app)

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/insights')
@cache.cached(timeout=300)  # Cache for 5 minutes
def api_insights():
    """Get all insights as JSON"""
    conn = get_connection()
    try:
        print("Generating insights...")
        insights = get_all_insights(conn)
        print("Insights generated successfully")
        return jsonify(insights)
    except Exception as e:
        import traceback
        error_msg = str(e)
        print(f"ERROR in api_insights: {error_msg}")
        traceback.print_exc()
        return jsonify({
            "error": error_msg,
            "message": "Failed to generate insights. Check server logs for details."
        }), 500
    finally:
        conn.close()

@app.route('/api/categories')
@cache.cached(timeout=300)
def api_categories():
    """Get category statistics"""
    conn = get_connection()
    try:
        from analyze_data import get_category_statistics
        categories = get_category_statistics(conn)
        return jsonify(categories)
    finally:
        conn.close()

@app.route('/api/top-products')
@cache.cached(timeout=300, query_string=True)  # Cache based on query parameters
def api_top_products():
    """Get top products with optional date filtering"""
    conn = get_connection()
    try:
        from analyze_data import get_top_products
        
        # Get date filter parameters from query string
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        limit = request.args.get('limit', 50, type=int)
        
        products = get_top_products(conn, limit=limit, start_date=start_date, end_date=end_date)
        return jsonify(products)
    finally:
        conn.close()

@app.route('/api/recent-activity')
def api_recent_activity():
    """Get recent activity"""
    conn = get_connection()
    try:
        from analyze_data import get_recent_activity
        activity = get_recent_activity(conn, 7)
        return jsonify(activity)
    finally:
        conn.close()

@app.route('/api/price-stats')
def api_price_stats():
    """Get price statistics"""
    conn = get_connection()
    try:
        from analyze_data import get_price_distribution
        stats = get_price_distribution(conn)
        return jsonify(stats)
    finally:
        conn.close()

@app.route('/api/measure-stats')
def api_measure_stats():
    """Get measurement unit statistics"""
    conn = get_connection()
    try:
        from analyze_data import get_measure_statistics
        stats = get_measure_statistics(conn)
        return jsonify(stats)
    finally:
        conn.close()

@app.route('/api/daily-trends')
def api_daily_trends():
    """Get daily posting trends"""
    conn = get_connection()
    try:
        from analyze_data import get_daily_posting_trends
        trends = get_daily_posting_trends(conn, 30)
        return jsonify(trends)
    finally:
        conn.close()

@app.route('/api/trending-products')
def api_trending_products():
    """Get trending products"""
    conn = get_connection()
    try:
        from analyze_data import get_trending_products
        trending = get_trending_products(conn, 7)
        return jsonify(trending)
    finally:
        conn.close()

@app.route('/api/weekly-monthly-trends')
def api_weekly_monthly_trends():
    """Get weekly and monthly trends"""
    conn = get_connection()
    try:
        from analyze_data import get_weekly_monthly_trends
        trends = get_weekly_monthly_trends(conn)
        return jsonify(trends)
    finally:
        conn.close()

@app.route('/api/update-status')
def api_update_status():
    """Get real-time update service status"""
    try:
        import os
        import json
        from datetime import datetime
        
        status = {
            "service_running": False,
            "last_update": None,
            "last_success": None,
            "total_updates": 0,
            "current_status": "unknown"
        }
        
        # Check if status file exists
        if os.path.exists("update_status.json"):
            with open("update_status.json", 'r') as f:
                status = json.load(f)
        
        # Check if cache should be cleared
        if os.path.exists("cache_control.json"):
            with open("cache_control.json", 'r') as f:
                cache_control = json.load(f)
                if cache_control.get("should_refresh", False):
                    # Clear Flask cache
                    cache.clear()
                    # Reset the flag
                    cache_control["should_refresh"] = False
                    with open("cache_control.json", 'w') as cf:
                        json.dump(cache_control, cf)
                    status["cache_just_cleared"] = True
        
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/clear-cache', methods=['POST'])
def api_clear_cache():
    """Manually clear the cache"""
    try:
        cache.clear()
        return jsonify({"success": True, "message": "Cache cleared successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("=" * 80)
    print("ПАНЕЛЬ УПРАВЛЕНИЯ РЫНКОМ COOPERATION.UZ")
    print("=" * 80)
    print("\n📊 Оптимизация производительности включена:")
    print("  ✓ Кэширование API (5 мин)")
    print("  ✓ Индексы базы данных")
    print("  ✓ Оптимизированные SQL-запросы")
    print("  ✓ Автоматические обновления")
    print("\nЗапуск сервера панели управления...")
    print("Откройте браузер: http://localhost:5000")
    print("\nНажмите Ctrl+C для остановки сервера")
    print("=" * 80)
    app.run(debug=True, host='0.0.0.0', port=5000)

