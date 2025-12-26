from analyze_data import get_connection, get_all_insights
import traceback

try:
    conn = get_connection()
    print("Testing get_all_insights...")
    insights = get_all_insights(conn)
    print("✓ Success - all insights generated")
    print(f"Keys: {list(insights.keys())}")
    
    # Check required fields
    required_fields = ['daily_trends', 'trending_products', 'weekly_monthly_trends']
    for field in required_fields:
        if field in insights:
            print(f"✓ {field}: present")
        else:
            print(f"✗ {field}: MISSING")
    
    conn.close()
    print("\nAll tests passed!")
except Exception as e:
    print(f"✗ Error: {e}")
    traceback.print_exc()




