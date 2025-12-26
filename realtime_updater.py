#!/usr/bin/env python3
"""
Real-time Database Updater Service

This service runs continuously in the background and automatically updates
the database with new data from cooperation.uz at regular intervals.

Features:
- Runs every N minutes (configurable)
- Only fetches new/changed data
- Clears dashboard cache when updates occur
- Tracks update status and statistics
- Handles errors gracefully
- Can run as a service/daemon
"""

import time
import sqlite3
import json
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import threading
import signal

# Import the update function
from update_database import (
    fetch_categories,
    update_category,
    fetch_all_offers_for_category,
    insert_or_update_offers,
    get_existing_offer_ids,
    remove_deleted_offers
)

DB_NAME = "cooperation_data.db"
STATUS_FILE = "update_status.json"

# Configuration
UPDATE_INTERVAL_MINUTES = 15  # Update every 15 minutes
MAX_CATEGORIES_PER_CYCLE = None  # None = all categories, or set a number to limit

class RealTimeUpdater:
    def __init__(self, interval_minutes=UPDATE_INTERVAL_MINUTES):
        self.interval_seconds = interval_minutes * 60
        self.running = False
        self.thread = None
        self.stats = {
            "last_update": None,
            "last_success": None,
            "last_error": None,
            "total_updates": 0,
            "total_new_offers": 0,
            "total_updated_offers": 0,
            "total_deleted_offers": 0,
            "current_status": "stopped"
        }
        self.load_stats()
    
    def load_stats(self):
        """Load statistics from file"""
        if os.path.exists(STATUS_FILE):
            try:
                with open(STATUS_FILE, 'r') as f:
                    saved_stats = json.load(f)
                    self.stats.update(saved_stats)
            except:
                pass
    
    def save_stats(self):
        """Save statistics to file"""
        try:
            with open(STATUS_FILE, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except:
            pass
    
    def clear_dashboard_cache(self):
        """Clear the dashboard cache by updating a cache control file"""
        try:
            cache_control = {
                "last_update": datetime.now().isoformat(),
                "should_refresh": True
            }
            with open("cache_control.json", 'w') as f:
                json.dump(cache_control, f)
        except:
            pass
    
    def update_data(self) -> Dict[str, Any]:
        """Perform a single update cycle"""
        print(f"\n{'='*80}")
        print(f"UPDATE CYCLE STARTED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        cycle_stats = {
            "new_offers": 0,
            "updated_offers": 0,
            "deleted_offers": 0,
            "categories_processed": 0,
            "errors": []
        }
        
        conn = None
        try:
            conn = sqlite3.connect(DB_NAME)
            
            # Fetch categories
            print("📥 Fetching categories...")
            categories = fetch_categories()
            print(f"✓ Found {len(categories)} categories\n")
            
            # Update categories in database
            for category in categories:
                try:
                    update_category(conn, category)
                except Exception as e:
                    print(f"⚠ Error updating category {category.get('id')}: {e}")
            
            # Determine which categories to update
            categories_to_update = categories
            if MAX_CATEGORIES_PER_CYCLE and len(categories) > MAX_CATEGORIES_PER_CYCLE:
                # Rotate through categories - update different ones each cycle
                cycle_num = self.stats.get("total_updates", 0)
                start_idx = (cycle_num * MAX_CATEGORIES_PER_CYCLE) % len(categories)
                categories_to_update = categories[start_idx:start_idx + MAX_CATEGORIES_PER_CYCLE]
                print(f"📋 Updating {len(categories_to_update)} of {len(categories)} categories this cycle")
            
            # Update offers for each category
            for category in categories_to_update:
                cat_id = category.get("id")
                cat_name = category.get("name", {}).get("ru", "Unknown")
                
                print(f"\n📦 Processing: {cat_name} (ID: {cat_id})")
                
                try:
                    # Get existing offer IDs
                    existing_ids = get_existing_offer_ids(conn, cat_id)
                    
                    # Fetch all current offers
                    offers = fetch_all_offers_for_category(cat_id)
                    
                    if not offers:
                        print(f"   ℹ No offers found")
                        cycle_stats["categories_processed"] += 1
                        continue
                    
                    # Insert or update offers
                    stats = insert_or_update_offers(conn, offers, cat_id, existing_ids)
                    
                    cycle_stats["new_offers"] += stats["new"]
                    cycle_stats["updated_offers"] += stats["updated"]
                    
                    print(f"   ✓ New: {stats['new']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")
                    
                    # Remove deleted offers
                    current_ids = {offer.get("id") for offer in offers}
                    deleted = remove_deleted_offers(conn, cat_id, current_ids)
                    cycle_stats["deleted_offers"] += deleted
                    
                    if deleted > 0:
                        print(f"   🗑 Removed {deleted} deleted offers")
                    
                    cycle_stats["categories_processed"] += 1
                    
                except Exception as e:
                    error_msg = f"Error processing category {cat_id}: {str(e)}"
                    print(f"   ❌ {error_msg}")
                    cycle_stats["errors"].append(error_msg)
            
            conn.close()
            
            # Update global stats
            self.stats["last_update"] = datetime.now().isoformat()
            self.stats["last_success"] = datetime.now().isoformat()
            self.stats["total_updates"] += 1
            self.stats["total_new_offers"] += cycle_stats["new_offers"]
            self.stats["total_updated_offers"] += cycle_stats["updated_offers"]
            self.stats["total_deleted_offers"] += cycle_stats["deleted_offers"]
            
            # Clear dashboard cache if data changed
            if cycle_stats["new_offers"] > 0 or cycle_stats["updated_offers"] > 0 or cycle_stats["deleted_offers"] > 0:
                self.clear_dashboard_cache()
                print(f"\n🔄 Dashboard cache cleared")
            
            self.save_stats()
            
            print(f"\n{'='*80}")
            print(f"UPDATE CYCLE COMPLETED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}")
            print(f"Summary:")
            print(f"  • Categories processed: {cycle_stats['categories_processed']}")
            print(f"  • New offers: {cycle_stats['new_offers']}")
            print(f"  • Updated offers: {cycle_stats['updated_offers']}")
            print(f"  • Deleted offers: {cycle_stats['deleted_offers']}")
            if cycle_stats["errors"]:
                print(f"  • Errors: {len(cycle_stats['errors'])}")
            print(f"{'='*80}\n")
            
            return cycle_stats
            
        except Exception as e:
            error_msg = f"Critical error in update cycle: {str(e)}"
            print(f"\n❌ {error_msg}\n")
            
            self.stats["last_error"] = datetime.now().isoformat()
            self.stats["last_error_message"] = error_msg
            self.save_stats()
            
            if conn:
                conn.close()
            
            cycle_stats["errors"].append(error_msg)
            return cycle_stats
    
    def run_loop(self):
        """Main update loop"""
        print(f"\n{'='*80}")
        print(f"REAL-TIME UPDATER SERVICE STARTED")
        print(f"{'='*80}")
        print(f"Update interval: {self.interval_seconds // 60} minutes")
        print(f"Press Ctrl+C to stop")
        print(f"{'='*80}\n")
        
        self.stats["current_status"] = "running"
        self.save_stats()
        
        while self.running:
            try:
                # Perform update
                self.update_data()
                
                # Wait for next cycle
                print(f"⏰ Next update in {self.interval_seconds // 60} minutes...")
                print(f"   Waiting until {(datetime.now() + timedelta(seconds=self.interval_seconds)).strftime('%H:%M:%S')}")
                
                # Sleep in small intervals to allow for clean shutdown
                for _ in range(self.interval_seconds):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n\n⏹ Stopping service...")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")
                print("Waiting 5 minutes before retry...")
                time.sleep(300)  # Wait 5 minutes before retry
        
        self.stats["current_status"] = "stopped"
        self.save_stats()
        
        print(f"\n{'='*80}")
        print(f"SERVICE STOPPED")
        print(f"{'='*80}\n")
    
    def start(self):
        """Start the updater service"""
        if self.running:
            print("Service is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self.run_loop, daemon=False)
        self.thread.start()
    
    def stop(self):
        """Stop the updater service"""
        print("\nStopping service...")
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
    
    def run_once(self):
        """Run a single update cycle (for testing)"""
        print("Running single update cycle...\n")
        return self.update_data()


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n\nReceived interrupt signal...')
    sys.exit(0)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time Database Updater Service')
    parser.add_argument('--interval', type=int, default=UPDATE_INTERVAL_MINUTES,
                       help=f'Update interval in minutes (default: {UPDATE_INTERVAL_MINUTES})')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit (for testing)')
    parser.add_argument('--max-categories', type=int, default=None,
                       help='Max categories to process per cycle (default: all)')
    
    args = parser.parse_args()
    
    # Set global config
    global MAX_CATEGORIES_PER_CYCLE
    MAX_CATEGORIES_PER_CYCLE = args.max_categories
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create updater instance
    updater = RealTimeUpdater(interval_minutes=args.interval)
    
    if args.once:
        # Run once and exit
        updater.run_once()
    else:
        # Run continuously
        try:
            updater.start()
            # Keep main thread alive
            while updater.running:
                time.sleep(1)
        except KeyboardInterrupt:
            updater.stop()
        finally:
            updater.stop()


if __name__ == "__main__":
    main()




