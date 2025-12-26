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
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import the update function
from update_database import (
    update_category,
    fetch_all_offers_for_category,
    insert_or_update_offers,
    get_existing_offer_ids,
    remove_deleted_offers
)

DB_NAME = "cooperation_data.db"
STATUS_FILE = "update_status.json"
BASE_URL = "https://new.cooperation.uz/ocelot/api-client/Client"

# Configuration
UPDATE_INTERVAL_MINUTES = 15  # Update every 15 minutes
MAX_CATEGORIES_PER_CYCLE = None  # None = all categories, or set a number to limit
REQUEST_TIMEOUT = 60  # Increase timeout to 60 seconds
MAX_RETRIES = 3  # Retry failed requests up to 3 times

def create_session_with_retries():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,  # Wait 2, 4, 8 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_categories_with_retry():
    """Fetch all categories from the API with retry logic"""
    url = f"{BASE_URL}/GetAllTnVedCategory"
    params = {"take": 100, "skip": 0}
    
    session = create_session_with_retries()
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  Попытка {attempt + 1}/{MAX_RETRIES} получения категорий...")
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            if data.get("statusCode") == 200 and "result" in data:
                categories = data["result"].get("data", [])
                print(f"  ✓ Получено {len(categories)} категорий")
                return categories
            else:
                print(f"  ⚠ Неверный формат ответа")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                return []
        except requests.exceptions.Timeout:
            print(f"  ⏱ Превышено время ожидания (попытка {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) * 2  # Exponential backoff: 2, 4, 8 seconds
                print(f"  Ожидание {wait_time} секунд перед повтором...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Не удалось получить категории после {MAX_RETRIES} попыток")
                return []
        except requests.exceptions.ConnectionError as e:
            print(f"  ⚠ Ошибка подключения: {str(e)[:100]}")
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) * 2
                print(f"  Ожидание {wait_time} секунд перед повтором...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ Не удалось подключиться после {MAX_RETRIES} попыток")
                return []
        except Exception as e:
            print(f"  ❌ Неожиданная ошибка: {str(e)[:100]}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
            else:
                return []
    
    return []

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
        print(f"ЦИКЛ ОБНОВЛЕНИЯ ЗАПУЩЕН - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            
            # Fetch categories with retry logic
            print("📥 Получение категорий с сервера...")
            categories = fetch_categories_with_retry()
            
            if not categories:
                error_msg = "Не удалось получить категории. Возможные причины: проблемы с сетью, сервер недоступен."
                print(f"\n⚠ {error_msg}")
                print("💡 Рекомендации:")
                print("   • Проверьте подключение к интернету")
                print("   • Проверьте доступность cooperation.uz")
                print("   • Попробуйте позже")
                
                cycle_stats["errors"].append(error_msg)
                self.stats["last_error"] = datetime.now().isoformat()
                self.stats["last_error_message"] = error_msg
                self.save_stats()
                
                if conn:
                    conn.close()
                return cycle_stats
            
            print(f"✓ Получено {len(categories)} категорий\n")
            
            # Update categories in database
            for category in categories:
                try:
                    update_category(conn, category)
                except Exception as e:
                    print(f"⚠ Ошибка обновления категории {category.get('id')}: {e}")
            
            # Determine which categories to update
            categories_to_update = categories
            if MAX_CATEGORIES_PER_CYCLE and len(categories) > MAX_CATEGORIES_PER_CYCLE:
                # Rotate through categories - update different ones each cycle
                cycle_num = self.stats.get("total_updates", 0)
                start_idx = (cycle_num * MAX_CATEGORIES_PER_CYCLE) % len(categories)
                categories_to_update = categories[start_idx:start_idx + MAX_CATEGORIES_PER_CYCLE]
                print(f"📋 Обновление {len(categories_to_update)} из {len(categories)} категорий в этом цикле")
            
            # Update offers for each category
            for idx, category in enumerate(categories_to_update, 1):
                cat_id = category.get("id")
                cat_name = category.get("name", {}).get("ru", "Unknown")
                
                print(f"\n📦 [{idx}/{len(categories_to_update)}] Обработка: {cat_name} (ID: {cat_id})")
                
                try:
                    # Get existing offer IDs
                    existing_ids = get_existing_offer_ids(conn, cat_id)
                    
                    # Fetch all current offers with retry
                    offers = fetch_all_offers_for_category(cat_id)
                    
                    if not offers:
                        print(f"   ℹ Предложений не найдено или ошибка получения")
                        cycle_stats["categories_processed"] += 1
                        continue
                    
                    # Insert or update offers
                    stats = insert_or_update_offers(conn, offers, cat_id, existing_ids)
                    
                    cycle_stats["new_offers"] += stats["new"]
                    cycle_stats["updated_offers"] += stats["updated"]
                    
                    print(f"   ✓ Новых: {stats['new']}, Обновлено: {stats['updated']}, Без изменений: {stats['unchanged']}")
                    
                    # Remove deleted offers
                    current_ids = {offer.get("id") for offer in offers}
                    deleted = remove_deleted_offers(conn, cat_id, current_ids)
                    cycle_stats["deleted_offers"] += deleted
                    
                    if deleted > 0:
                        print(f"   🗑 Удалено {deleted} предложений")
                    
                    cycle_stats["categories_processed"] += 1
                    
                except Exception as e:
                    error_msg = f"Ошибка обработки категории {cat_id}: {str(e)[:100]}"
                    print(f"   ❌ {error_msg}")
                    cycle_stats["errors"].append(error_msg)
                    # Continue with next category instead of failing completely
                    continue
            
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
                print(f"\n🔄 Кэш панели очищен")
            
            self.save_stats()
            
            print(f"\n{'='*80}")
            print(f"ЦИКЛ ОБНОВЛЕНИЯ ЗАВЕРШЕН - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}")
            print(f"Итого:")
            print(f"  • Обработано категорий: {cycle_stats['categories_processed']}")
            print(f"  • Новых предложений: {cycle_stats['new_offers']}")
            print(f"  • Обновлено предложений: {cycle_stats['updated_offers']}")
            print(f"  • Удалено предложений: {cycle_stats['deleted_offers']}")
            if cycle_stats["errors"]:
                print(f"  • Ошибок: {len(cycle_stats['errors'])}")
            print(f"{'='*80}\n")
            
            return cycle_stats
            
        except Exception as e:
            error_msg = f"Критическая ошибка в цикле обновления: {str(e)}"
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
        print(f"СЕРВИС ОБНОВЛЕНИЯ В РЕАЛЬНОМ ВРЕМЕНИ ЗАПУЩЕН")
        print(f"{'='*80}")
        print(f"Интервал обновления: {self.interval_seconds // 60} минут")
        print(f"Тайм-аут запросов: {REQUEST_TIMEOUT} секунд")
        print(f"Максимум попыток: {MAX_RETRIES}")
        print(f"Нажмите Ctrl+C для остановки")
        print(f"{'='*80}\n")
        
        self.stats["current_status"] = "running"
        self.save_stats()
        
        while self.running:
            try:
                # Perform update
                self.update_data()
                
                # Wait for next cycle
                next_update = datetime.now() + timedelta(seconds=self.interval_seconds)
                print(f"⏰ Следующее обновление через {self.interval_seconds // 60} минут...")
                print(f"   Следующий запуск: {next_update.strftime('%H:%M:%S')}")
                
                # Sleep in small intervals to allow for clean shutdown
                for _ in range(self.interval_seconds):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except KeyboardInterrupt:
                print("\n\n⏹ Остановка сервиса...")
                break
            except Exception as e:
                print(f"\n❌ Неожиданная ошибка: {e}")
                print("Ожидание 5 минут перед повтором...")
                time.sleep(300)  # Wait 5 minutes before retry
        
        self.stats["current_status"] = "stopped"
        self.save_stats()
        
        print(f"\n{'='*80}")
        print(f"СЕРВИС ОСТАНОВЛЕН")
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
        print("Запуск одного цикла обновления...\n")
        return self.update_data()


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n\nПолучен сигнал прерывания...')
    sys.exit(0)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Сервис обновления базы данных в реальном времени')
    parser.add_argument('--interval', type=int, default=UPDATE_INTERVAL_MINUTES,
                       help=f'Интервал обновления в минутах (по умолчанию: {UPDATE_INTERVAL_MINUTES})')
    parser.add_argument('--once', action='store_true',
                       help='Запустить один раз и выйти (для тестирования)')
    parser.add_argument('--max-categories', type=int, default=None,
                       help='Макс. категорий для обработки за цикл (по умолчанию: все)')
    parser.add_argument('--timeout', type=int, default=REQUEST_TIMEOUT,
                       help=f'Тайм-аут запроса в секундах (по умолчанию: {REQUEST_TIMEOUT})')
    
    args = parser.parse_args()
    
    # Set global config
    global MAX_CATEGORIES_PER_CYCLE, REQUEST_TIMEOUT
    MAX_CATEGORIES_PER_CYCLE = args.max_categories
    REQUEST_TIMEOUT = args.timeout
    
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




