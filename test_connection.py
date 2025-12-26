#!/usr/bin/env python3
"""
Тест подключения к API cooperation.uz
Проверяет доступность сервера и правильность запросов
"""

import requests
import json
from datetime import datetime

BASE_URL = "https://new.cooperation.uz/ocelot/api-client/Client"

# HTTP заголовки для имитации браузера
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://new.cooperation.uz/',
    'Origin': 'https://new.cooperation.uz'
}

def test_connection():
    """Тест базового подключения"""
    print("=" * 80)
    print("ТЕСТ ПОДКЛЮЧЕНИЯ К COOPERATION.UZ API")
    print("=" * 80)
    print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Тест 1: Получение категорий
    print("📋 Тест 1: Получение категорий...")
    url = f"{BASE_URL}/GetAllTnVedCategory"
    params = {"take": 5, "skip": 0}
    
    try:
        print(f"  URL: {url}")
        print(f"  Параметры: {params}")
        print(f"  Заголовки: Да (User-Agent и др.)")
        print(f"  Тайм-аут: 60 секунд")
        print(f"\n  Отправка запроса...", end='', flush=True)
        
        response = requests.get(url, params=params, headers=HEADERS, timeout=60)
        
        print(f" ✓")
        print(f"  Статус: {response.status_code}")
        print(f"  Время ответа: {response.elapsed.total_seconds():.2f} сек")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("statusCode") == 200 and "result" in data:
                categories = data["result"].get("data", [])
                print(f"  ✓ Успешно! Получено {len(categories)} категорий")
                
                if categories:
                    print(f"\n  Пример категории:")
                    cat = categories[0]
                    print(f"    ID: {cat.get('id')}")
                    print(f"    Название: {cat.get('name', {}).get('ru', 'N/A')}")
                    print(f"    Количество: {cat.get('count', 0)}")
            else:
                print(f"  ⚠ Неверный формат ответа:")
                print(f"    {json.dumps(data, indent=2, ensure_ascii=False)[:200]}...")
        else:
            print(f"  ❌ Ошибка HTTP: {response.status_code}")
            print(f"  Ответ: {response.text[:200]}")
            
    except requests.exceptions.Timeout:
        print(f"\n  ❌ ОШИБКА: Превышено время ожидания (60 сек)")
        print(f"  Возможные причины:")
        print(f"    • Сервер перегружен")
        print(f"    • Медленное интернет-соединение")
        print(f"    • Сервер недоступен")
    except requests.exceptions.ConnectionError as e:
        print(f"\n  ❌ ОШИБКА ПОДКЛЮЧЕНИЯ:")
        print(f"    {str(e)[:200]}")
        print(f"\n  Возможные причины:")
        print(f"    • Нет интернет-соединения")
        print(f"    • Сервер недоступен")
        print(f"    • Проблемы с DNS")
        print(f"    • Брандмауэр блокирует соединение")
    except requests.exceptions.HTTPError as e:
        print(f"\n  ❌ HTTP ОШИБКА: {e}")
    except Exception as e:
        print(f"\n  ❌ НЕОЖИДАННАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
    
    # Тест 2: Получение предложений
    print("\n" + "=" * 80)
    print("📦 Тест 2: Получение предложений (для категории 1)...")
    url = f"{BASE_URL}/GetAllOffer"
    params = {
        "OfferType": 1,
        "skip": 0,
        "take": 3,
        "productName": "",
        "firstTnvedCategoryId": 1
    }
    
    try:
        print(f"  URL: {url}")
        print(f"  Параметры: {params}")
        print(f"\n  Отправка запроса...", end='', flush=True)
        
        response = requests.get(url, params=params, headers=HEADERS, timeout=60)
        
        print(f" ✓")
        print(f"  Статус: {response.status_code}")
        print(f"  Время ответа: {response.elapsed.total_seconds():.2f} сек")
        
        if response.status_code == 200:
            data = response.json()
            if data.get("statusCode") == 200 and "result" in data:
                result = data["result"]
                total = result.get("total", 0)
                offers = result.get("data", [])
                print(f"  ✓ Успешно! Всего предложений: {total}, получено: {len(offers)}")
                
                if offers:
                    print(f"\n  Пример предложения:")
                    offer = offers[0]
                    print(f"    ID: {offer.get('id')}")
                    print(f"    Товар: {offer.get('productName', {}).get('ru', 'N/A')}")
                    print(f"    Цена: {offer.get('unitPrice', 0)} сум")
            else:
                print(f"  ⚠ Неверный формат ответа")
        else:
            print(f"  ❌ Ошибка HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"\n  ❌ ОШИБКА: {e}")
    
    # Итоги
    print("\n" + "=" * 80)
    print("ИТОГ ТЕСТИРОВАНИЯ")
    print("=" * 80)
    print("\nЕсли все тесты прошли успешно - API доступно и работает.")
    print("Если есть ошибки - проверьте:")
    print("  1. Подключение к интернету")
    print("  2. Доступность cooperation.uz в браузере")
    print("  3. Настройки брандмауэра/антивируса")
    print("  4. VPN/прокси настройки (если используются)")
    print("=" * 80)

if __name__ == "__main__":
    test_connection()

