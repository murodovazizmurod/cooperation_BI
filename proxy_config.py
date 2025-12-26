#!/usr/bin/env python3
"""
Конфигурация для работы через прокси/VPN

ИНСТРУКЦИЯ:
1. Если cooperation.uz доступен только из Узбекистана - настройте прокси
2. Отредактируйте этот файл и укажите данные вашего прокси
3. Импортируйте в других скриптах: from proxy_config import get_proxies, get_session
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================================
# НАСТРОЙКИ ПРОКСИ
# ============================================================================

# Установите True если используете прокси
USE_PROXY = False

# Настройки прокси (замените на ваши данные)
PROXY_CONFIG = {
    'http': 'http://your-proxy-server:8080',
    'https': 'http://your-proxy-server:8080',
    # Если прокси требует аутентификацию:
    # 'http': 'http://username:password@proxy-server:8080',
    # 'https': 'http://username:password@proxy-server:8080',
}

# ============================================================================
# HTTP ЗАГОЛОВКИ
# ============================================================================

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://new.cooperation.uz/',
    'Origin': 'https://new.cooperation.uz'
}

# ============================================================================
# ФУНКЦИИ
# ============================================================================

def get_proxies():
    """Получить настройки прокси"""
    if USE_PROXY:
        return PROXY_CONFIG
    return None

def get_session(max_retries=3, timeout=60):
    """Создать настроенную сессию с retry логикой"""
    session = requests.Session()
    
    # Retry стратегия
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Установить прокси
    if USE_PROXY:
        session.proxies.update(PROXY_CONFIG)
        print(f"🔒 Используется прокси: {PROXY_CONFIG['http']}")
    
    return session

def get_headers():
    """Получить HTTP заголовки"""
    return HEADERS.copy()

def test_proxy():
    """Тест прокси соединения"""
    print("Тестирование прокси настроек...")
    print(f"USE_PROXY: {USE_PROXY}")
    
    if USE_PROXY:
        print(f"Прокси: {PROXY_CONFIG['http']}")
    else:
        print("Прокси не используется")
    
    try:
        session = get_session()
        print("\nТест запроса...")
        
        # Тест на httpbin.org
        response = session.get('https://httpbin.org/ip', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Успешно! Ваш IP: {data.get('origin')}")
            return True
        else:
            print(f"❌ Ошибка: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

# ============================================================================
# ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("НАСТРОЙКА ПРОКСИ ДЛЯ COOPERATION.UZ")
    print("=" * 80)
    print()
    
    print("📋 Текущие настройки:")
    print(f"  USE_PROXY = {USE_PROXY}")
    if USE_PROXY:
        print(f"  HTTP Proxy: {PROXY_CONFIG['http']}")
        print(f"  HTTPS Proxy: {PROXY_CONFIG['https']}")
    else:
        print("  Прокси не настроен")
    
    print("\n" + "=" * 80)
    print("КАК НАСТРОИТЬ ПРОКСИ:")
    print("=" * 80)
    print("""
1. Откройте файл proxy_config.py в редакторе:
   nano proxy_config.py

2. Установите USE_PROXY = True

3. Укажите адрес вашего прокси сервера:
   PROXY_CONFIG = {
       'http': 'http://proxy.example.com:8080',
       'https': 'http://proxy.example.com:8080',
   }

4. Если прокси требует авторизацию:
   PROXY_CONFIG = {
       'http': 'http://username:password@proxy.example.com:8080',
       'https': 'http://username:password@proxy.example.com:8080',
   }

5. Сохраните файл и запустите тест:
   python3 proxy_config.py

6. После успешного теста - используйте в других скриптах
""")
    
    print("\n" + "=" * 80)
    print("ПОЛУЧЕНИЕ ПРОКСИ:")
    print("=" * 80)
    print("""
Бесплатные прокси (для тестирования):
  • https://www.proxy-list.download/
  • https://free-proxy-list.net/
  • https://www.sslproxies.org/

Платные прокси (рекомендуется):
  • Bright Data: https://brightdata.com/
  • Oxylabs: https://oxylabs.io/
  • SmartProxy: https://smartproxy.com/

VPN серверы в Узбекистане:
  • NordVPN
  • ExpressVPN
  • Surfshark
""")
    
    if USE_PROXY:
        print("\n" + "=" * 80)
        print("ТЕСТ СОЕДИНЕНИЯ")
        print("=" * 80)
        print()
        test_proxy()
    
    print()

