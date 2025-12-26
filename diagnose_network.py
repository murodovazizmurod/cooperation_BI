#!/usr/bin/env python3
"""
Диагностика сетевых проблем с cooperation.uz
"""

import socket
import subprocess
import sys

def check_dns():
    """Проверка DNS резолвинга"""
    print("=" * 80)
    print("1. ПРОВЕРКА DNS")
    print("=" * 80)
    
    hostname = "new.cooperation.uz"
    try:
        print(f"Резолвинг {hostname}...", end=' ', flush=True)
        ip = socket.gethostbyname(hostname)
        print(f"✓ IP адрес: {ip}")
        return ip
    except socket.gaierror as e:
        print(f"❌ Ошибка DNS: {e}")
        return None

def check_ping(hostname):
    """Проверка доступности через ping"""
    print("\n" + "=" * 80)
    print("2. ПРОВЕРКА PING")
    print("=" * 80)
    
    try:
        print(f"Ping {hostname}...\n")
        # Для Linux используем -c, для Windows -n
        param = '-n' if sys.platform.startswith('win') else '-c'
        result = subprocess.run(
            ['ping', param, '4', hostname],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print(result.stdout)
            print("✓ Сервер доступен через ping")
            return True
        else:
            print(result.stdout)
            print("❌ Сервер недоступен через ping")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Тайм-аут ping запроса")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def check_curl(url):
    """Проверка доступности через curl"""
    print("\n" + "=" * 80)
    print("3. ПРОВЕРКА CURL")
    print("=" * 80)
    
    try:
        print(f"Curl запрос к {url}...\n")
        result = subprocess.run(
            ['curl', '-I', '--connect-timeout', '30', '--max-time', '60', url],
            capture_output=True,
            text=True,
            timeout=70
        )
        
        if result.returncode == 0:
            print(result.stdout)
            print("✓ Сервер отвечает через curl")
            return True
        else:
            print(result.stdout)
            print(result.stderr)
            print("❌ Ошибка curl запроса")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Тайм-аут curl запроса")
        return False
    except FileNotFoundError:
        print("⚠ curl не установлен (установите: sudo apt install curl)")
        return None
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def check_browser_access():
    """Проверка доступности через браузер"""
    print("\n" + "=" * 80)
    print("4. ПРОВЕРКА ДОСТУПА ИЗ БРАУЗЕРА")
    print("=" * 80)
    print("\nПопробуйте открыть в браузере:")
    print("  https://new.cooperation.uz/")
    print("  https://new.cooperation.uz/ocelot/api-client/Client/GetAllTnVedCategory?take=5&skip=0")
    print("\nОткрывается? (y/n): ", end='')
    
    try:
        answer = input().strip().lower()
        return answer == 'y' or answer == 'yes' or answer == 'да'
    except:
        return None

def print_recommendations(dns_ok, ping_ok, curl_ok, browser_ok):
    """Вывод рекомендаций на основе результатов"""
    print("\n" + "=" * 80)
    print("ДИАГНОСТИКА И РЕШЕНИЯ")
    print("=" * 80)
    
    if not dns_ok:
        print("\n❌ ПРОБЛЕМА С DNS")
        print("Решения:")
        print("  1. Проверьте /etc/resolv.conf")
        print("  2. Используйте другой DNS (например, 8.8.8.8)")
        print("  3. Команда: sudo nano /etc/resolv.conf")
        print("     Добавьте: nameserver 8.8.8.8")
        return
    
    if not ping_ok:
        print("\n⚠ Сервер не отвечает на ping")
        print("Возможно:")
        print("  • Сервер блокирует ICMP пакеты (это нормально)")
        print("  • Проблемы с сетью")
        
    if curl_ok == False:
        print("\n❌ CURL НЕ РАБОТАЕТ")
        print("Возможные причины:")
        print("  • Географическая блокировка")
        print("  • Брандмауэр блокирует исходящие соединения")
        print("  • Прокси/VPN требуется")
    
    if browser_ok == True and curl_ok == False:
        print("\n⚠ РАБОТАЕТ В БРАУЗЕРЕ, НО НЕ В CURL/PYTHON")
        print("Это означает, что:")
        print("  • Сервер требует специальные заголовки (уже добавлены)")
        print("  • Возможно требуются cookies или сессия")
        print("  • Сервер использует защиту от ботов (Cloudflare, etc.)")
        print("\nРЕШЕНИЯ:")
        print("  1. Использовать Selenium с реальным браузером")
        print("  2. Настроить прокси/VPN на сервере")
        print("  3. Экспортировать cookies из браузера")
        
    if browser_ok == False:
        print("\n❌ НЕ РАБОТАЕТ ДАЖЕ В БРАУЗЕРЕ")
        print("РЕШЕНИЯ:")
        print("  1. Проверьте подключение к интернету:")
        print("     ping 8.8.8.8")
        print("  2. Используйте VPN для доступа к Узбекским ресурсам")
        print("  3. Проверьте настройки брандмауэра:")
        print("     sudo ufw status")
        print("  4. Возможно требуется доступ из Узбекистана")
        
    if browser_ok == None:
        print("\n📋 ОБЩИЕ РЕКОМЕНДАЦИИ:")
        print("\n1. ЕСЛИ САЙТ ДОСТУПЕН ТОЛЬКО ИЗ УЗБЕКИСТАНА:")
        print("   • Используйте VPN с узбекским сервером")
        print("   • Или настройте прокси из Узбекистана")
        print("\n2. НАСТРОЙКА VPN НА СЕРВЕРЕ:")
        print("   sudo apt install openvpn")
        print("   # Загрузите конфиг VPN")
        print("   sudo openvpn --config your-config.ovpn")
        print("\n3. ИСПОЛЬЗОВАНИЕ ПРОКСИ В PYTHON:")
        print("   Отредактируйте update_database.py и realtime_updater.py:")
        print("   ")
        print("   proxies = {")
        print("       'http': 'http://proxy.example.com:8080',")
        print("       'https': 'http://proxy.example.com:8080',")
        print("   }")
        print("   response = requests.get(url, proxies=proxies, ...)")

def main():
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "ДИАГНОСТИКА СЕТИ COOPERATION.UZ" + " " * 26 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    hostname = "new.cooperation.uz"
    url = f"https://{hostname}/"
    
    # Проверки
    ip = check_dns()
    dns_ok = ip is not None
    
    ping_ok = check_ping(hostname) if dns_ok else False
    curl_ok = check_curl(url) if dns_ok else False
    browser_ok = check_browser_access()
    
    # Рекомендации
    print_recommendations(dns_ok, ping_ok, curl_ok, browser_ok)
    
    print("\n" + "=" * 80)
    print("ДОПОЛНИТЕЛЬНАЯ ПОМОЩЬ")
    print("=" * 80)
    print("\nЕсли ничего не помогает:")
    print("  1. Свяжитесь с администратором cooperation.uz")
    print("  2. Запустите скрипт с компьютера в Узбекистане")
    print("  3. Используйте прокси-сервер в Узбекистане")
    print("=" * 80)
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Прервано пользователем")
        sys.exit(0)

