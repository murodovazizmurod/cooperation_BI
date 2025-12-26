# РЕШЕНИЕ ПРОБЛЕМЫ ДОСТУПА К COOPERATION.UZ

## Проблема
Сервер `cooperation.uz` не отвечает на запросы из-за:
- Географической блокировки (доступ только из Узбекистана)
- Защиты от ботов
- Сетевых ограничений

## Быстрая диагностика

Запустите:
```bash
python3 diagnose_network.py
```

Это покажет, в чем именно проблема.

## Решение 1: Использовать VPN (РЕКОМЕНДУЕТСЯ)

### Установка OpenVPN
```bash
sudo apt update
sudo apt install openvpn
```

### Подключение к VPN
```bash
# Получите конфиг файл VPN с узбекским сервером
# Затем подключитесь:
sudo openvpn --config uzbekistan-server.ovpn
```

После подключения к VPN запустите обновление:
```bash
python3 realtime_updater.py --once
```

## Решение 2: Использовать прокси сервер

### 1. Получите прокси в Узбекистане
- Платные: Bright Data, Oxylabs, SmartProxy
- Или настройте свой сервер в Узбекистане

### 2. Настройте прокси
```bash
nano proxy_config.py
```

Измените:
```python
USE_PROXY = True

PROXY_CONFIG = {
    'http': 'http://your-proxy:8080',
    'https': 'http://your-proxy:8080',
}
```

### 3. Протестируйте
```bash
python3 proxy_config.py
```

### 4. Обновите скрипты для использования прокси
Уже готово! Скрипты автоматически используют настройки из proxy_config.py

## Решение 3: Запуск на сервере в Узбекистане

Если у вас есть доступ к серверу/VPS в Узбекистане:

1. Склонируйте проект:
```bash
git clone https://github.com/murodovazizmurod/cooperation_BI.git
cd cooperation_BI
```

2. Установите зависимости:
```bash
pip3 install -r requirements.txt
```

3. Запустите обновление:
```bash
python3 realtime_updater.py
```

## Решение 4: Использовать другой источник данных

Если прямой доступ невозможен, рассмотрите:
- Скрейпинг через браузер (Selenium)
- Парсинг HTML страниц вместо API
- Запрос официального API ключа у cooperation.uz

## Проверка доступности

### Из командной строки:
```bash
# Проверка DNS
nslookup new.cooperation.uz

# Проверка ping
ping new.cooperation.uz

# Проверка HTTP
curl -I https://new.cooperation.uz/

# Проверка через Python
python3 test_connection.py
```

### Из браузера:
Откройте: https://new.cooperation.uz/

Если открывается - значит нужен VPN/прокси для Python скриптов.

## Временное решение: Ручное обновление

Если автоматическое обновление невозможно:

1. Откройте cooperation.uz в браузере
2. Экспортируйте данные вручную
3. Импортируйте в базу данных

## Контакты поддержки

Если проблема не решается:
1. Свяжитесь с администраторами cooperation.uz
2. Узнайте о требованиях к API доступу
3. Запросите whitelist для вашего IP

## Следующие шаги

После решения проблемы с доступом:

```bash
# Протестируйте соединение
python3 test_connection.py

# Запустите одно обновление
python3 realtime_updater.py --once

# Если успешно - запустите постоянный мониторинг
python3 realtime_updater.py

# Или в фоне
nohup python3 realtime_updater.py > updater.log 2>&1 &
```

