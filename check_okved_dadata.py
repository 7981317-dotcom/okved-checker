import pandas as pd
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ===== НАСТРОЙКИ =====
# Список API ключей (добавьте сколько нужно для увеличения лимита)
API_KEYS = [
    {
        "api_key": "6f2ce937e384d27d595fbf573188f0061a53eb34",
        "secret_key": "02be50420911c8dc2b9f0bd2c3592d152abc0342"
    },
    {
        "api_key": "dc17411a8a49402bd1edd914e25f055bf6b5d2e0",
        "secret_key": "0de6422d38a9ad3ad56bf2f4dae4f87a854ec330"
    }
]

# Текущий индекс используемого ключа
current_key_index = 0
key_switch_lock = threading.Lock()

# URL API DaData
URL_PARTY = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"
URL_OKVED = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/okved2"

def get_current_api_key():
    """Получает текущий активный API ключ"""
    with key_switch_lock:
        return API_KEYS[current_key_index]["api_key"]

def switch_api_key():
    """Переключает на следующий API ключ"""
    global current_key_index
    with key_switch_lock:
        old_index = current_key_index
        current_key_index = (current_key_index + 1) % len(API_KEYS)
        if current_key_index != old_index:
            print(f"\n!!! ПЕРЕКЛЮЧЕНИЕ НА API КЛЮЧ #{current_key_index + 1} !!!\n")
            return True
        return False

def get_headers():
    """Получает заголовки с текущим API ключом"""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {get_current_api_key()}"
    }

def get_okved_name(okved_code, retry_count=0):
    """
    Получает название ОКВЭД по коду через DaData API

    Args:
        okved_code: код ОКВЭД (например "73.11")
        retry_count: счетчик попыток (для предотвращения бесконечного цикла)

    Returns:
        str: Название ОКВЭД или пустая строка
    """
    if not okved_code:
        return ''

    try:
        data = {"query": okved_code}
        response = requests.post(URL_OKVED, json=data, headers=get_headers(), timeout=5)

        if response.status_code == 200:
            result = response.json()
            if result.get('suggestions') and len(result['suggestions']) > 0:
                # Ищем точное совпадение кода
                for suggestion in result['suggestions']:
                    if suggestion.get('data', {}).get('code') == okved_code:
                        return suggestion.get('data', {}).get('name', '')

                # Если точного совпадения нет, берём первый результат
                return result['suggestions'][0].get('data', {}).get('name', '')

        elif response.status_code == 403 and retry_count < len(API_KEYS):
            # Превышен лимит - переключаем ключ и повторяем
            switch_api_key()
            time.sleep(0.5)  # Небольшая задержка перед повтором
            return get_okved_name(okved_code, retry_count + 1)

        return ''
    except:
        return ''

def get_okved_by_inn(inn, retry_count=0):
    """
    Получает основной ОКВЭД (код и название) по ИНН через DaData API

    Args:
        inn: ИНН организации (строка или число)
        retry_count: счетчик попыток (для предотвращения бесконечного цикла)

    Returns:
        dict: Словарь с данными организации, кодом и названием ОКВЭД
    """
    try:
        # Конвертируем в строку и убираем пробелы
        inn_str = str(inn).strip().replace('.0', '')

        # Убираем научную нотацию если есть
        if 'e' in inn_str.lower():
            inn_str = str(int(float(inn_str)))

        print(f"Обрабатываем ИНН: {inn_str} ... ", end='', flush=True)

        # Формируем запрос для получения данных организации
        data = {"query": inn_str}

        # Отправляем запрос
        response = requests.post(URL_PARTY, json=data, headers=get_headers(), timeout=10)

        # Проверяем статус
        if response.status_code == 200:
            result = response.json()

            if result.get('suggestions') and len(result['suggestions']) > 0:
                org_data = result['suggestions'][0]['data']

                # Извлекаем основной ОКВЭД (код)
                okved_code = ''
                okved_name = ''

                # Сначала проверяем массив okveds (платный тариф)
                okveds = org_data.get('okveds')
                if okveds and isinstance(okveds, list) and len(okveds) > 0:
                    # Ищем основной ОКВЭД (где main=True)
                    main_okved = None
                    for okved in okveds:
                        if okved.get('main') == True:
                            main_okved = okved
                            break

                    # Если основной не найден, берём первый
                    if not main_okved:
                        main_okved = okveds[0]

                    if main_okved:
                        okved_code = main_okved.get('code', '')
                        okved_name = main_okved.get('name', '')

                # Если массив okveds недоступен, берём из поля okved (бесплатный тариф)
                if not okved_code:
                    okved_code = org_data.get('okved', '')

                # Если название не получили, запрашиваем отдельно по коду
                if okved_code and not okved_name:
                    print(f"получаем название ОКВЭД {okved_code}...", end='', flush=True)
                    okved_name = get_okved_name(okved_code)
                    time.sleep(0.3)  # Дополнительная задержка для второго запроса

                # Название организации
                org_name = org_data.get('name', {})
                if isinstance(org_name, dict):
                    org_name = org_name.get('short_with_opf', '')

                # Статус организации
                state = org_data.get('state', {})
                state_status = state.get('status', '') if isinstance(state, dict) else ''

                print("OK")

                return {
                    'ИНН': inn_str,
                    'Название': org_name,
                    'ОКВЭД_код': okved_code,
                    'ОКВЭД_название': okved_name,
                    'Статус_организации': state_status,
                    'Результат': 'Успешно'
                }
            else:
                print("не найдено")
                return {
                    'ИНН': inn_str,
                    'Название': '',
                    'ОКВЭД_код': '',
                    'ОКВЭД_название': '',
                    'Статус_организации': '',
                    'Результат': 'Не найдено в ЕГРЮЛ'
                }
        elif response.status_code == 403:
            if retry_count < len(API_KEYS):
                # Превышен лимит - переключаем ключ и повторяем
                print("лимит превышен! Переключаемся на другой ключ...")
                switch_api_key()
                time.sleep(0.5)  # Небольшая задержка перед повтором
                return get_okved_by_inn(inn, retry_count + 1)
            else:
                print("лимит превышен на всех ключах!")
                return {
                    'ИНН': inn_str,
                    'Название': '',
                    'ОКВЭД_код': '',
                    'ОКВЭД_название': '',
                    'Статус_организации': '',
                    'Результат': 'Превышен лимит на всех API ключах'
                }
        else:
            print(f"ошибка {response.status_code}")
            return {
                'ИНН': inn_str,
                'Название': '',
                'ОКВЭД_код': '',
                'ОКВЭД_название': '',
                'Статус_организации': '',
                'Результат': f'Ошибка API: код {response.status_code}'
            }

    except requests.exceptions.Timeout:
        print("таймаут")
        return {
            'ИНН': inn_str,
            'Название': '',
            'ОКВЭД_код': '',
            'ОКВЭД_название': '',
            'Статус_организации': '',
            'Результат': 'Таймаут запроса'
        }
    except Exception as e:
        print(f"ошибка: {str(e)[:30]}")
        return {
            'ИНН': inn_str,
            'Название': '',
            'ОКВЭД_код': '',
            'ОКВЭД_название': '',
            'Статус_организации': '',
            'Результат': f'Ошибка: {str(e)[:50]}'
        }

def process_excel_file(input_file, output_file=None, max_workers=10):
    """
    Обрабатывает Excel файл с ИНН и добавляет ОКВЭД (код и название)

    Args:
        input_file: путь к входному Excel файлу
        output_file: путь к выходному файлу (если None, генерируется автоматически)
        max_workers: количество параллельных потоков (по умолчанию 10)
    """
    print(f"\n{'='*70}")
    print(f"Начало обработки файла: {input_file}")
    print(f"{'='*70}\n")

    try:
        # Читаем Excel файл
        try:
            df = pd.read_excel(input_file, dtype={'ИНН': str})
        except:
            df = pd.read_excel(input_file, dtype={0: str})

        print(f"Загружено записей: {len(df)}")

        # Определяем столбец с ИНН
        if 'ИНН' in df.columns:
            inn_column = 'ИНН'
        elif 'инн' in df.columns:
            inn_column = 'инн'
        elif 'INN' in df.columns:
            inn_column = 'INN'
        else:
            # Берём первый столбец
            inn_column = df.columns[0]
            print(f"Столбец 'ИНН' не найден, используем первый столбец: '{inn_column}'")

        # Получаем список ИНН
        inn_list = df[inn_column].dropna().tolist()

        print(f"Найдено {len(inn_list)} ИНН для обработки")
        print(f"Используется {max_workers} параллельных потоков")
        print(f"{'='*70}\n")

        # Обрабатываем каждый ИНН параллельно
        results = []
        completed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Запускаем все задачи
            future_to_inn = {executor.submit(get_okved_by_inn, inn): inn for inn in inn_list}

            # Получаем результаты по мере завершения
            for future in as_completed(future_to_inn):
                completed_count += 1
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    inn = future_to_inn[future]
                    print(f"[{completed_count}/{len(inn_list)}] ИНН {inn}: ошибка - {str(e)[:50]}")
                    results.append({
                        'ИНН': str(inn),
                        'Название': '',
                        'ОКВЭД_код': '',
                        'ОКВЭД_название': '',
                        'Статус_организации': '',
                        'Результат': f'Ошибка потока: {str(e)[:50]}'
                    })

        # Создаём DataFrame с результатами
        results_df = pd.DataFrame(results)

        # Генерируем имя выходного файла если не указано
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"okved_results_{timestamp}.xlsx"

        # Сохраняем результат
        results_df.to_excel(output_file, index=False, engine='openpyxl')

        print(f"\n{'='*70}")
        print(f"Обработка завершена!")
        print(f"Результат сохранен в файл: {output_file}")
        print(f"{'='*70}\n")

        # Статистика
        success_count = len(results_df[results_df['Результат'] == 'Успешно'])
        with_okved_name = len(results_df[(results_df['Результат'] == 'Успешно') & (results_df['ОКВЭД_название'] != '')])

        print(f"Статистика:")
        print(f"   Успешно найдено организаций: {success_count}")
        print(f"   Получено названий ОКВЭД: {with_okved_name}")
        print(f"   Не найдено: {len(results_df) - success_count}")
        print(f"   Всего обработано: {len(results_df)}\n")

        return results_df

    except FileNotFoundError:
        print(f"\nОШИБКА: Файл '{input_file}' не найден!")
    except Exception as e:
        print(f"\nОШИБКА при обработке файла: {str(e)}")
        import traceback
        traceback.print_exc()

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
if __name__ == "__main__":
    total_requests = len(API_KEYS) * 10000
    total_inn = total_requests // 2

    print(f"""
    ====================================================================
       Скрипт проверки ОКВЭД (код + название) по ИНН
                  через DaData API (МНОГОПОТОЧНАЯ ВЕРСИЯ)
    ====================================================================

    ОСОБЕННОСТИ:
    - Извлекает код ОКВЭД из данных организации
    - Получает название ОКВЭД отдельным запросом к справочнику ОКВЭД
    - Работает на бесплатном тарифе DaData
    - Используется {len(API_KEYS)} API ключа
    - Лимит: {total_requests} запросов в день (~{total_inn} ИНН)
    - Автоматическое переключение ключей при превышении лимита
    - МНОГОПОТОЧНАЯ ОБРАБОТКА - в 5-10 раз быстрее!
    """)

    # УКАЖИТЕ ПУТЬ К ВАШЕМУ ФАЙЛУ ЗДЕСЬ:
    input_file = "inn_list.xlsx"  # Замените на имя вашего файла

    # Можно также указать путь к выходному файлу (опционально)
    output_file = "okved_results.xlsx"  # Или оставьте None для автогенерации

    # Количество параллельных потоков (можно увеличить до 20-30 для еще большей скорости)
    max_workers = 10

    # Запускаем обработку
    process_excel_file(input_file, output_file, max_workers=max_workers)

    print("\nГотово! Нажмите Enter для выхода...")
    input()
