from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify
import pandas as pd
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os
from werkzeug.utils import secure_filename
import tempfile

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-in-production'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

# ===== НАСТРОЙКИ DADATA =====
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

current_key_index = 0
key_switch_lock = threading.Lock()

URL_PARTY = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party"
URL_OKVED = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/okved2"

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
            print(f"Переключение на API ключ #{current_key_index + 1}")
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
    """Получает название ОКВЭД по коду через DaData API"""
    if not okved_code:
        return ''

    try:
        data = {"query": okved_code}
        response = requests.post(URL_OKVED, json=data, headers=get_headers(), timeout=5)

        if response.status_code == 200:
            result = response.json()
            if result.get('suggestions') and len(result['suggestions']) > 0:
                for suggestion in result['suggestions']:
                    if suggestion.get('data', {}).get('code') == okved_code:
                        return suggestion.get('data', {}).get('name', '')
                return result['suggestions'][0].get('data', {}).get('name', '')

        elif response.status_code == 403 and retry_count < len(API_KEYS):
            switch_api_key()
            time.sleep(0.5)
            return get_okved_name(okved_code, retry_count + 1)

        return ''
    except:
        return ''

def get_okved_by_inn(inn, retry_count=0):
    """Получает основной ОКВЭД (код и название) по ИНН через DaData API"""
    try:
        inn_str = str(inn).strip().replace('.0', '')

        if 'e' in inn_str.lower():
            inn_str = str(int(float(inn_str)))

        data = {"query": inn_str}
        response = requests.post(URL_PARTY, json=data, headers=get_headers(), timeout=10)

        if response.status_code == 200:
            result = response.json()

            if result.get('suggestions') and len(result['suggestions']) > 0:
                org_data = result['suggestions'][0]['data']

                okved_code = ''
                okved_name = ''

                okveds = org_data.get('okveds')
                if okveds and isinstance(okveds, list) and len(okveds) > 0:
                    main_okved = None
                    for okved in okveds:
                        if okved.get('main') == True:
                            main_okved = okved
                            break

                    if not main_okved:
                        main_okved = okveds[0]

                    if main_okved:
                        okved_code = main_okved.get('code', '')
                        okved_name = main_okved.get('name', '')

                if not okved_code:
                    okved_code = org_data.get('okved', '')

                if okved_code and not okved_name:
                    okved_name = get_okved_name(okved_code)
                    time.sleep(0.3)

                org_name = org_data.get('name', {})
                if isinstance(org_name, dict):
                    org_name = org_name.get('short_with_opf', '')

                state = org_data.get('state', {})
                state_status = state.get('status', '') if isinstance(state, dict) else ''

                return {
                    'ИНН': inn_str,
                    'Название': org_name,
                    'ОКВЭД_код': okved_code,
                    'ОКВЭД_название': okved_name,
                    'Статус_организации': state_status,
                    'Результат': 'Успешно'
                }
            else:
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
                switch_api_key()
                time.sleep(0.5)
                return get_okved_by_inn(inn, retry_count + 1)
            else:
                return {
                    'ИНН': inn_str,
                    'Название': '',
                    'ОКВЭД_код': '',
                    'ОКВЭД_название': '',
                    'Статус_организации': '',
                    'Результат': 'Превышен лимит на всех API ключах'
                }
        else:
            return {
                'ИНН': inn_str,
                'Название': '',
                'ОКВЭД_код': '',
                'ОКВЭД_название': '',
                'Статус_организации': '',
                'Результат': f'Ошибка API: код {response.status_code}'
            }

    except Exception as e:
        return {
            'ИНН': str(inn).strip().replace('.0', ''),
            'Название': '',
            'ОКВЭД_код': '',
            'ОКВЭД_название': '',
            'Статус_организации': '',
            'Результат': f'Ошибка: {str(e)[:50]}'
        }

def process_file(input_path, filter_trade=False, max_workers=10):
    """Обрабатывает Excel файл с ИНН"""
    try:
        # Читаем Excel файл
        try:
            df = pd.read_excel(input_path, dtype={'ИНН': str})
        except:
            df = pd.read_excel(input_path, dtype={0: str})

        # Определяем столбец с ИНН
        if 'ИНН' in df.columns:
            inn_column = 'ИНН'
        elif 'инн' in df.columns:
            inn_column = 'инн'
        elif 'INN' in df.columns:
            inn_column = 'INN'
        else:
            inn_column = df.columns[0]

        inn_list = df[inn_column].dropna().tolist()

        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_inn = {executor.submit(get_okved_by_inn, inn): inn for inn in inn_list}

            for future in as_completed(future_to_inn):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    inn = future_to_inn[future]
                    results.append({
                        'ИНН': str(inn),
                        'Название': '',
                        'ОКВЭД_код': '',
                        'ОКВЭД_название': '',
                        'Статус_организации': '',
                        'Результат': f'Ошибка потока: {str(e)[:50]}'
                    })

        results_df = pd.DataFrame(results)

        # Фильтрация по слову "торговля" если включен фильтр
        total_before_filter = len(results)
        if filter_trade:
            # Фильтруем только те строки, где в названии ОКВЭД есть слово "торговля" (независимо от регистра)
            results_df = results_df[results_df['ОКВЭД_название'].str.contains('торговл', case=False, na=False)]

        # Генерируем имя выходного файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], f"okved_results_{timestamp}.xlsx")

        results_df.to_excel(output_path, index=False, engine='openpyxl')

        return output_path, total_before_filter, len(results_df[results_df['Результат'] == 'Успешно'])

    except Exception as e:
        raise Exception(f"Ошибка обработки файла: {str(e)}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'Файл не выбран'}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({'success': False, 'error': 'Файл не выбран'}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_path = os.path.join(app.config['UPLOAD_FOLDER'], f"input_{timestamp}_{filename}")
        file.save(input_path)

        # Получаем значение фильтра из формы
        filter_trade = request.form.get('filter_trade') == '1'

        try:
            output_path, total, success = process_file(input_path, filter_trade=filter_trade)

            # Удаляем входной файл
            os.remove(input_path)

            # Получаем только имя файла из пути
            output_filename = os.path.basename(output_path)

            return jsonify({
                'success': True,
                'download_url': f'/download/{output_filename}',
                'total': total,
                'success_count': success,
                'filter_trade': filter_trade
            })

        except Exception as e:
            if os.path.exists(input_path):
                os.remove(input_path)
            return jsonify({'success': False, 'error': str(e)}), 500
    else:
        return jsonify({'success': False, 'error': 'Недопустимый формат файла. Используйте .xlsx или .xls'}), 400

@app.route('/download/<filename>')
def download_file(filename):
    """Отдает файл для скачивания"""
    try:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name='okved_results.xlsx')
        else:
            return jsonify({'error': 'Файл не найден'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
