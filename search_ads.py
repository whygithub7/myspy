"""
Универсальный скрипт для сбора объявлений из Facebook Ad Library с анализом медиа.
Ключевые слова передаются как аргументы, фильтрация происходит через Gemini.

Использование:
  python search_ads.py DE "bauch weg" "kilos verlieren"                    # Германия, новый файл
  python search_ads.py DE --append "bauch weg" "kilos verlieren"            # Германия, добавить в существующий
  python search_ads.py MX --append --limit 2 "adelgazar" "quemar grasa"   # Мексика, добавить по 2
  python search_ads.py US --file my_ads.json "lose weight" "burn fat"      # США, указать конкретный файл
"""

import json
import os
import sys
import argparse
from typing import Dict, List, Any, Optional
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

# Добавляем путь к модулям MCP
mcp_path = os.path.join(os.path.dirname(__file__), 'facebook-ads-library-mcp')
sys.path.insert(0, mcp_path)

# Меняем рабочую директорию для корректного импорта модулей MCP
original_cwd = os.getcwd()
os.chdir(mcp_path)

from src.services.scrapecreators_service import search_ads_by_keyword

# Импортируем функции анализа из mcp_server
import importlib.util
spec = importlib.util.spec_from_file_location("mcp_server", os.path.join(mcp_path, "mcp_server.py"))
mcp_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp_module)
analyze_ad_image = mcp_module.analyze_ad_image
analyze_ad_video = mcp_module.analyze_ad_video

# Импортируем Gemini для контекстного анализа
try:
    from src.services.gemini_service import configure_gemini, get_gemini_api_key
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except Exception as e:
    GEMINI_AVAILABLE = False
    print(f"Warning: Gemini not available for context analysis: {e}")

# Глобальная переменная для отслеживания квоты Gemini
GEMINI_QUOTA_EXHAUSTED = False

# Возвращаем рабочую директорию
os.chdir(original_cwd)

# Загружаем переменные окружения
load_dotenv()

# Исключаемые домены
EXCLUDED_DOMAINS = [
    'page.', '*.reader.', '*.read.', '*.book.',
    'hotmart', 'udemy', 'coursera', 'teachable',
    'pay.', 'g.co', 'amazon.com',
    'network.mynursingcommunity.com'
]

# Исключаемые пути в URL
EXCLUDED_URL_PATHS = [
    '/curso/', '/programa/', '/curso-online/', '/training/', '/academy/'
]


def is_excluded_domain(domain: str) -> bool:
    """Проверяет, является ли домен исключаемым."""
    if not domain:
        return False
    domain_lower = domain.lower()
    for excluded in EXCLUDED_DOMAINS:
        if excluded.replace('*', '') in domain_lower:
            return True
    return False


def is_excluded_url(url: str) -> bool:
    """Проверяет, содержит ли URL исключаемые пути."""
    if not url:
        return False
    url_lower = url.lower()
    for path in EXCLUDED_URL_PATHS:
        if path in url_lower:
            return True
    return False


def filter_ad(ad: Dict[str, Any], use_media_analysis: bool = False) -> bool:
    """Базовая фильтрация объявления."""
    # Проверка наличия внешних ссылок
    if not ad.get('has_external_links'):
        return False
    
    # Получаем внешние URL (has_external_links гарантирует что они есть)
    external_urls = ad.get('external_urls', [])
    if not external_urls:  # Дополнительная проверка на всякий случай
        return False
    
    primary_url = external_urls[0].get('full_url', '')
    if not primary_url:
        return False
    
    # Проверка домена
    domain = external_urls[0].get('domain', '')
    if is_excluded_domain(domain):
        return False
    
    # Проверка URL-путей
    if is_excluded_url(primary_url):
        return False
    
    # Проверка длины текста (body и title могут быть пустыми - это допустимо)
    body_text = ad.get('body', '') or ''
    title_text = ad.get('title', '') or ''
    
    # Проверяем общую длину текста (body + title)
    combined_text_length = len(body_text) + len(title_text)
    if combined_text_length > 4000:
        return False
    
    # Дополнительная фильтрация на основе анализа медиа (если включена)
    if use_media_analysis:
        media_analysis = ad.get('media_analysis', {})
        
        # Проверяем анализ изображений
        image_analysis = media_analysis.get('image_analysis')
        if image_analysis is not None:
            # Если есть готовый анализ (не только image_data)
            if isinstance(image_analysis, dict) and 'raw_analysis' in image_analysis:
                analysis_text = image_analysis.get('raw_analysis', '')
                if analysis_text:
                    # Используем Gemini для контекстного анализа
                    if check_if_excluded_content_via_gemini(analysis_text):
                        return False
        
        # Проверяем анализ видео
        video_analysis = media_analysis.get('video_analysis')
        if video_analysis is not None:
            if isinstance(video_analysis, dict) and 'raw_analysis' in video_analysis:
                analysis_text = video_analysis.get('raw_analysis', '')
                if analysis_text:
                    # Используем Gemini для контекстного анализа
                    if check_if_excluded_content_via_gemini(analysis_text):
                        return False
    
    return True


def check_if_excluded_content_via_gemini(analysis_text: str) -> bool:
    """
    Использует Gemini для контекстного анализа текста и определения,
    является ли контент курсом обучения, приложением для чтения или другим нецелевым контентом.
    
    Args:
        analysis_text: Текст анализа медиа (raw_analysis)
    
    Returns:
        True если контент должен быть исключен, False если подходит
    """
    global GEMINI_QUOTA_EXHAUSTED
    
    if not GEMINI_AVAILABLE or not analysis_text or GEMINI_QUOTA_EXHAUSTED:
        return False
    
    try:
        # Настраиваем Gemini
        model = configure_gemini()
        
        # Промпт для контекстного анализа
        context_prompt = """Analyze the following Facebook ad content analysis and determine if this ad promotes:

1. Online courses, training programs, educational courses (NOT medical treatment courses)
2. Reading applications, e-book apps, or similar applications
3. Educational platforms (Udemy, Coursera, Hotmart, Teachable, etc.)

IMPORTANT CONTEXT:
- "Course of treatment" or "medical course" (курс лечения) = ACCEPTABLE (medical treatment)
- "Training course" or "online course" = EXCLUDE (educational content)
- Medical devices like tonometer (тонометр) or glucometer (глюкометр) in medical product ads = ACCEPTABLE
- Reading apps, e-book apps = EXCLUDE

Respond with ONLY one word: "EXCLUDE" if the ad should be excluded, or "KEEP" if it should be kept.

Content analysis:
{analysis_text}
""".format(analysis_text=analysis_text[:8000])  # Ограничиваем длину для экономии токенов
        
        # Анализируем через Gemini
        response = model.generate_content(context_prompt)
        
        if response.text:
            result = response.text.strip().upper()
            # Если Gemini говорит исключить - возвращаем True
            return "EXCLUDE" in result or result == "EXCLUDE"
        
        return False
        
    except Exception as e:
        error_str = str(e).lower()
        # Проверяем, является ли ошибка связанной с квотой или заблокированным ключом
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
            if not GEMINI_QUOTA_EXHAUSTED:
                GEMINI_QUOTA_EXHAUSTED = True
                if 'leaked' in error_str or '403' in error_str:
                    print(f"\n⚠ ОШИБКА: API ключ Gemini заблокирован или утерян. Прерываем анализ медиа.")
                    print(f"   Ошибка: {e}")
                    print(f"   Пожалуйста, обновите API ключ Gemini и перезапустите скрипт.")
                else:
                    print(f"\n⚠ Квота Gemini исчерпана. Пропускаем анализ медиа для оставшихся объявлений.")
            return False
        # В случае другой ошибки не исключаем объявление (fail-safe)
        print(f"Warning: Gemini context check failed: {e}")
        return False


def analyze_media(ad: Dict[str, Any]) -> Dict[str, Any]:
    """Анализирует медиа объявления."""
    global GEMINI_QUOTA_EXHAUSTED
    
    media_type = ad.get('media_type', '')
    media_url = ad.get('media_url', '')
    ad_id = ad.get('ad_id', '')
    
    analysis_result = {
        'image_analysis': None,
        'video_analysis': None,
        'analysis_error': None
    }
    
    if not media_url or GEMINI_QUOTA_EXHAUSTED:
        return analysis_result
    
    try:
        if media_type == 'IMAGE':
            result = analyze_ad_image(
                media_urls=media_url,
                brand_name=None,
                ad_id=ad_id
            )
            if result.get('success'):
                if result.get('analysis'):
                    # Сохраняем полный анализ, включая raw_analysis
                    analysis_result['image_analysis'] = result.get('analysis', {})
                elif result.get('image_data'):
                    analysis_result['image_analysis'] = {
                        'has_image_data': True,
                        'analysis_instructions': result.get('analysis_instructions', '')
                    }
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                # Проверяем, является ли ошибка связанной с квотой или заблокированным ключом
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
                        if 'leaked' in error_str or '403' in error_str:
                            print(f"\n⚠ ОШИБКА: API ключ Gemini заблокирован или утерян. Прерываем анализ медиа.")
                            print(f"   Ошибка: {error}")
                            print(f"   Пожалуйста, обновите API ключ Gemini и перезапустите скрипт.")
                        else:
                            print(f"\n⚠ Квота Gemini исчерпана. Пропускаем анализ медиа для оставшихся объявлений.")
        
        elif media_type == 'VIDEO':
            result = analyze_ad_video(
                media_url=media_url,
                brand_name=None,
                ad_id=ad_id
            )
            if result.get('success'):
                # Сохраняем полный анализ, включая raw_analysis
                analysis_result['video_analysis'] = result.get('analysis', {})
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                # Проверяем, является ли ошибка связанной с квотой или заблокированным ключом
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
                        if 'leaked' in error_str or '403' in error_str:
                            print(f"\n⚠ ОШИБКА: API ключ Gemini заблокирован или утерян. Прерываем анализ медиа.")
                            print(f"   Ошибка: {error}")
                            print(f"   Пожалуйста, обновите API ключ Gemini и перезапустите скрипт.")
                        else:
                            print(f"\n⚠ Квота Gemini исчерпана. Пропускаем анализ медиа для оставшихся объявлений.")
        
        elif media_type == 'DCO':
            result = analyze_ad_image(
                media_urls=media_url,
                brand_name=None,
                ad_id=ad_id
            )
            if result.get('success'):
                if result.get('analysis'):
                    # Сохраняем полный анализ, включая raw_analysis
                    analysis_result['image_analysis'] = result.get('analysis', {})
                elif result.get('image_data'):
                    analysis_result['image_analysis'] = {
                        'has_image_data': True,
                        'analysis_instructions': result.get('analysis_instructions', '')
                    }
            else:
                error = result.get('error', 'Unknown error')
                analysis_result['analysis_error'] = error
                # Проверяем, является ли ошибка связанной с квотой или заблокированным ключом
                error_str = str(error).lower()
                if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503', 'exceeded', 'leaked', '403']):
                    if not GEMINI_QUOTA_EXHAUSTED:
                        GEMINI_QUOTA_EXHAUSTED = True
                        if 'leaked' in error_str or '403' in error_str:
                            print(f"\n⚠ ОШИБКА: API ключ Gemini заблокирован или утерян. Прерываем анализ медиа.")
                            print(f"   Ошибка: {error}")
                            print(f"   Пожалуйста, обновите API ключ Gemini и перезапустите скрипт.")
                        else:
                            print(f"\n⚠ Квота Gemini исчерпана. Пропускаем анализ медиа для оставшихся объявлений.")
    
    except Exception as e:
        error_str = str(e).lower()
        analysis_result['analysis_error'] = str(e)
        # Проверяем, является ли ошибка связанной с квотой
        if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
            GEMINI_QUOTA_EXHAUSTED = True
            print(f"\n⚠ Квота Gemini исчерпана. Пропускаем анализ медиа для оставшихся объявлений.")
    
    return analysis_result


def deduplicate_ads(ads_by_url: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Дедуплицирует объявления по внешним URL."""
    deduplicated = []
    
    for url, ads_list in ads_by_url.items():
        if len(ads_list) == 1:
            deduplicated.append(ads_list[0])
        elif len(ads_list) == 2:
            deduplicated.extend(ads_list)
        else:
            first_ad = ads_list[0].copy()
            first_ad['url_occurrences'] = len(ads_list)
            deduplicated.append(first_ad)
    
    return deduplicated


def collect_ads(keywords: List[str], country: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Собирает объявления по ключевым словам."""
    print(f"\n=== Сбор объявлений ===")
    if country:
        print(f"Страна: {country}")
    print(f"Ключевые слова: {', '.join(keywords)}")
    
    all_ads = []
    ads_by_url = defaultdict(list)
    
    for keyword in keywords:
        print(f"\nПоиск по ключевому слову: '{keyword}'")
        
        try:
            # Поиск объявлений
            ads = search_ads_by_keyword(
                query=keyword,
                limit=limit,
                country=country,
                active_status="ACTIVE",
                media_type="ALL",
                trim=False
            )
            
            print(f"Найдено объявлений: {len(ads)}")
            
            # Фильтрация и анализ медиа
            filtered_ads = []
            total_ads = len(ads)
            for idx, ad in enumerate(ads, 1):
                # Сначала базовая фильтрация
                if filter_ad(ad):
                    # Затем анализ медиа (выполняется до финальной фильтрации)
                    ad['search_query'] = keyword
                    print(f"  Анализ медиа {idx}/{total_ads}...", end='\r')
                    media_analysis = analyze_media(ad)
                    ad['media_analysis'] = media_analysis
                    
                    # Дополнительная фильтрация на основе анализа медиа
                    # Используем анализ для более точной фильтрации
                    if filter_ad(ad, use_media_analysis=True):
                        filtered_ads.append(ad)
            print()  # Новая строка после прогресса
            
            print(f"После фильтрации: {len(filtered_ads)}")
            
            # Группировка по внешним URL
            for ad in filtered_ads:
                external_urls = ad.get('external_urls', [])
                if external_urls:
                    primary_url = external_urls[0].get('full_url', '')
                    if primary_url:
                        ads_by_url[primary_url].append(ad)
            
            all_ads.extend(filtered_ads)
            
        except Exception as e:
            print(f"Ошибка при поиске по '{keyword}': {str(e)}")
            continue
    
    # Дедупликация
    print(f"\nВсего уникальных URL: {len(ads_by_url)}")
    deduplicated_ads = deduplicate_ads(ads_by_url)
    print(f"После дедупликации: {len(deduplicated_ads)} объявлений")
    
    return deduplicated_ads


def load_existing_ads(filepath: str) -> tuple:
    """Загружает существующие объявления и возвращает множество URL и данные."""
    if not os.path.exists(filepath):
        return set(), []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        existing_ads = json.load(f)
    
    # Собираем все существующие URL (формат из файла - список строк)
    existing_urls = set()
    for ad in existing_ads:
        external_urls = ad.get('external_urls', [])
        if external_urls:
            for url in external_urls:
                if isinstance(url, str) and url.strip():
                    existing_urls.add(url.strip())
                elif isinstance(url, dict) and url.get('full_url'):
                    existing_urls.add(url.get('full_url').strip())
    
    return existing_urls, existing_ads


def convert_ad_to_file_format(ad: dict) -> dict:
    """Преобразует объявление из формата collect_ads в формат файла."""
    ad_data = {
        'ad_id': ad.get('ad_id'),
        'ad_text': ad.get('body'),
        'external_urls': [u.get('full_url') if isinstance(u, dict) else u for u in ad.get('external_urls', [])],
        'fanpage_url': f"https://www.facebook.com/{ad.get('page_id', '')}" if ad.get('page_id') else None,
        'ad_url': f"https://www.facebook.com/ads/library/?id={ad.get('ad_id')}" if ad.get('ad_id') else None,
        'start_date': ad.get('start_date'),
        'end_date': ad.get('end_date'),
        'media_type': ad.get('media_type'),
        'media_url': ad.get('media_url'),
        'search_query': ad.get('search_query'),
        'url_occurrences': ad.get('url_occurrences', 1),
        'media_analysis': ad.get('media_analysis', {})
    }
    # Добавляем title только если он не пустой
    title = ad.get('title', '')
    if title and title.strip():
        ad_data['title'] = title
    
    return ad_data


def save_results(ads: list, filename: str):
    """Сохраняет результаты в JSON файл."""
    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    filepath = os.path.join(results_dir, filename)
    
    output_data = []
    for ad in ads:
        if isinstance(ad, dict) and 'ad_id' in ad:
            # Уже в формате файла
            output_data.append(ad)
        else:
            # Нужно преобразовать
            output_data.append(convert_ad_to_file_format(ad))
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nРезультаты сохранены в: {filepath}")


def filter_new_ads(ads: list, existing_urls: set, max_ads: int = None) -> list:
    """Фильтрует объявления, оставляя только те, которых нет в existing_urls."""
    new_ads = []
    for ad in ads:
        external_urls = ad.get('external_urls', [])
        if external_urls:
            # Формат из collect_ads - список словарей с 'full_url'
            if isinstance(external_urls[0], dict):
                primary_url = external_urls[0].get('full_url', '')
            else:
                primary_url = external_urls[0]
            
            if primary_url and primary_url.strip() and primary_url.strip() not in existing_urls:
                new_ads.append(ad)
                # Добавляем URL в множество, чтобы не дублировать в рамках одного запуска
                existing_urls.add(primary_url.strip())
                # Останавливаемся после нужного количества
                if max_ads and len(new_ads) >= max_ads:
                    break
    
    return new_ads


def check_api_limits():
    """Проверяет лимиты API для ScrapeCreators и Gemini."""
    print("\n=== Проверка лимитов API ===\n")
    
    # Проверка ScrapeCreators API
    print("ScrapeCreators API:")
    try:
        from src.services.scrapecreators_service import get_scrapecreators_api_key
        api_key = get_scrapecreators_api_key()
        print("  ✓ API ключ найден")
        
        # Попытка сделать тестовый запрос для проверки кредитов
        import requests
        response = requests.get(
            "https://api.scrapecreators.com/v1/facebook/adLibrary/search/ads",
            headers={"x-api-key": api_key},
            params={"query": "test", "limit": 1},
            timeout=10
        )
        
        # Проверяем заголовки на наличие информации о кредитах
        credit_info = {}
        headers = response.headers
        for header_name in ['x-credits-remaining', 'x-credit-remaining', 'credits-remaining']:
            if header_name in headers:
                try:
                    credit_info['credits_remaining'] = int(headers[header_name])
                    print(f"  ✓ Кредиты осталось: {credit_info['credits_remaining']}")
                except ValueError:
                    pass
        
        if not credit_info:
            if response.status_code == 200:
                print("  ✓ API доступен (информация о кредитах не найдена в заголовках)")
            elif response.status_code == 402:
                print("  ✗ Кредиты исчерпаны (402 Payment Required)")
            elif response.status_code == 429:
                retry_after = headers.get('retry-after', 'неизвестно')
                print(f"  ⚠ Превышен лимит запросов (429). Повторить через: {retry_after} сек")
            else:
                print(f"  ⚠ Статус: {response.status_code}")
        
    except Exception as e:
        print(f"  ✗ Ошибка: {str(e)}")
    
    # Проверка Gemini API
    print("\nGemini API:")
    try:
        if GEMINI_AVAILABLE:
            api_key = get_gemini_api_key()
            if api_key:
                print("  ✓ API ключ найден")
                # Попытка сделать тестовый запрос
                try:
                    model = configure_gemini()
                    test_response = model.generate_content("Test")
                    print("  ✓ API доступен")
                except Exception as e:
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ['quota', 'resource exhausted', 'credit', 'rate limit', '429', '503']):
                        print(f"  ✗ Квота исчерпана: {str(e)[:100]}")
                    else:
                        print(f"  ⚠ Ошибка: {str(e)[:100]}")
            else:
                print("  ✗ API ключ не найден")
        else:
            print("  ✗ Gemini недоступен (не установлен или ошибка импорта)")
    except Exception as e:
        print(f"  ✗ Ошибка: {str(e)}")
    
    print()


def main():
    """Основная функция сбора объявлений."""
    parser = argparse.ArgumentParser(
        description='Универсальный скрипт для сбора объявлений. Фильтрация через Gemini.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python search_ads.py DE "bauch weg" "kilos verlieren"                    # Германия, новый файл
  python search_ads.py DE --append "bauch weg" "kilos verlieren"            # Германия, добавить в существующий
  python search_ads.py MX --append --limit 2 "adelgazar" "quemar grasa"     # Мексика, добавить по 2
  python search_ads.py US --file my_ads.json "lose weight" "burn fat"       # США, указать конкретный файл
  python search_ads.py --check-limits                                       # Проверить лимиты API
        """
    )
    
    parser.add_argument('country', nargs='?', help='Код страны (DE, MX, US, ES и т.д.)')
    parser.add_argument('keywords', nargs='*', help='Ключевые слова для поиска (можно указать несколько)')
    parser.add_argument('--append', '-a', action='store_true', 
                       help='Добавить в существующий файл вместо создания нового')
    parser.add_argument('--file', '-f', type=str, 
                       help='Имя файла для сохранения/добавления (по умолчанию автоматически)')
    parser.add_argument('--limit', '-l', type=int, 
                       help='Максимальное количество новых объявлений')
    parser.add_argument('--check-limits', action='store_true',
                       help='Проверить лимиты API и выйти')
    
    args = parser.parse_args()
    
    # Если запрошена проверка лимитов
    if args.check_limits:
        check_api_limits()
        return
    
    # Проверяем обязательные аргументы
    if not args.country or not args.keywords:
        parser.print_help()
        return
    
    country = args.country.upper()
    keywords = args.keywords
    append_mode = args.append
    target_file = args.file
    max_ads = args.limit
    
    if append_mode:
        if not target_file:
            # Используем последний файл по умолчанию
            results_dir = "results"
            if os.path.exists(results_dir):
                # Ищем все JSON файлы, сортируем по дате изменения
                files = [f for f in os.listdir(results_dir) if f.endswith(".json")]
                if files:
                    # Сортируем по времени изменения (новые первыми)
                    files.sort(key=lambda f: os.path.getmtime(os.path.join(results_dir, f)), reverse=True)
                    target_file = files[0]
        
        if target_file:
            print(f"=== Добавление новых объявлений в существующий файл ===\n")
            print(f"Страна: {country}")
            print(f"Целевой файл: results/{target_file}\n")
            
            # Загружаем существующие объявления
            filepath = os.path.join("results", target_file)
            existing_urls, existing_ads = load_existing_ads(filepath)
            print(f"Загружено существующих объявлений: {len(existing_ads)}")
            print(f"Существующих URL: {len(existing_urls)}\n")
            
            updated_urls = existing_urls.copy()
            new_ads_count = 0
        else:
            print(f"⚠ Файл не найден, будет создан новый файл\n")
            append_mode = False
            existing_ads = []
            updated_urls = set()
            new_ads_count = 0
    else:
        # Если не указан файл, ищем существующий файл для этой страны
        if not target_file:
            results_dir = "results"
            if os.path.exists(results_dir):
                # Ищем файлы для этой страны по паттерну ads_{country}_*.json
                country_pattern = f"ads_{country.lower()}_"
                files = [f for f in os.listdir(results_dir) if f.endswith(".json") and f.startswith(country_pattern)]
                if files:
                    # Сортируем по времени изменения (новые первыми)
                    files.sort(key=lambda f: os.path.getmtime(os.path.join(results_dir, f)), reverse=True)
                    target_file = files[0]
                    print(f"=== Найден существующий файл для {country} ===\n")
                    print(f"Будет использован файл: results/{target_file}\n")
                    append_mode = True
        
        if append_mode and target_file:
            print(f"=== Добавление новых объявлений в существующий файл ===\n")
            print(f"Страна: {country}")
            print(f"Целевой файл: results/{target_file}\n")
            
            # Загружаем существующие объявления
            filepath = os.path.join("results", target_file)
            existing_urls, existing_ads = load_existing_ads(filepath)
            print(f"Загружено существующих объявлений: {len(existing_ads)}")
            print(f"Существующих URL: {len(existing_urls)}\n")
            
            updated_urls = existing_urls.copy()
            new_ads_count = 0
    else:
        print(f"=== Сбор объявлений из {country} ===\n")
        existing_ads = []
        updated_urls = set()
        new_ads_count = 0
    
    print(f"Ключевые слова: {', '.join(keywords)}\n")
    
    try:
        # Сбор объявлений
        limit = max_ads * 3 if max_ads else 50  # Увеличиваем лимит для поиска новых
        ads = collect_ads(
            keywords=keywords,
            country=country,
            limit=limit
        )
        
        # Если режим добавления - фильтруем новые
        if append_mode:
            ads = filter_new_ads(ads, updated_urls, max_ads)
        
        # Применяем лимит, если указан (не в режиме добавления)
        if not append_mode and max_ads and len(ads) > max_ads:
            ads = ads[:max_ads]
        
        # Преобразуем в формат файла
        all_ads = []
        for ad in ads:
            converted_ad = convert_ad_to_file_format(ad)
            all_ads.append(converted_ad)
        
        if all_ads:
            print(f"\n✓ Найдено {len(all_ads)} объявлений")
        else:
            print(f"\n⚠ Новых объявлений не найдено")
    
    except Exception as e:
        print(f"\n✗ Ошибка при сборе объявлений: {str(e)}")
        all_ads = []
    
    # Сохранение результатов
    if all_ads:
        
        if append_mode and target_file:
            # Добавляем в существующий файл
            existing_ads.extend(all_ads)
            filepath = os.path.join("results", target_file)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(existing_ads, f, ensure_ascii=False, indent=2)
            
            new_ads_count = len(all_ads)
            total_ads = len(existing_ads)
            print(f"\n{'='*60}")
            print(f"=== Обновление завершено ===")
            print(f"Добавлено новых объявлений: {new_ads_count}")
            print(f"Всего объявлений в файле: {total_ads}")
            print(f"Файл обновлен: results/{target_file}")
            print(f"{'='*60}")
        else:
            # Создаем новый файл
            if target_file:
                filename = target_file
            else:
                # Если файл не указан и не найден существующий - спрашиваем перед созданием
                print(f"\n⚠ Файл для страны {country} не найден.")
                print(f"Для создания нового файла используйте флаг --file или --append")
                print(f"Пример: python search_ads.py {country} --file my_file.json \"keyword1\" \"keyword2\"")
                return
            
            save_results(all_ads, filename)
            
            print(f"\n{'='*60}")
            print(f"=== Сбор завершен ===")
            total_ads = len(all_ads)
            print(f"Всего объявлений: {total_ads}")
            print(f"Файл сохранен: results/{filename}")
            print(f"{'='*60}")
    else:
        print("\n=== Объявления не найдены ===")


if __name__ == "__main__":
    main()
