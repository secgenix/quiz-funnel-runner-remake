import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor

from runner import run_funnel


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, default='config.json', help='Путь к конфигу')
    parser.add_argument('--parallel', action='store_true', help='Запуск воронок в параллельных потоках')
    parser.add_argument('--debug', action='store_true', help='Режим отладки: показать окна браузера')
    args = parser.parse_args()

    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Ошибка: конфиг не найден: {args.config}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Ошибка: невалидный JSON в конфиге {args.config}: {e}")

    funnels = config.get('funnels', [])
    max_f = config.get('max_funnels')
    if max_f is not None:
        funnels = funnels[:max_f]

    headless = not args.debug
    if args.debug:
        print("\n[DEBUG] Включен режим отладки: браузер запущен в видимом режиме.\n")
    if not funnels:
        raise SystemExit("Ошибка: список funnels пуст, нечего запускать.")

    if args.parallel:
        print(f"\n--- Запуск {len(funnels)} воронок в ПАРАЛЛЕЛЬНОМ режиме ---\n")
        workers = min(len(funnels), max(1, (os.cpu_count() or 2)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(run_funnel, url, config, headless) for url in funnels]
            all_summaries = [f.result() for f in futures]
    else:
        print(f"\n--- Запуск {len(funnels)} воронок в ПОСЛЕДОВАТЕЛЬНОМ режиме ---\n")
        all_summaries = []
        for url in funnels:
            print(f"\n>> Обработка: {url}")
            summary = run_funnel(url, config, headless)
            all_summaries.append(summary)

    os.makedirs('results', exist_ok=True)
    with open(os.path.join('results', 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(all_summaries, f, indent=4, ensure_ascii=False)

    success_count = 0
    failed_count = 0
    partial_count = 0

    for s in all_summaries:
        steps_total = int(s.get('steps_total', 0) or 0)
        is_success = bool(s.get('paywall_reached'))
        if is_success:
            success_count += 1
        elif steps_total < 5:
            failed_count += 1
        else:
            partial_count += 1

    print("\nПакетный запуск завершён.")
    for s in all_summaries:
        steps_total = int(s.get('steps_total', 0) or 0)
        if s.get('paywall_reached'):
            status = "УСПЕХ"
        elif steps_total < 5:
            status = "ОШИБКА"
        else:
            status = "ЧАСТИЧНО"
        print(f"[{status}] URL: {s['url']} | Шагов: {s['steps_total']}")

    print("\n--- Итоговая сводка ---")
    print(f"Всего: {len(all_summaries)}")
    print(f"Успешно: {success_count}")
    print(f"Частично: {partial_count}")
    print(f"Неуспешно: {failed_count}")
