import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from report import build_report
from runner import run_funnel


def status_of(summary: dict) -> str:
    if summary.get("paywall_reached"):
        return "УСПЕХ"
    if int(summary.get("steps_total", 0) or 0) < 5:
        return "ОШИБКА"
    return "ЧАСТИЧНО"


def fmt_duration(seconds: float) -> str:
    seconds = int(round(float(seconds or 0)))
    return f"{seconds // 60}:{seconds % 60:02d}"


def print_summary(summaries: list, wall_seconds: float) -> None:
    host_width = max([len(urlparse(s["url"]).netloc) for s in summaries] + [7])
    line = "-" * (host_width + 46)
    print("\n" + line)
    print(f"{'Воронка':<{host_width}}  {'Статус':<9} {'Шагов':>5}  {'Время':>6}  Цены на paywall")
    print(line)
    for s in summaries:
        prices = ", ".join((s.get("prices") or [])[:4]) or "—"
        print(
            f"{urlparse(s['url']).netloc:<{host_width}}  {status_of(s):<9} "
            f"{int(s.get('steps_total') or 0):>5}  {fmt_duration(s.get('duration_sec')):>6}  {prices}"
        )
    print(line)
    counts = {name: sum(1 for s in summaries if status_of(s) == name) for name in ("УСПЕХ", "ЧАСТИЧНО", "ОШИБКА")}
    steps = sum(int(s.get("steps_total") or 0) for s in summaries)
    print(
        f"Всего: {len(summaries)} | до оплаты: {counts['УСПЕХ']} | частично: {counts['ЧАСТИЧНО']} | "
        f"неуспешно: {counts['ОШИБКА']} | шагов: {steps} | время: {fmt_duration(wall_seconds)}"
    )


if __name__ == '__main__':
    # Консоль Windows по умолчанию не в UTF-8: без этого лог с «✅» роняет прогон.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="Прогон квиз-воронок до экрана оплаты")
    parser.add_argument('urls', nargs='*', help='URL воронок; если не заданы, берутся из конфига')
    parser.add_argument('--config', type=str, default='config.json', help='Путь к конфигу')
    parser.add_argument('--parallel', action='store_true', help='Запуск воронок в параллельных потоках')
    parser.add_argument('--workers', type=int, default=5, help='Сколько воронок идёт одновременно (по умолчанию 5)')
    parser.add_argument('--max-funnels', type=int, default=None, help='Взять только первые N воронок')
    parser.add_argument('--max-steps', type=int, default=None, help='Лимит шагов на воронку')
    parser.add_argument('--debug', action='store_true', help='Режим отладки: показать окна браузера')
    parser.add_argument('--no-report', action='store_true', help='Не собирать results/report.html')
    args = parser.parse_args()

    try:
        with open(args.config, 'r', encoding='utf-8-sig') as f:
            config = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Ошибка: конфиг не найден: {args.config}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Ошибка: невалидный JSON в конфиге {args.config}: {e}")

    if not isinstance(config.get('runner'), dict):
        config['runner'] = {}
    runner_cfg = config['runner']
    if args.max_steps:
        runner_cfg['max_steps'] = args.max_steps

    funnels = [u.strip() for u in (args.urls or config.get('funnels', [])) if u and u.strip()]
    # max_funnels живёт в секции runner; раньше читался только с верхнего уровня и не работал.
    max_f = args.max_funnels or (None if args.urls else runner_cfg.get('max_funnels') or config.get('max_funnels'))
    if max_f:
        funnels = funnels[:int(max_f)]

    headless = not args.debug
    if args.debug:
        print("\n[DEBUG] Включен режим отладки: браузер запущен в видимом режиме.\n")
    if not funnels:
        raise SystemExit("Ошибка: список funnels пуст, нечего запускать.")

    started = time.time()
    if args.parallel and len(funnels) > 1:
        workers = max(1, min(len(funnels), args.workers))
        print(f"\n--- Запуск {len(funnels)} воронок в ПАРАЛЛЕЛЬНОМ режиме ({workers} одновременно) ---\n")
        results = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(run_funnel, url, config, headless): url for url in funnels}
            for future in as_completed(futures):
                summary = future.result()
                results[futures[future]] = summary
                print(f"\n== [{status_of(summary)}] {urlparse(summary['url']).netloc}: "
                      f"{summary['steps_total']} шагов за {fmt_duration(summary.get('duration_sec'))} ==\n", flush=True)
        all_summaries = [results[url] for url in funnels]
        mode = "parallel"
    else:
        print(f"\n--- Запуск {len(funnels)} воронок в ПОСЛЕДОВАТЕЛЬНОМ режиме ---\n")
        all_summaries = []
        for url in funnels:
            print(f"\n>> Обработка: {url}")
            all_summaries.append(run_funnel(url, config, headless))
        mode = "sequential"
    wall = time.time() - started

    os.makedirs('results', exist_ok=True)
    with open(os.path.join('results', 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump({"mode": mode, "wall_sec": round(wall, 1), "funnels": all_summaries}, f, indent=4, ensure_ascii=False)

    print("\nПакетный запуск завершён.")
    print_summary(all_summaries, wall)

    if not args.no_report:
        report_path = build_report(all_summaries, 'results', mode)
        print(f"\nОтчёт: {os.path.abspath(report_path)}")
