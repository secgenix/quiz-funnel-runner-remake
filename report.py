"""HTML-отчёт по пакетному прогону: results/report.html.

Один самодостаточный файл рядом со скриншотами: сводка, по каждой воронке —
лента шагов, распределение типов экранов и цены с экрана оплаты. Картинки
подключаются относительными путями, поэтому папку results/ можно целиком
переслать или выложить в Google Drive.
"""

from __future__ import annotations

import html
import json
import os
from datetime import datetime
from urllib.parse import urlparse

TYPE_LABELS = {
    "question": "Вопрос",
    "info": "Инфо",
    "input": "Ввод",
    "email": "Email",
    "paywall": "Paywall",
    "checkout": "Checkout",
    "other": "Другое",
}
TYPE_ORDER = ["question", "info", "input", "email", "other", "paywall", "checkout"]


def _status(summary: dict) -> tuple[str, str]:
    if summary.get("paywall_reached"):
        return "ok", "Дошёл до оплаты"
    if summary.get("stopped"):
        return "warn", "Остановлен"
    if int(summary.get("steps_total") or 0) < 5:
        return "bad", "Не прошёл"
    return "warn", "Частично"


def _duration(seconds: float) -> str:
    seconds = int(round(float(seconds or 0)))
    return f"{seconds // 60} мин {seconds % 60:02d} с" if seconds >= 60 else f"{seconds} с"


def _esc(value) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _funnel_card(summary: dict, index: int) -> str:
    slug = summary.get("slug") or ""
    steps = summary.get("steps") or []
    host = urlparse(summary.get("url") or "").netloc or slug
    state, state_label = _status(summary)

    counts: dict[str, int] = {}
    for step in steps:
        counts[step.get("type") or "other"] = counts.get(step.get("type") or "other", 0) + 1
    total = max(sum(counts.values()), 1)
    bar = "".join(
        f'<i class="t-{t}" style="flex:{counts[t]}" title="{TYPE_LABELS.get(t, t)}: {counts[t]}"></i>'
        for t in TYPE_ORDER if counts.get(t)
    )
    legend = "".join(
        f'<span><i class="dot t-{t}"></i>{TYPE_LABELS.get(t, t)} <b>{counts[t]}</b></span>'
        for t in TYPE_ORDER if counts.get(t)
    )

    frames = []
    for step in steps:
        src = f"{slug}/{step.get('screenshot')}"
        stype = step.get("type") or "other"
        ui = step.get("ui_step")
        caption = f"{int(step.get('n') or 0):02d} · {TYPE_LABELS.get(stype, stype)}"
        title = caption + (f" · {ui}" if ui and ui != "unknown" else "")
        frames.append(
            f'<button type="button" class="frame t-{stype}" data-src="{_esc(src)}" data-cap="{_esc(host + " — " + title)}">'
            f'<span class="shot"><img src="{_esc(src)}" alt="{_esc(title)}" loading="lazy" decoding="async"></span>'
            f'<span class="cap"><i class="dot t-{stype}"></i>{_esc(caption)}</span></button>'
        )

    prices = summary.get("prices") or []
    price_html = (
        '<div class="prices"><span class="lbl">Цены на paywall</span>'
        + "".join(f"<b>{_esc(p)}</b>" for p in prices) + "</div>"
    ) if prices else ""

    plans = summary.get("plans_screenshot")
    plans_html = (
        f'<button type="button" class="plans" data-src="{_esc(slug + "/" + plans)}" data-cap="{_esc(host + " — блок тарифов")}">'
        f'<img src="{_esc(slug + "/" + plans)}" alt="Блок тарифов" loading="lazy"><span>Блок тарифов</span></button>'
    ) if plans else ""

    error = summary.get("error")
    error_html = f'<p class="err">Причина остановки: <code>{_esc(error)}</code></p>' if error and state != "ok" else ""
    last_url = summary.get("last_url") or summary.get("url") or ""

    return f"""
<section class="funnel" id="f{index}">
  <header>
    <div class="who">
      <span class="num">{index:02d}</span>
      <div><h2>{_esc(host)}</h2><a href="{_esc(summary.get('url'))}" rel="noopener noreferrer">{_esc(summary.get('url'))}</a></div>
    </div>
    <span class="badge {state}">{state_label}</span>
  </header>
  <dl class="facts">
    <div><dt>Шагов</dt><dd>{int(summary.get('steps_total') or 0)}</dd></div>
    <div><dt>Время</dt><dd>{_duration(summary.get('duration_sec'))}</dd></div>
    <div><dt>Экранов оплаты</dt><dd>{counts.get('paywall', 0) + counts.get('checkout', 0)}</dd></div>
    <div class="wide"><dt>Остановился на</dt><dd class="url">{_esc(last_url[:120])}</dd></div>
  </dl>
  <div class="mix"><div class="bar" aria-label="Типы экранов, всего {total}">{bar}</div><div class="legend">{legend}</div></div>
  {price_html}{error_html}
  <div class="strip">{''.join(frames)}{plans_html}</div>
</section>"""


CSS = """
:root{--bg:#f4f5f2;--card:#fff;--ink:#14201c;--mute:#5f6d67;--line:#dfe3dd;--ok:#12805c;--okbg:#dff3ea;--warn:#8a5a00;--warnbg:#fbeecb;--bad:#a3261c;--badbg:#fbe1dd;
--q:#3b6cf6;--i:#8f9aa3;--in:#b06ad9;--em:#12a5a5;--ot:#c4c9c2;--pw:#f08a24;--co:#d9463b}
@media (prefers-color-scheme:dark){:root{--bg:#0f1513;--card:#161e1b;--ink:#eaf0ec;--mute:#93a39b;--line:#26322d;--okbg:#113a2c;--ok:#5fd6a8;--warnbg:#3b2f0e;--warn:#f1c453;--badbg:#421a16;--bad:#ff9085;--ot:#3a4640}}
*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 "Segoe UI",system-ui,-apple-system,Roboto,Arial,sans-serif}
.wrap{max-width:1240px;margin:0 auto;padding:40px 24px 80px}
.top{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap;margin-bottom:28px}
.top h1{font-size:30px;line-height:1.15;margin:0 0 6px;letter-spacing:-.02em}
.top p{margin:0;color:var(--mute)}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:28px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 18px}
.kpi b{display:block;font-size:30px;line-height:1.1;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.kpi span{color:var(--mute);font-size:13px}
.toc{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:28px}
.toc a{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;background:var(--card);border:1px solid var(--line);color:var(--ink);text-decoration:none;font-size:13px}
.funnel{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:22px 22px 18px;margin-bottom:20px}
.funnel>header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start}
.who{display:flex;gap:14px;align-items:flex-start;min-width:0}
.num{font:600 13px/1 ui-monospace,Consolas,monospace;color:var(--mute);padding-top:8px}
.funnel h2{margin:0;font-size:21px;letter-spacing:-.01em}
.who a{color:var(--mute);font-size:12.5px;text-decoration:none;display:block;max-width:760px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge{flex:none;font-size:12.5px;font-weight:600;padding:5px 11px;border-radius:999px;white-space:nowrap}
.badge.ok,.dotst.ok{background:var(--okbg);color:var(--ok)}.badge.warn,.dotst.warn{background:var(--warnbg);color:var(--warn)}.badge.bad,.dotst.bad{background:var(--badbg);color:var(--bad)}
.dotst{width:8px;height:8px;border-radius:50%;padding:0;display:inline-block}
.dotst.ok{background:var(--ok)}.dotst.warn{background:var(--warn)}.dotst.bad{background:var(--bad)}
.facts{display:grid;grid-template-columns:110px 130px 150px minmax(0,1fr);gap:8px 24px;margin:18px 0 14px}
.facts div{min-width:0}.facts dt{color:var(--mute);font-size:12px}.facts dd{margin:0;font-size:19px;font-weight:600;font-variant-numeric:tabular-nums}
.facts dd.url{font:13px/1.9 ui-monospace,Consolas,monospace;font-weight:400;color:var(--mute);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mix .bar{display:flex;height:10px;border-radius:999px;overflow:hidden;gap:2px;background:var(--line)}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;color:var(--mute);font-size:12.5px}
.legend b{color:var(--ink);font-weight:600}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.t-question{background:var(--q)}.t-info{background:var(--i)}.t-input{background:var(--in)}.t-email{background:var(--em)}.t-other{background:var(--ot)}.t-paywall{background:var(--pw)}.t-checkout{background:var(--co)}
.prices{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-top:14px}
.prices .lbl{color:var(--mute);font-size:12.5px;margin-right:4px}
.prices b{font:600 13px/1 ui-monospace,Consolas,monospace;padding:6px 9px;border-radius:8px;background:color-mix(in srgb,var(--pw) 16%,transparent);border:1px solid color-mix(in srgb,var(--pw) 40%,transparent)}
.err{margin:12px 0 0;color:var(--mute);font-size:13px}.err code{color:var(--bad)}
.strip{display:flex;gap:12px;overflow-x:auto;padding:18px 2px 10px;scrollbar-width:thin}
.frame,.plans{flex:none;background:none;border:0;padding:0;cursor:zoom-in;color:inherit;font:inherit;text-align:left}
.frame.t-question,.frame.t-info,.frame.t-input,.frame.t-email,.frame.t-other,.frame.t-paywall,.frame.t-checkout{background:none}
.shot{display:block;width:132px;aspect-ratio:390/664;border-radius:16px;overflow:hidden;border:1px solid var(--line);background:var(--bg);box-shadow:0 1px 2px rgba(0,0,0,.05)}
.frame.t-paywall .shot,.frame.t-checkout .shot{outline:2px solid var(--pw);outline-offset:2px}
.shot img{width:100%;height:100%;object-fit:cover;object-position:top;display:block}
.cap{display:flex;align-items:center;margin-top:8px;font-size:12px;color:var(--mute);white-space:nowrap}
.plans{width:260px;align-self:flex-start}
.pw{margin-bottom:28px}.pw h3,.sec{font-size:13px;font-weight:600;color:var(--mute);text-transform:none;margin:0 0 12px}
.pwgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,224px));gap:16px}
.pwcard{background:var(--card);border:1px solid var(--line);border-radius:18px;padding:12px 12px 14px;cursor:zoom-in;font:inherit;color:inherit;text-align:left}
.pwcard .ph{display:block;aspect-ratio:9/16;border-radius:12px;overflow:hidden;background:var(--bg);border:1px solid var(--line)}
.pwcard img{width:100%;height:100%;object-fit:cover;object-position:top;display:block}
.pwcard b{display:block;margin-top:10px;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.pwcard small{display:block;color:var(--mute);font-size:12px;font-family:ui-monospace,Consolas,monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.plans img{width:100%;max-height:286px;object-fit:cover;object-position:top;border-radius:16px;border:1px solid var(--line);display:block}
.plans span{display:block;margin-top:8px;font-size:12px;color:var(--mute)}
.lb{position:fixed;inset:0;background:rgba(8,12,10,.86);display:none;align-items:center;justify-content:center;flex-direction:column;gap:12px;padding:24px;z-index:9;cursor:zoom-out}
.lb.on{display:block;overflow:auto;text-align:center}.lb img{width:min(430px,100%);border-radius:14px;display:block;margin:0 auto 10px}
.lb span{color:#fff;font-size:13px}
footer{color:var(--mute);font-size:12.5px;margin-top:28px}
@media (max-width:760px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.facts{grid-template-columns:1fr 1fr}.facts .wide{grid-column:1/-1}.wrap{padding:24px 16px 60px}}
"""

JS = """
const lb=document.getElementById('lb'),im=lb.querySelector('img'),cp=lb.querySelector('span');
document.addEventListener('click',e=>{const b=e.target.closest('[data-src]');
 if(b){im.src=b.dataset.src;cp.textContent=b.dataset.cap;lb.classList.add('on');return}
 if(e.target.closest('#lb'))lb.classList.remove('on')});
document.addEventListener('keydown',e=>{if(e.key==='Escape')lb.classList.remove('on')});
"""


def build_report(summaries: list, results_dir: str = "results", mode: str = "") -> str:
    """Собрать отчёт и вернуть путь к нему."""
    reached = sum(1 for s in summaries if s.get("paywall_reached"))
    steps = sum(int(s.get("steps_total") or 0) for s in summaries)
    longest = max((float(s.get("duration_sec") or 0) for s in summaries), default=0)
    total_time = sum(float(s.get("duration_sec") or 0) for s in summaries)
    wall = longest if mode == "parallel" else total_time
    stamp = datetime.now().strftime("%d.%m.%Y %H:%M")

    toc = "".join(
        f'<a href="#f{i}"><i class="dotst {_status(s)[0]}"></i>{_esc(urlparse(s.get("url") or "").netloc)}'
        f' · {int(s.get("steps_total") or 0)}</a>'
        for i, s in enumerate(summaries, 1)
    )
    cards = "".join(_funnel_card(s, i) for i, s in enumerate(summaries, 1))
    pay_cards = []
    for s in summaries:
        pay = [st for st in (s.get("steps") or []) if st.get("type") in ("paywall", "checkout")]
        if not pay:
            continue
        host = urlparse(s.get("url") or "").netloc
        src = f"{s.get('slug')}/{pay[-1].get('screenshot')}"
        prices = " · ".join((s.get("prices") or [])[:3]) or "цены не распознаны"
        pay_cards.append(
            f'<button type="button" class="pwcard" data-src="{_esc(src)}" data-cap="{_esc(host)} — экран оплаты">'
            f'<span class="ph"><img src="{_esc(src)}" alt="Экран оплаты {_esc(host)}" loading="lazy"></span>'
            f'<b>{_esc(host)}</b><small>{_esc(prices)}</small></button>'
        )
    paywalls = (
        '<section class="pw"><h3>Экраны оплаты рядом</h3><div class="pwgrid">' + "".join(pay_cards) + "</div></section>"
    ) if pay_cards else ""
    mode_label = {"parallel": "параллельный прогон", "sequential": "последовательный прогон"}.get(mode, "прогон")

    page = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quiz Funnel Runner — отчёт {stamp}</title><style>{CSS}</style></head>
<body><div class="wrap">
<div class="top"><div><h1>Отчёт о прогоне воронок</h1><p>Quiz Funnel Runner · {mode_label} · {stamp} · эмуляция iPhone 13</p></div></div>
<div class="kpis">
  <div class="kpi"><b>{len(summaries)}</b><span>воронок в пакете</span></div>
  <div class="kpi"><b>{reached} из {len(summaries)}</b><span>дошли до экрана оплаты</span></div>
  <div class="kpi"><b>{steps}</b><span>шагов пройдено и снято</span></div>
  <div class="kpi"><b>{_duration(wall)}</b><span>{'время пакета (по самой долгой)' if mode == 'parallel' else 'суммарное время'}</span></div>
</div>
{paywalls}
<nav class="toc">{toc}</nav>
{cards}
<footer>Скриншоты лежат рядом в папках воронок; журнал каждого прогона — <code>log.txt</code>, структура шагов — <code>manifest.json</code>.</footer>
</div>
<div class="lb" id="lb"><img alt=""><span></span></div>
<script>{JS}</script></body></html>"""

    os.makedirs(results_dir, exist_ok=True)
    path = os.path.join(results_dir, "report.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(page)
    return path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Пересобрать results/report.html из results/summary.json")
    parser.add_argument("--results", default="results")
    args = parser.parse_args()
    with open(os.path.join(args.results, "summary.json"), "r", encoding="utf-8") as fh:
        data = json.load(fh)
    items = data.get("funnels", data) if isinstance(data, dict) else data
    mode = data.get("mode", "") if isinstance(data, dict) else ""
    print(build_report(items, args.results, mode))
