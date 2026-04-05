import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from firecrawl import Firecrawl

load_dotenv()


ProgressCallback = Callable[[int, int, str, Optional[str]], None]
LogCallback = Callable[[str], None]


def _coerce_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return {}


def _deep_get(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current


def _extract_scrape_id(scrape_result: Any) -> str:
    scrape_id = (
        _deep_get(scrape_result, "metadata", "scrape_id")
        or _deep_get(scrape_result, "metadata", "scrapeId")
        or _deep_get(scrape_result, "data", "metadata", "scrape_id")
        or _deep_get(scrape_result, "data", "metadata", "scrapeId")
    )
    if not scrape_id:
        payload = _coerce_dict(scrape_result)
        raise RuntimeError(
            f"Unable to extract scrape_id from Firecrawl response: {json.dumps(payload, ensure_ascii=False, default=str)}"
        )
    return str(scrape_id)


def _extract_interact_output(interact_result: Any) -> str:
    return str(
        _deep_get(interact_result, "output")
        or _deep_get(interact_result, "result")
        or _deep_get(interact_result, "stdout")
        or ""
    )


def _extract_screenshot_url(scrape_result: Any) -> Optional[str]:
    candidates = [
        _deep_get(scrape_result, "screenshot"),
        _deep_get(scrape_result, "data", "screenshot"),
        _deep_get(scrape_result, "screenshots"),
        _deep_get(scrape_result, "data", "screenshots"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.startswith("http"):
            return candidate
        if isinstance(candidate, list) and candidate:
            first = candidate[0]
            if isinstance(first, str) and first.startswith("http"):
                return first
            if isinstance(first, dict):
                for key in ("url", "src", "screenshot", "href"):
                    value = first.get(key)
                    if isinstance(value, str) and value.startswith("http"):
                        return value
        if isinstance(candidate, dict):
            for key in ("url", "src", "screenshot", "href"):
                value = candidate.get(key)
                if isinstance(value, str) and value.startswith("http"):
                    return value
    return None


def _is_valid_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value.strip())
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(
        parsed.netloc and "." in parsed.netloc
    )


def _parse_interact_json(raw_output: str) -> dict[str, Any]:
    text = raw_output.strip()
    if not text:
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {"raw_output": text}

    return {"raw_output": text}


def _interact_one_step(app: Firecrawl, scrape_id: str) -> dict[str, Any]:
    prompt = """
You are operating a quiz funnel in a browser.

Your task for this call:
1. Inspect the current screen.
2. Decide whether the current screen is:
   - a normal quiz/question/progress screen that should be advanced by exactly one step, or
   - a monetization barrier, including any paywall, checkout, subscription offer, purchase screen, pricing screen, locked results screen, trial offer, or other screen asking the user to pay or subscribe.
3. If it is a normal quiz/question/progress screen, choose one plausible answer naturally, complete exactly one next step, and stop as soon as the next screen finishes loading.
4. If it is a monetization barrier, do not continue further, do not click purchase-related controls, and stop immediately.

Return only valid JSON with this exact schema:
{
  "status": "advanced" | "paywall" | "unknown",
  "current_url": "string",
  "summary": "short human-readable summary of what you saw and did",
  "selected_answer": "string or empty string",
  "next_action": "string or empty string"
}

Rules:
- Output JSON only, with no markdown fences and no extra text.
- If you advanced the funnel by one step, use status "advanced".
- If you stopped because the screen is a monetization barrier, use status "paywall".
- If you are genuinely uncertain or cannot act safely, use status "unknown".
""".strip()

    response = app.interact(scrape_id, prompt=prompt)
    raw_output = _extract_interact_output(response)
    parsed = _parse_interact_json(raw_output)
    parsed["raw_output"] = raw_output
    return parsed


@dataclass
class FirecrawlFallbackResult:
    used: bool
    status: str
    steps_total: int
    paywall_reached: bool
    last_url: str
    last_screenshot: Optional[str]
    screenshot_urls: list[str]
    progress_message: str
    error: Optional[str]
    provider: str = "firecrawl"


def run_firecrawl_fallback(
    start_url: str,
    max_steps: int = 25,
    progress_callback: Optional[ProgressCallback] = None,
    log_callback: Optional[LogCallback] = None,
) -> FirecrawlFallbackResult:
    def log(message: str) -> None:
        if log_callback:
            try:
                log_callback(message)
            except Exception:
                pass

    api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    if not api_key:
        return FirecrawlFallbackResult(
            used=False,
            status="disabled",
            steps_total=0,
            paywall_reached=False,
            last_url=start_url,
            last_screenshot=None,
            screenshot_urls=[],
            progress_message="Firecrawl fallback disabled: FIRECRAWL_API_KEY is not set",
            error="firecrawl_api_key_missing",
        )

    app = Firecrawl(api_key=api_key)
    current_url = start_url
    scrape_id: Optional[str] = None
    last_screenshot: Optional[str] = None
    screenshot_urls: list[str] = []
    steps_done = 0

    try:
        log(f"Firecrawl fallback: старт с URL {start_url}")
        initial = app.scrape(
            start_url,
            formats=[
                {
                    "type": "screenshot",
                    "fullPage": True,
                    "quality": 70,
                    "viewport": {"width": 430, "height": 932},
                }
            ],
            max_age=0,
            only_main_content=False,
        )
        scrape_id = _extract_scrape_id(initial)
        last_screenshot = _extract_screenshot_url(initial)
        if last_screenshot:
            screenshot_urls.append(last_screenshot)
            log(f"Firecrawl fallback: initial screenshot {last_screenshot}")
        log(f"Firecrawl fallback: scrape_id={scrape_id}")

        for step in range(1, max_steps + 1):
            message = f"Firecrawl fallback: шаг {step}/{max_steps}"
            log(f"{message} | current_url={current_url}")
            if progress_callback:
                progress_callback(step, max_steps, message, current_url)

            state = _interact_one_step(app, scrape_id)
            steps_done = step

            candidate_url = str(state.get("current_url") or "").strip()
            if _is_valid_http_url(candidate_url):
                current_url = candidate_url

            status = str(state.get("status") or "").strip().lower()
            summary = str(state.get("summary") or "").strip()
            selected_answer = str(state.get("selected_answer") or "").strip()
            next_action = str(state.get("next_action") or "").strip()
            log(
                "Firecrawl fallback: interact result "
                f"status={status or 'unknown'} | url={current_url} | "
                f"answer={selected_answer or '-'} | next={next_action or '-'} | "
                f"summary={summary or '-'}"
            )

            screenshot_result = app.scrape(
                current_url,
                formats=[
                    {
                        "type": "screenshot",
                        "fullPage": True,
                        "quality": 70,
                        "viewport": {"width": 430, "height": 932},
                    }
                ],
                max_age=0,
                only_main_content=False,
            )
            next_screenshot = _extract_screenshot_url(screenshot_result)
            if next_screenshot:
                last_screenshot = next_screenshot
                if next_screenshot not in screenshot_urls:
                    screenshot_urls.append(next_screenshot)
                log(f"Firecrawl fallback: screenshot {next_screenshot}")

            progress_message = (
                f"Firecrawl fallback: {summary}"
                if summary
                else f"Firecrawl fallback: status={status or 'unknown'}"
            )

            if status == "paywall":
                return FirecrawlFallbackResult(
                    used=True,
                    status="paywall",
                    steps_total=steps_done,
                    paywall_reached=True,
                    last_url=current_url,
                    last_screenshot=last_screenshot,
                    screenshot_urls=screenshot_urls,
                    progress_message=progress_message,
                    error=None,
                )
            if status == "unknown":
                return FirecrawlFallbackResult(
                    used=True,
                    status="unknown",
                    steps_total=steps_done,
                    paywall_reached=False,
                    last_url=current_url,
                    last_screenshot=last_screenshot,
                    screenshot_urls=screenshot_urls,
                    progress_message=progress_message,
                    error="firecrawl_unknown_state",
                )

            time.sleep(1)

        return FirecrawlFallbackResult(
            used=True,
            status="max_steps",
            steps_total=steps_done,
            paywall_reached=False,
            last_url=current_url,
            last_screenshot=last_screenshot,
            screenshot_urls=screenshot_urls,
            progress_message="Firecrawl fallback: достигнут лимит шагов",
            error="firecrawl_max_steps_reached",
        )
    except Exception as exc:
        log(f"Firecrawl fallback: ошибка {str(exc)[:180]}")
        return FirecrawlFallbackResult(
            used=True,
            status="error",
            steps_total=steps_done,
            paywall_reached=False,
            last_url=current_url,
            last_screenshot=last_screenshot,
            screenshot_urls=screenshot_urls,
            progress_message=f"Firecrawl fallback error: {str(exc)[:160]}",
            error=f"firecrawl_exception:{str(exc)[:180]}",
        )
    finally:
        if scrape_id:
            try:
                app.stop_interaction(scrape_id)
                log(
                    f"Firecrawl fallback: interaction stopped for scrape_id={scrape_id}"
                )
            except Exception:
                pass
