import base64
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

from dotenv import load_dotenv
from firecrawl import Firecrawl

load_dotenv()


ProgressCallback = Callable[[int, int, str, Optional[str]], None]
LogCallback = Callable[[str], None]
ArtifactCallback = Callable[[str, str], None]


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


def _parse_interact_json(raw_output: str) -> dict[str, Any]:
    text = raw_output.strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {"raw_output": text}
        except json.JSONDecodeError:
            return {"raw_output": text}

    return {"raw_output": text}


def _compact_debug_text(value: Any, limit: int = 800) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", "\\n")
    return text[:limit] + ("..." if len(text) > limit else "")


def _debug_response_snapshot(response: Any) -> dict[str, Any]:
    data = getattr(response, "data", None)
    snapshot = {
        "response_type": type(response).__name__,
        "output_preview": _compact_debug_text(getattr(response, "output", None)),
        "result_preview": _compact_debug_text(getattr(response, "result", None)),
        "stdout_preview": _compact_debug_text(getattr(response, "stdout", None)),
        "data_type": type(data).__name__ if data is not None else None,
        "data_preview": _compact_debug_text(data),
    }
    return snapshot


def _emit_artifact(
    file_path: Optional[str],
    artifact_callback: Optional[ArtifactCallback],
    drive_subdir: str = "",
) -> None:
    if not file_path or not artifact_callback:
        return
    try:
        artifact_callback(file_path, drive_subdir)
    except Exception:
        pass


def _ensure_dirs(base_dir: str) -> tuple[str, str]:
    os.makedirs(base_dir, exist_ok=True)
    classified_dir = os.path.join("results", "_classified")
    for category in [
        "question",
        "info",
        "input",
        "email",
        "paywall",
        "other",
        "checkout",
    ]:
        os.makedirs(os.path.join(classified_dir, category), exist_ok=True)
    return base_dir, classified_dir


def _classify_firecrawl_screen(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "").strip().lower()
    screen_type = (
        str(payload.get("screen_type") or payload.get("last_screen_type") or "")
        .strip()
        .lower()
    )
    summary = (
        str(payload.get("summary") or payload.get("raw_output") or "").strip().lower()
    )
    visible = " ".join(
        str(item).strip().lower()
        for item in payload.get("visible_signals", [])
        if str(item).strip()
    )
    text = " ".join(part for part in [status, screen_type, summary, visible] if part)

    if any(
        token in text
        for token in ["checkout", "payment method", "card details", "secure checkout"]
    ):
        return "checkout"
    if any(
        token in text
        for token in [
            "paywall",
            "pricing",
            "subscription",
            "trial offer",
            "locked results",
            "choose plan",
            "upgrade",
        ]
    ):
        return "paywall"
    if any(token in text for token in ["email", "lead form"]):
        return "email"
    if any(
        token in text
        for token in ["input", "form", "date of birth", "height", "weight", "name"]
    ):
        return "input"
    if any(
        token in text
        for token in [
            "transition",
            "loading",
            "analyzing",
            "results preparing",
            "spinner",
        ]
    ):
        return "info"
    if any(
        token in text for token in ["quiz", "question", "answer", "option", "progress"]
    ):
        return "question"
    return "other"


def _save_current_screen_screenshot(
    app: Firecrawl,
    scrape_id: str,
    output_dir: str,
    classified_dir: str,
    slug: str,
    step_index: int,
    screen_type: str,
    artifact_callback: Optional[ArtifactCallback],
) -> str:
    screenshot_response = app.interact(
        scrape_id,
        code="""await (async () => {
                const shotBuffer = await page.screenshot({
                    fullPage: true,
                    type: 'jpeg',
                    quality: 70,
                });
                return shotBuffer.toString('base64');
            })();""",
        language="node",
    )
    screenshot_base64 = str(getattr(screenshot_response, "result", "") or "").strip()
    if screenshot_base64.startswith(
        ("data:image/png;base64,", "data:image/jpeg;base64,")
    ):
        screenshot_base64 = screenshot_base64.split(",", 1)[1].strip()
    if not screenshot_base64:
        raise RuntimeError("Could not extract base64 screenshot from interact response")

    screen_type = (
        screen_type
        if screen_type
        in {"question", "info", "input", "email", "paywall", "other", "checkout"}
        else "other"
    )
    file_name = f"{step_index:02d}_{screen_type}.jpg"
    local_path = os.path.join(output_dir, file_name)
    with open(local_path, "wb") as screenshot_file:
        screenshot_file.write(base64.b64decode(screenshot_base64.encode("ascii")))

    classified_path = os.path.join(classified_dir, screen_type, f"{slug}__{file_name}")
    shutil.copy2(local_path, classified_path)
    _emit_artifact(local_path, artifact_callback)
    _emit_artifact(
        classified_path, artifact_callback, drive_subdir=f"_classified/{screen_type}"
    )
    return local_path


def _write_manifest(
    base_dir: str,
    start_url: str,
    status: str,
    steps_total: int,
    paywall_reached: bool,
    error: Optional[str],
    progress_message: str,
    screenshots: list[dict[str, Any]],
    artifact_callback: Optional[ArtifactCallback],
) -> str:
    manifest = {
        "url": start_url,
        "started_at": datetime.now().isoformat(),
        "completed_at": datetime.now().isoformat(),
        "status": status,
        "steps_total": steps_total,
        "paywall_reached": paywall_reached,
        "error": error,
        "progress_message": progress_message,
        "screenshots": screenshots,
    }
    manifest_path = os.path.join(base_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, indent=2, ensure_ascii=False)
    _emit_artifact(manifest_path, artifact_callback)
    return manifest_path


def _interact_one_step(app: Firecrawl, scrape_id: str) -> dict[str, Any]:
    prompt = """
You are operating a quiz funnel in a browser.

Your task for this call:
1. Inspect the current screen.
2. Decide whether the current screen is:
   - a normal quiz/question/progress screen that should be advanced by exactly one step,
   - an input/email/form screen that should be safely completed by exactly one step,
   - an informational/loading/transition/results-prep screen that should be advanced if safe,
   - or a monetization barrier, including any paywall, checkout, subscription offer, purchase screen, pricing screen, locked results screen, trial offer, or other screen asking the user to pay or subscribe.
3. If it is not a monetization barrier, complete exactly one safe next step and stop as soon as the next screen finishes loading.
4. If it is a monetization barrier, do not continue further, do not click purchase-related controls, and stop immediately.

Return only valid JSON with this exact schema:
{
  "status": "advanced" | "paywall" | "unknown",
  "screen_type": "question" | "info" | "input" | "email" | "paywall" | "checkout" | "other",
  "summary": "short human-readable summary of what you saw and did",
  "selected_answer": "string or empty string",
  "next_action": "string or empty string",
  "visible_signals": ["short phrase", "short phrase"]
}

Rules:
- Output JSON only, with no markdown fences and no extra text.
- Use status "paywall" for both paywall and checkout screens.
- Use screen_type "checkout" if payment details or card/payment form is visible.
- Use screen_type "paywall" if plans/pricing/subscription/trial offer is visible without payment entry.
- Use screen_type "email" for lead capture/email collection.
- Use screen_type "input" for other form fields.
- Use screen_type "info" for loading, transitions, analysis, or preparation screens.
- If you advanced the funnel by one step, use status "advanced".
- If you are genuinely uncertain or cannot act safely, use status "unknown".
""".strip()

    response = app.interact(scrape_id, prompt=prompt)
    raw_output = _extract_interact_output(response)
    parsed = _parse_interact_json(raw_output)
    parsed["raw_output"] = raw_output
    parsed["_debug_response"] = _debug_response_snapshot(response)
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
    manifest_path: Optional[str] = None
    provider: str = "firecrawl"


def run_firecrawl_fallback(
    start_url: str,
    max_steps: int = 25,
    progress_callback: Optional[ProgressCallback] = None,
    log_callback: Optional[LogCallback] = None,
    artifact_callback: Optional[ArtifactCallback] = None,
    results_dir: Optional[str] = None,
    slug: Optional[str] = None,
) -> FirecrawlFallbackResult:
    def log(message: str) -> None:
        if log_callback:
            try:
                log_callback(message)
            except Exception:
                pass

    api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
    safe_slug = str(slug or "firecrawl_fallback").strip() or "firecrawl_fallback"
    output_dir, classified_dir = _ensure_dirs(
        results_dir or os.path.join("results", safe_slug)
    )

    if not api_key:
        manifest_path = _write_manifest(
            output_dir,
            start_url,
            "disabled",
            0,
            False,
            "firecrawl_api_key_missing",
            "Firecrawl fallback disabled: FIRECRAWL_API_KEY is not set",
            [],
            artifact_callback,
        )
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
            manifest_path=manifest_path,
        )

    app = Firecrawl(api_key=api_key)
    scrape_id: Optional[str] = None
    last_screenshot: Optional[str] = None
    screenshot_urls: list[str] = []
    screenshot_entries: list[dict[str, Any]] = []
    steps_done = 0
    progress_message = ""

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
        log(f"Firecrawl fallback: scrape_id={scrape_id}")

        initial_screen_type = "question"
        last_screenshot = _save_current_screen_screenshot(
            app,
            scrape_id,
            output_dir,
            classified_dir,
            safe_slug,
            0,
            initial_screen_type,
            artifact_callback,
        )
        screenshot_entries.append(
            {"step": 0, "type": initial_screen_type, "path": last_screenshot}
        )
        log(f"Firecrawl fallback: initial screenshot {last_screenshot}")

        for step in range(1, max_steps + 1):
            message = f"Firecrawl fallback: шаг {step}/{max_steps}"
            log(message)
            if progress_callback:
                progress_callback(step, max_steps, message, None)

            state = _interact_one_step(app, scrape_id)
            steps_done = step

            status = str(state.get("status") or "").strip().lower()
            summary = str(state.get("summary") or "").strip()
            selected_answer = str(state.get("selected_answer") or "").strip()
            next_action = str(state.get("next_action") or "").strip()
            screen_type = _classify_firecrawl_screen(state)
            raw_output = str(state.get("raw_output") or "")
            debug_response = state.get("_debug_response") or {}
            log(
                "Firecrawl fallback: interact result "
                f"status={status or 'unknown'} | type={screen_type} | "
                f"answer={selected_answer or '-'} | next={next_action or '-'} | "
                f"summary={summary or '-'}"
            )
            if raw_output:
                log(
                    "Firecrawl fallback: interact raw_output_preview="
                    f"{_compact_debug_text(raw_output, 1200)}"
                )
            if debug_response:
                log(
                    "Firecrawl fallback: interact response snapshot="
                    f"{json.dumps(debug_response, ensure_ascii=False, default=str)}"
                )

            last_screenshot = _save_current_screen_screenshot(
                app,
                scrape_id,
                output_dir,
                classified_dir,
                safe_slug,
                step,
                screen_type,
                artifact_callback,
            )
            screenshot_entries.append(
                {"step": step, "type": screen_type, "path": last_screenshot}
            )
            log(f"Firecrawl fallback: screenshot {last_screenshot}")

            progress_message = (
                f"Firecrawl fallback: {summary}"
                if summary
                else f"Firecrawl fallback: status={status or 'unknown'}"
            )

            if status == "paywall":
                manifest_path = _write_manifest(
                    output_dir,
                    start_url,
                    "success",
                    steps_done,
                    True,
                    None,
                    progress_message,
                    screenshot_entries,
                    artifact_callback,
                )
                return FirecrawlFallbackResult(
                    used=True,
                    status="paywall",
                    steps_total=steps_done,
                    paywall_reached=True,
                    last_url=start_url,
                    last_screenshot=last_screenshot,
                    screenshot_urls=screenshot_urls,
                    progress_message=progress_message,
                    error=None,
                    manifest_path=manifest_path,
                )
            if status == "unknown":
                manifest_path = _write_manifest(
                    output_dir,
                    start_url,
                    "completed",
                    steps_done,
                    False,
                    "firecrawl_unknown_state",
                    progress_message,
                    screenshot_entries,
                    artifact_callback,
                )
                return FirecrawlFallbackResult(
                    used=True,
                    status="unknown",
                    steps_total=steps_done,
                    paywall_reached=False,
                    last_url=start_url,
                    last_screenshot=last_screenshot,
                    screenshot_urls=screenshot_urls,
                    progress_message=progress_message,
                    error="firecrawl_unknown_state",
                    manifest_path=manifest_path,
                )

            time.sleep(1)

        manifest_path = _write_manifest(
            output_dir,
            start_url,
            "completed",
            steps_done,
            False,
            "firecrawl_max_steps_reached",
            "Firecrawl fallback: достигнут лимит шагов",
            screenshot_entries,
            artifact_callback,
        )
        return FirecrawlFallbackResult(
            used=True,
            status="max_steps",
            steps_total=steps_done,
            paywall_reached=False,
            last_url=start_url,
            last_screenshot=last_screenshot,
            screenshot_urls=screenshot_urls,
            progress_message="Firecrawl fallback: достигнут лимит шагов",
            error="firecrawl_max_steps_reached",
            manifest_path=manifest_path,
        )
    except Exception as exc:
        error_message = f"firecrawl_exception:{str(exc)[:180]}"
        log(f"Firecrawl fallback: ошибка {str(exc)[:180]}")
        if "Invalid JSON response" in str(exc):
            try:
                debug_response = app.interact(
                    scrape_id,
                    code="""await (async () => {
                            return JSON.stringify({
                                url: page.url(),
                                title: document.title || '',
                                bodyText: (document.body?.innerText || '').slice(0, 3000),
                            });
                        })();""",
                    language="node",
                )
                log(
                    "Firecrawl fallback: invalid-json page snapshot="
                    f"{json.dumps(_debug_response_snapshot(debug_response), ensure_ascii=False, default=str)}"
                )
                debug_output = _extract_interact_output(debug_response)
                if debug_output:
                    log(
                        "Firecrawl fallback: invalid-json page output="
                        f"{_compact_debug_text(debug_output, 2000)}"
                    )
            except Exception as debug_exc:
                log(
                    "Firecrawl fallback: invalid-json debug capture failed "
                    f"{str(debug_exc)[:180]}"
                )
        manifest_path = _write_manifest(
            output_dir,
            start_url,
            "completed",
            steps_done,
            False,
            error_message,
            f"Firecrawl fallback error: {str(exc)[:160]}",
            screenshot_entries,
            artifact_callback,
        )
        return FirecrawlFallbackResult(
            used=True,
            status="error",
            steps_total=steps_done,
            paywall_reached=False,
            last_url=start_url,
            last_screenshot=last_screenshot,
            screenshot_urls=screenshot_urls,
            progress_message=f"Firecrawl fallback error: {str(exc)[:160]}",
            error=error_message,
            manifest_path=manifest_path,
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
