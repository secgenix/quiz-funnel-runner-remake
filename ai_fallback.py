import base64
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]
from playwright.sync_api import Page

from config import get_config
from main import (
    get_screen_hash,
    is_forbidden_button,
    is_nav_button,
    wait_for_transition,
)


@dataclass
class AiCandidate:
    label: str
    click_selector: str
    click_nth: int
    group: str  # "cta" or "choice"


AI_REQUEST_SEMAPHORE = threading.Semaphore(2)
_AI_CLIENT = None
_AI_CLIENT_LOCK = threading.Lock()


def _get_openai_client():
    global _AI_CLIENT
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    if OpenAI is None:
        return None
    with _AI_CLIENT_LOCK:
        if _AI_CLIENT is None:
            _AI_CLIENT = OpenAI(api_key=api_key)
    return _AI_CLIENT


def _is_ai_enabled() -> bool:
    cfg = get_config()
    if not cfg.ai_fallback.enabled:
        return False
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    return bool(api_key)


def _capture_screenshot_b64(page: Page) -> Tuple[str, str]:
    png_bytes = page.screenshot(full_page=False)
    b64 = base64.b64encode(png_bytes).decode("utf-8")
    return b64, "image/png"


def _extract_candidates(
    page: Page, log_func=None, max_candidates: int = 16
) -> List[AiCandidate]:
    # Order matters (stable indexes for the model).
    selectors = [
        "input[type='checkbox']:visible",
        "[role='checkbox']:visible",
        "[data-testid*='answer' i]:visible",
        "button:visible",
        "[role='button']:visible",
        "a:visible",
        "label:visible",
        "div[role='button']:visible",
    ]

    candidates: List[AiCandidate] = []
    seen_labels: Dict[str, int] = {}

    for sel in selectors:
        loc = page.locator(sel)
        count = loc.count()
        for i in range(count):
            if len(candidates) >= max_candidates:
                return candidates
            el = loc.nth(i)
            try:
                label = (el.inner_text() or "").strip()
            except Exception:
                label = ""

            if not label:
                try:
                    label = (el.get_attribute("aria-label") or "").strip()
                except Exception:
                    label = ""

            if not label:
                # Best-effort for checkboxes: use associated <label for=...> or closest label.
                try:
                    label = (
                        el.evaluate(
                            "(node) => {\n"
                            "  const aria = (node.getAttribute('aria-label') || '').trim();\n"
                            "  if (aria) return aria;\n"
                            "  const id = node.getAttribute('id');\n"
                            "  if (id) {\n"
                            "    const l = document.querySelector('label[for=' + JSON.stringify(id) + ']');\n"
                            "    if (l && l.innerText) return l.innerText.trim();\n"
                            "  }\n"
                            "  const closest = node.closest('label');\n"
                            "  if (closest && closest.innerText) return closest.innerText.trim();\n"
                            "  return '';\n"
                            "}"
                        )
                        or ""
                    ).strip()
                except Exception:
                    label = ""

            is_checkbox = "checkbox" in sel.lower() or "role='checkbox'" in sel.lower()
            if is_checkbox and not label:
                label = "Consent checkbox"

            if not label:
                continue

            lower = label.lower()
            if len(lower) > 120:
                label = " ".join(label.split())
                label = label[:110].strip() + "..."
                lower = label.lower()

            try:
                if is_forbidden_button(el, log_func=log_func):
                    continue
            except Exception:
                # Best-effort filtering.
                pass

            group = "cta" if is_nav_button(lower) else "choice"

            # Safety: avoid clicking legal document links (privacy/terms) that often
            # navigate away instead of accepting consent.
            # We only allow those when the element itself looks like an acceptance control.
            is_checkbox_like = "checkbox" in sel.lower() or lower.strip().startswith(
                "i agree"
            )
            consent_doc = any(
                k in lower
                for k in [
                    "privacy policy",
                    "privacy",
                    "terms",
                    "term of service",
                    "tos",
                    "cookie policy",
                ]
            )
            acceptance_kw = any(
                k in lower
                for k in [
                    "agree",
                    "accept",
                    "consent",
                    "i agree",
                    "allow",
                    "get my plan",
                    "continue",
                ]
            )
            if consent_doc and not (is_checkbox_like or acceptance_kw):
                continue

            # Keep some duplicates (same label can map to multiple elements).
            # But don't allow unlimited repeats.
            prev = seen_labels.get(lower, 0)
            if prev >= 2:
                continue
            seen_labels[lower] = prev + 1

            candidates.append(
                AiCandidate(label=label, click_selector=sel, click_nth=i, group=group)
            )

    return candidates


def _build_text_snippet(page: Page, limit_chars: int) -> str:
    try:
        text = page.evaluate("() => document.body.innerText") or ""
    except Exception:
        text = ""
    text = " ".join((text or "").split())
    if len(text) > limit_chars:
        text = text[:limit_chars]
    return text


def _build_prompt(
    url: str,
    text_snippet: str,
    candidates: List[AiCandidate],
) -> str:
    cand_lines = []
    for idx, c in enumerate(candidates):
        cand_lines.append(f"{idx}: [{c.group}] {c.label}")

    return (
        "You are helping an automated quiz runner progress to the next step. "
        "You will be given a screenshot, the current URL, a text snippet, and a list of clickable candidates.\n"
        "Return ONLY valid JSON with the exact shape: "
        '{"action":"click","target_index":number,"confidence":number,"reason":string}.\n'
        "- Choose the single candidate most likely to advance the quiz. If a consent/checkbox must be accepted first, click that checkbox first (even if it doesn't change URL immediately).\n"
        "- target_index must be a valid index from the candidates list.\n"
        "- Do not return any other actions.\n"
        "\n"
        f"URL: {url}\n"
        "---\n"
        f"TEXT_SNIPPET:\n{text_snippet}\n"
        "---\n"
        "CANDIDATES:\n" + "\n".join(cand_lines) + "\n"
    )


def _parse_click_action(
    resp_text: str, candidates_len: int
) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(resp_text)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None
    if data.get("action") != "click":
        return None
    idx = data.get("target_index")
    if idx is None:
        return None
    try:
        idx_int = int(idx)  # type: ignore[arg-type]
    except Exception:
        return None
    if idx_int < 0 or idx_int >= candidates_len:
        return None
    conf = data.get("confidence", 0.0)
    try:
        conf_f = float(conf)
    except Exception:
        conf_f = 0.0

    reason = data.get("reason")
    if not isinstance(reason, str):
        reason = ""

    return {
        "action": "click",
        "target_index": idx_int,
        "confidence": conf_f,
        "reason": reason,
    }


def _call_openai_for_click(
    model: str,
    url: str,
    text_snippet: str,
    screenshot_b64: str,
    candidates: List[AiCandidate],
    timeout_seconds: float,
    log_func=None,
) -> Optional[Dict[str, Any]]:
    client = _get_openai_client()
    if client is None:
        return None
    if not candidates:
        return None

    prompt = _build_prompt(url=url, text_snippet=text_snippet, candidates=candidates)

    # Concurrency cap across threads.
    with AI_REQUEST_SEMAPHORE:
        try:
            # OpenAI python supports JSON mode via response_format.
            resp = client.chat.completions.create(
                model=model,
                temperature=0.2,
                timeout=timeout_seconds,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{screenshot_b64}",
                                },
                            },
                        ],
                    }
                ],
            )
            resp_text = resp.choices[0].message.content or ""
            parsed = _parse_click_action(resp_text, candidates_len=len(candidates))
            if log_func:
                log_func(f"AI raw response: {resp_text[:500]}")
            return parsed
        except Exception as e:
            if log_func:
                log_func(f"AI error: {str(e)[:200]}")
            return None


def _apply_click(page: Page, candidate: AiCandidate, log_func=None) -> bool:
    try:
        lower = (candidate.label or "").lower()
        # Extra guardrail: never click consent/terms links.
        if any(
            k in lower
            for k in ["privacy policy", "privacy", "terms", "term of service", "tos"]
        ) and not any(
            k in lower for k in ["agree", "accept", "consent", "i agree", "allow"]
        ):
            return False

        el = page.locator(candidate.click_selector).nth(candidate.click_nth)
        label = candidate.label
        if log_func:
            log_func(f"AI click: [{candidate.group}] {label}")
        pre_hash = get_screen_hash(page)
        pre_url = page.url
        el.click(force=True, timeout=2000)
        moved = wait_for_transition(page, pre_url, pre_hash, timeout=8.0)
        return bool(moved)
    except Exception:
        return False


def try_ai_resolve_stuck(
    page: Page,
    log_func,
    slug: str,
    results_dir: str,
    model: str,
    max_calls: int = 2,
    max_candidates: int = 16,
    text_char_limit: int = 16000,
    timeout_seconds: float = 30.0,
) -> bool:
    if not _is_ai_enabled():
        return False

    os.makedirs(results_dir, exist_ok=True)
    cache_key = f"{page.url}|{get_screen_hash(page)}"
    ai_cache_path = os.path.join(results_dir, "ai_fallback_cache.json")

    history: List[Dict[str, Any]] = []

    for call_idx in range(max_calls):
        pre_url = page.url
        pre_hash = get_screen_hash(page)

        log_func(f"AI fallback attempt {call_idx + 1}/{max_calls} for stuck_loop")

        screenshot_b64, _mime = _capture_screenshot_b64(page)
        text_snippet = _build_text_snippet(page, limit_chars=text_char_limit)
        candidates = _extract_candidates(
            page, log_func=log_func, max_candidates=max_candidates
        )
        if log_func:
            top_labels = [c.label for c in candidates[:6]]
            log_func(f"AI candidates count={len(candidates)} top={top_labels}")

        action = _call_openai_for_click(
            model=model,
            url=pre_url,
            text_snippet=text_snippet,
            screenshot_b64=screenshot_b64,
            candidates=candidates,
            timeout_seconds=timeout_seconds,
            log_func=log_func,
        )

        if not action:
            continue
        target_idx = action["target_index"]
        if target_idx < 0 or target_idx >= len(candidates):
            continue

        candidate = candidates[target_idx]
        moved = _apply_click(page, candidate, log_func=log_func)

        transition_ok = moved and (
            page.url != pre_url or get_screen_hash(page) != pre_hash
        )

        history.append(
            {
                "call_idx": call_idx,
                "url": pre_url,
                "hash": pre_hash,
                "action": action,
                "candidate": {"label": candidate.label, "group": candidate.group},
                "transition_ok": transition_ok,
            }
        )

        try:
            with open(
                os.path.join(results_dir, "ai_fallback_last.json"),
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(history[-1], f, indent=2, ensure_ascii=False)
        except Exception:
            pass

        if transition_ok:
            return True

        time.sleep(0.5)

    # Save full attempts for debugging.
    try:
        with open(
            os.path.join(results_dir, "ai_fallback_attempts.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    # Cache placeholder (future improvement).
    try:
        data = {}
        if os.path.exists(ai_cache_path):
            try:
                with open(ai_cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            except Exception:
                data = {}
        data[cache_key] = {"attempted": True, "ok": False, "ts": time.time()}
        with open(ai_cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

    return False
