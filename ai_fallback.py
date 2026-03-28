import base64
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from playwright.sync_api import Page

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

if load_dotenv is not None:
    try:
        load_dotenv()
    except Exception:
        pass


SAFE_FORBIDDEN_TERMS = [
    "cookie", "cookies", "privacy settings", "cookie settings", "datenschutzeinstellungen",
    "share", "sharing", "tell a friend", "invite", "recommend", "view more", "show less",
    "read more", "app store", "google play", "download app", "contact us", "support",
    "help center", "cookie policy", "go back", "previous", "edit answer"
]

HIGH_RISK_TERMS = [
    "buy", "purchase", "pay", "card", "credit card", "checkout", "subscribe",
    "order", "billing", "apple pay", "google pay"
]

INPUT_HINT_MAP = {
    "email": ["email", "e-mail", "mail"],
    "name": ["name", "full name", "first name"],
    "age": ["age"],
    "height": ["height"],
    "weight": ["weight"],
    "goal_weight": ["goal weight", "target weight", "goal"],
    "date_of_birth": ["date of birth", "birthday", "birth", "dob"],
    "default_number": ["number", "amount"],
}

POST_INPUT_SUBMIT_KEYWORDS = [
    "continue", "next", "submit", "verify", "confirm", "get started", "get my",
    "show my results", "see my results", "i'm in", "i’m in", "proceed",
    "continuar", "siguiente", "weiter", "suivant", "продолжить", "подтвердить"
]


@dataclass
class AICandidate:
    element_id: str
    kind: str
    selector: str
    tag: str
    input_type: str
    role: str
    text: str
    aria_label: str
    placeholder: str
    name: str
    value: str
    href: str
    visible: bool
    enabled: bool
    editable: bool
    checked: bool
    forbidden: bool
    high_risk: bool
    suggested_value: Optional[str]
    bounding_box: Dict[str, float] = field(default_factory=dict)


@dataclass
class AIFallbackContext:
    url: str
    domain: str
    screen_type: str
    stuck_reason: str
    repeat_attempt: int
    ui_step: str
    screen_hash: str
    screenshot_path: str
    screenshot_b64: str
    page_text_excerpt: str
    recent_logs: List[str]
    fill_values: Dict[str, str]
    candidates: List[AICandidate]


@dataclass
class AIDecision:
    action: str = "abort"
    element_id: Optional[str] = None
    input_text: Optional[str] = None
    confidence: float = 0.0
    reason: str = ""
    risk_flags: List[str] = field(default_factory=list)
    alternative_element_ids: List[str] = field(default_factory=list)


def _safe_json_dump(path: str, payload: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
    except Exception:
        pass


def _extract_runner_section(config: dict) -> dict:
    runner = config.get("runner")
    return runner if isinstance(runner, dict) else config


def resolve_ai_fallback_settings(config: dict) -> dict:
    runner_cfg = _extract_runner_section(config)
    ai_cfg = config.get("ai_fallback") if isinstance(config.get("ai_fallback"), dict) else {}
    return {
        "enabled": bool(ai_cfg.get("enabled", False)),
        "model": str(ai_cfg.get("model", "gpt-4.1-mini")),
        "api_key_env": str(ai_cfg.get("api_key_env", "OPENAI_API_KEY")),
        "confidence_threshold": float(ai_cfg.get("confidence_threshold", 0.55)),
        "max_attempts_per_screen": int(ai_cfg.get("max_attempts_per_screen", 2)),
        "allow_paywall_navigation": bool(ai_cfg.get("allow_paywall_navigation", True)),
        "save_traces": bool(ai_cfg.get("save_traces", True)),
        "timeout_sec": float(ai_cfg.get("timeout_sec", 25)),
        "page_text_limit": int(ai_cfg.get("page_text_limit", 5000)),
        "recent_logs_limit": int(ai_cfg.get("recent_logs_limit", 25)),
        "screenshot_name": str(ai_cfg.get("screenshot_name", "ai_fallback_source.png")),
        "runner_max_steps": int(runner_cfg.get("max_steps", config.get("max_steps", 80))),
    }


def _normalize_text(*parts: str) -> str:
    return " ".join(p.strip() for p in parts if p and p.strip()).strip().lower()


def _compute_suggested_value(candidate: Dict[str, Any], fill_values: Dict[str, str]) -> Optional[str]:
    haystack = _normalize_text(
        str(candidate.get("text", "")),
        str(candidate.get("aria_label", "")),
        str(candidate.get("placeholder", "")),
        str(candidate.get("name", "")),
        str(candidate.get("input_type", "")),
    )
    input_type = str(candidate.get("input_type", "")).lower()
    if input_type == "email":
        return str(fill_values.get("email", "testuser{ts}@gmail.com")).replace("{ts}", str(int(time.time())))
    if input_type in {"number", "range"}:
        return str(fill_values.get("default_number", "25"))
    if input_type == "date":
        return str(fill_values.get("date_of_birth", "01/01/1990"))
    for key, hints in INPUT_HINT_MAP.items():
        if any(h in haystack for h in hints):
            value = str(fill_values.get(key, "")).strip()
            if value:
                if key == "email":
                    return value.replace("{ts}", str(int(time.time())))
                return value
    if input_type in {"text", "search", "textarea", "tel", "url", "password", ""}:
        return str(fill_values.get("name", "John"))
    return None


def _mark_candidate_risk(candidate: Dict[str, Any], current_url: str) -> Dict[str, bool]:
    combined = _normalize_text(
        str(candidate.get("text", "")),
        str(candidate.get("aria_label", "")),
        str(candidate.get("placeholder", "")),
        str(candidate.get("name", "")),
        str(candidate.get("href", "")),
        str(candidate.get("selector", "")),
    )
    href = str(candidate.get("href", "")).strip()
    forbidden = any(term in combined for term in SAFE_FORBIDDEN_TERMS)
    high_risk = any(term in combined for term in HIGH_RISK_TERMS)
    if href:
        try:
            current_host = urlparse(current_url).netloc.replace("www.", "")
            href_host = urlparse(href).netloc.replace("www.", "")
            if href_host and current_host and href_host != current_host:
                high_risk = True
        except Exception:
            pass
    return {"forbidden": forbidden, "high_risk": high_risk}


def collect_actionable_elements(page: Page, fill_values: Dict[str, str], max_candidates: int = 80) -> List[AICandidate]:
    raw_candidates = page.evaluate(
        r"""() => {
        const normalizeText = (...parts) => parts.filter(Boolean).join(' ').replace(/\s+/g, ' ').trim().toLowerCase();
        const isVisible = (el) => {
            if (!el || !el.isConnected) return false;
            if (el.closest('[hidden], [aria-hidden="true"], inert')) return false;
            const s = window.getComputedStyle(el);
            if (s.display === 'none' || s.visibility === 'hidden' || s.visibility === 'collapse') return false;
            if (parseFloat(s.opacity || '1') <= 0.02) return false;
            if (s.pointerEvents === 'none') return false;
            const r = el.getBoundingClientRect();
            return r.width > 8 && r.height > 8 && r.bottom > 0 && r.right > 0 && r.left < window.innerWidth && r.top < window.innerHeight;
        };
        const viewportRatio = (rect) => {
            const left = Math.max(0, rect.left);
            const top = Math.max(0, rect.top);
            const right = Math.min(window.innerWidth, rect.right);
            const bottom = Math.min(window.innerHeight, rect.bottom);
            const width = Math.max(0, right - left);
            const height = Math.max(0, bottom - top);
            const area = Math.max(1, rect.width * rect.height);
            return (width * height) / area;
        };
        const isEditableInput = (el) => {
            const tag = (el.tagName || '').toLowerCase();
            if (tag === 'textarea') return true;
            if (tag !== 'input') return false;
            const t = (el.getAttribute('type') || 'text').toLowerCase();
            return !['hidden', 'radio', 'checkbox', 'submit', 'button', 'image', 'reset', 'file'].includes(t);
        };
        const isClickable = (el) => {
            const tag = (el.tagName || '').toLowerCase();
            const role = (el.getAttribute('role') || '').toLowerCase();
            const style = window.getComputedStyle(el);
            return ['button', 'a', 'label', 'summary'].includes(tag)
                || role === 'button'
                || typeof el.onclick === 'function'
                || el.hasAttribute('data-testid')
                || el.hasAttribute('tabindex')
                || style.cursor === 'pointer';
        };
        const selectorFor = (el, index) => {
            if (el.id) return `#${el.id}`;
            if (el.getAttribute('data-testid')) return `[data-testid="${el.getAttribute('data-testid')}"]`;
            return `[data-ai-fallback-id="${index}"]`;
        };
        const getText = (el) => {
            return [
                (el.innerText || '').trim(),
                (el.value || '').trim(),
                (el.getAttribute('aria-label') || '').trim(),
                (el.getAttribute('title') || '').trim(),
                (el.getAttribute('alt') || '').trim()
            ].filter(Boolean).join(' ').trim();
        };
        const nodes = Array.from(document.querySelectorAll('button, [role="button"], a, label, input, textarea, select, div, span, summary'));
        const result = [];
        let seq = 0;
        for (const el of nodes) {
            const visible = isVisible(el);
            const inputLike = isEditableInput(el);
            const clickable = isClickable(el);
            if (!visible || (!inputLike && !clickable)) continue;
            const rect = el.getBoundingClientRect();
            const topEl = document.elementFromPoint(
                Math.min(window.innerWidth - 1, Math.max(0, rect.left + rect.width / 2)),
                Math.min(window.innerHeight - 1, Math.max(0, rect.top + rect.height / 2))
            );
            const unobstructed = !topEl || topEl === el || el.contains(topEl) || topEl.contains(el);
            const tag = (el.tagName || '').toLowerCase();
            const inputType = (el.getAttribute('type') || '').toLowerCase();
            const enabled = !el.disabled && (el.getAttribute('aria-disabled') || '').toLowerCase() !== 'true';
            const editable = !el.readOnly && !el.disabled;
            el.setAttribute('data-ai-fallback-id', String(seq));
            result.push({
                element_id: `el_${seq}`,
                kind: inputLike ? 'input' : 'clickable',
                selector: selectorFor(el, seq),
                tag,
                input_type: inputType,
                role: (el.getAttribute('role') || '').toLowerCase(),
                text: getText(el),
                aria_label: (el.getAttribute('aria-label') || '').trim(),
                placeholder: (el.getAttribute('placeholder') || '').trim(),
                name: (el.getAttribute('name') || '').trim(),
                value: (el.value || '').trim(),
                href: (el.getAttribute('href') || '').trim(),
                visible,
                enabled,
                editable,
                checked: !!el.checked,
                bounding_box: {
                    x: Math.round(rect.x),
                    y: Math.round(rect.y),
                    width: Math.round(rect.width),
                    height: Math.round(rect.height),
                    viewport_ratio: Number(viewportRatio(rect).toFixed(3)),
                    unobstructed: !!unobstructed
                }
            });
            seq += 1;
        }
        return result;
    }"""
    )
    candidates: List[AICandidate] = []
    for item in raw_candidates[:max_candidates]:
        risk = _mark_candidate_risk(item, page.url)
        suggested_value = _compute_suggested_value(item, fill_values) if item.get("kind") == "input" else None
        candidates.append(
            AICandidate(
                element_id=str(item.get("element_id", "")),
                kind=str(item.get("kind", "clickable")),
                selector=str(item.get("selector", "")),
                tag=str(item.get("tag", "")),
                input_type=str(item.get("input_type", "")),
                role=str(item.get("role", "")),
                text=str(item.get("text", ""))[:240],
                aria_label=str(item.get("aria_label", ""))[:180],
                placeholder=str(item.get("placeholder", ""))[:180],
                name=str(item.get("name", ""))[:120],
                value=str(item.get("value", ""))[:120],
                href=str(item.get("href", ""))[:240],
                visible=bool(item.get("visible", False)),
                enabled=bool(item.get("enabled", False)),
                editable=bool(item.get("editable", False)),
                checked=bool(item.get("checked", False)),
                forbidden=risk["forbidden"],
                high_risk=risk["high_risk"],
                suggested_value=suggested_value,
                bounding_box=item.get("bounding_box", {}) or {},
            )
        )
    return candidates


def build_ai_fallback_context(
    page: Page,
    results_dir: str,
    fill_values: Dict[str, str],
    screen_type: str,
    stuck_reason: str,
    repeat_attempt: int,
    ui_step: str,
    screen_hash: str,
    log_lines: List[str],
    settings: dict,
) -> AIFallbackContext:
    os.makedirs(results_dir, exist_ok=True)
    screenshot_path = os.path.join(results_dir, settings.get("screenshot_name", "ai_fallback_source.png"))
    page.screenshot(path=screenshot_path, full_page=True)
    with open(screenshot_path, "rb") as fh:
        screenshot_b64 = base64.b64encode(fh.read()).decode("ascii")
    try:
        page_text = (page.evaluate("() => (document.body && document.body.innerText) || ''") or "").strip()
    except Exception:
        page_text = ""
    return AIFallbackContext(
        url=page.url,
        domain=urlparse(page.url).netloc.replace("www.", ""),
        screen_type=screen_type,
        stuck_reason=stuck_reason,
        repeat_attempt=repeat_attempt,
        ui_step=ui_step,
        screen_hash=screen_hash,
        screenshot_path=screenshot_path,
        screenshot_b64=screenshot_b64,
        page_text_excerpt=page_text[: int(settings.get("page_text_limit", 5000))],
        recent_logs=log_lines[-int(settings.get("recent_logs_limit", 25)) :],
        fill_values=fill_values,
        candidates=collect_actionable_elements(page, fill_values),
    )


def _build_openai_input(context: AIFallbackContext) -> List[Dict[str, Any]]:
    return [
        {
            "role": "system",
            "content": [
                {
                    "type": "input_text",
                    "text": (
                        "You are a browser recovery assistant. Select only one action from the provided candidates. "
                        "Return strict JSON with fields: action, element_id, input_text, confidence, reason, risk_flags, alternative_element_ids. "
                        "Allowed actions: click, input, wait, abort. Never output coordinates or custom selectors. "
                        "Avoid cookie settings, share, destructive purchases, app downloads, or checkout submission."
                    ),
                }
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": json.dumps(
                        {
                            "url": context.url,
                            "domain": context.domain,
                            "screen_type": context.screen_type,
                            "stuck_reason": context.stuck_reason,
                            "repeat_attempt": context.repeat_attempt,
                            "ui_step": context.ui_step,
                            "screen_hash": context.screen_hash,
                            "recent_logs": context.recent_logs,
                            "page_text_excerpt": context.page_text_excerpt,
                            "candidates": [asdict(c) for c in context.candidates],
                        },
                        ensure_ascii=False,
                    ),
                },
                {
                    "type": "input_image",
                    "image_url": f"data:image/png;base64,{context.screenshot_b64}",
                },
            ],
        },
    ]


def request_ai_decision(context: AIFallbackContext, settings: dict, artifacts_dir: str) -> Dict[str, Any]:
    api_key = os.getenv(settings.get("api_key_env", "OPENAI_API_KEY"), "").strip()
    if not api_key:
        raise RuntimeError(f"AI fallback disabled: env var {settings.get('api_key_env', 'OPENAI_API_KEY')} is empty")
    if OpenAI is None:
        raise RuntimeError("AI fallback requires the openai package")
    client = OpenAI(api_key=api_key, timeout=float(settings.get("timeout_sec", 25)))
    payload = _build_openai_input(context)
    request_dump = {
        "model": settings.get("model"),
        "input": payload,
        "response_format": "json_object",
    }
    _safe_json_dump(os.path.join(artifacts_dir, "ai_request.json"), request_dump)
    response = client.responses.create(
        model=settings.get("model", "gpt-4.1-mini"),
        input=payload,
        text={"format": {"type": "json_object"}},
    )
    response_dump = response.model_dump() if hasattr(response, "model_dump") else {"output_text": getattr(response, "output_text", "")}
    _safe_json_dump(os.path.join(artifacts_dir, "ai_response.json"), response_dump)
    output_text = getattr(response, "output_text", "") or ""
    if not output_text and isinstance(response_dump, dict):
        output_text = json.dumps(response_dump, ensure_ascii=False)
    return json.loads(output_text)


def normalize_ai_decision(payload: Dict[str, Any]) -> AIDecision:
    try:
        confidence = float(payload.get("confidence", 0.0))
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    action = str(payload.get("action", "abort")).strip().lower()
    if action not in {"click", "input", "wait", "abort"}:
        action = "abort"
    alt_ids = payload.get("alternative_element_ids") or []
    if not isinstance(alt_ids, list):
        alt_ids = []
    risk_flags = payload.get("risk_flags") or []
    if not isinstance(risk_flags, list):
        risk_flags = []
    return AIDecision(
        action=action,
        element_id=(str(payload.get("element_id", "")).strip() or None),
        input_text=(str(payload.get("input_text", "")).strip() or None),
        confidence=confidence,
        reason=str(payload.get("reason", ""))[:400],
        risk_flags=[str(x)[:80] for x in risk_flags],
        alternative_element_ids=[str(x)[:80] for x in alt_ids],
    )


def validate_ai_decision(context: AIFallbackContext, decision: AIDecision, settings: dict) -> Dict[str, Any]:
    threshold = float(settings.get("confidence_threshold", 0.55))
    indexed = {c.element_id: c for c in context.candidates}
    if decision.action in {"wait", "abort"}:
        return {"ok": True, "reason": decision.action, "candidate": None}
    if decision.confidence < threshold:
        return {"ok": False, "reason": f"low_confidence:{decision.confidence:.2f}", "candidate": None}
    if not decision.element_id or decision.element_id not in indexed:
        return {"ok": False, "reason": "unknown_element_id", "candidate": None}
    candidate = indexed[decision.element_id]
    if candidate.forbidden:
        return {"ok": False, "reason": "forbidden_candidate", "candidate": candidate}
    if not candidate.visible or not candidate.enabled:
        return {"ok": False, "reason": "candidate_not_interactable", "candidate": candidate}
    try:
        viewport_ratio = float((candidate.bounding_box or {}).get("viewport_ratio", 0.0))
    except Exception:
        viewport_ratio = 0.0
    unobstructed = bool((candidate.bounding_box or {}).get("unobstructed", False))
    if viewport_ratio <= 0.15:
        return {"ok": False, "reason": "candidate_out_of_viewport", "candidate": candidate}
    if not unobstructed:
        return {"ok": False, "reason": "candidate_obstructed", "candidate": candidate}
    if decision.action == "click":
        if candidate.kind != "clickable":
            return {"ok": False, "reason": "click_on_non_clickable_candidate", "candidate": candidate}
        if candidate.high_risk and not bool(settings.get("allow_paywall_navigation", True)):
            return {"ok": False, "reason": "high_risk_click_rejected", "candidate": candidate}
        combined = _normalize_text(candidate.text, candidate.aria_label, candidate.placeholder, candidate.href)
        if any(term in combined for term in ["checkout", "purchase", "buy now", "place order", "pay now"]):
            return {"ok": False, "reason": "checkout_submit_rejected", "candidate": candidate}
    if decision.action == "input":
        if candidate.kind != "input":
            return {"ok": False, "reason": "input_on_non_input_candidate", "candidate": candidate}
        if not candidate.editable:
            return {"ok": False, "reason": "input_not_editable", "candidate": candidate}
        text = (decision.input_text or candidate.suggested_value or "").strip()
        if not text:
            return {"ok": False, "reason": "empty_input_text", "candidate": candidate}
        if candidate.input_type == "email" and "@" not in text:
            return {"ok": False, "reason": "invalid_email_input", "candidate": candidate}
    return {"ok": True, "reason": "validated", "candidate": candidate}


def execute_ai_decision(page: Page, decision: AIDecision, validation: Dict[str, Any], log_func: Callable[[str], None]) -> Dict[str, Any]:
    candidate: Optional[AICandidate] = validation.get("candidate")
    if decision.action in {"wait", "abort"}:
        if decision.action == "wait":
            time.sleep(2.0)
        return {"executed": True, "effect": decision.action, "element_id": decision.element_id}
    if not candidate:
        return {"executed": False, "effect": "missing_candidate", "element_id": decision.element_id}
    locator = page.locator(f"[data-ai-fallback-id='{candidate.element_id.replace('el_', '')}']").first
    if locator.count() == 0 and candidate.selector:
        locator = page.locator(candidate.selector).first
    if locator.count() == 0:
        return {"executed": False, "effect": "locator_not_found", "element_id": candidate.element_id}
    try:
        locator.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        if decision.action == "click":
            return {"executed": False, "effect": "candidate_not_visible_after_scroll", "element_id": candidate.element_id}
    if decision.action == "click":
        locator.click(force=True, timeout=2500)
        log_func(f"AI fallback: клик по {candidate.element_id} | text={candidate.text[:60]}")
        return {"executed": True, "effect": "click", "element_id": candidate.element_id}
    if decision.action == "input":
        text = (decision.input_text or candidate.suggested_value or "").strip()
        locator.click(force=True, timeout=2000)
        try:
            locator.fill(text, timeout=3000)
        except Exception:
            locator.press("Control+A")
            locator.type(text, delay=20)
        log_func(f"AI fallback: ввод в {candidate.element_id} | text={text[:60]}")
        combined = _normalize_text(candidate.text, candidate.aria_label, candidate.placeholder, candidate.name, candidate.input_type, page.url)
        should_submit = (
            candidate.input_type == "email"
            or any(term in combined for term in ["email", "e-mail", "/email", "step=email", "mail"])
        )
        if should_submit:
            try:
                cta = page.locator("button:visible, [role='button']:visible, a:visible, input[type='submit']:visible, input[type='button']:visible").first
                cta_count = page.locator("button:visible, [role='button']:visible, a:visible, input[type='submit']:visible, input[type='button']:visible").count()
                clicked = False
                for idx in range(cta_count):
                    curr = page.locator("button:visible, [role='button']:visible, a:visible, input[type='submit']:visible, input[type='button']:visible").nth(idx)
                    try:
                        txt = _normalize_text(curr.inner_text(), curr.get_attribute("aria-label") or "", curr.get_attribute("value") or "")
                    except Exception:
                        txt = ""
                    if txt and any(k in txt for k in POST_INPUT_SUBMIT_KEYWORDS):
                        curr.click(force=True, timeout=2000)
                        log_func(f"AI fallback: post-input submit | text={txt[:60]}")
                        clicked = True
                        break
                if not clicked:
                    locator.press("Enter")
                    log_func("AI fallback: post-input submit | key=Enter")
            except Exception:
                try:
                    locator.press("Enter")
                    log_func("AI fallback: post-input submit | key=Enter")
                except Exception:
                    pass
        return {"executed": True, "effect": "input", "element_id": candidate.element_id, "input_text": text}
    return {"executed": False, "effect": "unsupported_action", "element_id": candidate.element_id}


def run_ai_fallback(
    page: Page,
    config: dict,
    results_dir: str,
    fill_values: Dict[str, str],
    screen_type: str,
    stuck_reason: str,
    repeat_attempt: int,
    ui_step: str,
    screen_hash: str,
    log_lines: List[str],
    log_func: Callable[[str], None],
) -> Dict[str, Any]:
    settings = resolve_ai_fallback_settings(config)
    if not settings.get("enabled"):
        return {"status": "disabled"}
    artifacts_dir = os.path.join(results_dir, "ai_fallback", f"{int(time.time())}_{re.sub(r'[^a-zA-Z0-9_-]+', '_', stuck_reason or 'unknown')}")
    os.makedirs(artifacts_dir, exist_ok=True)
    try:
        context = build_ai_fallback_context(
            page=page,
            results_dir=artifacts_dir,
            fill_values=fill_values,
            screen_type=screen_type,
            stuck_reason=stuck_reason,
            repeat_attempt=repeat_attempt,
            ui_step=ui_step,
            screen_hash=screen_hash,
            log_lines=log_lines,
            settings=settings,
        )
        _safe_json_dump(os.path.join(artifacts_dir, "ai_context.json"), asdict(context))
        raw_decision = request_ai_decision(context, settings, artifacts_dir)
        decision = normalize_ai_decision(raw_decision)
        _safe_json_dump(os.path.join(artifacts_dir, "ai_decision.json"), asdict(decision))
        validation = validate_ai_decision(context, decision, settings)
        _safe_json_dump(os.path.join(artifacts_dir, "ai_validation.json"), {
            "ok": validation.get("ok"),
            "reason": validation.get("reason"),
            "candidate": asdict(validation["candidate"]) if validation.get("candidate") else None,
        })
        if not validation.get("ok"):
            log_func(f"AI fallback: решение отклонено | {validation.get('reason')}")
            return {
                "status": "rejected",
                "reason": validation.get("reason"),
                "artifacts_dir": artifacts_dir,
                "decision": asdict(decision),
            }
        execution = execute_ai_decision(page, decision, validation, log_func)
        _safe_json_dump(os.path.join(artifacts_dir, "ai_execution_result.json"), execution)
        return {
            "status": "executed" if execution.get("executed") else "failed",
            "reason": execution.get("effect"),
            "artifacts_dir": artifacts_dir,
            "decision": asdict(decision),
            "execution": execution,
        }
    except Exception as e:
        _safe_json_dump(os.path.join(artifacts_dir, "ai_error.json"), {"error": str(e)[:500]})
        log_func(f"AI fallback: ошибка: {str(e)[:180]}")
        return {"status": "error", "reason": str(e)[:180], "artifacts_dir": artifacts_dir}
