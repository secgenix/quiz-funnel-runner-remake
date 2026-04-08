import base64
import json
import os
import re
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from dotenv import load_dotenv
from firecrawl import Firecrawl

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

load_dotenv()


ProgressCallback = Callable[[int, int, str, Optional[str]], None]
LogCallback = Callable[[str], None]
ArtifactCallback = Callable[[str, str], None]

FIRECRAWL_MEDIATOR_MODEL = "gpt-5.4-nano"
FIRECRAWL_MEDIATOR_MAX_HISTORY = 6
FIRECRAWL_MEDIATOR_SCREEN_CACHE = 4
FIRECRAWL_MEDIATOR_REPEAT_THRESHOLD = 2
FIRECRAWL_MEDIATOR_PATTERN_THRESHOLD = 2
FIRECRAWL_MEDIATOR_STATE_LIMIT = 700
FIRECRAWL_MEDIATOR_MAX_WAIT_STREAK = 1
FIRECRAWL_MEDIATOR_MAX_SCREENSHOT_SAVES = 18
FIRECRAWL_MEDIATOR_AI_STEP_INTERVAL = 2
FIRECRAWL_PRE_WAIT_SECONDS = 0.8
FIRECRAWL_POST_STEP_SLEEP_SECONDS = 0.25
FIRECRAWL_UNKNOWN_STATE_RETRY_LIMIT = 1

FIRECRAWL_MEDIATOR_SYSTEM_PROMPT = """
You prevent looped Firecrawl quiz actions.

Input includes current screen, short recent history, repeat counters, cached screen fingerprints, and the default next action.

Decide one:
- proceed
- force_alternative
- wait
- stop_paywall
- stop_unknown

Rules:
- Repeated same-action or same-screen states mean loop risk.
- If answer already seems selected and Continue/Next is visible, prefer that instead of re-answering.
- Do not suggest buying, checkout, payment, or subscription actions.
- Use wait only for likely in-progress transitions and not repeatedly.
- Use stop_paywall for paywall or checkout.
- Use stop_unknown when safe progress is unlikely.

Return JSON only:
{"decision":"proceed|force_alternative|wait|stop_paywall|stop_unknown","reason":"short","loop_detected":true,"screen_changed":true,"recommended_instruction":"short","recommended_action_tag":"short","confidence":0.0,"signals":["short"]}
""".strip()


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


def _trim_list(values: list[Any], item_limit: int, text_limit: int) -> list[str]:
    result: list[str] = []
    for value in values[:item_limit]:
        text = str(value or "").strip()
        if text:
            result.append(text[:text_limit])
    return result


def _stable_text_fingerprint(*parts: str) -> str:
    combined = " | ".join(
        re.sub(r"\s+", " ", str(part or "")).strip().lower()
        for part in parts
        if str(part or "").strip()
    )
    return combined[:FIRECRAWL_MEDIATOR_STATE_LIMIT]


def _extract_state_signature(payload: dict[str, Any]) -> str:
    return _stable_text_fingerprint(
        str(payload.get("status") or ""),
        str(payload.get("screen_type") or payload.get("last_screen_type") or ""),
        str(payload.get("summary") or payload.get("raw_output") or ""),
        " | ".join(
            str(x) for x in payload.get("visible_signals", []) if str(x).strip()
        ),
    )


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
        timeout=60,  # 60 seconds timeout for screenshot
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


@dataclass
class FirecrawlActionRecord:
    step: int
    timestamp: str
    status: str
    screen_type: str
    summary: str
    selected_answer: str
    next_action: str
    action_tag: str
    state_signature: str
    raw_output_preview: str


@dataclass
class FirecrawlMediatorDecision:
    decision: str = "proceed"
    reason: str = ""
    loop_detected: bool = False
    screen_changed: bool = True
    recommended_instruction: str = ""
    recommended_action_tag: str = ""
    confidence: float = 0.0
    signals: list[str] = field(default_factory=list)
    source: str = "rules"


class FirecrawlLoopMediator:
    def __init__(
        self,
        log_func: Callable[[str], None],
        artifact_callback: Optional[ArtifactCallback] = None,
    ):
        self.log = log_func
        self.artifact_callback = artifact_callback
        self.history: list[FirecrawlActionRecord] = []
        self.screen_cache: list[str] = []
        self.same_action_streak = 0
        self.same_state_streak = 0
        self.wait_streak = 0
        self.ai_call_counter = 0
        self.last_action_tag = ""
        self.last_state_signature = ""
        self._client: Any = None

    def _client_or_none(self) -> Any:
        if self._client is not None:
            return self._client
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key or OpenAI is None:
            return None
        try:
            self._client = OpenAI(api_key=api_key, timeout=8)
        except Exception:
            self._client = None
        return self._client

    def _build_action_tag(self, payload: dict[str, Any]) -> str:
        return _stable_text_fingerprint(
            str(payload.get("selected_answer") or ""),
            str(payload.get("next_action") or ""),
            str(payload.get("summary") or ""),
        )

    def analyze_before_step(
        self, step: int, state_hint: Optional[dict[str, Any]] = None
    ) -> FirecrawlMediatorDecision:
        screen_type = str((state_hint or {}).get("screen_type") or "")
        if screen_type in {"paywall", "checkout"}:
            return FirecrawlMediatorDecision(
                decision="stop_paywall",
                reason="paywall_or_checkout_state_hint",
                loop_detected=False,
                screen_changed=True,
                confidence=1.0,
                signals=[screen_type],
            )
        return self._decide_with_rules(step)

    def analyze_after_step(
        self, step: int, payload: dict[str, Any], output_dir: str
    ) -> FirecrawlMediatorDecision:
        state_signature = _extract_state_signature(payload)
        action_tag = self._build_action_tag(payload)
        previous_signature = self.last_state_signature
        screen_changed = bool(state_signature and state_signature != previous_signature)

        if action_tag and action_tag == self.last_action_tag:
            self.same_action_streak += 1
        else:
            self.same_action_streak = 1 if action_tag else 0
        if state_signature and state_signature == previous_signature:
            self.same_state_streak += 1
        else:
            self.same_state_streak = 1 if state_signature else 0
        self.last_action_tag = action_tag
        self.last_state_signature = state_signature

        record = FirecrawlActionRecord(
            step=step,
            timestamp=datetime.now().isoformat(),
            status=str(payload.get("status") or ""),
            screen_type=str(
                payload.get("screen_type") or payload.get("last_screen_type") or ""
            ),
            summary=str(payload.get("summary") or "")[:280],
            selected_answer=str(payload.get("selected_answer") or "")[:160],
            next_action=str(payload.get("next_action") or "")[:160],
            action_tag=action_tag,
            state_signature=state_signature,
            raw_output_preview=_compact_debug_text(
                payload.get("raw_output") or "", 500
            ),
        )
        self.history.append(record)
        self.history = self.history[-FIRECRAWL_MEDIATOR_MAX_HISTORY:]
        if state_signature:
            self.screen_cache.append(state_signature)
            self.screen_cache = self.screen_cache[-FIRECRAWL_MEDIATOR_SCREEN_CACHE:]

        decision = self._decide_with_ai(step, payload, screen_changed)
        self._persist_debug(output_dir, step, payload, decision)
        return decision

    def _decide_with_rules(self, step: int) -> FirecrawlMediatorDecision:
        repeated_screen_count = (
            self.screen_cache.count(self.last_state_signature)
            if self.last_state_signature
            else 0
        )
        loop_detected = (
            self.same_action_streak >= FIRECRAWL_MEDIATOR_REPEAT_THRESHOLD
            and repeated_screen_count >= FIRECRAWL_MEDIATOR_PATTERN_THRESHOLD
        ) or self.same_state_streak >= FIRECRAWL_MEDIATOR_REPEAT_THRESHOLD
        if loop_detected:
            latest = self.history[-1] if self.history else None
            has_continue_signal = any(
                token in (latest.state_signature if latest else "")
                for token in ["continue button", "continue", "next button", "next"]
            )
            return FirecrawlMediatorDecision(
                decision="force_alternative",
                reason="repeated_action_on_same_screen",
                loop_detected=True,
                screen_changed=False,
                recommended_instruction=(
                    "The screen appears unchanged after recent actions. Do not repeat the previous answer. If a visible Continue or Next control exists, click it once. Otherwise choose a different safe visible option or safe form field."
                    if has_continue_signal
                    else "The screen appears unchanged after recent actions. Avoid repeating the previous answer or CTA. Choose a different safe visible option, fill the next safe field, or use a non-purchase Continue/Next control if present."
                ),
                recommended_action_tag="break_loop_alternative",
                confidence=0.9,
                signals=[
                    f"same_action_streak={self.same_action_streak}",
                    f"repeated_screen_count={repeated_screen_count}",
                    f"same_state_streak={self.same_state_streak}",
                ],
            )
        return FirecrawlMediatorDecision(
            decision="proceed",
            reason=f"no_loop_detected_step_{step}",
            loop_detected=False,
            screen_changed=True,
            confidence=0.7,
            signals=[f"same_action_streak={self.same_action_streak}"],
        )

    def _decide_with_ai(
        self, step: int, payload: dict[str, Any], screen_changed: bool
    ) -> FirecrawlMediatorDecision:
        if str(payload.get("status") or "").strip().lower() == "paywall":
            return FirecrawlMediatorDecision(
                decision="stop_paywall",
                reason="firecrawl_reported_paywall",
                loop_detected=False,
                screen_changed=screen_changed,
                confidence=1.0,
                signals=["status=paywall"],
            )

        rule_decision = self._decide_with_rules(step)
        if (
            rule_decision.decision != "proceed"
            or step == 1
            or self.same_state_streak >= FIRECRAWL_MEDIATOR_REPEAT_THRESHOLD
            or self.same_action_streak >= FIRECRAWL_MEDIATOR_REPEAT_THRESHOLD
        ):
            should_use_ai = True
        else:
            should_use_ai = step % FIRECRAWL_MEDIATOR_AI_STEP_INTERVAL == 0
        if not should_use_ai:
            return rule_decision

        client = self._client_or_none()
        if client is None:
            return rule_decision

        self.ai_call_counter += 1

        repeated_screen_count = (
            self.screen_cache.count(self.last_state_signature)
            if self.last_state_signature
            else 0
        )
        request_payload = {
            "step": step,
            "current": {
                "status": str(payload.get("status") or "")[:16],
                "screen_type": str(
                    payload.get("screen_type") or payload.get("last_screen_type") or ""
                )[:20],
                "summary": str(payload.get("summary") or "")[:140],
                "selected_answer": str(payload.get("selected_answer") or "")[:90],
                "next_action": str(payload.get("next_action") or "")[:90],
                "visible_signals": _trim_list(
                    payload.get("visible_signals", []), 4, 40
                ),
                "state_signature": self.last_state_signature[:180],
            },
            "history": [
                {
                    "s": item.step,
                    "st": item.status[:16],
                    "t": item.screen_type[:20],
                    "sum": item.summary[:90],
                    "a": item.selected_answer[:50],
                    "n": item.next_action[:50],
                    "tag": item.action_tag[:70],
                    "sig": item.state_signature[:120],
                }
                for item in self.history[-FIRECRAWL_MEDIATOR_MAX_HISTORY:]
            ],
            "screen_cache": [
                signature[:120]
                for signature in self.screen_cache[-FIRECRAWL_MEDIATOR_SCREEN_CACHE:]
            ],
            "same_action_streak": self.same_action_streak,
            "same_state_streak": self.same_state_streak,
            "wait_streak": self.wait_streak,
            "repeated_screen_count": repeated_screen_count,
            "rule": {
                "d": rule_decision.decision,
                "r": rule_decision.reason[:80],
                "i": rule_decision.recommended_instruction[:140],
            },
            "screen_changed": screen_changed,
        }
        try:
            response = client.responses.create(
                model=FIRECRAWL_MEDIATOR_MODEL,
                input=[
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": FIRECRAWL_MEDIATOR_SYSTEM_PROMPT,
                            }
                        ],
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "input_text",
                                "text": json.dumps(request_payload, ensure_ascii=False),
                            }
                        ],
                    },
                ],
                text={"format": {"type": "json_object"}},
                max_output_tokens=120,
            )
            output_text = getattr(response, "output_text", "") or "{}"
            raw = json.loads(output_text)
            decision = FirecrawlMediatorDecision(
                decision=str(raw.get("decision") or "proceed").strip().lower()
                or "proceed",
                reason=str(raw.get("reason") or "")[:220],
                loop_detected=bool(raw.get("loop_detected", False)),
                screen_changed=bool(raw.get("screen_changed", screen_changed)),
                recommended_instruction=str(raw.get("recommended_instruction") or "")[
                    :400
                ],
                recommended_action_tag=str(raw.get("recommended_action_tag") or "")[
                    :120
                ],
                confidence=max(0.0, min(1.0, float(raw.get("confidence", 0.0) or 0.0))),
                signals=[str(x)[:80] for x in raw.get("signals", [])[:8]]
                if isinstance(raw.get("signals"), list)
                else [],
                source="gpt-5.4-nano",
            )
            if decision.decision not in {
                "proceed",
                "force_alternative",
                "wait",
                "stop_paywall",
                "stop_unknown",
            }:
                return rule_decision
            if (
                decision.decision == "wait"
                and self.wait_streak >= FIRECRAWL_MEDIATOR_MAX_WAIT_STREAK
            ):
                return FirecrawlMediatorDecision(
                    decision="force_alternative",
                    reason="wait_limit_reached",
                    loop_detected=True,
                    screen_changed=screen_changed,
                    recommended_instruction="Do not wait again. The page has already been rechecked multiple times. Try a different safe visible action such as Continue/Next or a different answer choice instead of repeating the same click.",
                    recommended_action_tag="escalate_after_wait_limit",
                    confidence=0.88,
                    signals=[f"wait_streak={self.wait_streak}"],
                )
            return decision
        except Exception as exc:
            self.log(f"Firecrawl mediator: AI decision error {str(exc)[:180]}")
            return rule_decision

    def _persist_debug(
        self,
        output_dir: str,
        step: int,
        payload: dict[str, Any],
        decision: FirecrawlMediatorDecision,
    ) -> None:
        try:
            debug_dir = os.path.join(output_dir, "mediator")
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, f"{step:02d}_mediator.json")
            with open(debug_path, "w", encoding="utf-8") as debug_file:
                json.dump(
                    {
                        "step": step,
                        "history": [asdict(item) for item in self.history],
                        "screen_cache": self.screen_cache,
                        "same_action_streak": self.same_action_streak,
                        "same_state_streak": self.same_state_streak,
                        "wait_streak": self.wait_streak,
                        "current_payload": payload,
                        "decision": asdict(decision),
                    },
                    debug_file,
                    indent=2,
                    ensure_ascii=False,
                )
            _emit_artifact(debug_path, self.artifact_callback, drive_subdir="mediator")
        except Exception:
            pass


def _build_interact_prompt(mediator_instruction: str = "") -> str:
    prompt = """
Operate one quiz-funnel browser step.

Do exactly one safe action, then stop on the next stable screen.
If a paywall, pricing, subscription, trial, locked result, checkout, or payment form is visible, stop without clicking purchase controls.
If an answer already seems selected and Continue/Next is visible, click Continue/Next instead of re-answering.
Prefer the smallest action that changes state. Avoid repeating an ineffective action on an unchanged screen.

Return JSON only:
{"status":"advanced|paywall|unknown","screen_type":"question|info|input|email|paywall|checkout|other","summary":"short","selected_answer":"","next_action":"","visible_signals":["short","short"]}

Rules:
- Use `paywall` status for paywall and checkout.
- Use `checkout` screen_type when payment entry is visible.
- Use `paywall` screen_type for pricing/subscription/trial without payment entry.
- Use `email` for lead capture, `input` for other forms, `info` for loading/transition screens.
- Use `unknown` only if safe progress is unclear.
- Keep fields short.
""".strip()

    if mediator_instruction.strip():
        prompt += (
            "\n\nAdditional coordinator instruction:\n"
            f"- {mediator_instruction.strip()}\n"
            "- Do not repeat the exact same ineffective action if the screen appears unchanged.\n"
            "- Prefer one safe action, then stop after the next stable screen."
        )
    return prompt


def _interact_one_step(
    app: Firecrawl, scrape_id: str, mediator_instruction: str = ""
) -> dict[str, Any]:
    response = app.interact(
        scrape_id, prompt=_build_interact_prompt(mediator_instruction), timeout=60
    )
    raw_output = _extract_interact_output(response)
    parsed = _parse_interact_json(raw_output)
    parsed["raw_output"] = raw_output
    parsed["_debug_response"] = _debug_response_snapshot(response)
    return parsed


def _retry_unknown_state_once(
    app: Firecrawl,
    scrape_id: str,
    state: dict[str, Any],
    log: Callable[[str], None],
) -> dict[str, Any]:
    status = str(state.get("status") or "").strip().lower()
    if status != "unknown":
        return state

    summary = str(state.get("summary") or "").strip()
    screen_type = str(
        state.get("screen_type") or state.get("last_screen_type") or ""
    ).strip()
    log(
        "Firecrawl fallback: unknown state detected, retrying once"
        f" | type={screen_type or '-'} | summary={summary or '-'}"
    )
    time.sleep(FIRECRAWL_PRE_WAIT_SECONDS)
    retried_state = _interact_one_step(
        app,
        scrape_id,
        mediator_instruction=(
            "The previous step returned unknown on a likely transitional or informational screen. "
            "Wait briefly if content is still loading, then make one safe attempt to advance using a visible non-purchase Continue/Next control if it appears. "
            "Return unknown only if safe progress is still unclear after this retry."
        ),
    )
    retried_status = str(retried_state.get("status") or "").strip().lower()
    retried_summary = str(retried_state.get("summary") or "").strip()
    retried_type = str(
        retried_state.get("screen_type") or retried_state.get("last_screen_type") or ""
    ).strip()
    log(
        "Firecrawl fallback: unknown state retry result"
        f" status={retried_status or 'unknown'} | type={retried_type or '-'} | summary={retried_summary or '-'}"
    )
    return retried_state


def _should_save_step_screenshot(
    step: int,
    screen_type: str,
    state: dict[str, Any],
    mediator_decision: FirecrawlMediatorDecision,
    previous_state_signature: str,
    screenshots_saved: int,
) -> bool:
    current_signature = _extract_state_signature(state)
    if step <= 3:
        return True
    if screenshots_saved >= FIRECRAWL_MEDIATOR_MAX_SCREENSHOT_SAVES:
        return False
    if str(state.get("status") or "").strip().lower() in {"paywall", "unknown"}:
        return True
    if screen_type in {"paywall", "checkout", "email", "input"}:
        return True
    if mediator_decision.decision in {
        "force_alternative",
        "stop_paywall",
        "stop_unknown",
    }:
        return True
    if mediator_decision.loop_detected:
        return True
    if current_signature and current_signature != previous_state_signature:
        return True
    return False


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
    mediator = FirecrawlLoopMediator(log, artifact_callback)
    screenshots_saved = 1

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

            pre_decision = mediator.analyze_before_step(step)
            if pre_decision.decision == "stop_paywall":
                progress_message = (
                    "Firecrawl fallback: mediator stopped on paywall hint"
                )
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

            mediator_instruction = ""
            if pre_decision.decision == "force_alternative":
                mediator_instruction = pre_decision.recommended_instruction
                log(
                    "Firecrawl mediator: forcing alternative before interact | "
                    f"reason={pre_decision.reason}"
                )
            elif pre_decision.decision == "wait":
                log("Firecrawl mediator: wait before interact")
                mediator.wait_streak += 1
                time.sleep(FIRECRAWL_PRE_WAIT_SECONDS)
            else:
                mediator.wait_streak = 0

            previous_state_signature = mediator.last_state_signature
            state = _interact_one_step(
                app, scrape_id, mediator_instruction=mediator_instruction
            )
            unknown_retry_count = 0
            while (
                str(state.get("status") or "").strip().lower() == "unknown"
                and unknown_retry_count < FIRECRAWL_UNKNOWN_STATE_RETRY_LIMIT
            ):
                state = _retry_unknown_state_once(app, scrape_id, state, log)
                unknown_retry_count += 1
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

            mediator_decision = mediator.analyze_after_step(step, state, output_dir)
            log(
                "Firecrawl mediator: decision "
                f"decision={mediator_decision.decision} | source={mediator_decision.source} | "
                f"reason={mediator_decision.reason or '-'} | signals={','.join(mediator_decision.signals) or '-'}"
            )

            if mediator_decision.decision == "wait":
                mediator.wait_streak += 1
            else:
                mediator.wait_streak = 0

            if _should_save_step_screenshot(
                step,
                screen_type,
                state,
                mediator_decision,
                previous_state_signature,
                screenshots_saved,
            ):
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
                screenshots_saved += 1
                log(f"Firecrawl fallback: screenshot {last_screenshot}")
            else:
                log(
                    "Firecrawl fallback: screenshot skipped | "
                    f"step={step} | reason=unchanged_non_terminal_state"
                )

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
            if mediator_decision.decision == "stop_paywall":
                manifest_path = _write_manifest(
                    output_dir,
                    start_url,
                    "success",
                    steps_done,
                    True,
                    None,
                    progress_message or "Firecrawl mediator stopped at paywall",
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
                    progress_message=progress_message
                    or "Firecrawl mediator stopped at paywall",
                    error=None,
                    manifest_path=manifest_path,
                )
            if mediator_decision.decision == "stop_unknown":
                manifest_path = _write_manifest(
                    output_dir,
                    start_url,
                    "completed",
                    steps_done,
                    False,
                    "firecrawl_mediator_stop_unknown",
                    progress_message or "Firecrawl mediator stopped due to loop risk",
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
                    progress_message=progress_message
                    or "Firecrawl mediator stopped due to loop risk",
                    error="firecrawl_mediator_stop_unknown",
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

            time.sleep(FIRECRAWL_POST_STEP_SLEEP_SECONDS)

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
                    str(scrape_id or ""),
                    code="""await (async () => {
                            return JSON.stringify({
                                url: page.url(),
                                title: document.title || '',
                                bodyText: (document.body?.innerText || '').slice(0, 3000),
                            });
                        })();""",
                    language="node",
                    timeout=60,
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
