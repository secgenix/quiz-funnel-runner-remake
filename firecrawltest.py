import os
import json
import base64
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv
from firecrawl import Firecrawl


load_dotenv()
api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
if not api_key:
    raise RuntimeError("FIRECRAWL_API_KEY is not set")

app = Firecrawl(api_key=api_key)
start_url = "https://quiz.fitme.expert/intro-111?cohort=fitme_fl_111&uuid=5b2bd673-ca18-4212-b346-c75f37cbd47e"


def extract_output(response: Any) -> str:
    if response is None:
        return ""
    if hasattr(response, "output") and response.output is not None:
        return str(response.output)
    if hasattr(response, "result") and response.result is not None:
        return str(response.result)
    if hasattr(response, "stdout") and response.stdout is not None:
        return str(response.stdout)
    return str(response)


def extract_screenshot_url(scrape_result: Any) -> str | None:
    candidates = [
        getattr(scrape_result, "screenshot", None),
        getattr(scrape_result, "screenshots", None),
        getattr(getattr(scrape_result, "data", None), "screenshot", None),
        getattr(getattr(scrape_result, "data", None), "screenshots", None),
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


def is_valid_http_url(value: str) -> bool:
    try:
        parsed = urlparse(value.strip())
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(
        parsed.netloc and "." in parsed.netloc
    )


def extract_current_url(response: Any) -> str | None:
    direct_stdout = getattr(response, "stdout", None)
    if isinstance(direct_stdout, str):
        direct_stdout = direct_stdout.strip()
        if is_valid_http_url(direct_stdout):
            return direct_stdout

    direct_url = deep_get(response, "data", "output")
    if isinstance(direct_url, str):
        direct_url = direct_url.strip()
        if is_valid_http_url(direct_url):
            return direct_url

    raw = extract_output(response).strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        current_url = raw
    else:
        if isinstance(parsed, dict):
            current_url = str(
                parsed.get("current_url") or parsed.get("url") or ""
            ).strip()
        else:
            current_url = str(parsed).strip()
    return current_url if is_valid_http_url(current_url) else None


def debug_dump_response(label: str, response: Any) -> None:
    print(f"[{label}] response_type={type(response).__name__}")
    print(f"[{label}] extract_output={extract_output(response)!r}")

    data = getattr(response, "data", None)
    if data is not None:
        print(f"[{label}] data_type={type(data).__name__}")
        print(f"[{label}] data_repr={data!r}")

    for attr in ("output", "result", "stdout"):
        value = getattr(response, attr, None)
        if value is not None:
            print(f"[{label}] {attr}={value!r}")


def parse_json_output(response: Any) -> dict[str, Any]:
    raw = extract_output(response).strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def deep_get(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
    return current


def extract_interact_payload(response: Any) -> dict[str, Any]:
    direct_payload = parse_json_output(response)
    if direct_payload:
        return direct_payload

    nested_candidates = [
        deep_get(response, "data"),
        deep_get(response, "result"),
        deep_get(response, "output"),
        deep_get(response, "stdout"),
    ]
    for candidate in nested_candidates:
        if isinstance(candidate, dict):
            return candidate
        if isinstance(candidate, str) and candidate.strip():
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
    return {}


def infer_status_from_text(text: str) -> str:
    normalized = text.lower()
    paywall_markers = [
        "pricing",
        "subscription",
        "checkout",
        "paywall",
        "buy now",
        "start trial",
        "upgrade",
        "premium",
        "unlock",
        "choose plan",
        "monthly",
        "yearly",
        "per week",
        "per month",
        "$",
        "eur",
        "usd",
    ]
    quiz_markers = [
        "button",
        "statictext",
        "go back",
        " / ",
        "question",
        "mostly",
        "yes,",
        "no,",
    ]
    if any(marker in normalized for marker in paywall_markers):
        return "paywall_reached"
    if any(marker in normalized for marker in quiz_markers):
        return "not_reached"
    return "unknown"


# 1. Start an interactive scrape session
result = app.scrape(
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
scrape_id = getattr(getattr(result, "metadata", None), "scrape_id", None)
if not scrape_id:
    raise RuntimeError("Firecrawl did not return scrape_id")

# 2. Navigate the funnel step by step until the paywall is visible
navigation_prompt = """
You are controlling a browser session inside a quiz funnel.

Goal:
- Progress through the funnel until you reach the first actual pricing, checkout, subscription, purchase, or locked-results paywall screen that shows a paid offer.

Behavior rules:
- Inspect the current screen before acting.
- If the current screen is a quiz or onboarding step, choose one plausible answer and continue.
- Continue through intermediate steps such as email capture, prize wheel, transition, loading, and offer-prep screens.
- Treat email capture or lead forms as not yet the paywall unless a paid offer is already visible on the same screen.
- Repeat this until a real paid offer or pricing screen is visible.
- Do not click purchase, checkout, subscribe, or payment confirmation buttons.
- Stop immediately once the paywall or pricing screen has loaded.

Return only valid JSON in this exact shape:
{
  "status": "paywall_reached" | "not_reached" | "blocked",
  "steps_taken": number,
  "last_screen_type": "quiz" | "transition" | "paywall" | "unknown",
  "summary": "short summary"
}
""".strip()

current_url = None

while True:
    try:
        navigation_response = app.interact(scrape_id, prompt=navigation_prompt)
    except Exception as exc:
        if "404" in str(exc) or "Job not found" in str(exc):
            raise RuntimeError(
                "Firecrawl interactive session was not created or expired. "
                "Use app.scrape(...) with screenshot/browser-capable options before interact(...)."
            ) from exc
        raise

    try:
        url_response = app.interact(
            scrape_id,
            code="agent-browser get url",
            language="bash",
        )
    except Exception as exc:
        if "404" in str(exc) or "Job not found" in str(exc):
            raise RuntimeError(
                "Firecrawl interactive session was not created or expired. "
                "Use app.scrape(...) with screenshot/browser-capable options before interact(...)."
            ) from exc
        raise

    debug_dump_response("url_interact", url_response)
    navigation_payload = extract_interact_payload(navigation_response)
    if navigation_payload:
        print(json.dumps(navigation_payload, ensure_ascii=False, indent=2))
    else:
        print(extract_output(navigation_response))

    candidate_url = extract_current_url(url_response) or ""
    if is_valid_http_url(candidate_url):
        current_url = candidate_url

    status = str(navigation_payload.get("status") or "").strip().lower()
    if not status:
        status = infer_status_from_text(extract_output(navigation_response))
    if status == "paywall_reached":
        break
    if status in {"blocked", "unknown", ""}:
        raise RuntimeError(
            f"Failed to reach paywall, interact status={status or 'empty'}"
        )

# 3. Refresh URL on the final paywall screen
try:
    final_url_response = app.interact(
        scrape_id,
        code="agent-browser get url",
        language="bash",
    )
except Exception as exc:
    if "404" in str(exc) or "Job not found" in str(exc):
        raise RuntimeError(
            "Firecrawl interactive session was not created or expired. "
            "Use app.scrape(...) with screenshot/browser-capable options before interact(...)."
        ) from exc
    raise

debug_dump_response("url_interact_paywall", final_url_response)
final_paywall_url = extract_current_url(final_url_response)
if final_paywall_url:
    current_url = final_paywall_url

# 4. Extract pricing from the paywall screen
pricing_prompt = """
You are on a quiz funnel paywall or pricing screen.

Task:
- Identify every visible tariff, plan, or subscription option.
- For each option, capture its name and the displayed price exactly as written.
- If there are multiple billing periods or price labels, include the main visible offer text.
- Do not navigate away from the current page.

Return only valid JSON in this exact shape:
{
  "plans": [
    {
      "name": "string",
      "price": "string",
      "billing_period": "string"
    }
  ],
  "currency": "string or null",
  "paywall_detected": true
}

If no prices are visible, return:
{
  "plans": [],
  "currency": null,
  "paywall_detected": false
}
""".strip()

response = app.interact(scrape_id, prompt=pricing_prompt)
pricing_payload = extract_interact_payload(response)
if pricing_payload:
    print(json.dumps(pricing_payload, ensure_ascii=False, indent=2))
else:
    print(extract_output(response))

# 5. Capture screenshot from the final paywall screen using the current session
print(f"[screenshot] current_url={current_url!r}")

screenshot_response = app.interact(
    scrape_id,
    code="""const buffer = await page.screenshot();
            buffer.toString('base64');""",
    language="node",
)
screenshot_base64 = str(getattr(screenshot_response, "result", "") or "").strip()
if screenshot_base64.startswith(("data:image/png;base64,", "data:image/jpeg;base64,")):
    screenshot_base64 = screenshot_base64.split(",", 1)[1].strip()

debug_dump_response("screenshot_interact", screenshot_response)
print(f"[screenshot] result_preview={screenshot_base64[:120]!r}")
if not screenshot_base64:
    raise RuntimeError("Could not extract base64 screenshot from interact response")

screenshot_path = "paywall_screenshot.png"
raw_result = getattr(screenshot_response, "result", b"")
with open(screenshot_path, "wb") as screenshot_file:
    if screenshot_base64.startswith("\x89PNG") or screenshot_base64.startswith(
        "\ufffdPNG"
    ):
        if isinstance(raw_result, bytes):
            screenshot_file.write(raw_result)
        else:
            screenshot_file.write(str(raw_result).encode("latin-1", errors="ignore"))
    else:
        screenshot_file.write(base64.b64decode(screenshot_base64.encode("ascii")))

print(f"Screenshot saved to: {screenshot_path}")

# 6. Stop the session
app.stop_interaction(scrape_id)
