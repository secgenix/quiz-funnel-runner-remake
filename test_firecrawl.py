from firecrawl import Firecrawl

app = Firecrawl(api_key="fc-eb26c1c7c8e64577b1ce4b28988e7ee9")
result = app.scrape("https://amazon.com")
base_url = result.metadata.scrape_id


def quiz_interact(base_url):
    paywall = False
    while not paywall:
        result = app.interact(base_url, prompt="""
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
""")

app.interact(scrape_id, prompt="Search for 'mechanical keyboard'")
app.interact(scrape_id, prompt="Click the first result")