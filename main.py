import json, argparse, os, time, re, hashlib, shutil
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page, TimeoutError
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

COOKIE_BLACKLIST = [
    "settings", "preferences", "customize", "options", "more info", 
    "einstellungen", "optionen", "mehr informationen", 
    "cookie settings", "privacy settings", "datenschutzeinstellungen",
    "manage", "preferences"
]

SHARE_BLACKLIST = [
    "share", "sharing", "tell a friend", "invite", "recommend",
    "РїРѕРґРµР»РёС‚СЊСЃСЏ", "РїРѕРґРµР»РёСЃСЊ", "СЂР°СЃСЃРєР°Р·Р°С‚СЊ", "РїСЂРёРіР»Р°СЃРёС‚СЊ",
    "view more", "show less", "read more", "back", "zurГјck", "РЅР°Р·Р°Рґ"
]

FORBIDDEN_PATTERN = re.compile(r"\b(" + "|".join(re.escape(w) for w in SHARE_BLACKLIST) + r")\b")
FILLABLE_INPUT_SELECTOR = (
    "input:visible:not([type='hidden']):not([type='radio']):not([type='checkbox']):"
    "not([type='submit']):not([type='button']):not([type='image']):not([type='reset']):"
    "not([type='file']), textarea:visible"
)
CONSENT_TEXT_KEYS = [
    "terms", "privacy", "policy", "by continuing", "agree", "consent",
    "услов", "политик", "соглас"
]


def is_forbidden_button(el, log_func=None) -> bool:
    try:
        txt = (el.inner_text() or "").lower()
        alabel = (el.get_attribute("aria-label") or "").lower()
        cls = (el.get_attribute("class") or "").lower()
        html = (el.evaluate("el => el.innerHTML") or "").lower()
        full_text = f"{txt} {alabel} {cls} {html}"

        forbidden = False
        reason = ""
        if any(word in full_text for word in COOKIE_BLACKLIST):
            forbidden = True
            reason = "cookie"
        if FORBIDDEN_PATTERN.search(full_text):
            forbidden = True
            reason = "forbidden_ui"

        if forbidden and log_func and txt.strip():
            log_func(f"Пропущена запрещенная кнопка={reason} | текст: {txt.strip()[:30]}")
        return forbidden
    except:
        return False

def get_choices_text(page: Page, log_func=None):
    try:
        choice_sel = [
            "[data-testid*='answer' i]:visible", 
            "button:visible", 
            "[class*='Item' i]:visible", 
            "[class*='Card' i]:visible", 
            "label:visible"
        ]
        for s in choice_sel:
            els = page.locator(s)
            texts = []
            els_count = els.count()
            for i in range(els_count):
                curr = els.nth(i)
                if is_forbidden_button(curr, log_func): continue
                txt = (curr.inner_text() or "").strip()
                if txt and not re.search(r'\d+\s*/\s*\d+', txt) and len(txt) < 100:
                    texts.append(txt)
            if texts:
                return "|".join(texts[:3])
        return ""
    except: return ""

def get_ui_step(page: Page):
    try:
        # Improved regex: require total >= 10 to avoid 24/7, or ensure current <= total
        def extract_step(text):
            # Look for patterns like "3 / 23" or "3/23"
            matches = re.finditer(r'(\d+)\s*/\s*(\d+)', text)
            for m in matches:
                curr, total = int(m.group(1)), int(m.group(2))
                # Heuristic: quiz progress usually has total > 5 and curr <= total
                # and we specifically ignore 24/7
                if total > 5 and curr <= total and f"{curr}/{total}" != "24/7":
                    return f"{curr}/{total}"
            return None

        # Look in visible text first
        t = page.evaluate("() => document.body.innerText")
        res = extract_step(t)
        if res: return res
        
        # Look in all elements if not found
        res = page.evaluate(r"""() => {
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            let node;
            while(node = walker.nextNode()) {
                const m = node.textContent.match(/(\d+)\s*\/\s*(\d+)/);
                if (m) {
                    const c = parseInt(m[1]), t = parseInt(m[2]);
                    if (t > 5 && c <= t && m[0].trim() !== "24/7") return m[0].trim();
                }
            }
            return null;
        }""")
        if res: return res.replace(" ", "")
    except: pass
    return "unknown"

def get_slug(url: str):
    p = urlparse(url); d = p.netloc.replace('www.', ''); pth = p.path.strip('/').replace('/', '-')
    slug = f"{d}-{pth}" if pth else d
    if p.query:
        q_hash = hashlib.md5(p.query.encode()).hexdigest()[:6]
        slug = f"{slug}-{q_hash}"
    return slug

def get_screen_hash(page: Page):
    try:
        t = page.evaluate("() => (document.body.innerText || '').slice(0, 10000)")
        return hashlib.md5(t.encode('utf-8')).hexdigest()
    except: return ""

def close_popups(page: Page, log_func):
    try:
        whitelist = ['Accept', 'Accept all', 'Allow all', 'I agree', 'Agree', 'РџСЂРёРЅСЏС‚СЊ', 'РџСЂРёРЅСЏС‚СЊ РІСЃРµ', 'Р Р°Р·СЂРµС€РёС‚СЊ', 'РЎРѕРіР»Р°СЃРµРЅ', 'Alle akzeptieren', 'Alle ablehnen']
        clicked = False
        cookie_found = False
        
        btns = page.locator("button:visible, [role='button']:visible, a.button:visible")
        btn_count = btns.count()
        for i in range(btn_count):
            btn = btns.nth(i)
            txt = (btn.inner_text() or "").strip()
            if not txt: continue
            if is_forbidden_button(btn, log_func):
                cookie_found = True
                continue
            
            lower_txt = txt.lower()
            if any(w.lower() == lower_txt or w.lower() in lower_txt for w in whitelist):
                cookie_found = True
                try:
                    btn.click(timeout=1000)
                    log_func(f"Cookie: обычный клик | текст: {txt}")
                except:
                    btn.click(force=True, timeout=1000)
                    log_func(f"Cookie: принудительный клик | текст: {txt}")
                clicked = True; break
        
        hidden_provider = page.evaluate("""() => {
            const providers = [
                '#onetrust-banner-sdk', '#onetrust-accept-btn-handler', '.onetrust-close-btn-handler',
                '#truste-consent-track', '#consent_blackbar',
                '.qc-cmp2-container', '.qc-cmp2-summary-buttons button',
                '#cookie-law-info-bar', '#cookie_action_close_header', '.cky-btn-accept'
            ];
            let hidden = false;
            providers.forEach(s => { 
                document.querySelectorAll(s).forEach(el => {
                    if (el.style.display !== 'none') {
                        el.style.display = 'none';
                        hidden = true;
                    }
                });
            });
            return hidden;
        }""")
        
        if hidden_provider:
            cookie_found = True
            log_func("Cookie: скрыт CSS-правилами провайдера")

        hidden_fallback = page.evaluate("""() => {
            let hidden = false;
            const els = Array.from(document.querySelectorAll('*')).filter(el => {
                const style = window.getComputedStyle(el);
                return (style.position === 'fixed' || style.position === 'sticky') && style.display !== 'none';
            });
            
            els.forEach(el => {
                const t = el.innerText.toLowerCase();
                if (t.includes('cookie') || t.includes('consent') || t.includes('privacy')) {
                    const primary = ['continue', 'next', 'submit', 'get my plan', 'claim', 'spin', 'start'];
                    if (!primary.some(w => t.includes(w))) { 
                        el.style.display = 'none'; 
                        hidden = true;
                    }
                }
            });
            return hidden;
        }""")
        
        if hidden_fallback:
            cookie_found = True
            log_func("Cookie: скрыт fallback CSS")
        
        if cookie_found:
            log_func("Cookie-баннер обнаружен")
            
    except: pass


def ensure_privacy_checkbox_checked(page: Page, log_func) -> bool:
    try:
        keywords = ["I have read and understood", "consent to the processing", "personal data"]
        found_text = None
        for kw in keywords:
            elements = page.get_by_text(kw, exact=False)
            elements_count = elements.count()
            for i in range(elements_count):
                el = elements.nth(i)
                if el.is_visible(timeout=500): found_text = el; break
            if found_text: break
        
        if not found_text: return False
        container = found_text.locator("xpath=./ancestor::*[self::label or self::div][1]").first
        checkbox = container.locator("input[type='checkbox']").first
        if checkbox.count() > 0:
            if not checkbox.is_checked(): checkbox.click(force=True, timeout=1000)
            return checkbox.is_checked()
        container.click(force=True, timeout=1000)
        return True
    except: return False


def ensure_consent_checkbox_checked(page: Page, log_func) -> bool:
    try:
        page_text = ""
        try:
            page_text = (page.evaluate("() => document.body.innerText || ''") or "").lower()
        except:
            pass

        must_accept = any(k in page_text for k in CONSENT_TEXT_KEYS) or "to continue, please accept" in page_text
        checked_any = False

        role_boxes = page.locator("[role='checkbox']:visible")
        role_count = role_boxes.count()
        for i in range(role_count):
            rb = role_boxes.nth(i)
            try:
                state = (rb.get_attribute("aria-checked") or "").lower()
                if state == "true":
                    continue
                rb.click(force=True, timeout=1000)
                if (rb.get_attribute("aria-checked") or "").lower() == "true":
                    checked_any = True
                    log_func("Чекбокс согласия отмечен (role=checkbox)")
            except:
                pass

        checkboxes = page.locator("input[type='checkbox']")
        cb_count = checkboxes.count()
        for i in range(cb_count):
            cb = checkboxes.nth(i)
            try:
                if cb.is_checked():
                    continue
            except:
                continue

            try:
                wrapper_text = (
                    cb.locator("xpath=./ancestor::*[self::label or self::div][1]")
                    .inner_text()
                    .lower()
                )
            except:
                wrapper_text = ""

            is_relevant = any(k in wrapper_text for k in CONSENT_TEXT_KEYS) if wrapper_text else must_accept
            if not is_relevant:
                continue

            try:
                cb.check(timeout=1000)
            except:
                try:
                    cb.click(force=True, timeout=1000)
                except:
                    try:
                        cb_id = (cb.get_attribute("id") or "").strip()
                        if cb_id:
                            label_for = page.locator(f"label[for='{cb_id}']").first
                            if label_for.count() > 0:
                                label_for.click(force=True, timeout=1000)
                            else:
                                cb.locator("xpath=./ancestor::*[self::label or self::div][1]").click(force=True, timeout=1000)
                        else:
                            cb.locator("xpath=./ancestor::*[self::label or self::div][1]").click(force=True, timeout=1000)
                    except:
                        continue

            try:
                if cb.is_checked():
                    checked_any = True
                    log_func("Чекбокс согласия отмечен")
            except:
                pass

        if not checked_any and must_accept:
            try:
                consent_label = page.get_by_text("By continuing, you agree", exact=False).first
                if consent_label.count() > 0:
                    consent_label.click(force=True, timeout=1000)
                    log_func("Попытка отметить согласие кликом по тексту условий")
            except:
                pass

        return checked_any
    except:
        return False

DEBUG_CLASSIFY = True
WORKOUT_ISSUES_STEP_KEY = "stepid=step-workout-issues"
WORKOUT_FREQUENCY_STEP_KEY = "stepid=step-workout-frequency"
DATE_OF_BIRTH_STEP_KEY = "stepid=step-date-of-birth"


def is_probable_paywall_url(url: str) -> bool:
    try:
        u = (url or "").lower()
        path = urlparse(u).path
        paywall_path_keywords = [
            "purchase",
            "checkout",
            "subscription",
            "subscribe",
            "pricing",
            "paywall",
            "billing",
            "order"
        ]
        return any(k in path for k in paywall_path_keywords)
    except:
        return False


def resolve_fill_values(config: dict) -> dict:
    fill = config.get("fill_values", {}) or {}
    return {
        "name": str(fill.get("name", "John")),
        "email": str(fill.get("email", "testuser{ts}@gmail.com")),
        "age": str(fill.get("age", "30")),
        "height": str(fill.get("height", "170")),
        "weight": str(fill.get("weight", "70")),
        "goal_weight": str(fill.get("goal_weight", "60")),
        "default_number": str(fill.get("default_number", "25")),
        "date_of_birth": str(fill.get("date_of_birth", "01/01/1990")),
    }


def warmup_page_for_full_screenshot(page: Page, log_func) -> None:
    try:
        total_h = page.evaluate(
            "() => Math.max(document.body.scrollHeight || 0, document.documentElement.scrollHeight || 0)"
        ) or 0
        viewport_h = page.evaluate("() => window.innerHeight || 800") or 800
        if total_h <= viewport_h:
            return

        step = max(int(viewport_h * 0.85), 280)
        pos = 0
        while pos < total_h - viewport_h:
            pos = min(pos + step, total_h - viewport_h)
            page.evaluate("(y) => window.scrollTo(0, y)", pos)
            time.sleep(0.25)

        # Give lazy blocks/images a moment to load at the bottom.
        time.sleep(1.2)
        log_func("Подготовка скриншота: выполнена прокрутка страницы для догрузки контента")
    except Exception as e:
        log_func(f"Не удалось подготовить полный скриншот: {str(e)[:90]}")

def classify_screen(page: Page, log_func):
    def debug_return(ctype, reason):
        if DEBUG_CLASSIFY:
            log_func(f"Причина классификации={reason}")
        return ctype

    t = ""
    try: t = page.evaluate("() => (document.body.innerText || '').toLowerCase()")
    except: pass
    u = page.url.lower()
    
    # 1. Checkout
    checkout_kws = ["card number", "cvv", "mm/yy", "confirm payment", "paypal", "buy now"]
    if any(k in t for k in checkout_kws) or page.locator("input[name*='card']").count() > 0:
        return debug_return('checkout', "Checkout keywords or card inputs found")

    # 2. Paywall
    has_price = any(k in t for k in ["в‚¬", "$", "ВЈ", "в‚Ѕ"]) or re.search(r'\d+[.,]\d+\s*[в‚¬$ВЈв‚Ѕ]', t)        
    has_billing = any(k in t for k in ["subscribe", "trial", "billed", "week plan", "month plan", "pricing", "plans"])
    has_cta = any(k in t for k in ["get my plan", "checkout", "start trial"])
    is_subscription_url = any(k in u for k in ["subscriptions-utc", "subscription", "pricing", "paywall", "checkout"])
    is_madmuscles_final_url = "madmuscles.com/final/" in u
    if (
        ("coursiv.io" in u and "selling-page" in u)
        or (has_price and has_billing and has_cta)
        or is_subscription_url
        or is_probable_paywall_url(u)
        or is_madmuscles_final_url
    ):
        return debug_return('paywall', "Paywall signals detected")

    # 3. Game/Prize/Spin screens (should be info)
    game_kws = ["spin", "wheel", "prize"]
    if any(k in t for k in game_kws) or "prize-wheel" in u:
        # Check if there is an actual SPIN button
        btn_text = (page.evaluate("() => Array.from(document.querySelectorAll('button, a, [role=\"button\"]')).map(el => el.innerText).join(' ')") or "").lower()
        if any(k in btn_text for k in game_kws) or "prize-wheel" in u:
            return debug_return('info', "Game/Prize screen detected (keyword in buttons/URL)")

    # 4. Email
    has_email_input = page.locator("input[type='email'], input[autocomplete*='email' i]").count() > 0
    if not has_email_input:
        inputs = page.locator("input:not([type='hidden'])")
        inputs_count = inputs.count()
        for i in range(inputs_count):
            try:
                p = (inputs.nth(i).get_attribute("placeholder") or "").lower()
                if any(k in p for k in ["email", "e-mail", "mail@"]): has_email_input = True; break
            except: pass

    # If attributes are not enough, check if there is an input AND "email" keywords in text/URL
    if not has_email_input and page.locator("input:not([type='hidden'])").count() > 0:
        if any(k in t for k in ["email", "e-mail", "СЌР»РµРєС‚СЂРѕРЅРЅР°СЏ РїРѕС‡С‚Р°", "Р°РґСЂРµСЃ РїРѕС‡С‚С‹"]):
            # Only if it's not a profile data screen (age, weight, etc.)
            pd_kws = ["age", "height", "weight", "name", "СЂРѕСЃС‚", "РІРµСЃ", "РІРѕР·СЂР°СЃС‚", "РёРјСЏ"]
            if not any(k in t for k in pd_kws):
                has_email_input = True

    if has_email_input: return debug_return('email', "Email field found via attributes or text")

    # 4. Input (with animation safety)
    pd_kws = ["age", "height", "weight", "name", "cm", "kg", "years", "call you"]
    if any(k in t for k in pd_kws) or any(k in u for k in ["name", "age", "weight", "height"]):
        try: page.wait_for_selector(FILLABLE_INPUT_SELECTOR, timeout=2000)
        except: pass
    
    inputs = page.locator(FILLABLE_INPUT_SELECTOR)
    if inputs.count() >= 1: return debug_return('input', f"{inputs.count()} input(s) found")

    # 4.5 Email Consent / Notifications (Should be question)
    consent_kws = ["receive emails", "send me emails", "notifications", "updates", "stay in the loop", "newsletters", "consent", "i'm in"]
    if any(k in t for k in consent_kws) and ("email" in t or "mail" in t or "email-page" in u):
        return debug_return('question', "Email consent/notification screen detected")

    # 5. Question vs Info (Smart separation)
    nav_words = [
        "next", "continue", "skip", "back", "weiter", "zurГјck", "next step", "proceed", 
        "got it", "ok", "okay", "great", "understood", "yes", "i'm in", "let's go", 
        "see results", "start", "РїСЂРёРЅСЏС‚СЊ", "РѕРє", "РЅР°С‡Р°С‚СЊ", "РїРѕРЅСЏС‚РЅРѕ", "С…РѕСЂРѕС€Рѕ",
        "do it", "ready", "begin", "let's", "go", "transformation"
    ]
    
    # Get all potential interactive elements
    raw_els = page.locator("[data-testid*='answer' i]:visible, [class*='Card' i]:not([class*='testimonial' i]):not([class*='review' i]):visible, label:visible, button:visible, a:visible, [role='button']:visible, div[role='button']:visible")
    
    choices = []
    nav_btns = []
    
    raw_count = raw_els.count()
    for i in range(raw_count):
        el = raw_els.nth(i)
        if is_forbidden_button(el, log_func): continue

        # Verify it's genuinely interactive
        is_int = el.evaluate("""el => {
            const t = el.tagName.toLowerCase();
            if (t === 'button' || t === 'a' || t === 'label' || t === 'input') return true;
            if (el.getAttribute('role') === 'button') return true;
            return window.getComputedStyle(el).cursor === 'pointer';
        }""")
        if not is_int: continue

        txt = (el.inner_text() or "").lower().strip()
        
        if not txt:
            tag = el.evaluate("el => el.tagName").lower()
            if tag in ['button', 'label', 'input']:
                choices.append("empty_choice")
            continue

        is_nav = False
        for w in nav_words:
            if txt == w or txt.startswith(w + "!") or txt.startswith(w + ".") or (len(txt) < 20 and w in txt):
                is_nav = True
                break
        
        if is_nav:
            nav_btns.append(txt)
        else:
            choices.append(txt)

    total_interactive = len(choices) + len(nav_btns)

    has_skip = any("skip" in b for b in nav_btns)
    has_picker = False
    try:
        has_picker = page.locator("select:visible, [role='combobox']:visible, [role='listbox']:visible, [role='slider']:visible, [class*='picker' i]:visible").count() > 0
    except: pass

    # Core Logic based on exact number of interactive options
    if total_interactive == 0:
        return debug_return('other', "No clear type detected")
        
    if total_interactive == 1:
        if has_picker:
            return debug_return('question', "Single CTA but custom picker/selector found")
        # Only one available action option
        return debug_return('info', "Single interactive element detected, classifying as info")
        
    if total_interactive >= 2:
        # Two or more explicit interactive options
        if len(choices) >= 1 or has_skip or has_picker:
            # If at least one of them is a choice, or it has a skip button/picker, it's a question
            return debug_return('question', f"Multiple options detected ({total_interactive}), including choices/skip/picker")
        else:
            # Multiple nav buttons but no explicit choices (e.g., 'Back' and 'Next') -> Info
            return debug_return('info', f"Multiple nav buttons ({total_interactive}) but no choices, classifying as info")

    return debug_return('other', "No clear type detected")


def find_continue_button(page: Page, log_func=None):
    keywords = [
        'Continue', 'Next', 'Get my plan', 'Start', 'Take the quiz', 
        'Get started', 'Start quiz', 'Get my offer', 'Next step', 'Proceed', 
        'Submit', 'Show my results', 'See my results', "Let's", "Do it", "I'm in",
        'Got it', 'Got it!'
    ]
    # Restrict to actual interactive elements to avoid picking up headlines/prompts
    button_locator = page.locator("button:visible, [role='button']:visible, a.button:visible, a:visible")
    
    for text in keywords:
        # We search within buttons/links for the text
        btns = button_locator.get_by_text(text, exact=False)
        if btns.count() > 0:
            btn = btns.first
            if btn.is_visible(timeout=500):
                try:
                    if not btn.is_enabled():
                        continue
                except:
                    pass
                if not is_forbidden_button(btn, log_func): return btn

    # We purposefully do not fallback to random buttons here, as it causes 
    # the bot to mistakenly click choice variants as 'Next' buttons.
    return None

def wait_for_transition(page: Page, old_url: str, old_hash: str, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        if page.url != old_url or get_screen_hash(page) != old_hash:
            return True
        time.sleep(0.5)
    return False


def try_click_got_it(page: Page, log_func, timeout: float = 4.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            got_it_btns = page.locator("button:visible, [role='button']:visible, a:visible").get_by_text("Got it", exact=False)
            if got_it_btns.count() > 0:
                btn = got_it_btns.first
                if btn.is_enabled():
                    log_func("Найдена подсказка MadMuscles: нажимаю Got it")
                    pre_hash = get_screen_hash(page)
                    btn.click(force=True, timeout=1500)
                    moved = wait_for_transition(page, page.url, pre_hash, timeout=8.0)
                    if moved:
                        return True
        except:
            pass
        time.sleep(0.3)
    return False

NAV_KEYWORDS = [
    "next", "continue", "skip", "back", "weiter", "zurГјck", "next step", "proceed", 
    "got it", "ok", "РїСЂРёРЅСЏС‚СЊ", "РѕРє", "start", "get started", "take the quiz", 
    "accept", "allow", "agree", "alle akzeptieren", "alle ablehnen"
]

def is_nav_button(text: str) -> bool:
    if not text: return False
    t = text.lower().strip()
    return any(w == t or t.startswith(w + " ") for w in NAV_KEYWORDS)

def perform_action(page: Page, screen_type: str, log_func, results_dir: str, start_hash: str, start_url: str, fill_values: dict, repeat_attempt: int = 0):
    try:
        if "madmuscles.com/final/" in page.url.lower():
            return "stopped at paywall"
        if screen_type == 'paywall': return "stopped at paywall"
        if screen_type == 'checkout': return "checkout reached"

        if screen_type in ['email', 'input']:
            checked = ensure_privacy_checkbox_checked(page, log_func)
            if checked: log_func("Чекбокс политики конфиденциальности отмечен")
            
            # Wait for inputs to be visible
            try: page.wait_for_selector(FILLABLE_INPUT_SELECTOR, timeout=3000)
            except: pass
            
            inputs = page.locator(FILLABLE_INPUT_SELECTOR)
            inputs_count = inputs.count()
            if inputs_count == 0:
                log_func("Заполняемых полей не найдено, пропуск ветки input/email")
                return "no_fillable_inputs"
            for i in range(inputs_count):
                inp = inputs.nth(i)
                itype = (inp.get_attribute("type") or "text").lower()
                placeholder = (inp.get_attribute("placeholder") or "").lower()
                
                val = fill_values.get("name", "John")
                if screen_type == 'email' or any(k in placeholder for k in ["email", "e-mail"]):
                    email_tpl = fill_values.get("email", "testuser{ts}@gmail.com")
                    val = email_tpl.replace("{ts}", str(int(time.time())))
                
                context_text = (page.evaluate("() => document.body.innerText") or "").lower()
                context_url = page.url.lower()
                is_dob_step = DATE_OF_BIRTH_STEP_KEY in context_url
                is_date_field = (
                    is_dob_step
                    or "date of birth" in context_text
                    or "birthday" in context_text
                    or "dd / mm / yyyy" in context_text
                    or "dd/mm/yyyy" in placeholder
                    or "birth" in placeholder
                    or "dob" in placeholder
                )
                
                # Use word boundaries for numeric keywords to avoid false positives like 'age' in 'magic-page'
                num_pattern = r'\b(age|height|weight|РІРѕР·СЂР°СЃС‚|СЂРѕСЃС‚|РІРµСЃ|bmi|goal|С†РµР»СЊ)\b'
                is_numeric = itype == "number" or re.search(num_pattern, placeholder) or re.search(num_pattern, context_url)
                
                if is_date_field:
                    val = fill_values.get("date_of_birth", "01/01/1990")
                elif is_numeric:
                    if any(k in placeholder or k in context_url or k in context_text for k in ["height", "СЂРѕСЃС‚"]):
                        val = fill_values.get("height", "170")
                    elif any(k in placeholder or k in context_url or k in context_text for k in ["goal", "С†РµР»СЊ"]):
                        val = fill_values.get("goal_weight", "60")
                    elif any(k in placeholder or k in context_url or k in context_text for k in ["weight", "РІРµСЃ"]):
                        val = fill_values.get("weight", "70")
                    elif any(k in placeholder or k in context_url or k in context_text for k in ["age", "РІРѕР·СЂР°СЃС‚"]):
                        val = fill_values.get("age", "30")
                    else:
                        val = fill_values.get("default_number", "25")
                
                try:
                    log_func(f"Заполнение поля: тип={itype}, значение={val}")
                    if is_date_field:
                        # Masked DOB inputs often require sequential typing instead of fill().
                        inp.click(timeout=500)
                        inp.press("Control+A")
                        inp.type(str(val), delay=45)
                    else:
                        inp.fill(str(val))
                    time.sleep(0.3)
                except Exception as e:
                    if is_date_field:
                        fallback_val = re.sub(r"[^\d]", "", str(val))
                    else:
                        fallback_val = re.sub(r"[^\d]", "", str(val)) if is_numeric else str(val)
                    try:
                        inp.click(timeout=500)
                        inp.press("Control+A")
                        inp.type(fallback_val, delay=35 if is_date_field else 20)
                        time.sleep(0.2)
                        log_func(f"Заполнение fallback: значение={fallback_val}")
                    except Exception:
                        log_func(f"Ошибка заполнения: {str(e)[:80]}")

            btn = find_continue_button(page, log_func)
            if btn:
                btn_txt = " ".join((btn.inner_text() or "").split())
                log_func(f"Нажатие Continue: {btn_txt}")
                close_popups(page, log_func)
                try:
                    btn.click(force=True, timeout=2000)
                except:
                    page.keyboard.press("Enter")
            else: page.keyboard.press("Enter")
            log_func("Форма отправлена. Ожидание перехода...")
            wait_for_transition(page, start_url, start_hash, timeout=12.0)
            return "input_submitted"

        if screen_type in ['question', 'info', 'other']:
            is_workout_issues = "madmuscles.com" in page.url.lower() and WORKOUT_ISSUES_STEP_KEY in page.url.lower()
            is_workout_frequency = "madmuscles.com" in page.url.lower() and WORKOUT_FREQUENCY_STEP_KEY in page.url.lower()
            cont_btn = find_continue_button(page, log_func)

            if is_workout_frequency and repeat_attempt > 0:
                log_func("Повтор step-workout-frequency: пробую Continue -> Got it без нового выбора")
                if cont_btn and cont_btn.is_enabled():
                    try:
                        pre_cont_hash = get_screen_hash(page)
                        cont_btn.click(force=True, timeout=1500)
                        moved = wait_for_transition(page, page.url, pre_cont_hash, timeout=4.0)
                        if moved:
                            return "continue_clicked"
                    except:
                        pass
                if try_click_got_it(page, log_func, timeout=8.0):
                    return "continue_clicked"
            # Priority to "Start" buttons
            if cont_btn and any(k in (cont_btn.inner_text() or "").lower() for k in ["start", "get my", "get started", "take the", "offer", "claim", "discount", "spin"]):
                c_txt = " ".join((cont_btn.inner_text() or "").split())
                log_func(f"Найдена кнопка старта: {c_txt}. Нажимаю...")
                close_popups(page, log_func)
                pre_start_hash = get_screen_hash(page)
                cont_btn.click(force=True, timeout=1000)
                moved = wait_for_transition(page, start_url, start_hash)
                if not moved and is_probable_paywall_url(page.url):
                    log_func("Страница похожа на paywall после клика старта")
                    return "stopped at paywall"
                if not moved and get_screen_hash(page) == pre_start_hash:
                    log_func("Клик по старту не изменил экран")
                return "start_button_pressed"

            choice_sel = [
                "[data-testid*='answer' i]:visible", 
                "button:visible", 
                "[class*='Item' i]:visible", 
                "[class*='Card' i]:visible", 
                "label:visible"
            ]
            text_targets = []
            empty_targets = []
            # Pass 1: Try all selectors to find a choice WITH text (excluding Nav)
            for s in choice_sel:
                els = page.locator(s)
                els_count = els.count()
                for i in range(els_count):
                    curr = els.nth(i)
                    if is_forbidden_button(curr, log_func): continue
                    if is_workout_issues:
                        try:
                            cb = curr.locator("input[type='checkbox']").first
                            if cb.count() > 0 and cb.is_checked():
                                continue
                        except:
                            pass
                    txt = (curr.inner_text() or "").strip()
                    if txt and not re.search(r'^\d+\s*/\s*\d+$', txt) and len(txt) < 100 and not is_nav_button(txt):
                        text_targets.append(curr)

            # Pass 2: If no text choices found, try all selectors for empty choices
            if not text_targets:
                for s in choice_sel:
                    els = page.locator(s)
                    els_count = els.count()
                    for i in range(els_count):
                        curr = els.nth(i)
                        if is_forbidden_button(curr, log_func): continue
                        if not (curr.inner_text() or "").strip():
                            tag = curr.evaluate("el => el.tagName").lower()
                            if tag in ['button', 'input']:
                                empty_targets.append(curr)

            target_pool = text_targets if text_targets else empty_targets
            target = None
            if target_pool:
                chosen_idx = repeat_attempt % len(target_pool)
                target = target_pool[chosen_idx]
                if len(target_pool) > 1 and repeat_attempt > 0:
                    log_func(f"Повтор шага: пробую альтернативный вариант #{chosen_idx + 1} из {len(target_pool)}")

            if not target:
                if cont_btn:
                    c_txt = " ".join((cont_btn.inner_text() or "").split())
                    log_func(f"Нет вариантов, нажимаю Continue: {c_txt}")
                    close_popups(page, log_func)
                    cont_btn.click(force=True, timeout=1000)
                    moved = wait_for_transition(page, start_url, start_hash)
                    if moved:
                        return "info_continue_pressed"
                    log_func("Continue не изменил экран")
                return "no_choices_found"
            start_ui = get_ui_step(page)
            # 1. Click choice
            clean_target_text = " ".join((target.inner_text() or "").split())
            display_text = clean_target_text[:50] if clean_target_text else "<No text>"
            log_func(f"Нажатие выбора: {display_text}")
            try:
                target.scroll_into_view_if_needed(timeout=2000)
            except: pass
            close_popups(page, log_func)
            try:
                target.click(force=True, timeout=2000)
            except Exception as e:
                log_func(f"Ошибка клика: {str(e)[:50]}")
            
            # Short wait for auto-advance or progress change
            log_func("Ожидание авто-перехода...")
            start_wait = time.time()
            transitioned_auto = False
            while time.time() - start_wait < 2.0:
                curr_ui = get_ui_step(page)
                if page.url != start_url:
                    log_func(f"Обнаружен авто-переход по URL: {start_url} -> {page.url}")
                    transitioned_auto = True
                    break
                if curr_ui != start_ui and curr_ui != "unknown":
                    log_func(f"Обнаружен авто-переход (ui_step: {start_ui} -> {curr_ui})")
                    transitioned_auto = True
                    break
                time.sleep(0.3)
            
            if transitioned_auto:
                return "auto_advanced"

            # MadMuscles frequently shows a post-answer hint card with mandatory "Got it".
            if "madmuscles.com" in page.url.lower():
                if try_click_got_it(page, log_func, timeout=4.0):
                    return "continue_clicked"

            # This step often renders Continue -> info card -> Got it with slight delay.
            if is_workout_frequency:
                if try_click_got_it(page, log_func, timeout=5.0):
                    return "continue_clicked"

            if is_workout_issues:
                for label in ["Got it", "Continue"]:
                    btns = page.locator("button:visible, [role='button']:visible, a:visible").get_by_text(label, exact=False)
                    if btns.count() > 0:
                        try:
                            btn = btns.first
                            if btn.is_enabled():
                                log_func(f"Спец-шаг workout-issues: нажимаю {label}")
                                pre_hash = get_screen_hash(page)
                                btn.click(force=True, timeout=1500)
                                moved = wait_for_transition(page, page.url, pre_hash, timeout=8.0)
                                if moved:
                                    return "continue_clicked"
                        except:
                            pass

            # 2. Wait for Next button if no auto-advance
            start_time = time.time()
            clicked_continue = False
            while time.time() - start_time < 3.0:
                curr_ui = get_ui_step(page)
                if curr_ui != start_ui and curr_ui != "unknown":
                    break

                # Safe URL check: only break if the PATH changes
                if urlparse(page.url).path != urlparse(start_url).path:
                    break 

                curr_cont = find_continue_button(page, log_func)
                if curr_cont and curr_cont.is_enabled():
                    c_txt = " ".join((curr_cont.inner_text() or "").split())
                    log_func(f"Найдена кнопка Continue: {c_txt}. Нажимаю...")
                    close_popups(page, log_func)
                    pre_cont_hash = get_screen_hash(page)
                    try:
                        curr_cont.click(force=True, timeout=2000)
                    except: pass
                    clicked_continue = True
                    moved = wait_for_transition(page, page.url, pre_cont_hash, timeout=10.0)
                    if moved:
                        return "continue_clicked"
                    if "madmuscles.com" in page.url.lower():
                        got_it_timeout = 8.0 if is_workout_frequency else 3.0
                        if try_click_got_it(page, log_func, timeout=got_it_timeout):
                            return "continue_clicked"
                    log_func("Continue нажат, но переход не произошел")
                time.sleep(0.5)

            # 3. Multiselect
            if not clicked_continue:
                curr_cont = find_continue_button(page, log_func)
                if curr_cont and not curr_cont.is_enabled():
                    log_func("Обнаружен мультивыбор. Выбираю дополнительные варианты...")
                    for s in choice_sel:
                        els = page.locator(s)
                        els_count = els.count()
                        for i in range(1, min(els_count, 5)):
                            curr = els.nth(i)
                            txt = (curr.inner_text() or "").strip()
                            if txt and not re.search(r'^\d+\s*/\s*\d+$', txt) and not is_forbidden_button(curr, log_func):
                                close_popups(page, log_func)
                                curr.click(force=True, timeout=500)
                                if curr_cont.is_enabled():
                                    close_popups(page, log_func)
                                    pre_multi_hash = get_screen_hash(page)
                                    curr_cont.click(force=True, timeout=1000)
                                    moved = wait_for_transition(page, page.url, pre_multi_hash, timeout=10.0)
                                    if moved:
                                        return "multiselect_completed"
                                    log_func("После мультивыбора переход не произошел")

            # 4. Consent checkbox fallback for pages that block progress until terms are accepted.
            consent_checked = ensure_consent_checkbox_checked(page, log_func)
            if consent_checked:
                curr_cont = find_continue_button(page, log_func)
                if curr_cont and curr_cont.is_enabled():
                    c_txt = " ".join((curr_cont.inner_text() or "").split())
                    log_func(f"После согласия нажимаю Continue: {c_txt}")
                    close_popups(page, log_func)
                    pre_consent_hash = get_screen_hash(page)
                    try:
                        curr_cont.click(force=True, timeout=1500)
                    except:
                        pass
                    moved = wait_for_transition(page, page.url, pre_consent_hash, timeout=8.0)
                    if moved:
                        return "consent_continue_clicked"
                    log_func("После согласия Continue не привел к переходу")
                try:
                    log_func("После согласия повторяю клик по варианту ответа")
                    pre_retry_hash = get_screen_hash(page)
                    target.click(force=True, timeout=1000)
                    wait_for_transition(page, page.url, pre_retry_hash, timeout=8.0)
                    return "consent_choice_reclicked"
                except Exception as e:
                    log_func(f"Не удалось повторно кликнуть вариант после согласия: {str(e)[:80]}")

            wait_for_transition(page, start_url, start_hash, timeout=5.0)
            return "screen_interaction_completed"                
    except Exception as e: return f"err:{str(e)}"
    return "none"

def run_funnel(url: str, config: dict, is_headless: bool):
    slug = get_slug(url); res_dir = os.path.join('results', slug); os.makedirs(res_dir, exist_ok=True)
    classified_dir = os.path.join('results', '_classified')
    for cat in ['question', 'info', 'input', 'email', 'paywall', 'other', 'checkout']:
        os.makedirs(os.path.join(classified_dir, cat), exist_ok=True)
    
    summary = {"url": url, "slug": slug, "steps_total": 0, "paywall_reached": False, "last_url": "", "path": res_dir, "error": None}
    max_steps = int(config.get("max_steps", 80))
    slow_mo = int(config.get("slow_mo_ms", 100))
    fill_values = resolve_fill_values(config)
    
    with open(os.path.join(res_dir, 'log.txt'), 'w', encoding='utf-8') as f:
        def log(m):
            l = f"[{time.strftime('%H:%M:%S')}] {m}\n"; f.write(l); print(l.strip())
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=is_headless, slow_mo=slow_mo)
                page = browser.new_context(**p.devices['iPhone 13']).new_page()
                log(f"Переход на {url} (slug: {slug})")
                try:
                    page.goto(url, wait_until='load', timeout=60000)
                except TimeoutError:
                    summary["error"] = "navigation_timeout"
                    log("Ошибка: таймаут открытия страницы")
                    browser.close()
                    return summary
            
                step = 1
                history_counts = defaultdict(int)
                step_attempts = defaultdict(int)
                while step <= max_steps:
                    curr_u = page.url
                    if any(k in curr_u for k in ["magic", "analyzing", "loading", "preparePlan"]): 
                        time.sleep(10); curr_u = page.url
                    
                    close_popups(page, log)
                    time.sleep(1)
                    curr_h = get_screen_hash(page)
                    st = classify_screen(page, log)
                    ui_before = get_ui_step(page)
                    if st in ['paywall', 'checkout']:
                        warmup_page_for_full_screenshot(page, log)
                        curr_h = get_screen_hash(page)
                    step_key = f"{urlparse(curr_u).path}|{ui_before}|{st}"
                    repeat_attempt = step_attempts[step_key]
                    step_attempts[step_key] += 1
                    
                    curr_id = f"{curr_u}|{curr_h}"
                    history_counts[curr_id] += 1
                    loop_limit = 8 if WORKOUT_ISSUES_STEP_KEY in curr_u.lower() else 3
                    if history_counts[curr_id] >= loop_limit:
                        log(f"Обнаружено зацикливание на {curr_u}. Остановка.")
                        summary["error"] = "stuck_loop"
                        break
                    
                    screen_name = f"{step:02d}_{st}.png"
                    local_path = os.path.join(res_dir, screen_name)
                    try:
                        page.screenshot(path=local_path, full_page=True)
                        shutil.copy2(local_path, os.path.join(classified_dir, st, f"{slug}__{screen_name}"))
                    except Exception as e:
                        log(f"Ошибка сохранения скриншота: {str(e)[:120]}")
                    
                    log(f"Шаг:{step} | тип:{st} | ui_step:{ui_before} | url:{page.url[:60]}")
                    act = perform_action(page, st, log, res_dir, curr_h, curr_u, fill_values, repeat_attempt=repeat_attempt)
                    
                    time.sleep(1)
                    ui_after = get_ui_step(page)
                    
                    def parse_ui(s):
                        if not s or s == "unknown": return -1
                        m = re.match(r'(\d+)', s)
                        return int(m.group(1)) if m else -1
                    
                    ui_b_num = parse_ui(ui_before)
                    ui_a_num = parse_ui(ui_after)
                    
                    if ui_a_num > ui_b_num + 1 and ui_b_num > 0:
                        log(f"Обнаружен пропуск шага UI: {ui_before} -> {ui_after}")
                    
                    log(f"Результат действия:{act}")
                    
                    summary["steps_total"] = step; summary["last_url"] = page.url
                    if st in ['paywall', 'checkout'] or "stopped" in act or "reached" in act:
                        if st in ['paywall', 'checkout'] or "paywall" in act:
                            summary["paywall_reached"] = True
                        break
                    
                    step += 1
                browser.close()
        except Exception as e:
            summary["error"] = f"runner_exception:{str(e)[:180]}"
            log(f"Критическая ошибка раннера: {str(e)[:180]}")
    return summary

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
    
    headless = True
    if args.debug:
        headless = False
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
    
    with open(os.path.join('results', 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(all_summaries, f, indent=4)
    
    print("\nПакетный запуск завершён.")
    for s in all_summaries:
        status = "УСПЕХ" if s['paywall_reached'] else "ОШИБКА"
        print(f"[{status}] URL: {s['url']} | Шагов: {s['steps_total']}")

