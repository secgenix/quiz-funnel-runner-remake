import json, argparse, os, time, re, hashlib, shutil
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Page, TimeoutError
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime
from ai_fallback import run_ai_fallback, resolve_ai_fallback_settings

# Директория для сохранения артефактов ошибок
ERRORS_DIR = "errors"

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
    """
    Проверка кнопки на "запрещенность" (cookie, share и т.п.)
    
    Универсальная проверка - не маркируем варианты выбора (male/female и т.п.) как cookie
    """
    try:
        txt = (el.inner_text() or "").lower()
        alabel = (el.get_attribute("aria-label") or "").lower()
        cls = (el.get_attribute("class") or "").lower()
        html = (el.evaluate("el => el.innerHTML") or "").lower()
        full_text = f"{txt} {alabel} {cls} {html}"

        forbidden = False
        reason = ""
        
        # Проверяем на cookie-кнопки - только если есть явные маркеры cookie/settings
        # Не маркируем простые варианты выбора как cookie
        cookie_indicators = ["cookie", "cookies", "privacy settings", "cookie settings", 
                           "preferences", "customize", "options", "more info",
                           "einstellungen", "optionen", "mehr informationen",
                           "cookie settings", "privacy settings", "datenschutzeinstellungen",
                           "manage cookies", "cookie policy"]
        
        if any(indicator in full_text for indicator in cookie_indicators):
            # Дополнительная проверка - если это явно вариант выбора (короткий текст без cookie-терминов)
            # то не считаем его запрещенным
            if txt.strip() and len(txt.strip()) < 20 and "cookie" not in txt and "settings" not in txt:
                # Это вероятно вариант выбора, а не cookie кнопка
                pass
            else:
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
                txt = safe_inner_text(curr, "")
                if txt and not re.search(r'\d+\s*/\s*\d+', txt) and len(txt) < 100:
                    texts.append(txt)
            if texts:
                return "|".join(texts[:3])
        return ""
    except: return ""


def safe_inner_text(locator, fallback: str = "") -> str:
    try:
        return (locator.inner_text() or fallback).strip()
    except:
        try:
            return (locator.evaluate("el => (el.innerText || el.textContent || '').trim()") or fallback).strip()
        except:
            return fallback

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


def save_error_artifacts(page: Page, url: str, error_message: str, log_lines: list, log_func) -> str:
    """
    Сохраняет DOM-дерево, код страницы, скриншот и логи при ошибке.
    
    Args:
        page: Playwright страница
        url: URL где произошла ошибка
        error_message: Описание ошибки
        log_lines: Список строк лога
        log_func: Функция логирования
    
    Returns:
        Путь к папке с артефактами ошибки
    """
    try:
        # Создаем папку для ошибок
        os.makedirs(ERRORS_DIR, exist_ok=True)
        
        # Генерируем имя папки на основе домена и timestamp
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_folder = os.path.join(ERRORS_DIR, f"{domain}_{timestamp}")
        os.makedirs(error_folder, exist_ok=True)
        
        log_func(f"Сохранение артефактов ошибки в {error_folder}")
        
        # 1. Сохраняем DOM-дерево
        try:
            dom_content = page.evaluate("() => document.documentElement.outerHTML")
            dom_path = os.path.join(error_folder, "dom.html")
            with open(dom_path, 'w', encoding='utf-8') as f:
                f.write(dom_content)
            log_func(f"DOM сохранен: {dom_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения DOM: {str(e)[:80]}")
        
        # 2. Сохраняем весь JavaScript код со страницы
        try:
            scripts_content = []
            scripts = page.query_selector_all("script")
            for idx, script in enumerate(scripts):
                try:
                    src = script.get_attribute("src")
                    if src:
                        # Внешний скрипт - сохраняем ссылку
                        scripts_content.append(f"/* External script: {src} */")
                    else:
                        # Inline скрипт - сохраняем содержимое
                        inner_html = script.inner_text() or script.evaluate("el => el.innerHTML")
                        if inner_html:
                            scripts_content.append(f"/* Inline script #{idx} */\n{inner_html}")
                except:
                    pass
            
            scripts_path = os.path.join(error_folder, "scripts.js")
            with open(scripts_path, 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(scripts_content[:50]))  # Ограничиваем 50 скриптами
            log_func(f"Скрипты сохранены: {scripts_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения скриптов: {str(e)[:80]}")
        
        # 3. Сохраняем CSS стили
        try:
            styles_content = []
            styles = page.query_selector_all("style, link[rel='stylesheet']")
            for idx, style in enumerate(styles[:30]):  # Ограничиваем 30 стилями
                try:
                    href = style.get_attribute("href")
                    if href:
                        styles_content.append(f"/* External stylesheet: {href} */")
                    else:
                        inner_html = style.inner_text() or style.evaluate("el => el.innerHTML")
                        if inner_html:
                            styles_content.append(f"/* Inline style #{idx} */\n{inner_html}")
                except:
                    pass
            
            styles_path = os.path.join(error_folder, "styles.css")
            with open(styles_path, 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(styles_content))
            log_func(f"Стили сохранены: {styles_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения стилей: {str(e)[:80]}")
        
        # 4. Сохраняем скриншот
        try:
            screenshot_path = os.path.join(error_folder, "screenshot.png")
            page.screenshot(path=screenshot_path, full_page=True)
            log_func(f"Скриншот сохранен: {screenshot_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения скриншота: {str(e)[:80]}")
        
        # 5. Сохраняем логи
        try:
            log_path = os.path.join(error_folder, "error_log.txt")
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(f"URL: {url}\n")
                f.write(f"Ошибка: {error_message}\n")
                f.write(f"Время: {datetime.now().isoformat()}\n")
                f.write(f"User Agent: {page.evaluate('() => navigator.userAgent')}\n")
                f.write(f"Viewport: {page.evaluate('() => JSON.stringify({width: window.innerWidth, height: window.innerHeight})')}\n")
                f.write("\n=== Логи ===\n\n")
                f.write('\n'.join(log_lines[-200:]))  # Последние 200 строк лога
            log_func(f"Логи сохранены: {log_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения логов: {str(e)[:80]}")
        
        # 6. Сохраняем информацию о странице (metadata)
        try:
            metadata = {
                "url": url,
                "error": error_message,
                "timestamp": datetime.now().isoformat(),
                "user_agent": page.evaluate("() => navigator.userAgent"),
                "viewport": {
                    "width": page.evaluate("() => window.innerWidth"),
                    "height": page.evaluate("() => window.innerHeight"),
                },
                "page_title": page.title(),
                "cookies_count": len(page.context.cookies()),
                "local_storage": page.evaluate("() => { try { return Object.keys(localStorage).length; } catch(e) { return 0; } }"),
                "session_storage": page.evaluate("() => { try { return Object.keys(sessionStorage).length; } catch(e) { return 0; } }"),
            }
            metadata_path = os.path.join(error_folder, "metadata.json")
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            log_func(f"Метаданные сохранены: {metadata_path}")
        except Exception as e:
            log_func(f"Ошибка сохранения метаданных: {str(e)[:80]}")
        
        # 7. Сохраняем console.log ошибки если есть
        try:
            console_errors = page.evaluate("""() => {
                if (window.__qwen_console_errors) {
                    return window.__qwen_console_errors;
                }
                return [];
            }""")
            if console_errors:
                console_path = os.path.join(error_folder, "console_errors.txt")
                with open(console_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(console_errors))
                log_func(f"Console ошибки сохранены: {console_path}")
        except:
            pass
        
        log_func(f"✅ Артефакты ошибки сохранены в {error_folder}")
        return error_folder
        
    except Exception as e:
        log_func(f"❌ Критическая ошибка при сохранении артефактов: {str(e)[:120]}")
        return ""

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

        closed_modal = page.evaluate("""() => {
            const isVisible = (el) => {
                if (!el) return false;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden' || s.opacity === '0') return false;
                const r = el.getBoundingClientRect();
                return r.width > 24 && r.height > 24;
            };

            const overlays = Array.from(document.querySelectorAll('*')).filter(el => {
                if (!isVisible(el)) return false;
                const s = window.getComputedStyle(el);
                const txt = (el.innerText || '').toLowerCase();
                const r = el.getBoundingClientRect();
                const fixedLike = ['fixed', 'sticky', 'absolute'].includes(s.position);
                const helpLike = txt.includes('need some help') || txt.includes('we will be glad to assist') || txt.includes('support@');
                const dialogLike = el.getAttribute('role') === 'dialog' || el.getAttribute('aria-modal') === 'true';
                return fixedLike && r.width > window.innerWidth * 0.45 && r.height > 80 && (helpLike || dialogLike);
            });

            for (const overlay of overlays) {
                const candidates = Array.from(overlay.querySelectorAll('button, [role="button"], a, div, span')).filter(isVisible);
                for (const el of candidates) {
                    const txt = [
                        (el.innerText || '').trim(),
                        (el.getAttribute('aria-label') || '').trim(),
                        (el.getAttribute('title') || '').trim()
                    ].join(' ').toLowerCase();
                    const rect = el.getBoundingClientRect();
                    const looksClose = txt === '×' || txt === 'x' || txt.includes('close') || txt.includes('dismiss');
                    const smallCircle = rect.width <= 72 && rect.height <= 72;
                    if (looksClose || smallCircle) {
                        try { el.click(); return true; } catch (e) {}
                    }
                }
            }
            return false;
        """)

        if closed_modal:
            log_func("Popup/modal: закрыт generic close fallback")

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


def check_and_handle_form_blockers(page: Page, log_func) -> bool:
    """
    Универсальная JS-функция для обнаружения и обработки блокираторов форм:
    - Отсутствующие обязательные поля
    - Disabled кнопки
    - Скрытые ошибки валидации
    - Блокирующие overlay
    
    Returns True если были обнаружены и обработаны блокираторы
    """
    try:
        blockers_handled = page.evaluate("""() => {
            let handled = false;
            
            // 1. Проверяем есть ли disabled кнопка Continue/Next с незаполненными полями
            const disabledButtons = Array.from(document.querySelectorAll('button:disabled, [disabled] button, [aria-disabled="true"]'));
            const continueKeywords = ['continue', 'next', 'get started', 'submit', 'start', 'get my'];
            
            for (const btn of disabledButtons) {
                const txt = (btn.innerText || '').toLowerCase();
                if (continueKeywords.some(k => txt.includes(k))) {
                    // Кнопка disabled - ищем незаполненные обязательные поля
                    const requiredInputs = Array.from(document.querySelectorAll('input[required]:not([type="hidden"]), textarea[required]'));
                    const emptyRequired = requiredInputs.filter(inp => !inp.value || inp.value.trim() === '');
                    
                    if (emptyRequired.length > 0) {
                        // Пытаемся заполнить первое пустое поле
                        const firstEmpty = emptyRequired[0];
                        if (firstEmpty.type === 'email') {
                            firstEmpty.value = 'test' + Date.now() + '@gmail.com';
                        } else if (firstEmpty.type === 'tel') {
                            firstEmpty.value = '1234567890';
                        } else if (firstEmpty.type === 'number') {
                            firstEmpty.value = '25';
                        } else {
                            firstEmpty.value = 'John';
                        }
                        firstEmpty.dispatchEvent(new Event('input', { bubbles: true }));
                        firstEmpty.dispatchEvent(new Event('change', { bubbles: true }));
                        handled = true;
                        break;
                    }
                }
            }
            
            // 2. Проверяем наличие overlay блокирующих клики
            const overlays = Array.from(document.querySelectorAll('*')).filter(el => {
                const style = window.getComputedStyle(el);
                return (
                    style.position === 'fixed' && 
                    style.zIndex && 
                    parseInt(style.zIndex) > 1000 &&
                    style.display !== 'none' &&
                    style.opacity !== '0' &&
                    el.offsetWidth > window.innerWidth * 0.5 &&
                    el.offsetHeight > window.innerHeight * 0.5
                );
            });
            
            for (const overlay of overlays) {
                // Проверяем не является ли это cookie-баннером
                const overlayText = (overlay.innerText || '').toLowerCase();
                if (!overlayText.includes('cookie') && !overlayText.includes('privacy')) {
                    // Пытаемся скрыть overlay
                    overlay.style.display = 'none';
                    overlay.style.visibility = 'hidden';
                    overlay.style.pointerEvents = 'none';
                    handled = true;
                }
            }
            
            // 3. Проверяем скрытые ошибки валидации
            const errorMessages = Array.from(document.querySelectorAll('[role="alert"], .error-message, .validation-error, [class*="error" i], [class*="invalid" i]'));
            for (const err of errorMessages) {
                if (err.offsetWidth > 0 && err.offsetHeight > 0) {
                    const errText = (err.innerText || '').toLowerCase();
                    if (errText.includes('required') || errText.includes('must') || errText.includes('valid')) {
                        // Находим связанное поле и пытаемся исправить
                        const relatedInput = err.querySelector('input') || 
                                           document.querySelector('input:focus') ||
                                           document.querySelector('input[aria-invalid="true"]');
                        if (relatedInput) {
                            relatedInput.value = relatedInput.type === 'email' ? 
                                'test@gmail.com' : 'John';
                            relatedInput.dispatchEvent(new Event('input', { bubbles: true }));
                            handled = true;
                        }
                    }
                }
            }
            
            // 4. Проверяем чекбоксы согласия которые могут блокировать
            const consentCheckboxes = Array.from(document.querySelectorAll('input[type="checkbox"]')).filter(cb => {
                const label = cb.closest('label') || document.querySelector(`label[for="${cb.id}"]`);
                const labelTxt = (label?.innerText || '').toLowerCase();
                return labelTxt.includes('agree') || labelTxt.includes('terms') || labelTxt.includes('privacy');
            });
            
            for (const cb of consentCheckboxes) {
                if (!cb.checked) {
                    cb.checked = true;
                    cb.dispatchEvent(new Event('change', { bubbles: true }));
                    handled = true;
                }
            }
            
            return handled;
        }""")
        
        if blockers_handled:
            log_func("Form blockers: обнаружены и обработаны блокираторы формы через JS")
        
        return blockers_handled
        
    except Exception as e:
        log_func(f"Form blockers: ошибка проверки: {str(e)[:80]}")
        return False


def detect_page_stuck_state(page: Page, log_func) -> dict:
    """
    JS-функция для детекции "зависшего" состояния страницы.
    Возвращает информацию о состоянии и рекомендации.
    
    Returns:
        dict с полями: is_stuck, reason, suggestion
    """
    try:
        stuck_info = page.evaluate("""() => {
            const result = {
                is_stuck: false,
                reason: null,
                suggestion: null,
                details: {}
            };
            
            // 1. Проверяем наличие активных loading индикаторов
            const loadingElements = Array.from(document.querySelectorAll('[class*="loading" i], [class*="spinner" i], [role="progressbar"]'));
            const visibleLoading = loadingElements.some(el => {
                const style = window.getComputedStyle(el);
                return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
            });
            
            if (visibleLoading) {
                result.is_stuck = true;
                result.reason = 'loading_in_progress';
                result.suggestion = 'wait_longer';
                result.details.loading_count = loadingElements.length;
                return result;
            }
            
            // 2. Проверяем есть ли незаполненные обязательные поля при disabled кнопке
            const disabledContinue = Array.from(document.querySelectorAll('button:disabled')).find(btn => {
                const txt = (btn.innerText || '').toLowerCase();
                return ['continue', 'next', 'get started', 'submit'].some(k => txt.includes(k));
            });
            
            if (disabledContinue) {
                const requiredEmpty = Array.from(document.querySelectorAll('input[required]:not([type="hidden"])'))
                    .filter(inp => !inp.value || inp.value.trim() === '');
                
                if (requiredEmpty.length > 0) {
                    result.is_stuck = true;
                    result.reason = 'required_fields_empty';
                    result.suggestion = 'fill_required_fields';
                    result.details.empty_fields = requiredEmpty.length;
                    return result;
                }
            }
            
            // 3. Проверяем наличие blocking iframe (например cookie consent)
            const iframes = Array.from(document.querySelectorAll('iframe'));
            const blockingIframe = iframes.find(iframe => {
                const style = window.getComputedStyle(iframe);
                return (
                    style.position === 'fixed' &&
                    style.zIndex && 
                    parseInt(style.zIndex) > 999 &&
                    iframe.offsetWidth > window.innerWidth * 0.8 &&
                    iframe.offsetHeight > window.innerHeight * 0.8
                );
            });
            
            if (blockingIframe) {
                result.is_stuck = true;
                result.reason = 'blocking_iframe';
                result.suggestion = 'close_iframe_or_wait';
                result.details.iframe_src = blockingIframe.src;
                return result;
            }
            
            // 4. Проверяем наличие JavaScript ошибок в консоли
            if (window.__qwen_console_errors && window.__qwen_console_errors.length > 0) {
                const recentErrors = window.__qwen_console_errors.slice(-5);
                const hasBlockingError = recentErrors.some(err => 
                    err.includes('Uncaught') || 
                    err.includes('TypeError') || 
                    err.includes('ReferenceError')
                );
                
                if (hasBlockingError) {
                    result.is_stuck = true;
                    result.reason = 'javascript_errors';
                    result.suggestion = 'try_click_anyway_or_reload';
                    result.details.errors = recentErrors;
                    return result;
                }
            }
            
            // 5. Проверяем наличие скрытого контента который должен быть виден
            const hiddenContent = document.querySelector('[class*="hidden" i], [aria-hidden="true"]');
            if (hiddenContent) {
                // Проверяем не является ли это ожидаемым состоянием
                const expectedHidden = ['cookie', 'modal', 'popup', 'overlay'];
                const hiddenClass = (hiddenContent.className || '').toLowerCase();
                if (!expectedHidden.some(h => hiddenClass.includes(h))) {
                    // Возможно контент должен быть виден
                    result.details.potentially_hidden = true;
                }
            }
            
            return result;
        }""")
        
        if stuck_info.get('is_stuck'):
            log_func(f"Stuck state: reason={stuck_info['reason']}, suggestion={stuck_info['suggestion']}")
        
        return stuck_info
        
    except Exception as e:
        log_func(f"Stuck state detection: ошибка: {str(e)[:80]}")
        return {"is_stuck": False, "reason": "error", "suggestion": None}

DEBUG_CLASSIFY = True
WORKOUT_ISSUES_STEP_KEY = "stepid=step-workout-issues"
WORKOUT_FREQUENCY_STEP_KEY = "stepid=step-workout-frequency"
DATE_OF_BIRTH_STEP_KEY = "stepid=step-date-of-birth"


PAYWALL_URL_KEYWORDS = [
    "payment",
    "paywall",
    "pricing",
    "subscription",
    "subscribe",
    "plans",
    "premium",
    "membership",
    "checkout",
    "billing",
    "purchase",
    "upgrade",
    "trial",
]

PAYWALL_PLAN_KEYWORDS = [
    "pricing",
    "plans",
    "subscription",
    "subscribe",
    "premium",
    "membership",
    "membership plan",
    "subscription plan",
    "choose your plan",
    "select your plan",
    "upgrade plan",
    "monthly",
    "annual",
    "yearly",
    "weekly",
    "starter",
    "basic",
    "pro",
    "premium plan",
    "best value",
    "most popular",
    "unlimited access",
    "premium access",
    "premium membership",
    "premium plan",
]

PAYWALL_CTA_KEYWORDS = [
    "subscribe",
    "start trial",
    "free trial",
    "try free",
    "continue with plan",
    "choose plan",
    "select plan",
    "view plans",
    "upgrade now",
    "upgrade to premium",
    "get premium",
    "buy now",
    "purchase",
    "checkout",
    "unlock now",
    "get access",
    "continue to checkout",
    "start subscription",
    "start membership",
]

PAYWALL_LIMITATION_KEYWORDS = [
    "limited access",
    "unlock full access",
    "unlock premium",
    "continue reading",
    "members only",
    "subscriber-only",
    "subscription required",
    "access denied",
    "to continue",
    "get unlimited access",
    "full access",
    "buy access",
    "unlock your plan",
    "unlock premium results",
    "upgrade to continue",
    "available on premium",
    "premium only",
    "members only",
]

PAYWALL_TRIAL_KEYWORDS = [
    "trial",
    "free trial",
    "start your trial",
    "start free trial",
    "7-day trial",
    "3-day trial",
    "cancel anytime",
    "billed",
    "per week",
    "per month",
    "per year",
]

PAYWALL_COMPARISON_KEYWORDS = [
    "compare plans",
    "plan comparison",
    "what's included",
    "features included",
    "choose the plan",
    "select the plan",
    "most popular",
    "best deal",
    "best value",
    "save ",
]

PAYWALL_BILLING_KEYWORDS = [
    "billed",
    "billing",
    "renews",
    "auto-renew",
    "recurring",
    "charged",
    "payment method",
    "credit card",
    "card details",
    "secure checkout",
    "order summary",
    "purchase summary",
    "cancel anytime",
    "after trial",
    "today only",
]

PAYWALL_ACCESS_KEYWORDS = [
    "unlock access",
    "unlock premium",
    "full access",
    "premium access",
    "members area",
    "subscriber-only",
    "subscription required",
    "continue with subscription",
    "upgrade to continue",
    "access all content",
]

PAYWALL_NEGATIVE_KEYWORDS = [
    "income",
    "revenue",
    "salary",
    "earnings",
    "profit",
    "make money",
    "how much do you earn",
    "how much can you earn",
    "quiz",
    "survey",
    "question",
    "step",
    "application",
    "apply now",
    "qualify",
    "qualification",
    "lead",
    "lead form",
    "results quiz",
    "calculator",
    "estimate",
    "assessment",
    "booking",
    "appointment",
    "consultation",
    "quote",
    "your budget",
    "your income",
    "monthly income",
    "annual income",
    "desired salary",
]

PAYWALL_CARD_BADGE_KEYWORDS = [
    "most popular",
    "best value",
    "best deal",
    "recommended",
    "limited offer",
    "save ",
    "off",
]

PAYWALL_CHECKOUT_KEYWORDS = [
    "checkout",
    "secure checkout",
    "payment method",
    "card details",
    "order summary",
    "purchase summary",
    "complete purchase",
    "confirm purchase",
    "confirm subscription",
]

PAYWALL_PRICE_PATTERN = re.compile(
    r"(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))"
)

PAYWALL_PRICE_CONTEXT_PATTERN = re.compile(
    r"(?:"
    r"(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))"
    r"[^\n]{0,40}(?:/\s*(?:week|month|year)|per\s+(?:week|month|year)|monthly|yearly|weekly|billed|billing|trial|plan|subscription|membership)"
    r"|"
    r"(?:plan|subscription|membership|premium|trial|billing|billed|checkout|upgrade|subscribe)"
    r"[^\n]{0,40}(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))"
    r")"
)

PAYWALL_PERIODIC_PRICE_PATTERN = re.compile(
    r"(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))\s*(?:/\s*(?:week|month|year)|per\s+(?:week|month|year))"
)

PAYWALL_RANGE_PRICE_PATTERN = re.compile(
    r"\b\d{1,4}\s*(?:-|–|to)\s*\d{1,4}\s*(?:k|m|тыс\.?|тысяч)?\s*(?:[$€£₽]|usd|eur|gbp|rub)?\b"
)

PAYWALL_PLAN_DURATION_PATTERN = re.compile(
    r"\b\d{1,2}\s*[- ]?(?:day|week|month|year)s?\s+plan\b|\b(?:weekly|monthly|yearly|annual)\s+plan\b"
)

PAYWALL_DISCOUNT_PATTERN = re.compile(
    r"\b(?:save|off)\s*\d{1,3}%\b|\b\d{1,3}%\s*off\b"
)

PAYWALL_MULTI_PRICE_CLUSTER_PATTERN = re.compile(
    r"(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))(?:[^\n]{0,24}(?:[$€£₽]\s?\d{1,4}(?:[.,]\d{1,2})?|\d{1,4}(?:[.,]\d{1,2})?\s?(?:usd|eur|gbp|rub|₽|\$|€|£))){1,}"
)


def _keyword_hits(text: str, keywords: list[str]) -> list[str]:
    return [keyword for keyword in keywords if keyword in text]


def _count_plan_option_titles(text: str) -> int:
    option_groups = [
        ["basic", "pro", "premium"],
        ["monthly", "annual", "yearly"],
        ["starter", "pro", "enterprise"],
    ]
    best = 0
    for group in option_groups:
        hits = sum(1 for item in group if item in text)
        best = max(best, hits)
    return best


def _has_multiple_prices(text: str) -> bool:
    prices = PAYWALL_PRICE_PATTERN.findall(text)
    unique_prices = {price.strip() for price in prices if str(price).strip()}
    return len(unique_prices) >= 2


def _has_price_in_paywall_context(text: str) -> bool:
    return bool(PAYWALL_PRICE_CONTEXT_PATTERN.search(text) or PAYWALL_PERIODIC_PRICE_PATTERN.search(text))


def _has_quiz_money_context(text: str) -> bool:
    if not PAYWALL_PRICE_PATTERN.search(text):
        return False
    if PAYWALL_RANGE_PRICE_PATTERN.search(text):
        return True
    negative_hits = _keyword_hits(text, PAYWALL_NEGATIVE_KEYWORDS)
    money_hits = _keyword_hits(text, ["income", "revenue", "salary", "earnings", "profit", "budget"])
    return len(negative_hits) >= 2 or bool(money_hits)


def _count_commercial_price_signals(text: str) -> int:
    score = 0
    if PAYWALL_PERIODIC_PRICE_PATTERN.search(text):
        score += 2
    if PAYWALL_PLAN_DURATION_PATTERN.search(text):
        score += 2
    if PAYWALL_DISCOUNT_PATTERN.search(text):
        score += 2
    if PAYWALL_MULTI_PRICE_CLUSTER_PATTERN.search(text):
        score += 2
    if len(_keyword_hits(text, PAYWALL_CARD_BADGE_KEYWORDS)) >= 1:
        score += 1
    return score


def is_probable_paywall_url(url: str) -> bool:
    try:
        u = (url or "").lower()
        parsed = urlparse(u)
        combined = " ".join([
            parsed.netloc or "",
            parsed.path or "",
            parsed.query or "",
            parsed.fragment or "",
        ])
        strong_url_hits = sum(1 for keyword in PAYWALL_URL_KEYWORDS if keyword in combined)
        return strong_url_hits >= 1
    except:
        return False


def detect_paywall_signals(page: Page) -> tuple[bool, str]:
    try:
        text = page.evaluate("() => (document.body.innerText || '').toLowerCase()") or ""
    except:
        text = ""

    url = (page.url or "").lower()
    url_has_paywall_keyword = is_probable_paywall_url(url)

    plan_hits = _keyword_hits(text, PAYWALL_PLAN_KEYWORDS)
    cta_hits = _keyword_hits(text, PAYWALL_CTA_KEYWORDS)
    limitation_hits = _keyword_hits(text, PAYWALL_LIMITATION_KEYWORDS)
    trial_hits = _keyword_hits(text, PAYWALL_TRIAL_KEYWORDS)
    comparison_hits = _keyword_hits(text, PAYWALL_COMPARISON_KEYWORDS)
    billing_hits = _keyword_hits(text, PAYWALL_BILLING_KEYWORDS)
    access_hits = _keyword_hits(text, PAYWALL_ACCESS_KEYWORDS)
    negative_hits = _keyword_hits(text, PAYWALL_NEGATIVE_KEYWORDS)
    badge_hits = _keyword_hits(text, PAYWALL_CARD_BADGE_KEYWORDS)
    checkout_hits = _keyword_hits(text, PAYWALL_CHECKOUT_KEYWORDS)

    has_price_symbol = any(symbol in text for symbol in ["$", "€", "£", "₽"])
    has_price_amount = bool(PAYWALL_PRICE_PATTERN.search(text))
    has_price = has_price_symbol or has_price_amount
    has_multiple_prices = _has_multiple_prices(text)
    has_price_context = _has_price_in_paywall_context(text)
    has_periodic_price = bool(PAYWALL_PERIODIC_PRICE_PATTERN.search(text))
    has_quiz_money_context = _has_quiz_money_context(text)
    plan_option_count = _count_plan_option_titles(text)
    commercial_price_score = _count_commercial_price_signals(text)
    has_discount_signal = bool(PAYWALL_DISCOUNT_PATTERN.search(text))
    has_plan_duration = bool(PAYWALL_PLAN_DURATION_PATTERN.search(text))
    has_multi_price_cluster = bool(PAYWALL_MULTI_PRICE_CLUSTER_PATTERN.search(text))

    has_plan_offer = len(plan_hits) >= 2 or plan_option_count >= 2
    has_subscription_cta = len(cta_hits) >= 2 or (len(cta_hits) >= 1 and (has_price_context or len(billing_hits) >= 1))
    has_content_gating = len(limitation_hits) >= 1 or len(access_hits) >= 2
    has_trial_offer = len(trial_hits) >= 2 or (len(trial_hits) >= 1 and len(billing_hits) >= 1)
    has_plan_comparison = len(comparison_hits) >= 1 and (plan_option_count >= 2 or has_multiple_prices)
    has_billing_context = len(billing_hits) >= 2 or has_periodic_price
    has_checkout_context = len(checkout_hits) >= 1
    has_pricing_cards = (
        (has_multiple_prices or has_multi_price_cluster)
        and (has_plan_offer or has_plan_duration or len(badge_hits) >= 1)
    )

    strong_signals = 0
    if has_plan_offer and (has_multiple_prices or has_plan_comparison):
        strong_signals += 1
    if has_billing_context and has_price_context:
        strong_signals += 1
    if has_content_gating and (has_subscription_cta or has_trial_offer or has_plan_offer):
        strong_signals += 1
    if has_checkout_context and has_price_context:
        strong_signals += 1
    if has_trial_offer and has_billing_context:
        strong_signals += 1
    if has_pricing_cards and commercial_price_score >= 3:
        strong_signals += 1
    if url_has_paywall_keyword and (has_pricing_cards or has_checkout_context or has_billing_context):
        strong_signals += 1

    weak_signals = 0
    if url_has_paywall_keyword:
        weak_signals += 1
    if has_plan_offer:
        weak_signals += 1
    if has_subscription_cta:
        weak_signals += 1
    if has_content_gating:
        weak_signals += 1
    if has_trial_offer:
        weak_signals += 1
    if has_plan_comparison:
        weak_signals += 1
    if has_billing_context:
        weak_signals += 1
    if has_checkout_context:
        weak_signals += 1
    if has_price_context:
        weak_signals += 1
    if has_pricing_cards:
        weak_signals += 1
    if commercial_price_score >= 2:
        weak_signals += 1

    if has_quiz_money_context and strong_signals < 2 and commercial_price_score < 3:
        return False, "Money-related quiz/leadgen context detected without enough subscription evidence"

    if len(negative_hits) >= 3 and strong_signals < 2 and commercial_price_score < 3:
        return False, "Quiz/leadgen anti-signals outweigh weak pricing indicators"

    if has_price and not (has_price_context or has_billing_context or has_plan_comparison):
        return False, "Standalone prices or money amounts without billing/subscription context"

    if url_has_paywall_keyword and has_pricing_cards and (has_price_context or commercial_price_score >= 3):
        return True, "Payment-like URL combined with pricing-card paywall content"

    if has_pricing_cards and has_price_context and (has_subscription_cta or has_billing_context or has_discount_signal):
        return True, "Pricing cards with commercial price context and subscription/discount signals detected"

    if has_multi_price_cluster and has_plan_duration and (len(badge_hits) >= 1 or has_discount_signal or url_has_paywall_keyword):
        return True, "Multiple plan prices with duration and badge/discount signals detected"

    if url_has_paywall_keyword and strong_signals >= 2:
        return True, "Strong paywall URL combined with multiple independent paywall signals"

    if has_checkout_context and has_billing_context and has_price_context:
        return True, "Checkout flow with billing and price context detected"

    if has_content_gating and has_plan_offer and (has_billing_context or has_trial_offer or has_plan_comparison):
        return True, "Content gating combined with plan selection and billing/trial context"

    if has_plan_offer and has_plan_comparison and has_price_context and (has_billing_context or has_subscription_cta):
        return True, "Multiple plan options with pricing and billing/CTA context detected"

    if has_trial_offer and has_price_context and has_billing_context and (has_content_gating or has_plan_offer or has_subscription_cta):
        return True, "Trial with billing commitment and subscription context detected"

    if strong_signals >= 3 and weak_signals >= 4:
        return True, "Several strong paywall indicators detected together"

    return False, "No reliable paywall signals detected"


def resolve_fill_values(config: dict) -> dict:
    runner_cfg = config.get("runner", {}) if isinstance(config.get("runner"), dict) else {}
    fill = config.get("fill_values", {}) or runner_cfg.get("fill_values", {}) or {}
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


def has_meaningful_progress(start_url: str, end_url: str, start_hash: str, end_hash: str, ui_before: str, ui_after: str) -> bool:
    if (end_url or "") != (start_url or ""):
        return True
    if (end_hash or "") != (start_hash or ""):
        return True
    if (ui_before or "") != (ui_after or "") and (ui_after or "") != "unknown":
        return True
    return False


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
    is_paywall, paywall_reason = detect_paywall_signals(page)
    is_madmuscles_final_url = "madmuscles.com/final/" in u
    if (
        ("coursiv.io" in u and "selling-page" in u)
        or is_paywall
        or is_madmuscles_final_url
    ):
        return debug_return('paywall', paywall_reason)

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

    # If attributes are not enough, check if there is a genuinely fillable visible input AND email keywords.
    if not has_email_input and page.locator(FILLABLE_INPUT_SELECTOR).count() > 0:
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
        "do it", "ready", "begin", "let's", "go", "transformation", "continuar", "siguiente"
    ]
    
    # Get all potential interactive elements
    raw_els = page.locator("[data-testid*='answer' i]:visible, [class*='Card' i]:not([class*='testimonial' i]):not([class*='review' i]):visible, [class*='button' i]:visible, [class*='btn' i]:visible, label:visible, button:visible, a:visible, [role='button']:visible, div[role='button']:visible")
    
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

        txt = safe_inner_text(el, "").lower().strip()
        
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
        'Got it', 'Got it!', "continuar", "siguiente", 'weiter', 'suivant', 'РїСЂРѕРґРѕР»Р¶РёС‚СЊ', 'РїРѕРЅСЏС‚СЊ', 'РїСЂРёРЅСЏС‚СЊ', 'РѕРє', 'РЅР°С‡Р°С‚СЊ'
    ]
    # Restrict to likely interactive elements to avoid picking up headlines/prompts
    button_locator = page.locator(
        "button:visible, [role='button']:visible, a.button:visible, a:visible, "
        "input[type='submit']:visible, input[type='button']:visible"
    )
    
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


def try_click_continue_js(page: Page, log_func) -> bool:
    """
    Универсальный JS fallback для клика по CTA-кнопке продолжения.
    Полезно для кастомных div-кнопок (React/Next), где нет <button>.
    """
    try:
        result = page.evaluate(r"""() => {
            const positive = [
                'continue','next','proceed','submit','start','get started','get my',
                'show results','see results','go on','keep going','continue quiz',
                'continuar','continuar >','weiter','suivant','продолжить'
            ];
            const negative = [
                'cookie','privacy','settings','preferences','manage','share','back','zurück','назад'
            ];

            const isVisible = (el) => {
                if (!el) return false;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden' || s.opacity === '0') return false;
                const r = el.getBoundingClientRect();
                return r.width > 8 && r.height > 8 && r.bottom > 0 && r.right > 0;
            };

            const isDisabled = (el) => {
                const aria = (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                const dis = !!el.disabled || el.hasAttribute('disabled');
                return aria || dis;
            };

            const candidates = Array.from(document.querySelectorAll(
                "button, [role='button'], a, input[type='submit'], input[type='button'], div, span"
            ));

            let best = null;
            let bestScore = -1;
            for (const el of candidates) {
                if (!isVisible(el) || isDisabled(el)) continue;

                const txt = ((el.innerText || el.value || el.getAttribute('aria-label') || '').trim().toLowerCase());
                if (!txt || txt.length > 120) continue;
                if (negative.some(k => txt.includes(k))) continue;
                if (!positive.some(k => txt.includes(k))) continue;

                const st = window.getComputedStyle(el);
                const r = el.getBoundingClientRect();
                let score = 0;

                score += 50;
                if (el.tagName === 'BUTTON') score += 20;
                if (el.getAttribute('role') === 'button') score += 12;
                if (st.cursor === 'pointer') score += 10;
                if (r.top > window.innerHeight * 0.45) score += 10; // CTA чаще внизу экрана
                if (txt === 'continue' || txt === 'next' || txt === 'continuar') score += 8;

                if (score > bestScore) {
                    bestScore = score;
                    best = { el, txt };
                }
            }

            if (!best) return { clicked: false, text: null };

            const el = best.el;
            el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
            ['pointerdown','mousedown','mouseup','pointerup','click'].forEach(evt => {
                try { el.dispatchEvent(new MouseEvent(evt, { bubbles: true, cancelable: true, view: window })); } catch (e) {}
            });
            try { el.click(); } catch (e) {}
            return { clicked: true, text: best.txt || null };
        }""")

        if result and result.get("clicked"):
            log_func(f"JS CTA fallback: клик по кнопке '{(result.get('text') or '').strip()[:50]}'")
            return True
    except Exception as e:
        log_func(f"JS CTA fallback: ошибка: {str(e)[:80]}")
    return False


def try_click_question_option_js(page: Page, log_func, repeat_attempt: int = 0, prefer_skip: bool = False) -> bool:
    """
    Универсальный JS fallback для экранов-вопросов.
    Ищет не CTA, а именно вероятные варианты ответа, включая кастомные кнопки
    без текста (например, иконки/числовые value/карточки со SVG).
    """
    try:
        result = page.evaluate(r"""(params) => {
            const repeatAttempt = params.repeatAttempt || 0;
            const preferSkip = !!params.preferSkip;
            const negative = [
                'cookie','cookies','privacy','settings','preferences','manage','share',
                'view more','show less','read more','back','zurück','назад',
                'continue','next','start','submit','got it','let\'s do it','let’s do it'
            ];
            const skipTexts = [
                'skip this question', 'skip question', 'skip this step', 'skip step',
                'skip for now', 'skip', 'not sure yet', 'decide later'
            ];

            const isVisible = (el) => {
                if (!el) return false;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden' || s.opacity === '0') return false;
                const r = el.getBoundingClientRect();
                return r.width > 18 && r.height > 18 && r.bottom > 0 && r.right > 0;
            };

            const isDisabled = (el) => {
                const aria = (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                const dis = !!el.disabled || el.hasAttribute('disabled');
                return aria || dis;
            };

            const clickable = (el) => {
                const t = (el.tagName || '').toLowerCase();
                const role = (el.getAttribute('role') || '').toLowerCase();
                const s = window.getComputedStyle(el);
                return t === 'button' || t === 'label' || t === 'input' || t === 'a' || role === 'button' || typeof el.onclick === 'function' || s.cursor === 'pointer';
            };

            const getText = (el) => {
                const txt = (el.innerText || '').trim();
                const aria = (el.getAttribute('aria-label') || '').trim();
                const value = (el.getAttribute('value') || '').trim();
                const alt = (el.getAttribute('alt') || '').trim();
                return [txt, aria, value, alt].filter(Boolean).join(' ').trim();
            };

            const candidates = Array.from(document.querySelectorAll(
                "button, [role='button'], label, [data-testid], [class], li, div"
            ));

            const scored = [];
            for (let i = 0; i < candidates.length; i++) {
                const el = candidates[i];
                if (!isVisible(el) || isDisabled(el) || !clickable(el)) continue;

                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                const text = getText(el);
                const full = `${text} ${(el.className || '')} ${(el.getAttribute('data-testid') || '')}`.toLowerCase();
                const isSkip = skipTexts.some(k => full.includes(k));
                if (negative.some(k => full.includes(k)) && !(preferSkip && isSkip)) continue;

                let score = 0;
                const tag = (el.tagName || '').toLowerCase();
                if (preferSkip && isSkip) score += 200;
                if (tag === 'button') score += 25;
                if ((el.getAttribute('role') || '').toLowerCase() === 'button') score += 15;
                if (/(answer|option|choice|item|card|select)/i.test(full)) score += 40;
                if (/^\d+(\.\d+)?$/.test((el.getAttribute('value') || '').trim())) score += 30;
                if (el.querySelector('svg, img, canvas, path')) score += 18;
                if (!text || text.length <= 40) score += 10;
                if (rect.width >= 36 && rect.height >= 36) score += 10;
                if (style.position !== 'fixed' && style.position !== 'sticky') score += 10;
                if (rect.top > 40 && rect.bottom < window.innerHeight - 40) score += 8;
                if (preferSkip && isSkip && rect.top > window.innerHeight * 0.55) score += 25;

                const parent = el.parentElement;
                if (parent) {
                    const siblings = Array.from(parent.children).filter(sib => sib !== el && isVisible(sib));
                    if (siblings.length >= 2) score += 8;
                }

                if (score < 20) continue;
                scored.push({ index: i, score, text: text || (el.getAttribute('value') || '').trim() || '<icon-option>' });
            }

            if (!scored.length) return { clicked: false, text: null, count: 0 };

            scored.sort((a, b) => b.score - a.score || a.index - b.index);
            const unique = [];
            const seen = new Set();
            for (const item of scored) {
                const key = `${item.text}::${item.index}`;
                if (seen.has(key)) continue;
                seen.add(key);
                unique.push(item);
            }

            const pick = unique[Math.min(repeatAttempt, unique.length - 1)];
            const el = candidates[pick.index];
            el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
            ['pointerdown','mousedown','mouseup','pointerup','click'].forEach(evt => {
                try { el.dispatchEvent(new MouseEvent(evt, { bubbles: true, cancelable: true, view: window })); } catch (e) {}
            });
            try { el.click(); } catch (e) {}
            return { clicked: true, text: pick.text || null, count: unique.length };
        }""", {"repeatAttempt": repeat_attempt, "preferSkip": prefer_skip})

        if result and result.get("clicked"):
            log_func(
                f"JS option fallback: клик по варианту '{(result.get('text') or '').strip()[:50]}' "
                f"(кандидатов: {result.get('count', 0)})"
            )
            return True
    except Exception as e:
        log_func(f"JS option fallback: ошибка: {str(e)[:80]}")
    return False


def try_click_skip_question_js(page: Page, log_func) -> bool:
    """
    Универсальный fallback для шагов с явной ссылкой/кнопкой пропуска.
    Нужен для календарей и других нестандартных шагов, где выбор может
    зациклиться, но в UI есть безопасный выход через Skip.
    """
    try:
        result = page.evaluate(r"""() => {
            const skipTexts = [
                'skip this question', 'skip question', 'skip this step', 'skip step',
                'skip for now', 'skip', 'not sure yet', 'decide later'
            ];

            const isVisible = (el) => {
                if (!el) return false;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden' || s.opacity === '0') return false;
                const r = el.getBoundingClientRect();
                return r.width > 10 && r.height > 10 && r.bottom > 0 && r.right > 0;
            };

            const isDisabled = (el) => {
                const aria = (el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                const dis = !!el.disabled || el.hasAttribute('disabled');
                return aria || dis;
            };

            const getText = (el) => {
                return [
                    (el.innerText || '').trim(),
                    (el.getAttribute('aria-label') || '').trim(),
                    (el.getAttribute('title') || '').trim()
                ].filter(Boolean).join(' ').trim();
            };

            const candidates = Array.from(document.querySelectorAll("a, button, [role='button'], label, div"));
            let best = null;
            let bestScore = -1;

            for (const el of candidates) {
                if (!isVisible(el) || isDisabled(el)) continue;
                const txt = getText(el).toLowerCase();
                if (!txt) continue;
                if (!skipTexts.some(k => txt.includes(k))) continue;

                const rect = el.getBoundingClientRect();
                let score = 0;
                if ((el.tagName || '').toLowerCase() === 'a') score += 20;
                if ((el.tagName || '').toLowerCase() === 'button') score += 12;
                if ((el.getAttribute('role') || '').toLowerCase() === 'button') score += 10;
                if (txt.includes('skip this question')) score += 50;
                if (txt.includes('skip question')) score += 40;
                if (txt === 'skip') score += 10;
                if (rect.top > window.innerHeight * 0.55) score += 10;

                if (score > bestScore) {
                    bestScore = score;
                    best = { el, text: txt };
                }
            }

            if (!best) return { clicked: false, text: null };

            best.el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
            ['pointerdown','mousedown','mouseup','pointerup','click'].forEach(evt => {
                try { best.el.dispatchEvent(new MouseEvent(evt, { bubbles: true, cancelable: true, view: window })); } catch (e) {}
            });
            try { best.el.click(); } catch (e) {}
            return { clicked: true, text: best.text || null };
        }""")

        if result and result.get("clicked"):
            log_func(f"JS skip fallback: клик по '{(result.get('text') or '').strip()[:50]}'")
            return True
    except Exception as e:
        log_func(f"JS skip fallback: ошибка: {str(e)[:80]}")
    return False


def is_calendar_question(page: Page) -> bool:
    try:
        return bool(page.evaluate(r"""() => {
            return !!document.querySelector('.react-datepicker-wrapper, .react-datepicker, [class*="datepicker" i]');
        }"""))
    except:
        return False


def has_skip_question_control(page: Page) -> bool:
    try:
        return bool(page.evaluate(r"""() => {
            const skipTexts = ['skip this question', 'skip question', 'skip this step', 'skip step', 'skip for now'];
            const nodes = Array.from(document.querySelectorAll('a, button, [role="button"], label, div, span'));
            return nodes.some(el => {
                const txt = [
                    (el.innerText || '').trim(),
                    (el.getAttribute('aria-label') || '').trim(),
                    (el.getAttribute('title') || '').trim()
                ].filter(Boolean).join(' ').toLowerCase();
                return txt && skipTexts.some(k => txt.includes(k));
            });
        }"""))
    except:
        return False

def wait_for_transition(page: Page, old_url: str, old_hash: str, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        if page.url != old_url or get_screen_hash(page) != old_hash:
            return True
        time.sleep(0.5)
    return False


def detect_url_loop(page: Page, url_history: list, log_func) -> bool:
    """
    Детекция циклических переходов по URL.
    Проверяем последние 6 URL в истории.
    
    Returns True если обнаружен цикл
    """
    if len(url_history) < 6:
        return False
    
    # Получаем базовые URL без query параметров для сравнения
    def get_base_url(url):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        # Игнорируем некоторые параметры которые могут меняться
        exclude_params = ['timestamp', 't', '_', 'rnd', 'rand']
        query_params = parse_qs(parsed.query)
        filtered_query = {k: v for k, v in query_params.items() if k not in exclude_params}
        from urllib.parse import urlencode
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(filtered_query, doseq=True)}#{parsed.fragment}"
    
    # Проверяем последние 6 URL
    recent = [get_base_url(u) for u in url_history[-6:]]
    if len(set(recent)) == 1:
        log_func(f"Detected URL loop: одинаковый URL 6 раз подряд")
        return True
    
    # Проверяем паттерн A -> B -> A
    if len(url_history) >= 2:
        base_current = get_base_url(page.url)
        base_prev = get_base_url(url_history[-2])
        if base_current == base_prev:
            log_func(f"Detected URL loop: возврат на предыдущий URL")
            return True
    
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

        # Проверяем наличие file upload/input[type="file"]
        file_input = page.locator('input[type="file"]').first
        if file_input.count() > 0:
            log_func("Обнаружен file upload - создаем пустой файл и загружаем")
            try:
                # Создаем временный файл с тестовым изображением
                import base64
                # Минимальное PNG изображение (1x1 прозрачный пиксель)
                png_data = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==')
                temp_file = os.path.join(results_dir, 'temp_upload.png')
                with open(temp_file, 'wb') as f:
                    f.write(png_data)
                file_input.set_input_files(temp_file)
                time.sleep(1)
                log_func("Файл загружен, ожидаем перехода...")
                wait_for_transition(page, start_url, start_hash, timeout=5.0)
                return "file_uploaded"
            except Exception as e:
                log_func(f"Ошибка загрузки файла: {str(e)[:80]}")
                # Пробуем просто кликнуть если загрузка не сработала
                pass

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
                btn_txt = " ".join(safe_inner_text(btn, "").split())
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
            # Priority to "Start" buttons only on info-like screens.
            # For question screens this often causes loops when real action is selecting an option first.
            if screen_type == 'info' and cont_btn and any(k in (cont_btn.inner_text() or "").lower() for k in ["start", "get my", "get started", "take the", "offer", "claim", "discount", "spin"]):
                c_txt = " ".join((cont_btn.inner_text() or "").split())
                log_func(f"Найдена кнопка старта: {c_txt}. Нажимаю...")
                
                # Проверяем и обрабатываем блокираторы формы перед кликом
                check_and_handle_form_blockers(page, log_func)

                close_popups(page, log_func)
                pre_start_hash = get_screen_hash(page)
                cont_btn.click(force=True, timeout=1000)
                moved = wait_for_transition(page, start_url, start_hash)
                if not moved:
                    is_paywall, paywall_reason = detect_paywall_signals(page)
                else:
                    is_paywall, paywall_reason = False, ""
                if not moved and is_paywall:
                    log_func(f"Страница похожа на paywall после клика старта: {paywall_reason}")
                    return "stopped at paywall"
                if not moved and get_screen_hash(page) == pre_start_hash:
                    log_func("Клик по старту не изменил экран")
                    # Повторная проверка блокираторов если клик не сработал
                    if check_and_handle_form_blockers(page, log_func):
                        log_func("Повторный клик после обработки блокираторов")
                        pre_start_hash2 = get_screen_hash(page)
                        cont_btn.click(force=True, timeout=1000)
                        moved = wait_for_transition(page, start_url, pre_start_hash2, timeout=5.0)
                        if moved:
                            return "start_button_pressed_retry"
                return "start_button_pressed"

            choice_sel = [
                "[data-testid*='answer' i]:visible", 
                "button:visible", 
                "[class*='Item' i]:visible", 
                "[class*='Card' i]:visible", 
                "[class*='option' i]:visible",
                "[class*='answer' i]:visible",
                "[class*='choice' i]:visible",
                "label:visible"
            ]
            text_targets = []
            # Pass 1: Try all selectors to find a choice WITH text (excluding Nav)
            for s in choice_sel:
                els = page.locator(s)
                els_count = els.count()
                for i in range(els_count):
                    curr = els.nth(i)
                    if is_forbidden_button(curr, log_func): continue
                    try:
                        is_int = curr.evaluate("""el => {
                            const t = (el.tagName || '').toLowerCase();
                            if (t === 'button' || t === 'a' || t === 'label' || t === 'input') return true;
                            if ((el.getAttribute('role') || '').toLowerCase() === 'button') return true;
                            if (typeof el.onclick === 'function') return true;
                            const st = window.getComputedStyle(el);
                            return st.cursor === 'pointer';
                        }""")
                        if not is_int:
                            continue
                    except:
                        pass
                    if is_workout_issues:
                        try:
                            cb = curr.locator("input[type='checkbox']").first
                            if cb.count() > 0 and cb.is_checked():
                                continue
                        except:
                            pass
                    txt = safe_inner_text(curr, "")
                    if txt and not re.search(r'^\d+\s*/\s*\d+$', txt) and len(txt) < 100 and not is_nav_button(txt):
                        text_targets.append(curr)

            if screen_type == 'question' and is_calendar_question(page) and has_skip_question_control(page):
                # Для календарных шагов не пытаемся бесконечно перебирать даты через обычный text target.
                # Если после выбора дата не продвигает экран, ниже сработает fallback через Skip this question.
                target_pool = []
            else:
                target_pool = text_targets
            target = None
            if target_pool:
                # Улучшенный выбор: используем JS для получения уникальных идентификаторов элементов
                # Это помогает отличать элементы с одинаковым текстом но разными данными
                element_hashes = []
                for el in target_pool:
                    try:
                        el_hash = el.evaluate("""el => {
                            // Генерируем уникальный хеш для элемента на основе:
                            // - Позиции в DOM
                            // - Атрибутов (id, class, data-*)
                            // - Соседних элементов
                            const attrs = {
                                id: el.id,
                                class: el.className,
                                role: el.getAttribute('role'),
                                'data-testid': el.getAttribute('data-testid'),
                                'data-index': el.getAttribute('data-index'),
                                type: el.getAttribute('type'),
                                name: el.getAttribute('name')
                            };
                            
                            // Позиция родителя
                            const parent = el.parentElement;
                            const parentClass = parent?.className || '';
                            const siblingIndex = Array.from(parent?.children || []).indexOf(el);
                            
                            return JSON.stringify({
                                attrs,
                                parentClass,
                                siblingIndex,
                                tagName: el.tagName
                            });
                        }""")
                        element_hashes.append(el_hash)
                    except:
                        element_hashes.append(None)
                
                # Если есть повторная попытка, выбираем элемент с уникальным хешем
                if repeat_attempt > 0 and len(target_pool) > 1:
                    # Получаем хеши уже кликнутого элемента
                    clicked_hash = element_hashes[0] if element_hashes else None
                    
                    # Ищем элемент с отличающимся хешем
                    found_different = False
                    for idx, el_hash in enumerate(element_hashes):
                        if el_hash and el_hash != clicked_hash:
                            chosen_idx = idx
                            target = target_pool[chosen_idx]
                            found_different = True
                            log_func(f"Повтор шага: выбран альтернативный элемент #{chosen_idx + 1} (уникальный хеш)")
                            break
                    
                    # Если все элементы одинаковые, пробуем кликнуть по следующему
                    if not found_different:
                        chosen_idx = repeat_attempt % len(target_pool)
                        target = target_pool[chosen_idx]
                        log_func(f"Повтор шага: все элементы одинаковые, пробую #{chosen_idx + 1}")
                else:
                    chosen_idx = 0
                    target = target_pool[chosen_idx]

            if not target:
                if screen_type == 'question':
                    pre_option_hash = get_screen_hash(page)
                    clicked_option = try_click_question_option_js(
                        page,
                        log_func,
                        repeat_attempt=repeat_attempt,
                        prefer_skip=is_calendar_question(page) and has_skip_question_control(page)
                    )
                    if clicked_option:
                        moved = wait_for_transition(page, start_url, pre_option_hash, timeout=3.5)
                        if moved:
                            return "question_choice_clicked_js"

                        curr_cont = find_continue_button(page, log_func)
                        if curr_cont and curr_cont.is_enabled():
                            c_txt = " ".join((curr_cont.inner_text() or "").split())
                            log_func(f"После JS выбора нажимаю Continue: {c_txt}")
                            close_popups(page, log_func)
                            pre_cont_hash = get_screen_hash(page)
                            try:
                                curr_cont.click(force=True, timeout=1500)
                            except:
                                pass
                            moved = wait_for_transition(page, page.url, pre_cont_hash, timeout=8.0)
                            if moved:
                                return "question_choice_clicked_js_continue"
                            log_func("После JS выбора Continue не привел к переходу")

                        if is_calendar_question(page):
                            log_func("Обнаружен календарный шаг без доступного Continue. Пробую Skip")
                            pre_skip_hash = get_screen_hash(page)
                            if try_click_skip_question_js(page, log_func):
                                moved = wait_for_transition(page, page.url, pre_skip_hash, timeout=8.0)
                                if moved:
                                    return "question_skipped_js"

                    if is_calendar_question(page):
                        log_func("Для календарного шага не найден валидный выбор. Пробую Skip")
                        pre_skip_hash = get_screen_hash(page)
                        if try_click_skip_question_js(page, log_func):
                            moved = wait_for_transition(page, page.url, pre_skip_hash, timeout=8.0)
                            if moved:
                                return "question_skipped_js"

                    return "no_choices_found"

                if cont_btn:
                    c_txt = " ".join(safe_inner_text(cont_btn, "").split())
                    log_func(f"Нет вариантов, нажимаю Continue: {c_txt}")
                    close_popups(page, log_func)
                    cont_btn.click(force=True, timeout=1000)
                    moved = wait_for_transition(page, start_url, start_hash)
                    if moved:
                        return "info_continue_pressed"
                    log_func("Continue не изменил экран")
                else:
                    pre_js_hash = get_screen_hash(page)
                    if try_click_continue_js(page, log_func):
                        moved = wait_for_transition(page, start_url, pre_js_hash, timeout=8.0)
                        if moved:
                            return "info_continue_pressed_js"
                return "no_choices_found"
            start_ui = get_ui_step(page)
            # 1. Click choice
            clean_target_text = " ".join(safe_inner_text(target, "").split())
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
                    c_txt = " ".join(safe_inner_text(curr_cont, "").split())
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
                    pre_js_hash = get_screen_hash(page)
                    if try_click_continue_js(page, log_func):
                        moved_js = wait_for_transition(page, page.url, pre_js_hash, timeout=8.0)
                        if moved_js:
                            return "continue_clicked_js"
                time.sleep(0.5)

            if screen_type == 'question' and is_calendar_question(page):
                log_func("Календарный шаг не продвинулся после выбора. Пробую Skip")
                pre_skip_hash = get_screen_hash(page)
                if try_click_skip_question_js(page, log_func):
                    moved = wait_for_transition(page, page.url, pre_skip_hash, timeout=8.0)
                    if moved:
                        return "question_skipped_js"

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
                            txt = safe_inner_text(curr, "")
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
                    c_txt = " ".join(safe_inner_text(curr_cont, "").split())
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

    runner_cfg = config.get("runner", {}) if isinstance(config.get("runner"), dict) else {}
    summary = {"url": url, "slug": slug, "steps_total": 0, "paywall_reached": False, "last_url": "", "path": res_dir, "error": None}
    max_steps = int(runner_cfg.get("max_steps", config.get("max_steps", 80)))
    slow_mo = int(runner_cfg.get("slow_mo_ms", config.get("slow_mo_ms", 100)))
    fill_values = resolve_fill_values(config)
    ai_settings = resolve_ai_fallback_settings(config)
    
    # Список для хранения логов (нужен для сохранения при ошибке)
    log_lines = []

    with open(os.path.join(res_dir, 'log.txt'), 'w', encoding='utf-8') as f:
        def log(m):
            l = f"[{time.strftime('%H:%M:%S')}] {m}\n"
            f.write(l)
            print(l.strip())
            log_lines.append(l.strip())
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=is_headless, slow_mo=slow_mo)
                page = browser.new_context(**p.devices['iPhone 13']).new_page()
                
                # Перехватываем console.error для сбора JS ошибок
                console_errors = []
                def handle_console(msg):
                    if msg.type == "error":
                        console_errors.append(f"[{msg.type}] {msg.text}")
                        # Сохраняем в глобальный объект для последующего извлечения
                        try:
                            page.evaluate("""(text) => {
                                if (!window.__qwen_console_errors) {
                                    window.__qwen_console_errors = [];
                                }
                                window.__qwen_console_errors.push(text);
                                if (window.__qwen_console_errors.length > 100) {
                                    window.__qwen_console_errors.shift();
                                }
                            }""", msg.text)
                        except:
                            pass
                page.on("console", handle_console)
                
                log(f"Переход на {url} (slug: {slug})")
                try:
                    page.goto(url, wait_until='load', timeout=60000)
                except TimeoutError:
                    summary["error"] = "navigation_timeout"
                    log("Ошибка: таймаут открытия страницы")
                    # Сохраняем артефакты при ошибке навигации
                    save_error_artifacts(page, url, "navigation_timeout", log_lines, log)
                    browser.close()
                    return summary

                step = 1
                history_counts = defaultdict(int)
                step_attempts = defaultdict(int)
                ai_attempts = defaultdict(int)
                last_error_artifacts_saved = False
                url_history = [page.url]  # История URL для детекции циклов
                
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
                        ai_budget = int(ai_settings.get("max_attempts_per_screen", 2))
                        if ai_settings.get("enabled") and ai_attempts[curr_id] < ai_budget:
                            ai_attempts[curr_id] += 1
                            log(f"Обнаружено зацикливание на {curr_u}. Пробую AI fallback (attempt {ai_attempts[curr_id]}/{ai_budget})")
                            fallback_result = run_ai_fallback(
                                page=page,
                                config=config,
                                results_dir=res_dir,
                                fill_values=fill_values,
                                screen_type=st,
                                stuck_reason="stuck_loop",
                                repeat_attempt=repeat_attempt,
                                ui_step=ui_before,
                                screen_hash=curr_h,
                                log_lines=log_lines,
                                log_func=log,
                            )
                            log(f"AI fallback result: {fallback_result.get('status')} | reason={fallback_result.get('reason')}")
                            time.sleep(1.5)
                            after_ai_hash = get_screen_hash(page)
                            after_ai_ui = get_ui_step(page)
                            if has_meaningful_progress(curr_u, page.url, curr_h, after_ai_hash, ui_before, after_ai_ui):
                                history_counts[curr_id] = 0
                                continue
                        log(f"Обнаружено зацикливание на {curr_u}. Остановка.")
                        summary["error"] = "stuck_loop"
                        save_error_artifacts(page, url, "stuck_loop", log_lines, log)
                        last_error_artifacts_saved = True
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

                    curr_hash_after_action = get_screen_hash(page)
                    if ai_settings.get("enabled"):
                        stuck_state = detect_page_stuck_state(page, log)
                        no_progress = not has_meaningful_progress(curr_u, page.url, curr_h, curr_hash_after_action, ui_before, ui_after)
                        ai_reason = None
                        if stuck_state.get("is_stuck"):
                            ai_reason = str(stuck_state.get("reason") or "stuck_state")
                        elif no_progress and not any(k in act for k in ["paywall", "checkout", "reached", "stopped"]):
                            ai_reason = "no_progress_after_action"

                        ai_curr_id = f"{curr_u}|{curr_h}|{ui_before}|{st}"
                        ai_budget = int(ai_settings.get("max_attempts_per_screen", 2))
                        if ai_reason and ai_attempts[ai_curr_id] < ai_budget:
                            ai_attempts[ai_curr_id] += 1
                            log(f"AI fallback trigger: reason={ai_reason} | attempt {ai_attempts[ai_curr_id]}/{ai_budget}")
                            fallback_result = run_ai_fallback(
                                page=page,
                                config=config,
                                results_dir=res_dir,
                                fill_values=fill_values,
                                screen_type=st,
                                stuck_reason=ai_reason,
                                repeat_attempt=repeat_attempt,
                                ui_step=ui_before,
                                screen_hash=curr_h,
                                log_lines=log_lines,
                                log_func=log,
                            )
                            log(f"AI fallback result: {fallback_result.get('status')} | reason={fallback_result.get('reason')}")
                            time.sleep(1.5)

                    # Добавляем URL в историю
                    url_history.append(page.url)
                    # Проверяем на циклы
                    if detect_url_loop(page, url_history, log):
                        log(f"Обнаружен циклический переход. Пробуем альтернативное действие.")
                        # Увеличиваем счетчик попыток для текущего шага
                        step_attempts[step_key] = min(step_attempts[step_key] + 2, 5)
                    
                    summary["steps_total"] = step; summary["last_url"] = page.url
                    if st in ['paywall', 'checkout'] or "stopped" in act or "reached" in act:
                        if st in ['paywall', 'checkout'] or "paywall" in act:
                            summary["paywall_reached"] = True
                        break

                    step += 1
                
                # Если была ошибка и артефакты еще не сохранены - сохраняем
                if summary["error"] and not last_error_artifacts_saved:
                    save_error_artifacts(page, url, summary["error"], log_lines, log)
                
                browser.close()
        except Exception as e:
            summary["error"] = f"runner_exception:{str(e)[:180]}"
            log(f"Критическая ошибка раннера: {str(e)[:180]}")
            # Сохраняем артефакты при критической ошибке
            try:
                save_error_artifacts(page, url, summary["error"], log_lines, log)
            except:
                pass
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
