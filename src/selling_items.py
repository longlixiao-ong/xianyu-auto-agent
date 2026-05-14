from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from loguru import logger


GOOFISH_BASE_URL = "https://www.goofish.com"
SELLING_ITEMS_PROFILE_URL = f"{GOOFISH_BASE_URL}/personal"
DEFAULT_SCROLL_ROUNDS = 30
DEFAULT_STALE_ROUNDS = 2
DEFAULT_SCROLL_DELTA = 900
DEFAULT_PROFILE_SECTIONS = [
    {"key": "selling", "label": "在售", "selectors": [':text("在售")', 'span:text-is("在售")', 'div[class*="tab"]:has-text("在售")']},
    {"key": "offline", "label": "已下架", "selectors": [':text("已下架")', 'span:text-is("已下架")', 'div[class*="tab"]:has-text("已下架")']},
    {"key": "draft", "label": "草稿", "selectors": [':text("草稿箱")', ':text("草稿")', 'span:text-is("草稿箱")', 'div[class*="tab"]:has-text("草稿")']},
]
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/133.0.0.0 Safari/537.36"
)
DEFAULT_NODE_CANDIDATES = []
DEFAULT_NODE_MODULES_CANDIDATES = []

COLLECT_SELLING_ITEMS_JS = """
() => {
    const results = [];
    const debugCards = [];
    const links = document.querySelectorAll('a:has(img[class*="feeds-image"])');
    links.forEach((a, idx) => {
        const href = a.getAttribute('href') || '';
        if (!href) return;

        // 优先用特定选择器找标题
        const titleSelectors = [
            '[class*="title"]', '[class*="Title"]', '[class*="subject"]',
            '[class*="name"]', '[class*="desc"]', 'h3', 'h4',
        ];
        let titleEl = null;
        for (const sel of titleSelectors) {
            titleEl = a.querySelector(sel);
            if (titleEl && (titleEl.textContent || '').trim().length > 0) break;
            titleEl = null;
        }

        let title;
        if (titleEl) {
            const attrTitle = titleEl.getAttribute('title');
            const text = (titleEl.textContent || '').trim();
            // title 属性通常包含完整标题，textContent 可能被截断
            title = (attrTitle && attrTitle.length > text.length) ? attrTitle.trim() : text;
        } else {
            // 兜底：遍历文本节点，取最长的非价格文本
            const textNodes = [];
            const walker = document.createTreeWalker(a, NodeFilter.SHOW_TEXT);
            while (walker.nextNode()) {
                const t = (walker.currentNode.textContent || '').trim();
                if (t && !t.startsWith('¥') && !/^[\\d.,]+$/.test(t) && t.length > 2) {
                    textNodes.push(t);
                }
            }
            textNodes.sort((x, y) => y.length - x.length);
            title = textNodes[0] || a.getAttribute('title') || '(无标题)';
        }
        title = title.slice(0, 80);

        const priceEl = a.querySelector('[class*="price"]') ||
                        a.querySelector('[class*="Price"]') ||
                        Array.from(a.querySelectorAll('*')).find(el => {
                            const t = el.textContent.trim();
                            return t.startsWith('¥') || t.startsWith('￥');
                        });
        const price = priceEl ? priceEl.textContent.trim().slice(0, 20) : '';

        // 记录前3个卡片的 HTML 结构用于排查
        if (idx < 3) {
            const clone = a.cloneNode(true);
            clone.querySelectorAll('img').forEach(img => { img.src = img.src.slice(0,60) + '...'; });
            debugCards.push({
                href: href.slice(0, 80),
                html: clone.outerHTML.slice(0, 600),
                title_found: title,
                price_found: price,
            });
        }

        results.push({ href, title, price });
    });
    return { results, debugCards };
}
"""


def normalize_selling_cards(cards, base_url=GOOFISH_BASE_URL, status_key=None, status_label=None):
    items = []
    seen_hrefs = set()
    for card in cards or []:
        href = (card or {}).get("href", "")
        if not href:
            continue
        full_href = href if href.startswith("http") else f"{base_url}{href}"
        if full_href in seen_hrefs:
            continue
        seen_hrefs.add(full_href)
        item_id = (card or {}).get("item_id", "")
        if not item_id:
            m = re.search(r"[?&]id=(\d+)", full_href)
            if m:
                item_id = m.group(1)
        if not item_id and not href:
            continue
        item = {
            "title": ((card or {}).get("title") or "(无标题)")[:60],
            "price": (card or {}).get("price", ""),
            "href": full_href,
            "item_id": item_id or None,
        }
        if status_key:
            item["status_key"] = status_key
        if status_label:
            item["status_label"] = status_label
        items.append(item)
    return items


def collect_selling_items_from_page(
    page,
    status_key=None,
    status_label=None,
    max_scroll_rounds=DEFAULT_SCROLL_ROUNDS,
    stale_rounds=DEFAULT_STALE_ROUNDS,
    scroll_delta=DEFAULT_SCROLL_DELTA,
    delay_fn=None,
):
    if delay_fn is None:
        delay_fn = lambda *_args, **_kwargs: time.sleep(1.5)

    seen_hrefs = set()
    items = []
    stale_count = 0

    for round_idx in range(max_scroll_rounds):
        js_result = page.evaluate(COLLECT_SELLING_ITEMS_JS)
        raw_cards = js_result.get("results", []) if isinstance(js_result, dict) else js_result
        debug_cards = js_result.get("debugCards", []) if isinstance(js_result, dict) else []

        # 首次采集时输出卡片 HTML 结构用于排查标题/价格选择器
        if round_idx == 0 and debug_cards:
            logger.warning("[卡片结构诊断] 前3张卡片的原始 HTML:")
            for dc in debug_cards:
                logger.warning(f"  href={dc['href']}")
                logger.warning(f"  title={dc['title_found']}, price={dc['price_found']}")
                logger.warning(f"  html={dc['html'][:500]}")

        normalized = normalize_selling_cards(
            raw_cards,
            status_key=status_key,
            status_label=status_label,
        )

        previous_count = len(items)
        for item in normalized:
            if item["href"] in seen_hrefs:
                continue
            seen_hrefs.add(item["href"])
            items.append(item)

        if len(items) == previous_count:
            stale_count += 1
            if stale_count >= stale_rounds:
                break
        else:
            stale_count = 0

        page.mouse.wheel(0, scroll_delta)
        delay_fn()

    return items


def click_section_tab(page, section, delay_fn=None):
    if delay_fn is None:
        delay_fn = lambda *_args, **_kwargs: time.sleep(1.5)

    for selector in section.get("selectors", []):
        try:
            locator = page.locator(selector).first
            if locator.is_visible(timeout=3000):
                locator.click()
                delay_fn()
                logger.debug(f"成功点击标签: {section['label']} (选择器: {selector})")
                return selector
        except Exception:
            continue
    logger.debug(f"未找到“{section['label']}”标签，跳过该状态采集")
    return None


def _dump_page_structure(page):
    """输出页面关键 HTML 结构用于排查，截断防止日志爆炸。"""
    try:
        html = page.content()
        if not html:
            logger.warning("[页面结构] 页面 HTML 为空")
            return
        headings = []
        for tag in ("h1", "h2", "h3", "h4"):
            elements = page.locator(tag).all()
            for el in elements[:10]:
                text = (el.text_content() or "").strip()
                if text:
                    headings.append(f"  <{tag}>{text[:80]}</{tag}>")
        tabs = page.locator('[class*="tab"], [class*="Tab"], [role="tab"]').all()
        tab_texts = [(t.text_content() or "").strip()[:40] for t in tabs[:10]]
        item_links = len(page.locator('a[href*="item"]').all())
        img_count = len(page.locator("img").all())
        logger.warning(
            f"[页面结构] URL: {page.url}, 标题标签: {len(headings)}, "
            f"Tab 文字: {tab_texts}, 商品链接: {item_links}, 图片: {img_count}"
        )
        if headings:
            logger.warning(f"[页面结构] 标题:\n" + "\n".join(headings[:20]))
    except Exception as e:
        logger.warning(f"[页面结构] 无法提取: {e}")


def collect_items_for_sections(
    page,
    sections=None,
    max_scroll_rounds=DEFAULT_SCROLL_ROUNDS,
    stale_rounds=DEFAULT_STALE_ROUNDS,
    scroll_delta=DEFAULT_SCROLL_DELTA,
    delay_fn=None,
):
    if sections is None:
        sections = DEFAULT_PROFILE_SECTIONS
    if delay_fn is None:
        delay_fn = lambda *_args, **_kwargs: time.sleep(1.5)

    all_items = []
    section_counts = {}

    for section in sections:
        clicked = click_section_tab(page, section, delay_fn=delay_fn)
        if clicked is None:
            section_counts[section["key"]] = 0
            continue
        section_items = collect_selling_items_from_page(
            page,
            status_key=section["key"],
            status_label=section["label"],
            max_scroll_rounds=max_scroll_rounds,
            stale_rounds=stale_rounds,
            scroll_delta=scroll_delta,
            delay_fn=delay_fn,
        )
        section_counts[section["key"]] = len(section_items)
        all_items.extend(section_items)

    if len(all_items) == 0:
        logger.warning("[商品采集] 所有分区均未采集到商品，输出页面结构:")
        _dump_page_structure(page)

    return {
        "item_count": len(all_items),
        "items": all_items,
        "section_counts": section_counts,
    }


def write_selling_items_snapshot(output_path, items, metadata=None):
    payload = {
        "fetched_at": datetime.now().isoformat(),
        "item_count": len(items),
        "items": items,
    }
    if metadata:
        payload["metadata"] = metadata

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def format_selling_items_text(items):
    if not items:
        return "未找到任何商品。"

    lines = [f"共找到 {len(items)} 件商品："]
    for index, item in enumerate(items, 1):
        price_str = f"  价格：{item['price']}" if item.get("price") else ""
        status_str = f"[{item['status_label']}]" if item.get("status_label") else ""
        lines.append(f"{index}. {status_str}【{item['title']}】{price_str}\n   链接：{item['href']}")
    return "\n".join(lines)


def cookie_string_to_playwright_cookies(cookie_string):
    cookies = []
    for pair in cookie_string.split(";"):
        if "=" not in pair:
            continue
        name, value = pair.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": ".goofish.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
            }
        )
    return cookies


def _collect_candidates(node_candidates, node_modules_candidates):
    """从环境变量和 PATH 扩展候选列表。"""
    node_list = list(node_candidates or [])
    modules_list = list(node_modules_candidates or [])

    env_node = os.getenv("NODE_PATH", "").strip()
    if env_node:
        node_list.append(Path(env_node))

    env_modules = os.getenv("NPM_NODE_MODULES", "").strip()
    if env_modules:
        modules_list.append(Path(env_modules))

    which_node = shutil.which("node")
    if which_node:
        node_bin = Path(which_node)
        node_list.append(node_bin)
        modules_list.append(node_bin.parent.parent / "node_modules")

    return node_list, modules_list


def resolve_node_playwright_runtime(node_candidates=None, node_modules_candidates=None):
    node_list, modules_list = _collect_candidates(node_candidates, node_modules_candidates)

    resolved_node = next((Path(path) for path in node_list if Path(path).exists()), None)
    resolved_node_modules = next((Path(path) for path in modules_list if Path(path).exists()), None)

    if not resolved_node or not resolved_node_modules:
        raise RuntimeError(
            "未找到可复用的 Node Playwright 运行时。"
            "请设置 NODE_PATH 和 NPM_NODE_MODULES 环境变量，或确保 node 在 PATH 中。"
        )

    return resolved_node, resolved_node_modules


class SellingItemsBrowserClient:
    def __init__(self, cookie_string, headless=True, user_agent=DEFAULT_USER_AGENT, sections=None):
        self.cookie_string = cookie_string
        self.headless = headless
        self.user_agent = user_agent
        self.sections = list(sections or DEFAULT_PROFILE_SECTIONS)

    def _import_playwright(self):
        from playwright.sync_api import sync_playwright

        return sync_playwright

    def _fetch_with_node_fallback(self):
        script_path = Path(__file__).resolve().parent / "scripts" / "list_my_items_node.cjs"
        node_executable, node_modules_path = resolve_node_playwright_runtime()
        env = os.environ.copy()
        env["NODE_PATH"] = str(node_modules_path)
        env["PLAYWRIGHT_COOKIE_STRING"] = self.cookie_string
        env["PLAYWRIGHT_HEADLESS"] = "1" if self.headless else "0"
        env["PLAYWRIGHT_SECTIONS_JSON"] = json.dumps(self.sections, ensure_ascii=False)
        result = subprocess.run(
            [str(node_executable), str(script_path)],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"Node Playwright fallback 执行失败: {stderr or result.stdout.strip()}")
        return json.loads(result.stdout)

    def _is_logged_in(self, page):
        current_url = page.url or ""
        if "login.taobao.com" in current_url or "login.xianyu" in current_url:
            return False

        not_logged_in_texts = ["立即登录", "登录后可以更懂你", "请先登录", "请登录"]
        for text in not_logged_in_texts:
            try:
                if page.get_by_text(text, exact=False).first.is_visible(timeout=300):
                    return False
            except Exception:
                logger.debug(f"_is_logged_in: 检查文本'{text}'超时或不可见，跳过")

        logged_in_indicators = [
            'img[class*="avatar"]',
            '[class*="avatar"] img',
            '[class*="nickname"]',
            '[class*="user-info"]',
            '[class*="userInfo"]',
            '[class*="profile"] [class*="name"]',
        ]
        for selector in logged_in_indicators:
            try:
                if page.locator(selector).first.is_visible(timeout=500):
                    return True
            except Exception:
                logger.debug(f"_is_logged_in: 检查选择器'{selector}'超时或不可见，跳过")
        return False

    def fetch(self):
        try:
            sync_playwright = self._import_playwright()
        except ModuleNotFoundError:
            return self._fetch_with_node_fallback()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(user_agent=self.user_agent)
            context.add_cookies(cookie_string_to_playwright_cookies(self.cookie_string))
            page = context.new_page()
            page.goto(SELLING_ITEMS_PROFILE_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            if not self._is_logged_in(page):
                raise RuntimeError("未登录或 Cookie 无效，无法进入个人中心在售页。")
            payload = collect_items_for_sections(page, sections=self.sections)
            if payload["item_count"] == 0:
                logger.warning(f"[浏览器采集] 采集到 0 个商品, URL: {page.url}")
            browser.close()
            return payload


def get_selling_items_via_browser(cookie_string, output_path=None, headless=True, sections=None):
    client = SellingItemsBrowserClient(
        cookie_string=cookie_string,
        headless=headless,
        sections=sections,
    )
    payload = client.fetch()
    payload = {
        "fetched_at": datetime.now().isoformat(),
        "item_count": payload["item_count"],
        "items": payload["items"],
        "section_counts": payload["section_counts"],
        "metadata": {
            "source": "seller_profile_browser",
            "profile_url": SELLING_ITEMS_PROFILE_URL,
            "headless": headless,
            "sections": [section["key"] for section in (sections or DEFAULT_PROFILE_SECTIONS)],
        },
    }
    if output_path:
        write_selling_items_snapshot(output_path, payload["items"], metadata={**payload["metadata"], "section_counts": payload["section_counts"]})
    return payload
