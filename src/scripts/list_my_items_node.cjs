const { chromium } = require('playwright');

const GOOFISH_BASE_URL = 'https://www.goofish.com';
const SELLING_ITEMS_PROFILE_URL = `${GOOFISH_BASE_URL}/personal`;

const COLLECT_SELLING_ITEMS_JS = `
() => {
  const results = [];
  const links = document.querySelectorAll('a:has(img[class*="feeds-image"])');
  links.forEach(a => {
    const href = a.getAttribute('href') || '';
    if (!href) return;
    const allText = Array.from(a.querySelectorAll('*'))
      .flatMap(el => Array.from(el.childNodes)
        .filter(n => n.nodeType === 3)
        .map(n => (n.textContent || '').trim()))
      .filter(t => t && !t.startsWith('¥') && !/^[\\d.,]+$/.test(t));
    const title = (allText[0] || a.getAttribute('title') || '(无标题)').slice(0, 60);
    const priceEl = a.querySelector('[class*="price"]') ||
      Array.from(a.querySelectorAll('*')).find(el => (el.textContent || '').trim().startsWith('¥'));
    const price = priceEl ? priceEl.textContent.trim().slice(0, 20) : '';
    results.push({ href, title, price });
  });
  return results;
}
`;

function cookieStringToPlaywrightCookies(cookieString) {
  return cookieString.split(';').map(part => part.trim()).filter(Boolean).map(pair => {
    const idx = pair.indexOf('=');
    if (idx <= 0) return null;
    return {
      name: pair.slice(0, idx).trim(),
      value: pair.slice(idx + 1).trim(),
      domain: '.goofish.com',
      path: '/',
      httpOnly: false,
      secure: true,
    };
  }).filter(Boolean);
}

function normalizeSellingCards(cards, statusKey, statusLabel) {
  const seen = new Set();
  const items = [];
  for (const card of cards || []) {
    const href = (card && card.href) || '';
    if (!href) continue;
    const fullHref = href.startsWith('http') ? href : `${GOOFISH_BASE_URL}${href}`;
    if (seen.has(fullHref)) continue;
    seen.add(fullHref);
    items.push({
      title: ((card && card.title) || '(无标题)').slice(0, 60),
      price: (card && card.price) || '',
      href: fullHref,
      status_key: statusKey,
      status_label: statusLabel,
    });
  }
  return items;
}

async function collectSellingItemsFromPage(page, section) {
  const seen = new Set();
  const items = [];
  let staleCount = 0;

  for (let round = 0; round < 30; round += 1) {
    const rawCards = await page.evaluate(COLLECT_SELLING_ITEMS_JS);
    const normalized = normalizeSellingCards(rawCards, section.key, section.label);
    const previousCount = items.length;
    for (const item of normalized) {
      if (seen.has(item.href)) continue;
      seen.add(item.href);
      items.push(item);
    }
    if (items.length === previousCount) {
      staleCount += 1;
      if (staleCount >= 2) break;
    } else {
      staleCount = 0;
    }
    await page.mouse.wheel(0, 900);
    await page.waitForTimeout(1500);
  }
  return items;
}

async function clickSectionTab(page, section) {
  for (const selector of section.selectors || []) {
    const locator = page.locator(selector).first();
    if (await locator.count()) {
      try {
        await locator.click({ timeout: 3000 });
        await page.waitForTimeout(1500);
        return;
      } catch (_err) {
      }
    }
  }
  throw new Error(`未找到“${section.label}”标签，无法采集该状态商品。`);
}

async function isLoggedIn(page) {
  const currentUrl = page.url() || '';
  if (currentUrl.includes('login.taobao.com') || currentUrl.includes('login.xianyu')) return false;
  const notLoggedInTexts = ['立即登录', '登录后可以更懂你', '请先登录', '请登录'];
  for (const text of notLoggedInTexts) {
    try {
      const locator = page.getByText(text, { exact: false }).first();
      if (await locator.isVisible({ timeout: 300 })) return false;
    } catch (_err) {
    }
  }
  return true;
}

async function main() {
  const cookieString = process.env.PLAYWRIGHT_COOKIE_STRING || '';
  const headless = process.env.PLAYWRIGHT_HEADLESS !== '0';
  const sections = JSON.parse(process.env.PLAYWRIGHT_SECTIONS_JSON || '[]');
  if (!cookieString) {
    throw new Error('缺少 PLAYWRIGHT_COOKIE_STRING');
  }

  const browser = await chromium.launch({ headless });
  try {
    const context = await browser.newContext();
    await context.addCookies(cookieStringToPlaywrightCookies(cookieString));
    const page = await context.newPage();
    await page.goto(SELLING_ITEMS_PROFILE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await page.waitForTimeout(2000);

    if (!(await isLoggedIn(page))) {
      throw new Error('未登录或 Cookie 无效，无法进入个人中心页。');
    }

    const allItems = [];
    const sectionCounts = {};
    for (const section of sections) {
      await clickSectionTab(page, section);
      const sectionItems = await collectSellingItemsFromPage(page, section);
      sectionCounts[section.key] = sectionItems.length;
      allItems.push(...sectionItems);
    }

    process.stdout.write(JSON.stringify({
      item_count: allItems.length,
      items: allItems,
      section_counts: sectionCounts,
    }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(err => {
  process.stderr.write(String(err && err.stack || err));
  process.exit(1);
});
