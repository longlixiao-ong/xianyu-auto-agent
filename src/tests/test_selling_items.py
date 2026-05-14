import json
import subprocess
import unittest
from pathlib import Path
import sys
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from selling_items import (
    collect_items_for_sections,
    collect_selling_items_from_page,
    format_selling_items_text,
    normalize_selling_cards,
    resolve_node_playwright_runtime,
    write_selling_items_snapshot,
)


class FakeMouse:
    def __init__(self):
        self.wheels = []

    def wheel(self, x, y):
        self.wheels.append((x, y))


class FakePage:
    def __init__(self, batches):
        self._batches = list(batches)
        self._index = 0
        self.mouse = FakeMouse()

    def evaluate(self, _script):
        if self._index >= len(self._batches):
            return self._batches[-1]
        batch = self._batches[self._index]
        self._index += 1
        return batch


class FakeTabLocator:
    def __init__(self, page, selector):
        self.page = page
        self.selector = selector
        self.first = self

    def is_visible(self, timeout=None):
        return self.selector in self.page.visible_selectors

    def click(self):
        self.page.clicked_selectors.append(self.selector)
        target = self.page.selector_to_section.get(self.selector)
        if target is not None:
            self.page.active_section = target


class FakeSectionPage:
    def __init__(self, section_batches, visible_selectors=None, selector_to_section=None):
        self.section_batches = {
            key: list(value) for key, value in section_batches.items()
        }
        self.section_indexes = {key: 0 for key in self.section_batches}
        self.visible_selectors = set(visible_selectors or [])
        self.selector_to_section = dict(selector_to_section or {})
        self.clicked_selectors = []
        self.mouse = FakeMouse()
        self.active_section = None

    def locator(self, selector):
        return FakeTabLocator(self, selector)

    def evaluate(self, _script):
        batches = self.section_batches[self.active_section]
        index = self.section_indexes[self.active_section]
        if index >= len(batches):
            return batches[-1]
        batch = batches[index]
        self.section_indexes[self.active_section] = index + 1
        return batch


class SellingItemsTests(unittest.TestCase):
    def test_normalize_cards_deduplicates_and_expands_links(self):
        cards = [
            {"href": "/item?id=1", "title": "A", "price": "¥1"},
            {"href": "https://www.goofish.com/item?id=1", "title": "A2", "price": "¥2"},
            {"href": "/item?id=2", "title": "", "price": ""},
            {"href": "", "title": "skip", "price": "¥3"},
        ]

        items = normalize_selling_cards(cards)

        self.assertEqual(
            items,
            [
                {
                    "title": "A",
                    "price": "¥1",
                    "href": "https://www.goofish.com/item?id=1",
                },
                {
                    "title": "(无标题)",
                    "price": "",
                    "href": "https://www.goofish.com/item?id=2",
                },
            ],
        )

    def test_normalize_cards_keeps_status_metadata_when_provided(self):
        cards = [{"href": "/item?id=1", "title": "A", "price": "¥1"}]

        items = normalize_selling_cards(cards, status_key="selling", status_label="在售")

        self.assertEqual(
            items,
            [
                {
                    "title": "A",
                    "price": "¥1",
                    "href": "https://www.goofish.com/item?id=1",
                    "status_key": "selling",
                    "status_label": "在售",
                }
            ],
        )

    def test_collect_items_scrolls_until_stale_round_threshold(self):
        page = FakePage(
            [
                [{"href": "/item?id=1", "title": "A", "price": "¥1"}],
                [
                    {"href": "/item?id=1", "title": "A", "price": "¥1"},
                    {"href": "/item?id=2", "title": "B", "price": "¥2"},
                ],
                [
                    {"href": "/item?id=1", "title": "A", "price": "¥1"},
                    {"href": "/item?id=2", "title": "B", "price": "¥2"},
                ],
                [
                    {"href": "/item?id=1", "title": "A", "price": "¥1"},
                    {"href": "/item?id=2", "title": "B", "price": "¥2"},
                ],
            ]
        )

        items = collect_selling_items_from_page(
            page,
            max_scroll_rounds=10,
            stale_rounds=2,
            delay_fn=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(len(items), 2)
        self.assertEqual(page.mouse.wheels, [(0, 900), (0, 900), (0, 900)])

    def test_write_snapshot_persists_payload_shape(self):
        items = [{"title": "A", "price": "¥1", "href": "https://www.goofish.com/item?id=1"}]
        temp_dir = PROJECT_ROOT / "tests" / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        output_path = temp_dir / f"{uuid.uuid4().hex}.json"
        try:
            payload = write_selling_items_snapshot(output_path, items)

            self.assertEqual(payload["item_count"], 1)
            saved = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["items"][0]["title"], "A")
            self.assertIn("fetched_at", saved)
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_collect_items_for_sections_merges_all_statuses(self):
        page = FakeSectionPage(
            section_batches={
                "selling": [
                    [{"href": "/item?id=1", "title": "A", "price": "¥1"}],
                    [{"href": "/item?id=1", "title": "A", "price": "¥1"}],
                ],
                "offline": [
                    [{"href": "/item?id=2", "title": "B", "price": "¥2"}],
                    [{"href": "/item?id=2", "title": "B", "price": "¥2"}],
                ],
                "draft": [
                    [{"href": "/item?id=3", "title": "C", "price": ""}],
                    [{"href": "/item?id=3", "title": "C", "price": ""}],
                ],
            },
            visible_selectors={
                ':text("在售")',
                ':text("已下架")',
                ':text("草稿箱")',
            },
            selector_to_section={
                ':text("在售")': "selling",
                ':text("已下架")': "offline",
                ':text("草稿箱")': "draft",
            },
        )

        payload = collect_items_for_sections(
            page,
            sections=[
                {"key": "selling", "label": "在售", "selectors": [':text("在售")']},
                {"key": "offline", "label": "已下架", "selectors": [':text("已下架")']},
                {"key": "draft", "label": "草稿", "selectors": [':text("草稿箱")']},
            ],
            delay_fn=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(payload["item_count"], 3)
        self.assertEqual(payload["section_counts"], {"selling": 1, "offline": 1, "draft": 1})
        self.assertEqual([item["status_key"] for item in payload["items"]], ["selling", "offline", "draft"])

    def test_format_text_groups_items_by_status(self):
        text = format_selling_items_text(
            [
                {"title": "A", "price": "¥1", "href": "https://www.goofish.com/item?id=1", "status_label": "在售"},
                {"title": "B", "price": "¥2", "href": "https://www.goofish.com/item?id=2", "status_label": "已下架"},
                {"title": "C", "price": "", "href": "https://www.goofish.com/item?id=3", "status_label": "草稿"},
            ]
        )

        self.assertIn("共找到 3 件商品：", text)
        self.assertIn("[在售]", text)
        self.assertIn("[已下架]", text)
        self.assertIn("[草稿]", text)

    def test_cli_script_help_runs_from_repo_root(self):
        script_path = PROJECT_ROOT / "scripts" / "list_my_items.py"
        result = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            cwd=str(PROJECT_ROOT.parent),
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("列出当前闲鱼账号所有商品", result.stdout)

    def test_resolve_node_playwright_runtime_prefers_explicit_paths(self):
        node_path = PROJECT_ROOT / "tests" / "tmp" / "node.exe"
        node_modules_path = PROJECT_ROOT / "tests" / "tmp" / "node_modules"
        node_path.parent.mkdir(parents=True, exist_ok=True)
        node_modules_path.mkdir(parents=True, exist_ok=True)
        node_path.write_text("", encoding="utf-8")

        try:
            resolved_node, resolved_modules = resolve_node_playwright_runtime(
                node_candidates=[node_path],
                node_modules_candidates=[node_modules_path],
            )
            self.assertEqual(resolved_node, node_path)
            self.assertEqual(resolved_modules, node_modules_path)
        finally:
            if node_path.exists():
                node_path.unlink()


if __name__ == "__main__":
    unittest.main()
