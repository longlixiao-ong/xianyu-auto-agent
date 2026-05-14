import importlib
import json
import os
import sys
import types
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ORIGINAL_MODULES = {
    name: sys.modules.get(name)
    for name in (
        "loguru",
        "websockets",
        "dotenv",
        "xianyu_apis",
        "xianyu_agent",
        "context_manager",
        "policy_engine",
        "utils.xianyu_utils",
    )
}


def ensure_stub_module(name, **attrs):
    if name not in sys.modules:
        sys.modules[name] = types.SimpleNamespace(**attrs)


logger_stub = types.SimpleNamespace(
    info=lambda *args, **kwargs: None,
    debug=lambda *args, **kwargs: None,
    warning=lambda *args, **kwargs: None,
    error=lambda *args, **kwargs: None,
)
ensure_stub_module("loguru", logger=logger_stub)
ensure_stub_module("websockets")
ensure_stub_module("dotenv", load_dotenv=lambda *args, **kwargs: None, set_key=lambda *args, **kwargs: None)
ensure_stub_module("xianyu_apis", XianyuApis=object, RiskControlError=Exception)
ensure_stub_module("xianyu_agent", XianyuReplyBot=object)


class ChatContextManagerStub:
    def __init__(self, *args, **kwargs):
        pass


ensure_stub_module("context_manager", ChatContextManager=ChatContextManagerStub)


def _noop_purchase_signal(*args, **kwargs):
    return {"purchase_status": "not_purchased", "purchase_confidence": "low", "reason": "stub"}


ensure_stub_module(
    "policy_engine",
    PURCHASE_STATUS_CONFIRMED="confirmed_purchased",
    PURCHASE_STATUS_NOT="not_purchased",
    PURCHASE_STATUS_SUSPECTED="suspected_purchased",
    REPLY_ACTION_HANDOFF="handoff",
    REPLY_ACTION_NO_REPLY="no_reply",
    detect_purchase_signal=_noop_purchase_signal,
)
ensure_stub_module(
    "utils.xianyu_utils",
    generate_mid=lambda: "mid",
    generate_uuid=lambda: "uuid",
    trans_cookies=lambda cookies: {"unb": "owner-1"},
    generate_device_id=lambda _uid: "device-1",
    decrypt=lambda value: value,
)

from main import XianyuLive

for module_name, original_module in ORIGINAL_MODULES.items():
    if original_module is None:
        sys.modules.pop(module_name, None)
    else:
        sys.modules[module_name] = original_module

importlib.invalidate_caches()


class ItemOwnershipTests(unittest.TestCase):
    def test_extract_owned_item_ids_reads_snapshot_items(self):
        owned = XianyuLive.extract_owned_item_ids(
            {
                "items": [
                    {"item_id": "1001"},
                    {"item_id": 1002},
                    {"item_id": ""},
                    {},
                ]
            }
        )
        self.assertEqual(owned, {"1001", "1002"})

    def test_extract_seller_id_prefers_track_params(self):
        seller_id = XianyuLive.extract_seller_id(
            {
                "trackParams": {"sellerId": "seller-a"},
                "sellerDO": {"id": "seller-b"},
            }
        )
        self.assertEqual(seller_id, "seller-a")

    def test_load_owned_item_ids_falls_back_to_legacy_snapshot(self):
        temp_dir = PROJECT_ROOT / "tests" / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = temp_dir / "ownership-legacy.json"
        try:
            legacy_path.write_text(
                json.dumps({"items": [{"item_id": "2001"}]}, ensure_ascii=False),
                encoding="utf-8",
            )

            live = XianyuLive.__new__(XianyuLive)
            live.selling_items_snapshot_path = str(temp_dir / "missing.json")
            live.legacy_selling_items_snapshot_path = str(legacy_path)

            owned = XianyuLive.load_owned_item_ids(live)
            self.assertEqual(owned, {"2001"})
        finally:
            if legacy_path.exists():
                legacy_path.unlink()

    def test_is_owned_item_accepts_snapshot_match(self):
        live = XianyuLive.__new__(XianyuLive)
        live.owned_item_ids = {"3001"}
        live.myid = "owner-1"

        self.assertTrue(XianyuLive.is_owned_item(live, "3001"))

    def test_load_owned_item_ids_prefers_empty_primary_snapshot_over_stale_legacy(self):
        temp_dir = PROJECT_ROOT / "tests" / "tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        primary_path = temp_dir / "ownership-primary-empty.json"
        legacy_path = temp_dir / "ownership-legacy-nonempty.json"
        try:
            primary_path.write_text(
                json.dumps({"items": []}, ensure_ascii=False),
                encoding="utf-8",
            )
            legacy_path.write_text(
                json.dumps({"items": [{"item_id": "9999"}]}, ensure_ascii=False),
                encoding="utf-8",
            )

            live = XianyuLive.__new__(XianyuLive)
            live.selling_items_snapshot_path = str(primary_path)
            live.legacy_selling_items_snapshot_path = str(legacy_path)

            owned = XianyuLive.load_owned_item_ids(live)
            self.assertEqual(owned, set())
        finally:
            if primary_path.exists():
                primary_path.unlink()
            if legacy_path.exists():
                legacy_path.unlink()

    def test_is_owned_item_accepts_same_seller_id(self):
        live = XianyuLive.__new__(XianyuLive)
        live.owned_item_ids = set()
        live.myid = "owner-1"

        allowed = XianyuLive.is_owned_item(
            live,
            "3002",
            item_info={"trackParams": {"sellerId": "owner-1"}},
        )

        self.assertTrue(allowed)
        self.assertIn("3002", live.owned_item_ids)

    def test_is_owned_item_rejects_other_seller(self):
        live = XianyuLive.__new__(XianyuLive)
        live.owned_item_ids = {"3001"}
        live.myid = "owner-1"

        allowed = XianyuLive.is_owned_item(
            live,
            "4001",
            item_info={"trackParams": {"sellerId": "other-seller"}},
        )

        self.assertFalse(allowed)


if __name__ == "__main__":
    unittest.main()
