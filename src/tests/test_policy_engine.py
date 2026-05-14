import os
import sys
import tempfile
import unittest
import types

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if "loguru" not in sys.modules:
    logger_stub = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    sys.modules["loguru"] = types.SimpleNamespace(logger=logger_stub)

from context_manager import ChatContextManager
from policy_engine import (
    PURCHASE_STATUS_CONFIRMED,
    PURCHASE_STATUS_NOT,
    PURCHASE_STATUS_SUSPECTED,
    REPLY_ACTION_HANDOFF,
    REPLY_ACTION_NO_REPLY,
    REPLY_ACTION_REPLY,
    detect_purchase_signal,
    heuristic_reply_action,
)


class PolicyEngineTests(unittest.TestCase):
    def test_detect_purchase_signal_from_red_reminder(self):
        result = detect_purchase_signal(message_text="", red_reminder="等待卖家发货")
        self.assertEqual(result["purchase_status"], PURCHASE_STATUS_CONFIRMED)
        self.assertEqual(result["reason"], "system_waiting_seller_ship")

    def test_detect_purchase_signal_from_text(self):
        result = detect_purchase_signal(message_text="我拍了，发货吧")
        self.assertEqual(result["purchase_status"], PURCHASE_STATUS_SUSPECTED)
        self.assertEqual(result["purchase_confidence"], "medium")

    def test_detect_purchase_signal_for_non_purchase(self):
        result = detect_purchase_signal(message_text="这个还能便宜点吗")
        self.assertEqual(result["purchase_status"], PURCHASE_STATUS_NOT)

    def test_heuristic_reply_action_handoffs_purchase(self):
        result = heuristic_reply_action("发货吧", purchase_status=PURCHASE_STATUS_SUSPECTED)
        self.assertEqual(result["action"], REPLY_ACTION_HANDOFF)

    def test_heuristic_reply_action_no_reply_for_ads(self):
        result = heuristic_reply_action("加微合作吗")
        self.assertEqual(result["action"], REPLY_ACTION_NO_REPLY)

    def test_heuristic_reply_action_keeps_image_only_as_reply(self):
        result = heuristic_reply_action("", has_image=True)
        self.assertEqual(result["action"], REPLY_ACTION_REPLY)

    def test_heuristic_reply_action_handoffs_abuse(self):
        abuse_messages = [
            "你妈死了",
            "傻逼吧你",
            "去死吧",
            "nmsl",
            "tmd",
        ]
        for msg in abuse_messages:
            with self.subTest(msg=msg):
                result = heuristic_reply_action(msg)
                self.assertEqual(result["action"], REPLY_ACTION_HANDOFF)
                self.assertEqual(result["reason"], "buyer_abuse")

    def test_heuristic_reply_action_normal_message_not_abuse(self):
        normal_messages = [
            "这个多少钱",
            "发货了吗",
            "能便宜点吗",
        ]
        for msg in normal_messages:
            with self.subTest(msg=msg):
                result = heuristic_reply_action(msg)
                self.assertNotEqual(result["reason"], "buyer_abuse")


class ContextManagerRuntimeTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.ctx = ChatContextManager(db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_upsert_runtime_state(self):
        self.ctx.upsert_chat_runtime_state(
            chat_id="chat-1",
            item_id="item-1",
            purchase_status=PURCHASE_STATUS_CONFIRMED,
            purchase_confidence="high",
            reply_decision=REPLY_ACTION_HANDOFF,
            last_reason="purchase_detected",
            details={"message": "我拍了"},
        )
        state = self.ctx.get_chat_runtime_state("chat-1")
        self.assertEqual(state["purchase_status"], PURCHASE_STATUS_CONFIRMED)
        self.assertEqual(state["reply_decision"], REPLY_ACTION_HANDOFF)
        self.assertEqual(state["details"]["message"], "我拍了")

    def test_manual_review_queue_deduplicates_pending_reason(self):
        self.ctx.enqueue_manual_review("chat-1", "item-1", "purchase_detected", {"a": 1})
        self.ctx.enqueue_manual_review("chat-1", "item-1", "purchase_detected", {"a": 2})
        items = self.ctx.get_manual_review_items()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["details"]["a"], 2)


if __name__ == "__main__":
    unittest.main()
