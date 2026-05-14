import os
import shutil
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from admin_service import AdminService


class BotStub:
    def __init__(self):
        self.prompt_reloads = 0
        self.runtime_reloads = 0

    def reload_prompts(self):
        self.prompt_reloads += 1

    def reload_runtime_config(self):
        self.runtime_reloads += 1
        return True


class ContextStub:
    def get_manual_review_items(self, status="pending"):
        return [{"chat_id": "c1", "status": status}]

    def list_chat_runtime_states(self, limit=50):
        return [{"chat_id": "c1", "item_id": "i1"}]

    def list_recent_image_observations(self, limit=50):
        return [{"chat_id": "c1", "observation": "obs"}]


class LiveStub:
    def __init__(self):
        self.context_manager = ContextStub()
        self.reload_calls = 0
        self.refresh_calls = 0
        self.toggle_calls = []
        self.cookie_updates = []
        self.service_started = 0
        self.service_stopped = 0

    def reload_runtime_settings(self):
        self.reload_calls += 1

    def refresh_selling_items_snapshot(self):
        self.refresh_calls += 1
        return {"item_count": 3}

    def get_snapshot_status(self):
        return {"item_count": 3, "path": "data/my_items.json"}

    def get_status_snapshot(self):
        return {"account_id": "4186709882"}

    def get_runtime_status_file(self):
        return {"status": "token_ok"}

    def toggle_manual_mode(self, chat_id):
        self.toggle_calls.append(chat_id)
        return "manual"

    def update_cookie_string(self, cookie_string):
        self.cookie_updates.append(cookie_string)

    def start_service(self):
        self.service_started += 1

    def stop_service(self):
        self.service_stopped += 1


class AdminServiceTests(unittest.TestCase):
    def setUp(self):
        base_tmp = Path(__file__).resolve().parent / "tmp" / "admin_service"
        base_tmp.mkdir(parents=True, exist_ok=True)
        self.case_dir = base_tmp / f"case_{uuid.uuid4().hex}"
        self.case_dir.mkdir(parents=True, exist_ok=True)
        self.env_path = self.case_dir / ".env"
        self.prompt_dir = self.case_dir / "prompts"
        self.prompt_dir.mkdir(parents=True, exist_ok=True)
        self.env_path.write_text(
            "MODEL_BASE_URL=https://example.com/v1\n"
            "MODEL_NAME=demo-model\n"
            "TEXT_MODEL_NAME=demo-text\n"
            "VISION_MODEL_NAME=demo-vision\n"
            "API_KEY=top-secret\n"
            "ITEMS_REFRESH_INTERVAL=18000\n",
            encoding="utf-8",
        )
        for name in ("classify_prompt.txt", "price_prompt.txt", "tech_prompt.txt", "default_prompt.txt"):
            (self.prompt_dir / name).write_text(name, encoding="utf-8")
        self.bot = BotStub()
        self.live = LiveStub()
        self.service = AdminService(self.bot, self.live, env_path=self.env_path, prompt_dir=self.prompt_dir)

    def tearDown(self):
        shutil.rmtree(self.case_dir, ignore_errors=True)

    def test_get_model_config_masks_api_key(self):
        config = self.service.get_model_config()
        self.assertEqual(config["MODEL_NAME"], "demo-model")
        self.assertTrue(config["API_KEY_CONFIGURED"])
        self.assertNotIn("API_KEY", config)

    def test_update_model_config_writes_env_and_hot_applies(self):
        result = self.service.update_model_config({"MODEL_NAME": "next-model", "API_KEY": "new-secret"})
        content = self.env_path.read_text(encoding="utf-8")
        self.assertIn("MODEL_NAME='next-model'", content)
        self.assertIn("API_KEY='new-secret'", content)
        self.assertEqual(result["status"], "hot_applied")
        self.assertEqual(self.bot.runtime_reloads, 1)

    def test_update_prompt_saves_and_reload_prompts(self):
        result = self.service.update_prompt("price", "new prompt body")
        self.assertEqual((self.prompt_dir / "price_prompt.txt").read_text(encoding="utf-8"), "new prompt body")
        self.assertEqual(result["status"], "hot_applied")
        self.assertEqual(self.bot.prompt_reloads, 1)

    def test_update_runtime_config_calls_live_reload(self):
        result = self.service.update_runtime_config({"ITEMS_REFRESH_INTERVAL": "600"})
        self.assertEqual(result["status"], "hot_applied")
        self.assertEqual(self.live.reload_calls, 1)

    def test_review_endpoints_proxy_existing_runtime_data(self):
        self.assertEqual(self.service.get_manual_review()["items"][0]["chat_id"], "c1")
        self.assertEqual(self.service.get_runtime_states()["items"][0]["item_id"], "i1")
        self.assertEqual(self.service.get_recent_image_observations()["items"][0]["observation"], "obs")

    def test_update_cookie_config_writes_env_and_updates_live(self):
        result = self.service.update_cookie_config({"COOKIES_STR": "a=b; c=d"})
        content = self.env_path.read_text(encoding="utf-8")
        self.assertIn("COOKIES_STR='a=b; c=d'", content)
        self.assertEqual(self.live.cookie_updates[-1], "a=b; c=d")
        self.assertEqual(result["status"], "saved_restart_required")

    def test_service_start_stop_proxy_to_live(self):
        self.service.start_service()
        self.service.stop_service()
        self.assertEqual(self.live.service_started, 1)
        self.assertEqual(self.live.service_stopped, 1)


if __name__ == "__main__":
    unittest.main()
