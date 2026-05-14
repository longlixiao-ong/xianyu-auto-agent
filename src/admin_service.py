import json
import os
from pathlib import Path

from dotenv import dotenv_values

from admin_runtime import ADMIN_LOG_BUFFER, ENV_FILE_LOCK, safe_update_env, unix_to_local_text


MODEL_CONFIG_KEYS = [
    "MODEL_BASE_URL",
    "MODEL_NAME",
    "TEXT_MODEL_NAME",
    "VISION_MODEL_NAME",
    "TEXT_MODEL_BASE_URL",
    "VISION_MODEL_BASE_URL",
    "TEXT_API_KEY",
    "VISION_API_KEY",
]

SECRET_CONFIG_KEYS = [
    "COOKIES_STR",
]

RUNTIME_CONFIG_KEYS = [
    "HEARTBEAT_INTERVAL",
    "HEARTBEAT_TIMEOUT",
    "TOKEN_REFRESH_INTERVAL",
    "TOKEN_RETRY_INTERVAL",
    "ITEMS_REFRESH_INTERVAL",
    "MANUAL_MODE_TIMEOUT",
    "MESSAGE_EXPIRE_TIME",
    "TOGGLE_KEYWORDS",
    "SIMULATE_HUMAN_TYPING",
    "LOG_LEVEL",
    "MY_ITEMS_SNAPSHOT_ON_START",
    "MY_ITEMS_SNAPSHOT_PATH",
    "SELLING_ITEMS_SNAPSHOT_ON_START",
    "SELLING_ITEMS_SNAPSHOT_PATH",
    "LEGACY_SELLING_ITEMS_SNAPSHOT_PATH",
    "SELLING_ITEMS_BROWSER_HEADLESS",
    "RISK_CONTROL_RETRY_INTERVAL",
    "TEXT_MODEL_NAME",
    "VISION_MODEL_NAME",
]

RUNTIME_CONFIG_DEFAULTS = {
    "HEARTBEAT_INTERVAL": "15",
    "HEARTBEAT_TIMEOUT": "5",
    "TOKEN_REFRESH_INTERVAL": "3600",
    "TOKEN_RETRY_INTERVAL": "300",
    "ITEMS_REFRESH_INTERVAL": str(5 * 3600),
    "MANUAL_MODE_TIMEOUT": "3600",
    "MESSAGE_EXPIRE_TIME": "300000",
    "TOGGLE_KEYWORDS": "。",
    "SIMULATE_HUMAN_TYPING": "False",
    "LOG_LEVEL": "INFO",
    "MY_ITEMS_SNAPSHOT_ON_START": "False",
    "MY_ITEMS_SNAPSHOT_PATH": os.path.join("data", "dev_my_items.json"),
    "SELLING_ITEMS_SNAPSHOT_ON_START": "False",
    "SELLING_ITEMS_SNAPSHOT_PATH": os.path.join("data", "dev_my_items.json"),
    "LEGACY_SELLING_ITEMS_SNAPSHOT_PATH": os.path.join("data", "my_selling_items.json"),
    "SELLING_ITEMS_BROWSER_HEADLESS": "True",
    "RISK_CONTROL_RETRY_INTERVAL": "300",
    "TEXT_MODEL_NAME": "qwen-max",
    "VISION_MODEL_NAME": "qwen-vl-max",
}

PROMPT_FILES = {
    "classify": "classify_prompt.txt",
    "price": "price_prompt.txt",
    "tech": "tech_prompt.txt",
    "default": "default_prompt.txt",
}

MAX_PROMPT_BYTES = 32 * 1024


class AdminService:
    def __init__(self, bot, live, cards_manager, env_path=".env", prompt_dir="prompts"):
        self.bot = bot
        self.live = live
        self.cards_manager = cards_manager
        self.env_path = Path(env_path)
        self.prompt_dir = Path(prompt_dir)

    def _read_env(self):
        return {k: (v if v is not None else "") for k, v in dotenv_values(self.env_path).items()}

    def _write_env_values(self, values):
        self.env_path.parent.mkdir(parents=True, exist_ok=True)
        changed = safe_update_env(self.env_path, values)
        for key in changed:
            os.environ[key] = str(values[key])
        return changed

    def get_model_config(self):
        values = self._read_env()
        return {
            "MODEL_BASE_URL": values.get("MODEL_BASE_URL", ""),
            "MODEL_NAME": values.get("MODEL_NAME", ""),
            "TEXT_MODEL_NAME": values.get("TEXT_MODEL_NAME", ""),
            "VISION_MODEL_NAME": values.get("VISION_MODEL_NAME", ""),
            "TEXT_MODEL_BASE_URL": values.get("TEXT_MODEL_BASE_URL", ""),
            "VISION_MODEL_BASE_URL": values.get("VISION_MODEL_BASE_URL", ""),
            "TEXT_API_KEY_CONFIGURED": bool(values.get("TEXT_API_KEY")),
            "VISION_API_KEY_CONFIGURED": bool(values.get("VISION_API_KEY")),
            "API_KEY_CONFIGURED": bool(values.get("API_KEY")),
        }

    def get_secret_config(self):
        values = self._read_env()
        return {
            "COOKIES_STR_CONFIGURED": bool(values.get("COOKIES_STR")),
        }

    def update_model_config(self, payload):
        updates = {key: payload[key] for key in MODEL_CONFIG_KEYS if key in payload}
        # 处理密钥字段：留空不更新
        for key_field in ["API_KEY", "TEXT_API_KEY", "VISION_API_KEY"]:
            if key_field in payload and str(payload[key_field]).strip():
                updates[key_field] = str(payload[key_field]).strip()

        changed = self._write_env_values(updates)
        if not changed:
            return {"status": "no_changes", "changed": [], "effects": {}}

        if self.bot:
            switched = self.bot.reload_runtime_config()
        else:
            switched = False
        effects = {key: "hot_applied" if switched else "saved" for key in changed}

        return {
            "status": "hot_applied" if switched else "saved",
            "changed": changed,
            "effects": effects,
            "message": "模型配置已保存" + ("并热更新" if switched else "，重启后生效"),
        }

    def get_runtime_config(self):
        values = self._read_env()
        return {
            key: values.get(key) if values.get(key) not in (None, "") else RUNTIME_CONFIG_DEFAULTS.get(key, "")
            for key in RUNTIME_CONFIG_KEYS
        }

    def update_runtime_config(self, payload):
        updates = {}
        # 整数参数校验
        INT_KEYS = {"HEARTBEAT_INTERVAL", "HEARTBEAT_TIMEOUT", "TOKEN_REFRESH_INTERVAL",
                    "TOKEN_RETRY_INTERVAL", "ITEMS_REFRESH_INTERVAL", "MANUAL_MODE_TIMEOUT",
                    "MESSAGE_EXPIRE_TIME", "RISK_CONTROL_RETRY_INTERVAL"}
        BOOL_KEYS = {"SIMULATE_HUMAN_TYPING", "MY_ITEMS_SNAPSHOT_ON_START",
                     "SELLING_ITEMS_SNAPSHOT_ON_START", "SELLING_ITEMS_BROWSER_HEADLESS"}

        for key in RUNTIME_CONFIG_KEYS:
            if key not in payload or str(payload[key]).strip() == "":
                continue
            val = str(payload[key]).strip()
            if key in INT_KEYS:
                try:
                    int(val)
                except ValueError:
                    raise ValueError(f"{key} 必须是整数，当前值: {val}")
                if int(val) <= 0:
                    raise ValueError(f"{key} 必须为正整数，当前值: {val}")
            elif key in BOOL_KEYS:
                if val.lower() not in ("true", "false"):
                    raise ValueError(f"{key} 必须是 true 或 false，当前值: {val}")
            updates[key] = val

        if not updates:
            return {"status": "no_changes", "changed": [], "effects": {}}

        changed = self._write_env_values(updates)
        if self.live:
            self.live.reload_runtime_settings()
        effects = {key: "hot_applied" for key in changed}
        return {
            "status": "hot_applied" if self.live else "saved",
            "changed": changed,
            "effects": effects,
            "message": "运行参数已保存" + ("并热更新" if self.live else "，重启后生效"),
        }

    def get_all_prompts(self):
        data = {}
        for name, filename in PROMPT_FILES.items():
            path = self.prompt_dir / filename
            data[name] = path.read_text(encoding="utf-8") if path.exists() else ""
        return data

    def update_prompt(self, name, content):
        if name not in PROMPT_FILES:
            raise KeyError(f"unknown prompt: {name}")

        data = content.encode("utf-8")
        if len(data) > MAX_PROMPT_BYTES:
            raise ValueError(f"prompt 内容过长 ({len(data)} 字节，上限 {MAX_PROMPT_BYTES})")

        self.prompt_dir.mkdir(parents=True, exist_ok=True)
        path = self.prompt_dir / PROMPT_FILES[name]
        path.write_text(content, encoding="utf-8")
        self.bot.reload_prompts()
        return {
            "status": "hot_applied",
            "changed": [name],
            "effects": {name: "hot_applied"},
            "message": f"{name} 提示词已保存并热更新",
        }

    def reload_prompts(self):
        self.bot.reload_prompts()
        return {"status": "hot_applied", "message": "提示词已重新加载"}

    def reload_runtime(self):
        self.live.reload_runtime_settings()
        return {"status": "hot_applied", "message": "运行参数已重新加载"}

    def update_cookie_config(self, payload):
        cookie_value = str(payload.get("COOKIES_STR", "")).strip()
        if not cookie_value:
            raise ValueError("COOKIES_STR 不能为空")

        changed = self._write_env_values({"COOKIES_STR": cookie_value})
        self.live.update_cookie_string(cookie_value)
        return {
            "status": "saved_restart_required",
            "changed": changed or ["COOKIES_STR"],
            "effects": {"COOKIES_STR": "saved_restart_required"},
            "message": "Cookie 已写入 .env。请点击“启动客服”立即重连，或重启容器使其完全生效。",
        }

    def refresh_items(self):
        payload = self.live.refresh_selling_items_snapshot()
        snapshot = self.live.get_snapshot_status()
        items = (payload or {}).get("items") or snapshot.get("items") or []
        source = (payload or {}).get("metadata", {}).get("source", snapshot.get("path", "unknown"))
        return {
            "status": "hot_applied",
            "message": f"商品列表已刷新 (来源: {source})",
            "item_count": (payload or {}).get("item_count", snapshot.get("item_count", 0)),
            "snapshot": snapshot,
            "items": items[:20],
        }

    def toggle_manual_mode(self, chat_id):
        mode = self.live.toggle_manual_mode(chat_id)
        return {
            "status": "hot_applied",
            "chat_id": chat_id,
            "mode": mode,
            "message": "已切到人工模式" if mode == "manual" else "已恢复自动回复",
        }

    def get_overview(self):
        if not self.live:
            return {
                "live": {"service_state": "stopped", "service_message": "Bot 未初始化，请配置后重启", "service_enabled": False,
                         "service_started_at": 0, "server_time": 0, "uptime_seconds": 0,
                         "current_token_ready": False, "manual_mode_count": 0, "owned_item_count": 0},
                "models": self.get_model_config(),
                "secrets": self.get_secret_config(),
                "runtime_status": {},
                "snapshot": {"item_count": 0, "items": []},
            }
        runtime_status = self.live.get_runtime_status_file()
        return {
            "live": self.live.get_status_snapshot(),
            "models": self.get_model_config(),
            "secrets": self.get_secret_config(),
            "runtime_status": runtime_status,
            "snapshot": self.live.get_snapshot_status(),
        }

    def get_logs(self, limit=120):
        return {"items": ADMIN_LOG_BUFFER.list_entries(limit=limit)}

    def get_manual_review(self, status="pending"):
        if not self.live:
            return {"items": []}
        return {"items": self.live.context_manager.get_manual_review_items(status=status)}

    def update_manual_review_status(self, review_id, new_status):
        if not self.live:
            return {"success": False}
        return {"success": self.live.context_manager.update_manual_review_status(review_id, new_status)}

    def get_runtime_states(self, limit=50):
        if not self.live:
            return {"items": []}
        return {"items": self.live.context_manager.list_chat_runtime_states(limit=limit)}

    def get_recent_image_observations(self, limit=50):
        if not self.live:
            return {"items": []}
        return {"items": self.live.context_manager.list_recent_image_observations(limit=limit)}

    def start_service(self):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化，请先配置模型和 Cookie"}
        self.live.start_service()
        return {
            "status": "hot_applied",
            "service_state": self.live.get_status_snapshot().get("service_state"),
            "message": "已请求启动客服",
        }

    def stop_service(self):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化"}
        self.live.stop_service()
        return {
            "status": "hot_applied",
            "service_state": self.live.get_status_snapshot().get("service_state"),
            "message": "已停止客服",
        }

    def reload_prompts(self):
        if not self.bot:
            return {"status": "error", "message": "Bot 未初始化"}
        self.bot.reload_prompts()
        return {"status": "hot_applied", "message": "提示词已重新加载"}

    def reload_runtime(self):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化"}
        self.live.reload_runtime_settings()
        return {"status": "hot_applied", "message": "运行参数已重新加载"}

    def update_cookie_config(self, payload):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化"}
        cookie_value = str(payload.get("COOKIES_STR", "")).strip()
        if not cookie_value:
            raise ValueError("COOKIES_STR 不能为空")
        changed = self._write_env_values({"COOKIES_STR": cookie_value})
        self.live.update_cookie_string(cookie_value)
        return {
            "status": "saved",
            "changed": changed or ["COOKIES_STR"],
            "message": "Cookie 已写入 .env，请点击「启动客服」或重启容器使其生效。",
        }

    def refresh_items(self):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化"}
        payload = self.live.refresh_selling_items_snapshot()
        snapshot = self.live.get_snapshot_status()
        items = (payload or {}).get("items") or snapshot.get("items") or []
        source = (payload or {}).get("metadata", {}).get("source", snapshot.get("path", "unknown"))
        return {
            "status": "hot_applied",
            "message": f"商品列表已刷新 (来源: {source})",
            "item_count": (payload or {}).get("item_count", snapshot.get("item_count", 0)),
            "snapshot": snapshot,
            "items": items[:20],
        }

    def toggle_manual_mode(self, chat_id):
        if not self.live:
            return {"status": "error", "message": "Bot 未初始化"}
        mode = self.live.toggle_manual_mode(chat_id)
        return {
            "status": "hot_applied",
            "chat_id": chat_id,
            "mode": mode,
            "message": "已切到人工模式" if mode == "manual" else "已恢复自动回复",
        }

    def get_conversations(self, item_id=None, limit=50, offset=0):
        if not self.live:
            return {"items": []}
        return {"items": self.live.context_manager.list_conversations(item_id=item_id, limit=limit, offset=offset)}

    def get_conversation_detail(self, chat_id, limit=200, offset=0):
        if not self.live:
            return {"messages": []}
        return {"messages": self.live.context_manager.get_conversation_detail(chat_id, limit=limit, offset=offset)}

    def get_delivery_log(self, item_id=None, limit=50):
        return self.cards_manager.list_delivery_log(item_id=item_id, limit=limit)

    def get_delivery_stats(self, days=30):
        return self.cards_manager.get_delivery_stats(days=days)

    def get_refund_stats(self, days=30):
        return self.cards_manager.get_refund_stats(days=days)

    def reset_delivery_job(self, chat_id, item_id):
        return {"success": self.cards_manager.reset_delivery_job(chat_id, item_id)}
