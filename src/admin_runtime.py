import threading
import re
from collections import deque
from datetime import datetime


# 脱敏正则
_RE_COOKIE = re.compile(r"(cookie2|cna|t=|_tb_token_|sgcookie|csg|havana_lgc)[^;]{4,}", re.IGNORECASE)
_RE_API_KEY = re.compile(r"(API_KEY|api_key|token)[=:]\s*[^\s,;]{8,}", re.IGNORECASE)
_RE_CARD_FIELDS = re.compile(r"(卡密|密码|password|pass|pwd)[=:]\s*[^\s,;]{4,}", re.IGNORECASE)


def _sanitize_log(text: str) -> str:
    """对日志内容进行脱敏"""
    text = _RE_COOKIE.sub(lambda m: m.group(1) + "***", text)
    text = _RE_API_KEY.sub(lambda m: m.group(1) + "=***", text)
    text = _RE_CARD_FIELDS.sub(lambda m: m.group(1) + "=***", text)
    return text


class AdminLogBuffer:
    """保留最近日志，给本地后台查看摘要。"""

    def __init__(self, maxlen=300):
        self._entries = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def sink(self, message):
        record = message.record
        raw_msg = record["message"]
        entry = {
            "time": record["time"].strftime("%Y-%m-%d %H:%M:%S"),
            "level": record["level"].name,
            "message": _sanitize_log(raw_msg),
            "name": record["name"],
            "function": record["function"],
            "line": record["line"],
        }
        with self._lock:
            self._entries.append(entry)

    def list_entries(self, limit=200):
        with self._lock:
            if limit <= 0:
                return []
            return list(self._entries)[-limit:]


ADMIN_LOG_BUFFER = AdminLogBuffer()

ENV_FILE_LOCK = threading.Lock()


def safe_update_env(env_path, updates):
    """安全更新 .env 文件：读内容→内存修改→原地写回→失败回滚。

    Args:
        env_path: .env 文件路径
        updates: {key: value} 字典，值为 None 时删除该 key

    Returns:
        list: 实际变更的 key 列表

    Raises:
        OSError: 读取或写入失败
    """
    changed = []
    with ENV_FILE_LOCK:
        if not env_path.exists():
            env_path.write_text("", encoding="utf-8")

        with open(env_path, "r", encoding="utf-8") as f:
            content = f.read()

        backup = content
        lines = content.split("\n") if content.strip() else []

        for key, value in updates.items():
            text_value = "" if value is None else str(value)
            # 值含特殊字符（= ; 空格 #）时用单引号包裹
            if any(c in text_value for c in ('=', ';', ' ', '#')):
                text_value = f"'{text_value}'"
            new_line = f"{key}={text_value}"

            replaced = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith(f"{key}=") or stripped == key:
                    lines[i] = new_line
                    replaced = True
                    break

            if not replaced:
                lines.append(new_line)

            changed.append(key)

        new_content = "\n".join(lines)
        if new_content and not new_content.endswith("\n"):
            new_content += "\n"

        try:
            with open(env_path, "w", encoding="utf-8") as f:
                f.write(new_content)
        except OSError:
            try:
                with open(env_path, "w", encoding="utf-8") as f:
                    f.write(backup)
            except Exception:
                pass
            raise

    return changed


def unix_to_local_text(ts):
    if not ts:
        return None
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
