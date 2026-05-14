import re
from loguru import logger


class MessageClassifierMixin:

    @staticmethod
    def is_chat_message(message):
        """判断是否为用户聊天消息"""
        try:
            return (
                isinstance(message, dict)
                and "1" in message
                and isinstance(message["1"], dict)
                and "10" in message["1"]
                and isinstance(message["1"]["10"], dict)
                and "reminderContent" in message["1"]["10"]
            )
        except Exception:
            return False

    @staticmethod
    def is_sync_package(message_data):
        """判断是否为同步包消息"""
        try:
            return (
                isinstance(message_data, dict)
                and "body" in message_data
                and "syncPushPackage" in message_data["body"]
                and "data" in message_data["body"]["syncPushPackage"]
                and len(message_data["body"]["syncPushPackage"]["data"]) > 0
            )
        except Exception:
            return False

    @staticmethod
    def is_typing_status(message):
        """判断是否为用户正在输入状态消息"""
        try:
            return (
                isinstance(message, dict)
                and "1" in message
                and isinstance(message["1"], list)
                and len(message["1"]) > 0
                and isinstance(message["1"][0], dict)
                and "1" in message["1"][0]
                and isinstance(message["1"][0]["1"], str)
                and "@goofish" in message["1"][0]["1"]
            )
        except Exception:
            return False

    @staticmethod
    def is_system_message(message):
        """判断是否为系统消息"""
        try:
            return (
                isinstance(message, dict)
                and "3" in message
                and isinstance(message["3"], dict)
                and "needPush" in message["3"]
                and message["3"]["needPush"] == "false"
            )
        except Exception:
            return False

    @staticmethod
    def is_bracket_system_message(message):
        """检查是否为带中括号的系统消息"""
        try:
            if not message or not isinstance(message, str):
                return False
            clean_message = message.strip()
            if clean_message.startswith('[') and clean_message.endswith(']'):
                logger.debug(f"检测到系统消息: {clean_message}")
                return True
            return False
        except Exception as e:
            logger.error(f"检查系统消息失败: {e}")
            return False

    @staticmethod
    def extract_image_urls(payload):
        """从消息结构中递归提取图片URL。"""
        urls = []

        def walk(node):
            if isinstance(node, dict):
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)
            elif isinstance(node, str):
                text = node.strip()
                if text.startswith(("http://", "https://")):
                    if re.search(r"(\.jpg|\.jpeg|\.png|\.webp|\.gif)(\?|$)", text, re.IGNORECASE) or \
                       re.search(r"(image|img|pic|photo|snapshot)", text, re.IGNORECASE):
                        urls.append(text)

        walk(payload)
        uniq = []
        for u in urls:
            if u not in uniq:
                uniq.append(u)
        return uniq[:4]
