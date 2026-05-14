import re
import time
from loguru import logger


class ManualModeMixin:

    def check_toggle_keywords(self, message):
        message_stripped = message.strip()
        keywords = [k.strip() for k in self.toggle_keywords.replace('，', ',').split(',') if k.strip()]
        return message_stripped in keywords

    def is_manual_mode(self, chat_id):
        if chat_id not in self.manual_mode_conversations:
            return False
        current_time = time.time()
        if chat_id in self.manual_mode_timestamps:
            if current_time - self.manual_mode_timestamps[chat_id] > self.manual_mode_timeout:
                self.exit_manual_mode(chat_id)
                return False
        return True

    def enter_manual_mode(self, chat_id):
        self.manual_mode_conversations.add(chat_id)
        self.manual_mode_timestamps[chat_id] = time.time()

    def exit_manual_mode(self, chat_id):
        self.manual_mode_conversations.discard(chat_id)
        if chat_id in self.manual_mode_timestamps:
            del self.manual_mode_timestamps[chat_id]

    def toggle_manual_mode(self, chat_id):
        if self.is_manual_mode(chat_id):
            self.exit_manual_mode(chat_id)
            return "auto"
        else:
            self.enter_manual_mode(chat_id)
            return "manual"
