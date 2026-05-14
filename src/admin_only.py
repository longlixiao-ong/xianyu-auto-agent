"""启动管理后台 + 客服进程。Cookie 过期时会自动等待，管理面板始终可用。"""

import os
import sys
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
os.chdir(Path(__file__).parent)

from dotenv import load_dotenv
from loguru import logger

if os.path.exists(".env"):
    load_dotenv()

log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
logger.remove()
logger.add(sys.stderr, level=log_level,
           format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

from admin_runtime import ADMIN_LOG_BUFFER
logger.add(ADMIN_LOG_BUFFER.sink, level=log_level, format="{message}")

from xianyu_agent import XianyuReplyBot
from main import XianyuLive
from admin_service import AdminService
from admin_server import start_admin_server

# 由于 Cookie 可能过期，初始化 bot 和 live 时允许
# XianyuApis 正常创建 session（不调用 API）

try:
    bot = XianyuReplyBot()
except Exception as e:
    logger.error(f"Bot 初始化失败: {e}")
    sys.exit(1)

cookies_str = os.getenv("COOKIES_STR", "")
try:
    xianyuLive = XianyuLive(cookies_str, bot=bot)
except Exception as e:
    logger.critical(f"客服进程初始化失败: {e}")
    sys.exit(1)

admin_service = AdminService(bot, xianyuLive, xianyuLive.cards_manager,
                              env_path=".env", prompt_dir="prompts")
admin_port = int(os.getenv("ADMIN_PORT", "18061"))
admin_host = os.getenv("ADMIN_HOST", "127.0.0.1")
start_admin_server(admin_service, host=admin_host, port=admin_port, static_dir="admin_static")
logger.info(f"管理后台已启动: http://127.0.0.1:{admin_port}")

# 启动客服主循环（Cookie 过期时会自动进入等待/重连逻辑，
# 管理后台可在等待期间更新 Cookie 并点击"启动客服"触发重连）
asyncio.run(xianyuLive.main())
