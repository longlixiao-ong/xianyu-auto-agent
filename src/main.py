import base64
import json
import asyncio
import time
import os
import websockets
from loguru import logger
from dotenv import load_dotenv
from xianyu_apis import XianyuApis, RiskControlError, CookieInvalidError
import sys
import random
import threading
from pathlib import Path


from utils.xianyu_utils import generate_mid, generate_uuid, trans_cookies, generate_device_id, decrypt, account_id_from_cookies, REG_APP_KEY
from xianyu_agent import XianyuReplyBot
from context_manager import ChatContextManager
from policy_engine import (
    PURCHASE_STATUS_CONFIRMED,
    PURCHASE_STATUS_NOT,
    PURCHASE_STATUS_SUSPECTED,
    REPLY_ACTION_HANDOFF,
    REPLY_ACTION_NO_REPLY,
    detect_purchase_signal,
)
from admin_runtime import ADMIN_LOG_BUFFER, ENV_FILE_LOCK, safe_update_env, unix_to_local_text
from admin_service import AdminService, RUNTIME_CONFIG_DEFAULTS
from admin_server import start_admin_server
from message_classifier import MessageClassifierMixin
from item_ownership import ItemOwnershipMixin
from manual_mode import ManualModeMixin
from cards_manager import CardsManager


class XianyuLive(MessageClassifierMixin, ItemOwnershipMixin, ManualModeMixin):
    def __init__(self, cookies_str, bot=None):
        self.xianyu = XianyuApis()
        self.base_url = 'wss://wss-goofish.dingtalk.com/'
        self.cookies_str = cookies_str
        self.cookies = trans_cookies(cookies_str)
        self.myid = account_id_from_cookies(self.cookies)
        self.device_id = generate_device_id(self.myid)
        self.xianyu.session.cookies.update(self.cookies)  # 直接使用 session.cookies.update
        self.context_manager = ChatContextManager()
        self.cards_manager = CardsManager()
        self.bot = bot
        self.loop = None
        self._state_lock = threading.Lock()
        self._owned_items_lock = threading.Lock()
        self.retry_signal = None
        self.service_enabled = True
        self.service_state = "starting"
        self.service_message = "初始化中"
        
        # 心跳相关配置
        self.heartbeat_interval = int(os.getenv("HEARTBEAT_INTERVAL", RUNTIME_CONFIG_DEFAULTS["HEARTBEAT_INTERVAL"]))
        self.heartbeat_timeout = int(os.getenv("HEARTBEAT_TIMEOUT", RUNTIME_CONFIG_DEFAULTS["HEARTBEAT_TIMEOUT"]))
        self.last_heartbeat_time = 0
        self.last_heartbeat_response = 0
        self.heartbeat_task = None
        self.ws = None
        
        # Token刷新相关配置
        self.token_refresh_interval = int(os.getenv("TOKEN_REFRESH_INTERVAL", RUNTIME_CONFIG_DEFAULTS["TOKEN_REFRESH_INTERVAL"]))
        self.token_retry_interval = int(os.getenv("TOKEN_RETRY_INTERVAL", RUNTIME_CONFIG_DEFAULTS["TOKEN_RETRY_INTERVAL"]))
        self.last_token_refresh_time = 0
        self.current_token = None
        self.token_refresh_task = None
        self.items_refresh_interval = int(os.getenv("ITEMS_REFRESH_INTERVAL", RUNTIME_CONFIG_DEFAULTS["ITEMS_REFRESH_INTERVAL"]))
        self.items_refresh_task = None
        self.connection_restart_flag = False  # 连接重启标志
        
        # 人工接管相关配置
        self.manual_mode_conversations = set()  # 存储处于人工接管模式的会话ID
        self.manual_mode_timeout = int(os.getenv("MANUAL_MODE_TIMEOUT", RUNTIME_CONFIG_DEFAULTS["MANUAL_MODE_TIMEOUT"]))
        self.manual_mode_timestamps = {}  # 记录进入人工模式的时间
        
        # 消息过期时间配置
        self.message_expire_time = int(os.getenv("MESSAGE_EXPIRE_TIME", RUNTIME_CONFIG_DEFAULTS["MESSAGE_EXPIRE_TIME"]))
        self.selling_items_snapshot_on_start = os.getenv(
            "MY_ITEMS_SNAPSHOT_ON_START",
            os.getenv("SELLING_ITEMS_SNAPSHOT_ON_START", RUNTIME_CONFIG_DEFAULTS["MY_ITEMS_SNAPSHOT_ON_START"]),
        ).lower() == "true"
        self.selling_items_snapshot_path = os.getenv(
            "MY_ITEMS_SNAPSHOT_PATH",
            os.getenv("SELLING_ITEMS_SNAPSHOT_PATH", RUNTIME_CONFIG_DEFAULTS["MY_ITEMS_SNAPSHOT_PATH"]),
        )
        self.legacy_selling_items_snapshot_path = os.getenv(
            "LEGACY_SELLING_ITEMS_SNAPSHOT_PATH",
            RUNTIME_CONFIG_DEFAULTS["LEGACY_SELLING_ITEMS_SNAPSHOT_PATH"],
        )
        self.owned_item_ids = self.load_owned_item_ids()
        
        # 人工接管关键词，从环境变量读取
        self.toggle_keywords = os.getenv("TOGGLE_KEYWORDS", RUNTIME_CONFIG_DEFAULTS["TOGGLE_KEYWORDS"])
        
        # 模拟人工输入配置
        self.simulate_human_typing = os.getenv("SIMULATE_HUMAN_TYPING", RUNTIME_CONFIG_DEFAULTS["SIMULATE_HUMAN_TYPING"]).lower() == "true"
        self._service_started_at = 0.0
        self._update_service_state("starting", "初始化中")

    def _update_service_state(self, state, message=None):
        with self._state_lock:
            self.service_state = state
            if message is not None:
                self.service_message = message

    def _set_retry_signal(self):
        if self.retry_signal:
            self.retry_signal.set()

    def _close_active_ws(self):
        if self.ws and not self.ws.closed:
            result = self.ws.close()
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)

    def update_cookie_string(self, cookie_string):
        self.cookies_str = cookie_string
        self.cookies = trans_cookies(cookie_string)
        self.xianyu.session.cookies.clear()
        self.xianyu.session.cookies.update(self.cookies)
        self.myid = account_id_from_cookies(self.cookies)
        self.device_id = generate_device_id(self.myid)
        self._update_service_state("stopped", "Cookie 已更新，等待重新连接")
        if self.loop:
            self.loop.call_soon_threadsafe(self._set_retry_signal)

    def start_service(self):
        self.service_enabled = True
        self._update_service_state("starting", "已请求启动客服")
        if self.loop:
            self.loop.call_soon_threadsafe(self._set_retry_signal)
            self.loop.call_soon_threadsafe(self._close_active_ws)

    def stop_service(self):
        self.service_enabled = False
        self._service_started_at = 0.0
        self._update_service_state("stopped", "客服已停止")
        if self.loop:
            self.loop.call_soon_threadsafe(self._set_retry_signal)
            self.loop.call_soon_threadsafe(self._close_active_ws)

    @staticmethod
    def merge_purchase_status(current_status, new_status):
        priority = {
            PURCHASE_STATUS_NOT: 0,
            PURCHASE_STATUS_SUSPECTED: 1,
            PURCHASE_STATUS_CONFIRMED: 2,
        }
        return new_status if priority.get(new_status, 0) >= priority.get(current_status, 0) else current_status

    def update_purchase_runtime_state(self, chat_id, item_id, purchase_signal, reply_decision=None, details=None):
        current = self.context_manager.get_chat_runtime_state(chat_id) or {}
        merged_status = self.merge_purchase_status(
            current.get("purchase_status", PURCHASE_STATUS_NOT),
            purchase_signal["purchase_status"],
        )
        confidence = purchase_signal.get("purchase_confidence", current.get("purchase_confidence", "low"))
        reason = purchase_signal.get("reason", current.get("last_reason"))
        merged_details = dict(current.get("details", {}))
        if details:
            merged_details.update(details)
        self.context_manager.upsert_chat_runtime_state(
            chat_id=chat_id,
            item_id=item_id,
            purchase_status=merged_status,
            purchase_confidence=confidence,
            reply_decision=reply_decision or current.get("reply_decision"),
            last_reason=reason,
            details=merged_details,
        )
        return merged_status

    def enqueue_handoff(self, chat_id, item_id, reason, details=None):
        self.context_manager.enqueue_manual_review(
            chat_id=chat_id,
            item_id=item_id,
            reason=reason,
            details=details or {},
        )

    async def refresh_token(self):
        """刷新token"""
        try:
            logger.info("开始刷新token...")
            
            # 获取新token（Cookie 失效时 get_token 抛出 CookieInvalidError）
            token_result = self.xianyu.get_token(self.device_id)
            if 'data' in token_result and 'accessToken' in token_result['data']:
                new_token = token_result['data']['accessToken']
                self.current_token = new_token
                self.last_token_refresh_time = time.time()
                logger.info("Token刷新成功")
                return new_token
            else:
                logger.error(f"Token刷新失败: {token_result}")
                return None
        except CookieInvalidError:
            raise
        except RiskControlError:
            raise
        except Exception as e:
            if "风控拦截" in str(e):
                raise RiskControlError(str(e))
            logger.error(f"Token刷新异常: {str(e)}")
            return None

    async def token_refresh_loop(self):
        """Token刷新循环"""
        while True:
            try:
                current_time = time.time()
                
                # 检查是否需要刷新token
                if current_time - self.last_token_refresh_time >= self.token_refresh_interval:
                    logger.info("Token即将过期，准备刷新...")
                    
                    new_token = await self.refresh_token()
                    if new_token:
                        logger.info("Token刷新成功，准备重新建立连接...")
                        # 设置连接重启标志
                        self.connection_restart_flag = True
                        # 关闭当前WebSocket连接，触发重连
                        if self.ws:
                            await self.ws.close()
                        break
                    else:
                        logger.error(f"Token刷新失败，将在{self.token_retry_interval // 60}分钟后重试")
                        await asyncio.sleep(self.token_retry_interval)  # 使用配置的重试间隔
                        continue
                
                # 每分钟检查一次
                await asyncio.sleep(60)

            except CookieInvalidError as e:
                logger.error(f"Cookie 无效，Token 刷新中止: {e}")
                await asyncio.sleep(self.token_retry_interval)
            except RiskControlError as e:
                logger.error(f"风控拦截，Token 刷新中止: {e}")
                await asyncio.sleep(self.token_retry_interval)
            except Exception as e:
                logger.error(f"Token刷新循环出错: {e}")
                await asyncio.sleep(60)

    async def items_refresh_loop(self):
        """定时刷新商品列表快照。"""
        while True:
            try:
                await asyncio.sleep(self.items_refresh_interval)
                logger.info(f"定时刷新商品列表快照，周期 {self.items_refresh_interval} 秒")
                self.refresh_selling_items_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"商品列表定时刷新出错: {e}")
                await asyncio.sleep(60)

    def reload_runtime_settings(self):
        """后台写回 .env 后，立刻同步内存态。"""
        db = RUNTIME_CONFIG_DEFAULTS
        old_items_refresh_interval = self.items_refresh_interval
        self.heartbeat_interval = int(os.getenv("HEARTBEAT_INTERVAL", db["HEARTBEAT_INTERVAL"]))
        self.heartbeat_timeout = int(os.getenv("HEARTBEAT_TIMEOUT", db["HEARTBEAT_TIMEOUT"]))
        self.token_refresh_interval = int(os.getenv("TOKEN_REFRESH_INTERVAL", db["TOKEN_REFRESH_INTERVAL"]))
        self.token_retry_interval = int(os.getenv("TOKEN_RETRY_INTERVAL", db["TOKEN_RETRY_INTERVAL"]))
        self.items_refresh_interval = int(os.getenv("ITEMS_REFRESH_INTERVAL", db["ITEMS_REFRESH_INTERVAL"]))
        self.manual_mode_timeout = int(os.getenv("MANUAL_MODE_TIMEOUT", db["MANUAL_MODE_TIMEOUT"]))
        self.message_expire_time = int(os.getenv("MESSAGE_EXPIRE_TIME", db["MESSAGE_EXPIRE_TIME"]))
        self.toggle_keywords = os.getenv("TOGGLE_KEYWORDS", db["TOGGLE_KEYWORDS"])
        self.simulate_human_typing = os.getenv("SIMULATE_HUMAN_TYPING", db["SIMULATE_HUMAN_TYPING"]).lower() == "true"
        self.selling_items_snapshot_on_start = os.getenv(
            "MY_ITEMS_SNAPSHOT_ON_START",
            os.getenv("SELLING_ITEMS_SNAPSHOT_ON_START", db["MY_ITEMS_SNAPSHOT_ON_START"]),
        ).lower() == "true"
        self.selling_items_snapshot_path = os.getenv(
            "MY_ITEMS_SNAPSHOT_PATH",
            os.getenv("SELLING_ITEMS_SNAPSHOT_PATH", db["MY_ITEMS_SNAPSHOT_PATH"]),
        )
        self.legacy_selling_items_snapshot_path = os.getenv(
            "LEGACY_SELLING_ITEMS_SNAPSHOT_PATH",
            db["LEGACY_SELLING_ITEMS_SNAPSHOT_PATH"],
        )
        self.xianyu.runtime_status_path = os.getenv("RUNTIME_STATUS_PATH", os.path.join("data", "runtime_status.json"))
        self.xianyu.risk_control_retry_interval = int(os.getenv("RISK_CONTROL_RETRY_INTERVAL", db["RISK_CONTROL_RETRY_INTERVAL"]))
        configure_logging(os.getenv("LOG_LEVEL", db["LOG_LEVEL"]).upper())

        if self.loop and self.items_refresh_task and old_items_refresh_interval != self.items_refresh_interval:
            self.loop.call_soon_threadsafe(self._restart_items_refresh_task)

    def _restart_items_refresh_task(self):
        if self.items_refresh_task:
            self.items_refresh_task.cancel()
        self.items_refresh_task = asyncio.create_task(self.items_refresh_loop())

    async def send_msg(self, ws, cid, toid, text):
        text = {
            "contentType": 1,
            "text": {
                "text": text
            }
        }
        text_base64 = str(base64.b64encode(json.dumps(text).encode('utf-8')), 'utf-8')
        msg = {
            "lwp": "/r/MessageSend/sendByReceiverScope",
            "headers": {
                "mid": generate_mid()
            },
            "body": [
                {
                    "uuid": generate_uuid(),
                    "cid": f"{cid}@goofish",
                    "conversationType": 1,
                    "content": {
                        "contentType": 101,
                        "custom": {
                            "type": 1,
                            "data": text_base64
                        }
                    },
                    "redPointPolicy": 0,
                    "extension": {
                        "extJson": "{}"
                    },
                    "ctx": {
                        "appVersion": "1.0",
                        "platform": "web"
                    },
                    "mtags": {},
                    "msgReadStatusSetting": 1
                },
                {
                    "actualReceivers": [
                        f"{toid}@goofish",
                        f"{self.myid}@goofish"
                    ]
                }
            ]
        }
        await ws.send(json.dumps(msg))

    async def init(self, ws):
        # 如果没有token或者token过期，获取新token
        if not self.current_token or (time.time() - self.last_token_refresh_time) >= self.token_refresh_interval:
            delay = random.uniform(3, 15)
            logger.info(f"获取初始token前随机等待 {delay:.1f} 秒...")
            await asyncio.sleep(delay)
            await self.refresh_token()
        
        if not self.current_token:
            logger.error("无法获取有效token，初始化失败")
            raise Exception("Token获取失败")

        if self.selling_items_snapshot_on_start:
            self.refresh_selling_items_snapshot()
            
        msg = {
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": REG_APP_KEY,
                "token": self.current_token,
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5",
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "sync": "0,0;0;0;",
                "did": self.device_id,
                "mid": generate_mid()
            }
        }
        await ws.send(json.dumps(msg))
        # 等待一段时间，确保连接注册完成
        await asyncio.sleep(1)
        msg = {"lwp": "/r/SyncStatus/ackDiff", "headers": {"mid": "5701741704675979 0"}, "body": [
            {"pipeline": "sync", "tooLong2Tag": "PNM,1", "channel": "sync", "topic": "sync", "highPts": 0,
             "pts": int(time.time() * 1000) * 1000, "seq": 0, "timestamp": int(time.time() * 1000)}]}
        await ws.send(json.dumps(msg))
        logger.info('连接注册完成')

    async def _auto_deliver(self, item_id, chat_id, user_url=""):
        """自动发货：检测虚拟商品并发送卡密。"""
        # 使用 delivery_jobs 表保证发货任务唯一性
        if not self.cards_manager.begin_delivery_job(chat_id, item_id):
            logger.info(f"跳过重复发货任务 chat={chat_id} item={item_id}")
            return

        try:
            claimed = self.cards_manager.claim_one(item_id, chat_id)
        except Exception as e:
            logger.error(f"自动发货取卡异常 (item={item_id}): {e}")
            self.cards_manager.update_delivery_job(chat_id, item_id, "failed", error=str(e))
            self.enqueue_handoff(
                chat_id=chat_id,
                item_id=item_id,
                reason="delivery_claim_error",
                details={"error": str(e), "user_url": user_url},
            )
            return

        if not claimed:
            try:
                await self.send_msg(
                    self.ws, chat_id, chat_id,
                    "您好，您的订单已收到，正在为您处理，请稍等片刻～"
                )
            except Exception as se:
                logger.error(f"发送库存不足通知失败: {se}")
            self.cards_manager.update_delivery_job(chat_id, item_id, "failed", error="stock_out")
            self.enqueue_handoff(
                chat_id=chat_id,
                item_id=item_id,
                reason="stock_out",
                details={"user_url": user_url},
            )
            logger.warning(f"自动发货库存不足 item={item_id} chat={chat_id}")
            return

        mode = claimed.get("mode", "stock")
        try:
            if mode == "fixed":
                delivery_text = claimed.get("content", "")
            else:
                fields = claimed.get("fields", {})
                parts = [f"{k}: {v}" for k, v in fields.items()]
                delivery_text = "\n".join(parts)

            if not delivery_text.strip():
                logger.warning(f"自动发货内容为空, item={item_id}")
                if mode == "stock" and claimed.get("id"):
                    self.cards_manager.mark_delivery_status(claimed["id"], False)
                elif mode == "fixed":
                    self.cards_manager.record_fixed_delivery(item_id, chat_id, False)
                self.cards_manager.update_delivery_job(chat_id, item_id, "failed", error="delivery_content_empty")
                self.enqueue_handoff(
                    chat_id=chat_id,
                    item_id=item_id,
                    reason="delivery_content_empty",
                    details={"mode": mode, "user_url": user_url},
                )
                return

            logger.info(
                f"[自动发货] item={item_id} chat={chat_id} "
                f"mode={mode} content_len={len(delivery_text)}"
            )
            await self.send_msg(self.ws, chat_id, chat_id, delivery_text)
            if mode == "stock" and claimed.get("id"):
                self.cards_manager.mark_delivery_status(claimed["id"], True)
                self.cards_manager.update_delivery_job(chat_id, item_id, "success", card_id=claimed["id"])
            elif mode == "fixed":
                self.cards_manager.record_fixed_delivery(item_id, chat_id, True)
                self.cards_manager.update_delivery_job(chat_id, item_id, "success")
            logger.info(
                f"自动发货成功 item={item_id} chat={chat_id} "
                f"mode={mode} user_url={user_url}"
            )
        except Exception as e:
            logger.error(f"自动发货发送失败 (item={item_id}): {e}")
            if mode == "stock" and claimed.get("id"):
                self.cards_manager.mark_delivery_status(claimed["id"], False)
                self.cards_manager.update_delivery_job(chat_id, item_id, "failed", card_id=claimed["id"], error=str(e))
            elif mode == "fixed":
                self.cards_manager.record_fixed_delivery(item_id, chat_id, False)
                self.cards_manager.update_delivery_job(chat_id, item_id, "failed", error=str(e))
            if mode == "stock":
                self.enqueue_handoff(
                    chat_id=chat_id,
                    item_id=item_id,
                    reason="delivery_send_failed",
                    details={"error": str(e), "user_url": user_url},
                )
            elif mode == "fixed":
                self.enqueue_handoff(
                    chat_id=chat_id,
                    item_id=item_id,
                    reason="fixed_delivery_send_failed",
                    details={"error": str(e), "user_url": user_url},
                )

    def get_status_snapshot(self):
        with self._owned_items_lock:
            owned_count = len(self.owned_item_ids)
        return {
            "account_id": self.myid,
            "last_heartbeat_time": unix_to_local_text(self.last_heartbeat_time),
            "last_heartbeat_response": unix_to_local_text(self.last_heartbeat_response),
            "last_token_refresh_time": unix_to_local_text(self.last_token_refresh_time),
            "current_token_ready": bool(self.current_token),
            "manual_mode_count": len(self.manual_mode_conversations),
            "owned_item_count": owned_count,
            "connection_restart_flag": self.connection_restart_flag,
            "items_refresh_interval": self.items_refresh_interval,
            "simulate_human_typing": self.simulate_human_typing,
            "service_enabled": self.service_enabled,
            "service_state": self.service_state,
            "service_message": self.service_message,
            "service_started_at": self._service_started_at,
            "server_time": time.time(),
            "uptime_seconds": int(time.time() - self._service_started_at) if self._service_started_at else 0,
        }

    async def handle_message(self, message_data, websocket):
        """处理所有类型的消息"""
        try:

            # 如果不是同步包消息，直接返回
            if not self.is_sync_package(message_data):
                return

            # 获取并解密数据
            sync_data = message_data["body"]["syncPushPackage"]["data"][0]
            
            # 检查是否有必要的字段
            if "data" not in sync_data:
                logger.debug("同步包中无data字段")
                return

            # 解密数据
            try:
                data = sync_data["data"]
                try:
                    data = base64.b64decode(data).decode("utf-8")
                    message = json.loads(data)
                except Exception:
                    decrypted_data = decrypt(data)
                    message = json.loads(decrypted_data)
            except Exception as e:
                logger.error(f"消息解密失败: {e}")
                return

            if isinstance(message.get("3"), dict):
                try:
                    # 诊断日志：输出 message["3"] 的完整结构
                    logger.info(f"[付款消息结构] message[3] keys={list(message.get('3',{}).keys())}")
                    logger.debug(f"[付款消息结构] message[3] 完整: {json.dumps(message.get('3',{}), ensure_ascii=False)[:800]}")
                    # 判断是否为订单消息,需要自行编写付款后的逻辑
                    if message['3']['redReminder'] == '等待买家付款':
                        user_id = message['1'].split('@')[0]
                        user_url = f'https://www.goofish.com/personal?userId={user_id}'
                        logger.info(f'等待买家 {user_url} 付款')
                        self.context_manager.upsert_chat_runtime_state(
                            chat_id=user_id,
                            item_id=None,
                            purchase_status=PURCHASE_STATUS_NOT,
                            purchase_confidence="high",
                            reply_decision=REPLY_ACTION_NO_REPLY,
                            last_reason="system_waiting_buyer_pay",
                            details={"red_reminder": "等待买家付款", "user_url": user_url},
                        )
                        return
                    elif message['3']['redReminder'] == '交易关闭':
                        user_id = message['1'].split('@')[0]
                        user_url = f'https://www.goofish.com/personal?userId={user_id}'
                        logger.info(f'买家 {user_url} 交易关闭')
                        self.context_manager.upsert_chat_runtime_state(
                            chat_id=user_id,
                            item_id=None,
                            purchase_status=PURCHASE_STATUS_NOT,
                            purchase_confidence="high",
                            reply_decision=REPLY_ACTION_NO_REPLY,
                            last_reason="system_trade_closed",
                            details={"red_reminder": "交易关闭", "user_url": user_url},
                        )
                        return
                    elif message['3']['redReminder'] == '等待卖家发货':
                        user_id = message['1'].split('@')[0]
                        user_url = f'https://www.goofish.com/personal?userId={user_id}'
                        logger.info(f'交易成功 {user_url} 等待卖家发货')

                        # 提取商品 ID（精确路径优先，再递归兜底）
                        order_item_id = None
                        try:
                            msg3 = message.get("3") or {}
                            # 精确路径优先
                            order_item_id = str(msg3.get("itemId") or msg3.get("item_id") or "")
                            if not order_item_id:
                                ext = (msg3.get("extension") or msg3.get("extensions") or {})
                                order_item_id = str(ext.get("itemId") or ext.get("item_id") or "")
                            # 兜底递归查找（跳过泛型 id）
                            if not order_item_id:
                                def _find_item_id(obj, depth=0):
                                    if depth > 6 or not isinstance(obj, dict):
                                        return None
                                    for key in ("itemId", "item_id"):
                                        val = obj.get(key, "")
                                        if val and str(val).isdigit():
                                            return str(val)
                                    for v in obj.values():
                                        if isinstance(v, dict):
                                            r = _find_item_id(v, depth + 1)
                                            if r:
                                                return r
                                    return None
                                order_item_id = _find_item_id(msg3)
                        except Exception:
                            pass

                        self.context_manager.upsert_chat_runtime_state(
                            chat_id=user_id,
                            item_id=order_item_id or None,
                            purchase_status=PURCHASE_STATUS_CONFIRMED,
                            purchase_confidence="high",
                            reply_decision=REPLY_ACTION_HANDOFF,
                            last_reason="system_waiting_seller_ship",
                            details={"red_reminder": "等待卖家发货", "user_url": user_url},
                        )

                        # 自动发货
                        auto_delivered = False
                        if order_item_id and self.ws and not self.ws.closed:
                            await self._auto_deliver(order_item_id, user_id, user_url)
                            auto_delivered = True

                        if not auto_delivered:
                            self.enqueue_handoff(
                                chat_id=user_id,
                                item_id=order_item_id or None,
                                reason="purchase_detected",
                                details={"source": "redReminder", "red_reminder": "等待卖家发货", "user_url": user_url},
                            )
                        return

                except (KeyError, TypeError) as e:
                    logger.debug("非订单类消息，跳过 redReminder 处理: {}", e)

            # 判断消息类型
            if self.is_typing_status(message):
                logger.debug("用户正在输入")
                return
            elif not self.is_chat_message(message):
                logger.debug("其他非聊天消息")
                logger.debug(f"原始消息: {message}")
                return

            # 处理聊天消息
            create_time = int(message["1"]["5"])
            send_user_name = message["1"]["10"]["reminderTitle"]
            send_user_id = message["1"]["10"]["senderUserId"]
            send_message = message["1"]["10"]["reminderContent"]
            image_urls = self.extract_image_urls(message)
            
            # 时效性验证（过滤5分钟前消息）
            if (time.time() * 1000 - create_time) > self.message_expire_time:
                logger.debug("过期消息丢弃")
                return
                
            # 获取商品ID和会话ID
            url_info = message["1"]["10"]["reminderUrl"]
            item_id = url_info.split("itemId=")[1].split("&")[0] if "itemId=" in url_info else None
            chat_id = message["1"]["2"].split('@')[0]
            
            if not item_id:
                logger.warning("无法获取商品ID")
                return

            # 检查是否为卖家（自己）发送的控制命令
            if send_user_id == self.myid:
                logger.debug("检测到卖家消息，检查是否为控制命令")

                # 检查切换命令
                if self.check_toggle_keywords(send_message):
                    mode = self.toggle_manual_mode(chat_id)
                    if mode == "manual":
                        logger.info(f"🔴 已接管会话 {chat_id} (商品: {item_id})")
                    else:
                        logger.info(f"🟢 已恢复会话 {chat_id} 的自动回复 (商品: {item_id})")
                    return
                
                # 记录卖家人工回复
                self.context_manager.add_message_by_chat(chat_id, self.myid, item_id, "assistant", send_message)
                logger.debug(f"卖家人工回复 (会话: {chat_id}, 商品: {item_id}): {send_message}")
                return
            
            logger.info(f"用户: {send_user_name} (ID: {send_user_id}), 商品: {item_id}, 会话: {chat_id}")
            logger.debug(f"用户消息内容: {send_message}")
            if image_urls:
                logger.info(f"检测到图片消息，图片数: {len(image_urls)}")
            
            
            # 如果当前会话处于人工接管模式，不进行自动回复
            if self.is_manual_mode(chat_id):
                logger.info(f"🔴 会话 {chat_id} 处于人工接管模式，跳过自动回复")
                # 添加用户消息到上下文
                self.context_manager.add_message_by_chat(chat_id, send_user_id, item_id, "user", send_message)
                return
            # 检查是否为带中括号的系统消息
            if self.is_bracket_system_message(send_message):
                logger.info(f"检测到系统消息：'{send_message}'，跳过自动回复")
                return
            if self.is_system_message(message):
                logger.debug("系统消息，跳过处理")
                return
            # 从数据库中获取商品信息，如果不存在则从API获取并保存
            item_info = self.context_manager.get_item_info(item_id)
            if not item_info:
                logger.info(f"从API获取商品信息: {item_id}")
                api_result = self.xianyu.get_item_info(item_id)
                if 'data' in api_result and 'itemDO' in api_result['data']:
                    item_info = api_result['data']['itemDO']
                    # 保存商品信息到数据库
                    self.context_manager.save_item_info(item_id, item_info)
                else:
                    logger.warning(f"获取商品信息失败: {api_result}")
                    return
            else:
                logger.info(f"从数据库获取商品信息: {item_id}")

            if not self.is_owned_item(item_id, item_info=item_info):
                seller_id = self.extract_seller_id(item_info)
                logger.warning(
                    f"商品 {item_id} 不属于当前账号 {self.myid}，实际卖家 {seller_id or 'unknown'}，跳过自动回复"
                )
                return

            item_description=f"当前商品的信息如下：{self.build_item_description(item_info)}"
            
            # 获取完整的对话上下文
            context = self.context_manager.get_context_by_chat(chat_id)
            runtime_state = self.context_manager.get_chat_runtime_state(chat_id) or {}
            latest_image_observation = self.context_manager.get_image_observation_by_chat(chat_id)
            if latest_image_observation and not image_urls:
                context = context + [{
                    "role": "system",
                    "content": latest_image_observation.get("observation", "")
                }]

            purchase_signal = detect_purchase_signal(
                message_text=send_message,
                red_reminder=((message.get("3") or {}).get("redReminder", "")),
                image_urls=image_urls,
            )
            purchase_status = self.update_purchase_runtime_state(
                chat_id=chat_id,
                item_id=item_id,
                purchase_signal=purchase_signal,
                details={
                    "last_user_message": send_message,
                    "has_image": bool(image_urls),
                },
            )

            decision = self.bot.decide_reply_action(
                user_msg=send_message,
                item_desc=item_description,
                context=context,
                purchase_status=purchase_status,
                has_image=bool(image_urls),
            )

            self.context_manager.add_message_by_chat(chat_id, send_user_id, item_id, "user", send_message)

            self.context_manager.upsert_chat_runtime_state(
                chat_id=chat_id,
                item_id=item_id,
                purchase_status=purchase_status,
                purchase_confidence=(runtime_state.get("purchase_confidence") or purchase_signal.get("purchase_confidence", "low")),
                reply_decision=decision["action"],
                last_reason=decision["reason"],
                details={
                    "last_user_message": send_message,
                    "has_image": bool(image_urls),
                    "purchase_reason": purchase_signal.get("reason"),
                },
            )

            if decision["action"] == REPLY_ACTION_NO_REPLY:
                logger.info(f"[无需回复] 会话 {chat_id} 被识别为 no_reply, reason={decision['reason']}")
                return

            if decision["action"] == REPLY_ACTION_HANDOFF:
                if decision["reason"] == "buyer_abuse":
                    logger.warning(f"[辱骂拦截] 会话 {chat_id} 买家辱骂，自动转人工")
                else:
                    logger.info(f"[人工处理] 会话 {chat_id} 进入 handoff, reason={decision['reason']}")
                self.enqueue_handoff(
                    chat_id=chat_id,
                    item_id=item_id,
                    reason=decision["reason"],
                    details={
                        "message": send_message,
                        "purchase_status": purchase_status,
                        "image_urls": image_urls,
                    },
                )
                return

            # 生成回复
            # 有图优先走多模态回复
            if image_urls:
                try:
                    observation = await self.bot.observe_images(
                        send_message,
                        item_description,
                        context=context,
                        image_urls=image_urls,
                    )
                    observation_note = XianyuReplyBot.build_image_observation_note(observation)
                    self.context_manager.save_image_observation_by_chat(
                        chat_id,
                        observation_note,
                        image_urls=image_urls,
                    )
                    context = context + [{"role": "system", "content": observation_note}]
                    bot_reply = await self.bot.generate_reply_with_images(
                        send_message,
                        item_description,
                        context=context,
                        image_urls=image_urls,
                        observation=observation,
                    )
                except Exception as img_err:
                    logger.warning(f"图片处理失败，降级为纯文字回复: {img_err}")
                    bot_reply = self.bot.generate_reply(
                        send_message,
                        item_description,
                        context=context
                    )
            else:
                bot_reply = self.bot.generate_reply(
                    send_message,
                    item_description,
                    context=context
                )
            # 二次兜底合规，避免泄露思考过程或出现站外导流
            bot_reply = XianyuReplyBot.enforce_platform_reply_policy(bot_reply)
            
            # 检查是否需要回复
            if bot_reply == "-":
                logger.info(f"[无需回复] 用户 {send_user_name} 的消息被识别为无需回复类型")
                return
            
            # 检查是否为价格意图，如果是则增加议价次数
            if self.bot.last_intent == "price":
                self.context_manager.increment_bargain_count_by_chat(chat_id)
                bargain_count = self.context_manager.get_bargain_count_by_chat(chat_id)
                logger.info(f"用户 {send_user_name} 对商品 {item_id} 的议价次数: {bargain_count}")
            
            # 添加机器人回复到上下文
            self.context_manager.add_message_by_chat(chat_id, self.myid, item_id, "assistant", bot_reply)

            # 当用户已经给出文字补充并完成一次正式回复后，清理最近图片观察，
            # 避免旧图片在后续无关话题里持续干扰判断。
            if latest_image_observation and XianyuReplyBot.has_meaningful_text(send_message):
                self.context_manager.clear_image_observation_by_chat(chat_id)
            
            logger.info(f"机器人回复: {bot_reply}")
            
            # 模拟人工输入延迟
            if self.simulate_human_typing:
                # 基础延迟 0-1秒 + 每字 0.1-0.3秒
                base_delay = random.uniform(0, 1)
                typing_delay = len(bot_reply) * random.uniform(0.1, 0.3)
                total_delay = base_delay + typing_delay
                # 设置最大延迟上限，防止过长回复等待太久
                total_delay = min(total_delay, 10.0)
                
                logger.info(f"模拟人工输入，延迟发送 {total_delay:.2f} 秒...")
                await asyncio.sleep(total_delay)
                
            await self.send_msg(websocket, chat_id, send_user_id, bot_reply)
            
        except Exception as e:
            logger.error(f"处理消息时发生错误: {str(e)}")
            logger.debug(f"原始消息: {message_data}")

    async def send_heartbeat(self, ws):
        """发送心跳包并等待响应"""
        try:
            heartbeat_mid = generate_mid()
            heartbeat_msg = {
                "lwp": "/!",
                "headers": {
                    "mid": heartbeat_mid
                }
            }
            await ws.send(json.dumps(heartbeat_msg))
            self.last_heartbeat_time = time.time()
            logger.debug("心跳包已发送")
            return heartbeat_mid
        except Exception as e:
            logger.error(f"发送心跳包失败: {e}")
            raise

    async def heartbeat_loop(self, ws):
        """心跳维护循环"""
        while True:
            try:
                current_time = time.time()
                
                # 检查是否需要发送心跳
                if current_time - self.last_heartbeat_time >= self.heartbeat_interval:
                    await self.send_heartbeat(ws)
                
                # 检查上次心跳响应时间，如果超时则认为连接已断开
                if (current_time - self.last_heartbeat_response) > (self.heartbeat_interval + self.heartbeat_timeout):
                    logger.warning("心跳响应超时，可能连接已断开")
                    break
                
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"心跳循环出错: {e}")
                break

    async def handle_heartbeat_response(self, message_data):
        """处理心跳响应"""
        try:
            if (
                isinstance(message_data, dict)
                and "headers" in message_data
                and "mid" in message_data["headers"]
                and "code" in message_data
                and message_data["code"] == 200
            ):
                self.last_heartbeat_response = time.time()
                logger.debug("收到心跳响应")
                return True
        except Exception as e:
            logger.error(f"处理心跳响应出错: {e}")
        return False

    async def _stuck_recovery_loop(self):
        """每10分钟检查并恢复卡在发货中的记录"""
        while True:
            await asyncio.sleep(600)
            try:
                stuck = self.cards_manager.recover_stuck_deliveries(timeout_minutes=10)
                if stuck:
                    for record in stuck:
                        self.enqueue_handoff(
                            chat_id=record["chat_id"],
                            item_id=record["item_id"],
                            reason="delivery_stuck_recovered",
                            details={"card_id": record["card_id"], "used_at": record["used_at"], "source": "periodic_recovery"},
                        )
                    logger.warning(f"定时恢复: 已将 {len(stuck)} 条卡死的发货记录加入人工队列")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"卡死恢复循环出错: {e}")

    async def main(self):
        self.loop = asyncio.get_running_loop()
        self.retry_signal = asyncio.Event()
        # 启动时恢复卡死的发货记录
        stuck_records = self.cards_manager.recover_stuck_deliveries(timeout_minutes=10)
        for record in stuck_records:
            self.enqueue_handoff(
                chat_id=record["chat_id"],
                item_id=record["item_id"],
                reason="delivery_stuck_recovered",
                details={"card_id": record["card_id"], "used_at": record["used_at"], "source": "startup_recovery"},
            )
        # 定时恢复任务
        self._stuck_recovery_task = asyncio.create_task(self._stuck_recovery_loop())
        while True:
            if not self.service_enabled:
                self._update_service_state("stopped", "客服已停止")
                await asyncio.sleep(1)
                continue
            try:
                # 重置连接重启标志
                self.connection_restart_flag = False
                self._update_service_state("connecting", "正在连接闲鱼")
                
                headers = {
                    "Cookie": self.cookies_str,
                    "Host": "wss-goofish.dingtalk.com",
                    "Connection": "Upgrade",
                    "Pragma": "no-cache",
                    "Cache-Control": "no-cache",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                    "Origin": "https://www.goofish.com",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                }

                async with websockets.connect(self.base_url, extra_headers=headers) as websocket:
                    self.ws = websocket
                    await self.init(websocket)
                    
                    # 初始化心跳时间
                    self.last_heartbeat_time = time.time()
                    self.last_heartbeat_response = time.time()
                    
                    # 启动心跳任务
                    self.heartbeat_task = asyncio.create_task(self.heartbeat_loop(websocket))
                    
                    # 启动token刷新任务
                    self.token_refresh_task = asyncio.create_task(self.token_refresh_loop())

                    # 启动商品列表定时刷新任务
                    self.items_refresh_task = asyncio.create_task(self.items_refresh_loop())
                    self._update_service_state("running", "客服运行中")
                    if not self._service_started_at:
                        self._service_started_at = time.time()
                    
                    async for message in websocket:
                        try:
                            # 检查是否需要重启连接
                            if self.connection_restart_flag:
                                logger.info("检测到连接重启标志，准备重新建立连接...")
                                break
                                
                            message_data = json.loads(message)
                            
                            # 处理心跳响应
                            if await self.handle_heartbeat_response(message_data):
                                continue
                            
                            # 发送通用ACK响应
                            if "headers" in message_data and "mid" in message_data["headers"]:
                                ack = {
                                    "code": 200,
                                    "headers": {
                                        "mid": message_data["headers"]["mid"],
                                        "sid": message_data["headers"].get("sid", "")
                                    }
                                }
                                # 复制其他可能的header字段
                                for key in ["app-key", "ua", "dt"]:
                                    if key in message_data["headers"]:
                                        ack["headers"][key] = message_data["headers"][key]
                                await websocket.send(json.dumps(ack))
                            
                            # 处理其他消息
                            await self.handle_message(message_data, websocket)
                                
                        except json.JSONDecodeError:
                            logger.error("消息解析失败")
                        except Exception as e:
                            logger.error(f"处理消息时发生错误: {str(e)}")
                            logger.debug(f"原始消息: {message}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket连接已关闭")
                if self.service_enabled:
                    self._update_service_state("reconnecting", "连接已关闭，准备重连")
                
            except CookieInvalidError as e:
                wait_seconds = int(os.getenv("RISK_CONTROL_RETRY_INTERVAL", RUNTIME_CONFIG_DEFAULTS["RISK_CONTROL_RETRY_INTERVAL"]))
                logger.error(f"Cookie 无效: {e}")
                logger.info(f"等待 {wait_seconds} 秒或收到启动信号后重试，请更新 .env 中的 COOKIES_STR")
                self._update_service_state("blocked", str(e))
                if self.retry_signal:
                    self.retry_signal.clear()
                    try:
                        await asyncio.wait_for(self.retry_signal.wait(), timeout=wait_seconds)
                    except asyncio.TimeoutError:
                        pass
            except RiskControlError as e:
                wait_seconds = int(os.getenv("RISK_CONTROL_RETRY_INTERVAL", RUNTIME_CONFIG_DEFAULTS["RISK_CONTROL_RETRY_INTERVAL"]))
                logger.error(f"风控阻塞: {e}")
                logger.info(f"等待 {wait_seconds} 秒后重试，请先更新 .env 中的 COOKIES_STR")
                self._update_service_state("blocked", "风控拦截，请更新 Cookie 后重新启动客服")
                if self.retry_signal:
                    self.retry_signal.clear()
                    try:
                        await asyncio.wait_for(self.retry_signal.wait(), timeout=wait_seconds)
                    except asyncio.TimeoutError:
                        pass

            except Exception as e:
                logger.error(f"连接发生错误: {e}")
                self._update_service_state("error", str(e))
                self._service_started_at = 0.0
                
            finally:
                # 清理任务
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                    try:
                        await self.heartbeat_task
                    except asyncio.CancelledError:
                        pass
                        
                if self.token_refresh_task:
                    self.token_refresh_task.cancel()
                    try:
                        await self.token_refresh_task
                    except asyncio.CancelledError:
                        pass

                if self.items_refresh_task:
                    self.items_refresh_task.cancel()
                    try:
                        await self.items_refresh_task
                    except asyncio.CancelledError:
                        pass
                if self._stuck_recovery_task:
                    self._stuck_recovery_task.cancel()
                    try:
                        await self._stuck_recovery_task
                    except asyncio.CancelledError:
                        pass
                self.ws = None
                
                # 如果是主动重启，立即重连；否则等待5秒
                if not self.service_enabled:
                    self._update_service_state("stopped", "客服已停止")
                    await asyncio.sleep(1)
                elif self.connection_restart_flag:
                    logger.info("主动重启连接，立即重连...")
                else:
                    logger.info("等待5秒后重连...")
                    await asyncio.sleep(5)



def check_and_complete_env():
    """检查并补全关键环境变量"""
    # 定义关键变量及其默认无效值（占位符）
    critical_vars = {
        "API_KEY": "默认使用通义千问,apikey通过百炼模型平台获取",
        "COOKIES_STR": "your_cookies_here"
    }
    
    env_path = ".env"
    updated = False
    interactive = sys.stdin is not None and sys.stdin.isatty()
    
    for key, placeholder in critical_vars.items():
        curr_val = os.getenv(key)
        
        # 如果变量未设置，或者值等于占位符
        if not curr_val or curr_val == placeholder:
            if not interactive:
                raise RuntimeError(f"后台模式缺少必要配置: {key}")
            logger.warning(f"配置项 [{key}] 未设置或为默认值，请输入")
            while True:
                val = input(f"请输入 {key}: ").strip()
                if val:
                    # 更新当前环境
                    os.environ[key] = val
                    
                    # 尝试持久化到 .env
                    try:
                        safe_update_env(Path(env_path), {key: val})
                        updated = True
                    except Exception as e:
                        logger.warning(f"无法自动写入.env文件，请手动保存: {e}")
                    break
                else:
                    print(f"{key} 不能为空，请重新输入")
    
    if updated:
        logger.info("新的配置已保存/更新至 .env 文件中")


def configure_logging(log_level):
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(
        ADMIN_LOG_BUFFER.sink,
        level=log_level,
        format="{message}"
    )


if __name__ == '__main__':
    # 加载环境变量
    if os.path.exists(".env"):
        load_dotenv()
        logger.info("已加载 .env 配置")
    
    if os.path.exists(".env.example"):
        load_dotenv(".env.example")  # 不会覆盖已存在的变量
        logger.info("已加载 .env.example 默认配置")
    
    # 配置日志级别
    log_level = os.getenv("LOG_LEVEL", RUNTIME_CONFIG_DEFAULTS["LOG_LEVEL"]).upper()
    configure_logging(log_level)
    logger.info(f"日志级别设置为: {log_level}")

    if not (os.getenv("ADMIN_API_TOKEN") or "").strip():
        logger.warning(
            "未设置 ADMIN_API_TOKEN：管理后台所有写操作（PUT/POST）将被拒绝；"
            "客服主进程不受影响。请在 .env 中配置 ADMIN_API_TOKEN 后重启。"
        )

    # 交互式检查并补全配置
    check_and_complete_env()
    
    cookies_str = os.getenv("COOKIES_STR")
    bot = None
    xianyuLive = None
    try:
        bot = XianyuReplyBot()
        xianyuLive = XianyuLive(cookies_str, bot=bot)
    except Exception as e:
        logger.warning(f"Bot 初始化失败（后台仍可用，请在后台填写配置后重新启动客服）: {e}")

    cards_mgr = xianyuLive.cards_manager if xianyuLive else CardsManager()
    admin_service = AdminService(bot, xianyuLive, cards_mgr, env_path=".env", prompt_dir="prompts")
    admin_port = int(os.getenv("ADMIN_PORT", "18061"))
    admin_host = os.getenv("ADMIN_HOST", "127.0.0.1")
    start_admin_server(admin_service, host=admin_host, port=admin_port, static_dir="admin_static")
    # 常驻进程
    if xianyuLive:
        asyncio.run(xianyuLive.main())
    else:
        logger.warning("Bot 未初始化，仅管理后台运行。请在后台配置模型和 Cookie 后重启。")
        import time as _time
        while True:
            _time.sleep(3600)
