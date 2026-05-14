import re
from typing import List, Dict
import os
import json
import threading
import asyncio
import base64
import httpx
from openai import OpenAI
from loguru import logger
from policy_engine import (
    PURCHASE_STATUS_CONFIRMED,
    PURCHASE_STATUS_NOT,
    PURCHASE_STATUS_SUSPECTED,
    REPLY_ACTION_HANDOFF,
    REPLY_ACTION_NO_REPLY,
    REPLY_ACTION_REPLY,
    heuristic_reply_action,
)
from admin_service import RUNTIME_CONFIG_DEFAULTS


class XianyuReplyBot:
    def __init__(self):
        self._client_lock = threading.RLock()
        self.client = None
        self._init_client()
        self._init_system_prompts()
        self._init_agents()
        self.router = IntentRouter(self.agents['classify'])
        self.last_intent = None  # 记录最后一次意图

    def _init_client(self):
        api_key = (os.getenv("API_KEY") or "").strip()
        if not api_key:
            raise ValueError("API_KEY 未设置或为空，请在 .env 中配置 API_KEY 后重新启动")
        # 文字模型客户端
        text_base_url = os.getenv("TEXT_MODEL_BASE_URL") or os.getenv("MODEL_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        text_api_key = (os.getenv("TEXT_API_KEY") or "").strip() or api_key
        self.text_client = OpenAI(api_key=text_api_key, base_url=text_base_url)
        # 视觉模型客户端
        vision_base_url = os.getenv("VISION_MODEL_BASE_URL") or os.getenv("MODEL_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        vision_api_key = (os.getenv("VISION_API_KEY") or "").strip() or api_key
        self.vision_client = OpenAI(api_key=vision_api_key, base_url=vision_base_url)
        # 兼容旧代码
        self.client = self.text_client


    def _init_agents(self):
        """初始化各领域Agent"""
        self.agents = {
            'classify':ClassifyAgent(self.client, self.classify_prompt, self._safe_filter, self._client_lock),
            'price': PriceAgent(self.client, self.price_prompt, self._safe_filter, self._client_lock),
            'tech': TechAgent(self.client, self.tech_prompt, self._safe_filter, self._client_lock),
            'default': DefaultAgent(self.client, self.default_prompt, self._safe_filter, self._client_lock),
        }

    def _init_system_prompts(self):
        """初始化各Agent专用提示词，优先加载用户自定义文件，否则使用Example默认文件"""
        prompt_dir = "prompts"
        
        def load_prompt_content(name: str) -> str:
            """尝试加载提示词文件"""
            # 优先尝试加载 target.txt
            target_path = os.path.join(prompt_dir, f"{name}.txt")
            if os.path.exists(target_path):
                file_path = target_path
            else:
                # 尝试默认提示词 target_example.txt
                file_path = os.path.join(prompt_dir, f"{name}_example.txt")

            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                logger.debug(f"已加载 {name} 提示词，路径: {file_path}, 长度: {len(content)} 字符")
                return content

        try:
            # 加载分类提示词
            self.classify_prompt = load_prompt_content("classify_prompt")
            # 加载价格提示词
            self.price_prompt = load_prompt_content("price_prompt")
            # 加载技术提示词
            self.tech_prompt = load_prompt_content("tech_prompt")
            # 加载默认提示词
            self.default_prompt = load_prompt_content("default_prompt")
                
            logger.info("成功加载所有提示词")
        except Exception as e:
            logger.error(f"加载提示词时出错: {e}")
            raise

    def _safe_filter(self, text: str) -> str:
        """安全过滤模块"""
        blocked_phrases = ["微信", "QQ", "支付宝", "银行卡", "线下"]
        return "[安全提醒]请通过平台沟通" if any(p in text for p in blocked_phrases) else text

    # 图片域名白名单
    ALLOWED_IMAGE_DOMAINS = {
        "alicdn.com", "taobao.com", "tmall.com",
        "goofish.com", "aliyuncs.com",
    }
    # 内网 IP 前缀
    BLOCKED_IP_PREFIXES = ("127.", "10.", "192.168.", "169.254.", "::1", "[::1]")

    @staticmethod
    def _is_allowed_image_url(url: str) -> bool:
        """校验图片 URL 是否在白名单内"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            hostname = parsed.hostname or ""
            # 检查内网地址
            if hostname.startswith(XianyuReplyBot.BLOCKED_IP_PREFIXES):
                logger.warning(f"拦截内网图片地址: {hostname}")
                return False
            # 检查域名白名单
            for domain in XianyuReplyBot.ALLOWED_IMAGE_DOMAINS:
                if hostname == domain or hostname.endswith("." + domain):
                    return True
            logger.warning(f"图片域名不在白名单: {hostname}")
            return False
        except Exception:
            return False

    @staticmethod
    async def _download_image_as_base64(url: str, max_size: int = 3 * 1024 * 1024) -> str:
        """下载图片并转为 base64 data URI，失败返回空字符串"""
        # 校验原始 URL
        if not XianyuReplyBot._is_allowed_image_url(url):
            return ""
        try:
            async with httpx.AsyncClient(timeout=5, follow_redirects=True) as client:
                # 先用 HEAD 检查大小
                try:
                    head = await client.head(url)
                    content_length = int(head.headers.get("content-length", 0))
                    if content_length > max_size:
                        logger.warning(f"图片过大跳过 ({content_length / 1024 / 1024:.1f}MB): {url[:80]}")
                        return ""
                except Exception:
                    pass

                resp = await client.get(url)
                resp.raise_for_status()

                # 重定向后校验最终地址
                final_url = str(resp.url)
                if final_url != url and not XianyuReplyBot._is_allowed_image_url(final_url):
                    logger.warning(f"重定向到非白名单地址: {final_url[:80]}")
                    return ""

                # 下载后检查大小
                if len(resp.content) > max_size:
                    logger.warning(f"图片过大跳过 ({len(resp.content) / 1024 / 1024:.1f}MB): {url[:80]}")
                    return ""

                # 验证 content-type
                content_type = resp.headers.get("content-type", "image/jpeg")
                if ";" in content_type:
                    content_type = content_type.split(";")[0].strip()
                if not content_type.startswith("image/"):
                    logger.warning(f"跳过非图片内容: {url[:80]} content-type={content_type}")
                    return ""
                if content_type == "image/svg+xml":
                    logger.warning(f"跳过 SVG 图片: {url[:80]}")
                    return ""

                b64 = base64.b64encode(resp.content).decode()
                return f"data:{content_type};base64,{b64}"
        except Exception as e:
            logger.warning(f"图片下载失败 {url[:80]}: {e}")
            return ""

    async def _prepare_image_parts(self, image_urls: List[str]) -> List[Dict]:
        """将图片URL列表转为 vision API 格式，并发下载，失败的图片会被跳过"""
        valid_urls = [u for u in image_urls if isinstance(u, str) and u.startswith(("http://", "https://"))][:4]
        if not valid_urls:
            return []
        tasks = [self._download_image_as_base64(url) for url in valid_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        parts = []
        for r in results:
            if isinstance(r, str) and r:
                parts.append({"type": "image_url", "image_url": {"url": r}})
        return parts

    @staticmethod
    def get_text_model_name() -> str:
        return os.getenv("TEXT_MODEL_NAME", os.getenv("MODEL_NAME", RUNTIME_CONFIG_DEFAULTS["TEXT_MODEL_NAME"]))

    @staticmethod
    def get_vision_model_name() -> str:
        return os.getenv("VISION_MODEL_NAME", os.getenv("MODEL_NAME", RUNTIME_CONFIG_DEFAULTS["VISION_MODEL_NAME"]))

    @staticmethod
    def sanitize_model_output(text: str) -> str:
        """
        清洗模型输出，防止把思考过程/标签泄露给买家。
        优先取 <final_reply> / <answer>，否则移除 think 标签和常见分析腔。
        """
        if not text:
            return ""

        raw = text.strip()

        final_match = re.search(r"<final_reply>\s*(.*?)\s*</final_reply>", raw, re.IGNORECASE | re.DOTALL)
        if final_match:
            return final_match.group(1).strip()

        answer_match = re.search(r"<answer>\s*(.*?)\s*</answer>", raw, re.IGNORECASE | re.DOTALL)
        if answer_match:
            cleaned = answer_match.group(1).strip()
            return cleaned

        cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"<analysis>.*?</analysis>", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"^.*?(最终回复[:：])", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"</?answer>", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"</?final_reply>", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"</?[a-zA-Z0-9_:-]+(?:\s[^>]*)?>", "", cleaned)

        analysis_prefixes = [
            "从图片来看", "从这张图片来看", "从您发的图片来看", "根据图片来看",
            "我先分析一下", "我来分析一下", "分析如下", "综合来看", "综合分析",
            "可以这样回复", "建议回复", "可以回复", "作为客服", "根据上下文",
            "这张图片显示", "图片里可以看到", "我判断", "我的判断是",
        ]
        for prefix in analysis_prefixes:
            if cleaned.startswith(prefix):
                cleaned = re.sub(r"^.*?[，。:：]\s*", "", cleaned, count=1)
                break

        return cleaned.strip()

    @staticmethod
    def enforce_platform_reply_policy(text: str, max_len: int = 90) -> str:
        """平台回复合规兜底：纯文本、禁导流、限制长度。"""
        cleaned = XianyuReplyBot.sanitize_model_output(text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        risky_patterns = [
            r"微\s*信", r"\bwx\b", r"\bV\b", r"\bQQ\b",
            r"手机号", r"电话", r"\b1\d{10}\b",
            r"加我", r"线下", r"私下", r"平台外", r"转到.*(微信|QQ|电话)"
        ]
        if any(re.search(p, cleaned, re.IGNORECASE) for p in risky_patterns):
            return "为保障交易安全，咱们通过闲鱼聊天沟通就行。"

        if not cleaned:
            return "在的，您可以直接说下想了解哪方面。"

        if len(cleaned) > max_len:
            cleaned = cleaned[:max_len].rstrip("，,。.！!？? ") + "。"

        return cleaned

    def format_history(self, context: List[Dict]) -> str:
        """格式化对话历史，返回完整的对话记录"""
        visible_msgs = []
        for msg in context:
            role = msg.get('role')
            content = msg.get('content', '')
            if role in ['user', 'assistant']:
                visible_msgs.append(f"{role}: {content}")
            elif role == 'system' and content.startswith("图片观察结果:"):
                visible_msgs.append(content)
        return "\n".join(visible_msgs)

    @staticmethod
    def has_meaningful_text(text: str) -> bool:
        if not text:
            return False
        return bool(re.search(r"[\u4e00-\u9fa5A-Za-z0-9]", text))

    @staticmethod
    def needs_clarification_for_image_only(user_msg: str) -> bool:
        return not XianyuReplyBot.has_meaningful_text(user_msg)

    @staticmethod
    def build_customer_reply_protocol() -> str:
        return (
            "输出协议（严格遵守）：\n"
            "1. 你的内部分析绝不能展示给买家。\n"
            "2. 最终只允许输出一个标签：<final_reply>发给买家的一句话</final_reply>\n"
            "3. 不要输出任何其他文字、解释、分析、markdown、json、标签说明。\n"
            "4. 如果信息不足，优先礼貌追问澄清，不要猜测。"
        )

    @staticmethod
    def build_image_observation_protocol() -> str:
        return (
            "你现在不是直接回复买家，而是做图片观察记录。\n"
            "请基于图片、商品信息、历史对话和买家文字，输出严格 JSON："
            "{\"scene\":\"图片里是什么\",\"issue\":\"用户可能相关的问题\",\"visible_text\":\"截图里能读到的关键文案或错误码\","
            "\"key_details\":\"与问题相关的关键细节\",\"certainty\":\"high|medium|low\","
            "\"needs_clarification\":false,\"suggested_reply\":\"一条简短回复\"}\n"
            "要求：\n"
            "- 如果图片内容清晰（如截图、报错信息、商品图片、支付凭证），needs_clarification 必须为 false，suggested_reply 给出直接回复。\n"
            "- 如果图片模糊或完全无法判断意图，needs_clarification 才设为 true。\n"
            "- certainty 表示你对图片意图的判断：high=意图明确，medium=大致能猜，low=完全不明。\n"
            "- 对报错截图、系统截图，尽量提取可见的错误码、报错文案、按钮或页面位置。\n"
            "- 不要编造图片中看不清的信息。\n"
            "- suggested_reply 必须是发给买家的一句话，不超过 60 字。\n"
            "- 只输出 JSON，不要输出其他内容。"
        )

    @staticmethod
    def build_multimodal_reply_protocol() -> str:
        return (
            "你是闲鱼客服助手。你可以直接看图并结合商品信息、历史对话、买家文字给出最终回复。\n"
            "规则：\n"
            "- 直接输出 <final_reply>发给买家的一句话</final_reply>\n"
            "- 不要输出任何分析过程、解释、markdown、json。\n"
            "- 对报错截图，要尽量利用图里的错误码、报错文案和界面信息来回答。\n"
            "- 如果信息仍不足，礼貌追问，不要猜。"
        )

    @staticmethod
    def build_reply_decision_protocol() -> str:
        return (
            "你在做客服回复决策，不是直接回复买家。\n"
            "请根据商品信息、历史对话、当前消息和购买状态，输出严格 JSON："
            "{\"action\":\"reply|no_reply|handoff\",\"reason\":\"简短英文原因\"}\n"
            "规则：\n"
            "- 广告、导流、骚扰、明显无关消息优先 no_reply。\n"
            "- 退款纠纷、售后争议、购买后发货、边界不清问题优先 handoff。\n"
            "- 正常售前咨询返回 reply。\n"
            "- 只输出 JSON。"
        )

    @staticmethod
    def parse_image_observation(raw: str) -> Dict:
        fallback = {
            "scene": "买家发送了一张图片，但图片含义仍需结合后续说明判断。",
            "issue": "当前无法仅凭图片确定买家具体问题。",
            "visible_text": "",
            "key_details": "",
            "certainty": "low",
            "needs_clarification": True,
            "suggested_reply": "我先看到了图片，您是想确认哪个问题？可以补一句说明，我再帮您判断。",
        }
        if not raw:
            return fallback

        text = raw.strip()
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return fallback

        try:
            data = json.loads(match.group(0))
        except Exception:
            return fallback

        parsed = {
            "scene": str(data.get("scene", fallback["scene"])).strip() or fallback["scene"],
            "issue": str(data.get("issue", fallback["issue"])).strip() or fallback["issue"],
            "visible_text": str(data.get("visible_text", fallback["visible_text"])).strip(),
            "key_details": str(data.get("key_details", fallback["key_details"])).strip(),
            "certainty": str(data.get("certainty", fallback["certainty"])).strip() or fallback["certainty"],
            "needs_clarification": bool(data.get("needs_clarification", fallback["needs_clarification"])),
            "suggested_reply": str(data.get("suggested_reply", fallback["suggested_reply"])).strip() or fallback["suggested_reply"],
        }
        parsed["suggested_reply"] = XianyuReplyBot.enforce_platform_reply_policy(parsed["suggested_reply"], max_len=60)
        return parsed

    @staticmethod
    def build_image_observation_note(observation: Dict) -> str:
        return (
            "图片观察结果:"
            f" 场景={observation.get('scene', '')};"
            f" 可能问题={observation.get('issue', '')};"
            f" 可见文本={observation.get('visible_text', '')};"
            f" 关键细节={observation.get('key_details', '')};"
            f" 置信度={observation.get('certainty', 'low')};"
            f" 是否需澄清={observation.get('needs_clarification', True)}"
        )

    async def observe_images(
        self,
        user_msg: str,
        item_desc: str,
        context: List[Dict],
        image_urls: List[str],
    ) -> Dict:
        """仅做图片观察，不直接承担最终客服回复。"""
        formatted_context = self.format_history(context)
        effective_msg = user_msg.strip() if user_msg and user_msg.strip() else "买家发送了图片，暂未附带明确文字问题。"
        image_parts = await self._prepare_image_parts(image_urls)
        if not image_parts:
            return self.parse_image_observation("")

        vision_model = self.get_vision_model_name()
        system_text = (
            f"【商品信息】{item_desc}\n"
            f"【历史对话】{formatted_context}\n"
            f"{self.build_image_observation_protocol()}"
        )
        user_parts = [{"type": "text", "text": f"【买家文字】{effective_msg}"}]
        user_parts.extend(image_parts)

        with self._client_lock:
            response = self.vision_client.chat.completions.create(
                model=vision_model,
                messages=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": user_parts},
                ],
                temperature=0.2,
                max_tokens=400,
                top_p=0.8,
            )
        raw = response.choices[0].message.content
        return self.parse_image_observation(raw)

    def decide_reply_action(
        self,
        user_msg: str,
        item_desc: str,
        context: List[Dict],
        purchase_status: str = PURCHASE_STATUS_NOT,
        has_image: bool = False,
    ) -> Dict:
        heuristic = heuristic_reply_action(user_msg, purchase_status=purchase_status, has_image=has_image)
        if heuristic["action"] != REPLY_ACTION_REPLY:
            return heuristic

        formatted_context = self.format_history(context)
        system_text = (
            f"【商品信息】{item_desc}\n"
            f"【你与客户对话历史】{formatted_context}\n"
            f"【当前购买状态】{purchase_status}\n"
            f"{self.build_reply_decision_protocol()}"
        )
        with self._client_lock:
            response = self.client.chat.completions.create(
                model=self.get_text_model_name(),
                messages=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": user_msg or "(空消息)"},
                ],
                temperature=0.1,
                max_tokens=80,
                top_p=0.8,
            )
        raw = (response.choices[0].message.content or "").strip()
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return heuristic

        try:
            parsed = json.loads(match.group(0))
        except Exception:
            return heuristic

        action = str(parsed.get("action", REPLY_ACTION_REPLY)).strip().lower()
        reason = str(parsed.get("reason", heuristic["reason"])).strip() or heuristic["reason"]
        if action not in {REPLY_ACTION_REPLY, REPLY_ACTION_NO_REPLY, REPLY_ACTION_HANDOFF}:
            return heuristic
        return {"action": action, "reason": reason}

    def generate_reply(self, user_msg: str, item_desc: str, context: List[Dict]) -> str:
        """生成回复主流程"""
        # 记录用户消息
        # logger.debug(f'用户所发消息: {user_msg}')
        
        formatted_context = self.format_history(context)
        # logger.debug(f'对话历史: {formatted_context}')
        
        # 1. 路由决策
        detected_intent = self.router.detect(user_msg, item_desc, formatted_context)

        # 2. 获取对应Agent
        internal_intents = {'classify'}  # 定义不对外开放的Agent

        if detected_intent == 'no_reply':
            # 无需回复的情况
            logger.info(f'意图识别完成: no_reply - 无需回复')
            self.last_intent = 'no_reply'
            return "-"  # 返回特殊标记，表示无需回复
        elif detected_intent in self.agents and detected_intent not in internal_intents:
            agent = self.agents[detected_intent]
            logger.info(f'意图识别完成: {detected_intent}')
            self.last_intent = detected_intent  # 保存当前意图
        else:
            agent = self.agents['default']
            logger.info(f'意图识别完成: default')
            self.last_intent = 'default'  # 保存当前意图
        
        # 3. 获取议价次数
        bargain_count = self._extract_bargain_count(context)
        logger.info(f'议价次数: {bargain_count}')

        # 4. 生成回复
        return agent.generate(
            user_msg=user_msg,
            item_desc=item_desc,
            context=formatted_context,
            bargain_count=bargain_count
        )

    async def generate_reply_with_images(
        self,
        user_msg: str,
        item_desc: str,
        context: List[Dict],
        image_urls: List[str],
        observation: Dict | None = None,
    ) -> str:
        """图片消息回复：由多模态模型直接结合图像与上下文输出最终客服回复。"""
        observation = observation or await self.observe_images(user_msg, item_desc, context, image_urls)
        observation_note = self.build_image_observation_note(observation)
        enriched_context = list(context) + [{"role": "system", "content": observation_note}]

        # 判断是否需要追问
        certainty = observation.get("certainty", "low")
        needs_clarification = observation.get("needs_clarification", True)
        has_context = any(msg.get("role") in ("user", "assistant") for msg in context)
        is_image_only = self.needs_clarification_for_image_only(user_msg)

        should_clarify = False
        if is_image_only:
            # high: 直接回复; medium: 有上下文直接回复，无上下文追问; low: 有上下文直接回复，无上下文追问
            if certainty == "low" and not has_context:
                should_clarify = True
            elif certainty == "medium" and not has_context and needs_clarification:
                should_clarify = True

        if should_clarify:
            return observation.get(
                "suggested_reply",
                "我先看到了图片，您方便补一句具体想确认的问题吗？我再帮您判断。"
            )
        formatted_context = self.format_history(enriched_context)
        effective_msg = user_msg.strip() if user_msg and user_msg.strip() else "买家发送了图片，请结合图片内容回复。"
        image_parts = await self._prepare_image_parts(image_urls)
        user_parts = [
            {
                "type": "text",
                "text": (
                    f"【商品信息】{item_desc}\n"
                    f"【历史对话】{formatted_context}\n"
                    f"【买家文字】{effective_msg}"
                ),
            }
        ]
        user_parts.extend(image_parts)

        with self._client_lock:
            response = self.vision_client.chat.completions.create(
                model=self.get_vision_model_name(),
                messages=[
                    {
                        "role": "system",
                        "content": self.build_multimodal_reply_protocol(),
                    },
                    {
                        "role": "user",
                        "content": user_parts,
                    },
                ],
                temperature=0.3,
                max_tokens=400,
                top_p=0.8,
            )
        raw = response.choices[0].message.content
        safe = self._safe_filter(self.sanitize_model_output(raw))
        return self.enforce_platform_reply_policy(safe)
    
    def _extract_bargain_count(self, context: List[Dict]) -> int:
        """
        从上下文中提取议价次数信息
        
        Args:
            context: 对话历史
            
        Returns:
            int: 议价次数，如果没有找到则返回0
        """
        # 查找系统消息中的议价次数信息
        for msg in context:
            if msg['role'] == 'system' and '议价次数' in msg['content']:
                try:
                    # 提取议价次数
                    match = re.search(r'议价次数[:：]\s*(\d+)', msg['content'])
                    if match:
                        return int(match.group(1))
                except Exception:
                    pass
        return 0

    def reload_prompts(self):
        """重新加载所有提示词"""
        logger.info("正在重新加载提示词...")
        with self._client_lock:
            self._init_system_prompts()
            self._init_agents()
        logger.info("提示词重新加载完成")

    def reload_runtime_config(self):
        """重建模型客户端并让 agent 使用最新环境配置。"""
        logger.info("正在重载模型运行配置...")
        try:
            with self._client_lock:
                self._init_client()
                self._init_agents()
                self.router = IntentRouter(self.agents['classify'])
            logger.info("模型运行配置热切换完成")
            return True
        except Exception as e:
            logger.error(f"模型运行配置热切换失败: {e}")
            return False


class IntentRouter:
    """意图路由决策器"""

    def __init__(self, classify_agent):
        self.rules = {
            'tech': {  # 技术类优先判定
                'keywords': ['参数', '规格', '型号', '连接', '对比'],
                'patterns': [
                    r'和.+比'             
                ]
            },
            'price': {
                'keywords': ['便宜', '价', '砍价', '少点'],
                'patterns': [r'\d+元', r'能少\d+']
            }
        }
        self.classify_agent = classify_agent

    def detect(self, user_msg: str, item_desc, context) -> str:
        """三级路由策略（技术优先）"""
        text_clean = re.sub(r'[^\w\u4e00-\u9fa5]', '', user_msg)
        
        # 1. 技术类关键词优先检查
        if any(kw in text_clean for kw in self.rules['tech']['keywords']):
            # logger.debug(f"技术类关键词匹配: {[kw for kw in self.rules['tech']['keywords'] if kw in text_clean]}")
            return 'tech'
            
        # 2. 技术类正则优先检查
        for pattern in self.rules['tech']['patterns']:
            if re.search(pattern, text_clean):
                # logger.debug(f"技术类正则匹配: {pattern}")
                return 'tech'

        # 3. 价格类检查
        for intent in ['price']:
            if any(kw in text_clean for kw in self.rules[intent]['keywords']):
                # logger.debug(f"价格类关键词匹配: {[kw for kw in self.rules[intent]['keywords'] if kw in text_clean]}")
                return intent
            
            for pattern in self.rules[intent]['patterns']:
                if re.search(pattern, text_clean):
                    # logger.debug(f"价格类正则匹配: {pattern}")
                    return intent
        
        # 4. 大模型兜底
        # logger.debug("使用大模型进行意图分类")
        return self.classify_agent.generate(
            user_msg=user_msg,
            item_desc=item_desc,
            context=context
        )


class BaseAgent:
    """Agent基类"""

    def __init__(self, client, system_prompt, safety_filter, client_lock=None):
        self.client = client
        self.system_prompt = system_prompt
        self.safety_filter = safety_filter
        self._client_lock = client_lock or threading.RLock()

    def generate(self, user_msg: str, item_desc: str, context: str, bargain_count: int = 0) -> str:
        """生成回复模板方法"""
        messages = self._build_messages(user_msg, item_desc, context)
        response = self._call_llm(messages)
        cleaned = XianyuReplyBot.sanitize_model_output(response)
        safe = self.safety_filter(cleaned)
        return XianyuReplyBot.enforce_platform_reply_policy(safe)

    def _build_messages(self, user_msg: str, item_desc: str, context: str) -> List[Dict]:
        """构建消息链"""
        return [
            {"role": "system", "content": f"【商品信息】{item_desc}\n【你与客户对话历史】{context}\n{self.system_prompt}\n{XianyuReplyBot.build_customer_reply_protocol()}"},
            {"role": "user", "content": user_msg}
        ]

    def _call_llm(self, messages: List[Dict], temperature: float = 0.4) -> str:
        """调用大模型"""
        with self._client_lock:
            response = self.client.chat.completions.create(
                model=XianyuReplyBot.get_text_model_name(),
                messages=messages,
                temperature=temperature,
                max_tokens=500,
                top_p=0.8
            )
        return response.choices[0].message.content


class PriceAgent(BaseAgent):
    """议价处理Agent"""

    def generate(self, user_msg: str, item_desc: str, context: str, bargain_count: int=0) -> str:
        """重写生成逻辑"""
        dynamic_temp = self._calc_temperature(bargain_count)
        messages = self._build_messages(user_msg, item_desc, context)
        messages[0]['content'] += f"\n▲当前议价轮次：{bargain_count}"

        with self._client_lock:
            response = self.client.chat.completions.create(
                model=XianyuReplyBot.get_text_model_name(),
                messages=messages,
                temperature=dynamic_temp,
                max_tokens=500,
                top_p=0.8
            )
        cleaned = XianyuReplyBot.sanitize_model_output(response.choices[0].message.content)
        safe = self.safety_filter(cleaned)
        return XianyuReplyBot.enforce_platform_reply_policy(safe)

    def _calc_temperature(self, bargain_count: int) -> float:
        """动态温度策略"""
        return min(0.3 + bargain_count * 0.15, 0.9)


class TechAgent(BaseAgent):
    """技术咨询Agent"""
    def generate(self, user_msg: str, item_desc: str, context: str, bargain_count: int=0) -> str:
        """重写生成逻辑"""
        messages = self._build_messages(user_msg, item_desc, context)
        # messages[0]['content'] += "\n▲知识库：\n" + self._fetch_tech_specs()

        with self._client_lock:
            create_kwargs = dict(
                model=XianyuReplyBot.get_text_model_name(),
                messages=messages,
                temperature=0.4,
                max_tokens=500,
                top_p=0.8,
            )
            model_name = XianyuReplyBot.get_text_model_name()
            if model_name.startswith("qwen"):
                create_kwargs["extra_body"] = {"enable_search": True}
            response = self.client.chat.completions.create(**create_kwargs)

        cleaned = XianyuReplyBot.sanitize_model_output(response.choices[0].message.content)
        safe = self.safety_filter(cleaned)
        return XianyuReplyBot.enforce_platform_reply_policy(safe)


class ClassifyAgent(BaseAgent):
    """意图识别Agent"""

    def generate(self, **args) -> str:
        user_msg = args.get("user_msg", "")
        item_desc = args.get("item_desc", "")
        context = args.get("context", "")
        messages = [
            {"role": "system", "content": f"【商品信息】{item_desc}\n【你与客户对话历史】{context}\n{self.system_prompt}"},
            {"role": "user", "content": user_msg}
        ]
        with self._client_lock:
            response = self.client.chat.completions.create(
                model=XianyuReplyBot.get_text_model_name(),
                messages=messages,
                temperature=0.1,
                max_tokens=20,
                top_p=0.8
            )
        raw = (response.choices[0].message.content or "").strip().lower()
        for label in ("price", "tech", "default", "no_reply"):
            if label in raw:
                return label
        return "default"


class DefaultAgent(BaseAgent):
    """默认处理Agent"""

    def _call_llm(self, messages: List[Dict], *args) -> str:
        """限制默认回复长度"""
        response = super()._call_llm(messages, temperature=0.7)
        return response
