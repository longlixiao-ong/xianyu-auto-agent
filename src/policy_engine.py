import re


PURCHASE_STATUS_NOT = "not_purchased"
PURCHASE_STATUS_SUSPECTED = "suspected_purchased"
PURCHASE_STATUS_CONFIRMED = "confirmed_purchased"

REPLY_ACTION_REPLY = "reply"
REPLY_ACTION_NO_REPLY = "no_reply"
REPLY_ACTION_HANDOFF = "handoff"


def normalize_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", "", str(text)).lower()


def detect_purchase_signal(message_text="", red_reminder="", image_urls=None):
    text = normalize_text(message_text)
    reminder = normalize_text(red_reminder)
    image_urls = image_urls or []

    confirmed_markers = [
        "等待卖家发货",
        "我已拍下请发货",
        "我已拍下",
        "已拍下",
        "请发货",
        "已付款",
        "已支付",
    ]
    suspected_markers = [
        "我拍了",
        "拍下了",
        "付款了",
        "发卡",
        "发码",
        "卡密",
        "多久到账",
        "怎么发货",
    ]

    if "等待卖家发货" in reminder:
        return {
            "purchase_status": PURCHASE_STATUS_CONFIRMED,
            "purchase_confidence": "high",
            "reason": "system_waiting_seller_ship",
        }

    if "等待买家付款" in reminder:
        return {
            "purchase_status": PURCHASE_STATUS_NOT,
            "purchase_confidence": "high",
            "reason": "system_waiting_buyer_pay",
        }

    if "交易关闭" in reminder:
        return {
            "purchase_status": PURCHASE_STATUS_NOT,
            "purchase_confidence": "high",
            "reason": "system_trade_closed",
        }

    if any(marker in text for marker in confirmed_markers):
        return {
            "purchase_status": PURCHASE_STATUS_CONFIRMED,
            "purchase_confidence": "medium",
            "reason": "confirmed_purchase_text",
        }

    if any(marker in text for marker in suspected_markers):
        return {
            "purchase_status": PURCHASE_STATUS_SUSPECTED,
            "purchase_confidence": "medium",
            "reason": "suspected_purchase_text",
        }

    if image_urls and any(marker in text for marker in ("订单", "付款", "发货", "卡密")):
        return {
            "purchase_status": PURCHASE_STATUS_SUSPECTED,
            "purchase_confidence": "low",
            "reason": "purchase_related_image_context",
        }

    return {
        "purchase_status": PURCHASE_STATUS_NOT,
        "purchase_confidence": "low",
        "reason": "no_purchase_signal",
    }


def heuristic_reply_action(message_text="", purchase_status=PURCHASE_STATUS_NOT, has_image=False):
    text = normalize_text(message_text)

    if purchase_status in (PURCHASE_STATUS_CONFIRMED, PURCHASE_STATUS_SUSPECTED):
        return {
            "action": REPLY_ACTION_HANDOFF,
            "reason": "purchase_detected",
        }

    if not text and has_image:
        return {
            "action": REPLY_ACTION_REPLY,
            "reason": "image_only_needs_clarification",
        }

    if not text:
        return {
            "action": REPLY_ACTION_NO_REPLY,
            "reason": "empty_message",
        }

    abuse_patterns = [
        "你妈", "他妈", "妈的", "草泥马", "尼玛", "nmsl",
        "傻逼", "煞笔", "沙比", "智障", "白痴", "蠢货",
        "去死", "滚蛋", "操你", "fuck", "shit", "stfu",
        "废物", "垃圾", "贱人", "狗东西",
        "日你", "干你", "肏",
        "tmd", "sb", "nc", "zz",
    ]
    if any(pattern in text for pattern in abuse_patterns):
        return {
            "action": REPLY_ACTION_HANDOFF,
            "reason": "buyer_abuse",
        }

    ad_patterns = [
        "加微",
        "vx",
        "v:",
        "v信",
        "合作吗",
        "代发",
        "引流",
        "刷单",
        "兼职",
        "推广",
        "收徒",
        "返现",
        "关注店铺",
    ]
    if any(pattern in text for pattern in ad_patterns):
        return {
            "action": REPLY_ACTION_NO_REPLY,
            "reason": "ad_or_spam",
        }

    uncertain_patterns = [
        "投诉",
        "举报",
        "退款",
        "售后",
        "维权",
        "平台介入",
        "骗",
    ]
    if any(pattern in text for pattern in uncertain_patterns):
        return {
            "action": REPLY_ACTION_HANDOFF,
            "reason": "needs_human_judgement",
        }

    return {
        "action": REPLY_ACTION_REPLY,
        "reason": "shopping_related_or_normal",
    }
