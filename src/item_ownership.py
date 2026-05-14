import json
import os
import time
from pathlib import Path
from loguru import logger
from admin_service import RUNTIME_CONFIG_DEFAULTS


class ItemOwnershipMixin:

    def refresh_selling_items_snapshot(self):
        try:
            logger.info("开始刷新商品列表快照...")
            payload = self.xianyu.get_my_items(
                output_path=self.selling_items_snapshot_path,
                headless=os.getenv("SELLING_ITEMS_BROWSER_HEADLESS", RUNTIME_CONFIG_DEFAULTS["SELLING_ITEMS_BROWSER_HEADLESS"]).lower() == "true",
            )
            with self._owned_items_lock:
                self.owned_item_ids = self.extract_owned_item_ids(payload)
            logger.info(f"商品列表快照已刷新，共 {payload.get('item_count', 0)} 条")
            return payload
        except Exception as e:
            logger.warning(f"刷新商品列表快照失败，但不会影响消息服务：{e}")
            return None

    @staticmethod
    def extract_owned_item_ids(snapshot_payload):
        owned_item_ids = set()
        if not isinstance(snapshot_payload, dict):
            return owned_item_ids
        for item in snapshot_payload.get("items", []) or []:
            item_id = str((item or {}).get("item_id") or "").strip()
            if item_id:
                owned_item_ids.add(item_id)
        return owned_item_ids

    def load_owned_item_ids(self):
        snapshot_candidates = [
            self.selling_items_snapshot_path,
            self.legacy_selling_items_snapshot_path,
        ]
        for idx, snapshot_path in enumerate(snapshot_candidates):
            if not snapshot_path or not os.path.exists(snapshot_path):
                continue
            try:
                with open(snapshot_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                owned_item_ids = self.extract_owned_item_ids(payload)
                if idx == 0:
                    logger.info(f"已加载主商品归属快照 {snapshot_path}，共 {len(owned_item_ids)} 个商品ID")
                    return owned_item_ids
                if owned_item_ids:
                    logger.info(f"已加载商品归属快照 {snapshot_path}，共 {len(owned_item_ids)} 个商品ID")
                    return owned_item_ids
            except Exception as e:
                logger.warning(f"加载商品归属快照失败 {snapshot_path}: {e}")
        logger.warning("未找到可用的商品归属快照，将退化为依赖商品详情中的卖家ID校验")
        return set()

    def get_runtime_status_file(self):
        status_path = Path(self.xianyu.runtime_status_path)
        if not status_path.exists():
            return {"status": "missing", "message": "runtime_status.json 不存在"}
        try:
            return json.loads(status_path.read_text(encoding="utf-8"))
        except Exception as e:
            return {"status": "read_error", "message": str(e)}

    def get_snapshot_status(self):
        snapshot_path = Path(self.selling_items_snapshot_path)
        if not snapshot_path.exists():
            with self._owned_items_lock:
                owned_count = len(self.owned_item_ids)
            return {"path": str(snapshot_path), "exists": False, "item_count": owned_count}
        try:
            payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
            item_count = payload.get("item_count")
            if item_count is None:
                item_count = len(payload.get("items", []) or [])
            return {
                "path": str(snapshot_path),
                "exists": True,
                "item_count": item_count,
                "last_updated": payload.get("generated_at") or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot_path.stat().st_mtime)),
                "items": (payload.get("items") or [])[:20],
            }
        except Exception as e:
            with self._owned_items_lock:
                owned_count = len(self.owned_item_ids)
            return {"path": str(snapshot_path), "exists": True, "item_count": owned_count, "error": str(e)}

    def is_owned_item(self, item_id, item_info=None):
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return False

        with self._owned_items_lock:
            if normalized_item_id in self.owned_item_ids:
                return True

            seller_id = self.extract_seller_id(item_info)
            if seller_id and seller_id == self.myid:
                self.owned_item_ids.add(normalized_item_id)
                return True

        return False

    @staticmethod
    def extract_seller_id(item_info):
        if not isinstance(item_info, dict):
            return None

        track_params = item_info.get("trackParams") or {}
        seller_id = str(track_params.get("sellerId") or "").strip()
        if seller_id:
            return seller_id

        for key in ("sellerId", "seller_id", "userId", "user_id"):
            value = str(item_info.get(key) or "").strip()
            if value:
                return value

        seller_data = item_info.get("sellerDO") or item_info.get("userDO") or {}
        if isinstance(seller_data, dict):
            for key in ("sellerId", "seller_id", "userId", "user_id", "id"):
                value = str(seller_data.get(key) or "").strip()
                if value:
                    return value

        return None

    @staticmethod
    def format_price(price):
        try:
            return round(float(price) / 100, 2)
        except (ValueError, TypeError):
            return 0.0

    def build_item_description(self, item_info):
        clean_skus = []
        raw_sku_list = item_info.get('skuList', [])

        for sku in raw_sku_list:
            specs = [p['valueText'] for p in sku.get('propertyList', []) if p.get('valueText')]
            spec_text = " ".join(specs) if specs else "默认规格"
            clean_skus.append({
                "spec": spec_text,
                "price": self.format_price(sku.get('price', 0)),
                "stock": sku.get('quantity', 0)
            })

        valid_prices = [s['price'] for s in clean_skus if s['price'] > 0]

        if valid_prices:
            min_price = min(valid_prices)
            max_price = max(valid_prices)
            if min_price == max_price:
                price_display = f"¥{min_price}"
            else:
                price_display = f"¥{min_price} - ¥{max_price}"
        else:
            main_price = round(float(item_info.get('soldPrice', 0)), 2)
            price_display = f"¥{main_price}"

        summary = {
            "title": item_info.get('title', ''),
            "desc": item_info.get('desc', ''),
            "price_range": price_display,
            "total_stock": item_info.get('quantity', 0),
            "sku_details": clean_skus
        }

        return json.dumps(summary, ensure_ascii=False)
