import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from loguru import logger


class CardsManager:

    def __init__(self, db_path="data/chat_history.db"):
        self.db_path = db_path
        self._locks_mutex = threading.Lock()
        self._claim_locks = {}
        self._init_db()

    def _init_db(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS virtual_items (
            item_id       TEXT PRIMARY KEY,
            label         TEXT,
            delivery_mode TEXT DEFAULT 'stock',
            fixed_content TEXT DEFAULT '',
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # 兼容旧表结构迁移
        cursor.execute("PRAGMA table_info(virtual_items)")
        cols = [c[1] for c in cursor.fetchall()]
        if 'delivery_mode' not in cols:
            cursor.execute("ALTER TABLE virtual_items ADD COLUMN delivery_mode TEXT DEFAULT 'stock'")
        if 'fixed_content' not in cols:
            cursor.execute("ALTER TABLE virtual_items ADD COLUMN fixed_content TEXT DEFAULT ''")

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS cards (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id         TEXT NOT NULL,
            fields          TEXT NOT NULL,
            used            INTEGER NOT NULL DEFAULT 0,
            chat_id         TEXT,
            used_at         DATETIME,
            delivery_status INTEGER DEFAULT 0,
            refund_status   INTEGER DEFAULT 0,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_cards_item ON cards (item_id, used)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_cards_used_at ON cards (used, used_at DESC)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_cards_chat_item ON cards (chat_id, item_id)
        ''')

        # 迁移旧表
        cursor.execute("PRAGMA table_info(cards)")
        card_cols = [c[1] for c in cursor.fetchall()]
        if 'delivery_status' not in card_cols:
            cursor.execute("ALTER TABLE cards ADD COLUMN delivery_status INTEGER DEFAULT 0")
        if 'refund_status' not in card_cols:
            cursor.execute("ALTER TABLE cards ADD COLUMN refund_status INTEGER DEFAULT 0")

        # 发货任务表，保证 chat_id + item_id 唯一
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS delivery_jobs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    TEXT NOT NULL,
            item_id    TEXT NOT NULL,
            mode       TEXT,
            status     TEXT NOT NULL DEFAULT 'pending',
            card_id    INTEGER,
            error      TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chat_id, item_id)
        )
        ''')

        conn.commit()
        conn.close()
        logger.debug("卡密数据库表初始化完成")

    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)

    @contextmanager
    def _read_txn(self):
        """读操作事务，不加写锁"""
        conn = self._connect()
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _txn(self):
        """写操作事务，使用 BEGIN IMMEDIATE"""
        conn = self._connect()
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _run_with_retry(self, fn, retries=3):
        """带重试的写操作执行"""
        last_error = None
        for attempt in range(retries):
            try:
                with self._txn() as conn:
                    return fn(conn)
            except sqlite3.OperationalError as e:
                last_error = e
                if "locked" not in str(e).lower() or attempt == retries - 1:
                    raise
                time.sleep(0.1 * (attempt + 1))
        raise last_error

    # ── 虚拟商品注册 ──────────────────────────────

    MAX_FIXED_CONTENT_BYTES = 10 * 1024

    def register_virtual_item(self, item_id, label="", delivery_mode="stock", fixed_content=""):
        if delivery_mode not in ("stock", "fixed"):
            raise ValueError(f"无效的发货模式: {delivery_mode}，仅支持 stock 或 fixed")
        if delivery_mode == "fixed" and fixed_content and len(fixed_content.encode("utf-8")) > MAX_FIXED_CONTENT_BYTES:
            raise ValueError(f"固定内容过长 ({len(fixed_content.encode('utf-8'))} 字节，上限 {MAX_FIXED_CONTENT_BYTES})")
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO virtual_items (item_id, label, delivery_mode, fixed_content, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (item_id, label or "", delivery_mode, fixed_content or "", datetime.now(timezone.utc).isoformat())
                )
            return {"status": "registered", "item_id": item_id, "delivery_mode": delivery_mode}
        except Exception as e:
            logger.error(f"注册虚拟商品失败: {e}")
            raise

    def unregister_virtual_item(self, item_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM virtual_items WHERE item_id = ?", (item_id,))
            return {"status": "unregistered", "item_id": item_id}
        except Exception as e:
            logger.error(f"取消注册虚拟商品失败: {e}")
            raise

    def list_virtual_items(self):
        try:
            with self._read_txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT v.item_id, v.label, v.delivery_mode, v.fixed_content, "
                    "c.total, c.available "
                    "FROM virtual_items v "
                    "LEFT JOIN ("
                    "  SELECT item_id, COUNT(*) as total, SUM(CASE WHEN used=0 THEN 1 ELSE 0 END) as available "
                    "  FROM cards GROUP BY item_id"
                    ") c ON v.item_id = c.item_id "
                    "ORDER BY v.created_at DESC"
                )
                rows = cursor.fetchall()
                return [
                    {
                        "item_id": row[0],
                        "label": row[1] or "",
                        "delivery_mode": row[2] or "stock",
                        "fixed_content": row[3] or "",
                        "total": row[4] or 0,
                        "available": row[5] or 0,
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"获取虚拟商品列表失败: {e}")
            return []

    # ── 凭证导入 ──────────────────────────────────

    def import_cards(self, item_id, raw_text):
        raw_text = (raw_text or "").strip()
        if not raw_text:
            raise ValueError("导入内容不能为空")

        with self._txn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_id, delivery_mode FROM virtual_items WHERE item_id = ?", (item_id,))
            vi = cursor.fetchone()
            if not vi:
                raise ValueError(f"商品 {item_id} 未注册为虚拟商品，请先注册后再导入")
            if vi[1] == "fixed":
                raise ValueError(f"商品 {item_id} 是固定内容模式，不支持批量导入库存")

        lines = [ln.strip() for ln in raw_text.split("\n") if ln.strip()]
        if len(lines) < 2:
            raise ValueError("至少需要字段名行和一条数据行")

        field_names = [f.strip() for f in lines[0].split("----")]
        if not field_names or not any(field_names):
            raise ValueError("首行必须包含至少一个有效字段名")
        if len(field_names) != len(set(field_names)):
            raise ValueError("字段名有重复")

        imported = 0
        skipped = 0
        skipped_lines = []
        now = datetime.now(timezone.utc).isoformat()

        for idx, line in enumerate(lines[1:], start=2):
            values = [v.strip() for v in line.split("----")]
            if len(values) != len(field_names):
                skipped += 1
                skipped_lines.append(f"第{idx}行: 字段数={len(values)}，期望={len(field_names)}")
                continue
            fields_json = json.dumps(
                {field_names[i]: values[i] for i in range(len(field_names))},
                ensure_ascii=False
            )
            try:
                with self._txn() as conn:
                    cursor = conn.cursor()
                    # 检查是否已存在相同内容
                    cursor.execute(
                        "SELECT id FROM cards WHERE item_id = ? AND fields = ? AND used = 0",
                        (item_id, fields_json)
                    )
                    if cursor.fetchone():
                        skipped += 1
                        skipped_lines.append(f"第{idx}行: 重复数据，已跳过")
                        continue
                    cursor.execute(
                        "INSERT INTO cards (item_id, fields, created_at) VALUES (?, ?, ?)",
                        (item_id, fields_json, now)
                    )
                imported += 1
            except Exception as e:
                skipped += 1
                skipped_lines.append(f"第{idx}行: DB写入失败 ({e})")
                logger.warning(f"导入单条卡密失败: {e}")

        logger.info(f"商品 {item_id} 导入完成: {imported} 条, 跳过 {skipped} 条, 字段: {field_names}")
        if skipped_lines:
            logger.warning(f"跳过的行明细: {'; '.join(skipped_lines[:10])}")
        return {
            "status": "imported",
            "item_id": item_id,
            "imported_count": imported,
            "skipped_count": skipped,
            "skipped_lines": skipped_lines[:20],
            "fields": field_names,
        }

    # ── 库存与查询 ────────────────────────────────

    def get_stock(self, item_id=None):
        try:
            with self._read_txn() as conn:
                cursor = conn.cursor()
                if item_id:
                    cursor.execute(
                        "SELECT item_id, COUNT(*), SUM(CASE WHEN used=0 THEN 1 ELSE 0 END), "
                        "SUM(CASE WHEN used=1 THEN 1 ELSE 0 END) "
                        "FROM cards WHERE item_id = ? GROUP BY item_id",
                        (item_id,)
                    )
                else:
                    cursor.execute(
                        "SELECT item_id, COUNT(*), SUM(CASE WHEN used=0 THEN 1 ELSE 0 END), "
                        "SUM(CASE WHEN used=1 THEN 1 ELSE 0 END) "
                        "FROM cards GROUP BY item_id ORDER BY item_id"
                    )
                rows = cursor.fetchall()
                items = []
                for row in rows:
                    cursor.execute(
                        "SELECT fields FROM cards WHERE item_id = ? LIMIT 1", (row[0],)
                    )
                    sample = cursor.fetchone()
                    field_keys = list(json.loads(sample[0]).keys()) if sample else []
                    items.append({
                        "item_id": row[0],
                        "total": row[1],
                        "available": row[2] or 0,
                        "used": row[3] or 0,
                        "fields": field_keys,
                    })
                return {"items": items}
        except Exception as e:
            logger.error(f"获取库存失败: {e}")
            return {"items": []}

    def list_cards(self, item_id=None, used=None, limit=200):
        try:
            with self._read_txn() as conn:
                cursor = conn.cursor()
                conditions = []
                params = []

                if item_id:
                    conditions.append("item_id = ?")
                    params.append(item_id)
                if used is not None:
                    conditions.append("used = ?")
                    params.append(int(used))

                where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
                params.append(int(limit))

                cursor.execute(
                    f"SELECT id, item_id, fields, used, chat_id, used_at, created_at "
                    f"FROM cards {where} ORDER BY id ASC LIMIT ?",
                    params
                )
                rows = cursor.fetchall()
                return {
                    "items": [
                        {
                            "id": row[0],
                            "item_id": row[1],
                            "fields": json.loads(row[2]),
                            "used": row[3],
                            "chat_id": row[4],
                            "used_at": row[5],
                            "created_at": row[6],
                        }
                        for row in rows
                    ]
                }
        except Exception as e:
            logger.error(f"获取卡密列表失败: {e}")
            return {"items": []}

    # ── 发货任务管理 ─────────────────────────────

    def begin_delivery_job(self, chat_id, item_id):
        """创建发货任务，返回 True 表示成功，False 表示重复"""
        try:
            def _do(conn):
                c = conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                c.execute(
                    "INSERT INTO delivery_jobs (chat_id, item_id, status, created_at, updated_at) "
                    "VALUES (?, ?, 'pending', ?, ?)",
                    (chat_id, item_id, now, now)
                )
            self._run_with_retry(_do)
            return True
        except sqlite3.IntegrityError:
            return False

    def update_delivery_job(self, chat_id, item_id, status, card_id=None, error=None):
        """更新发货任务状态"""
        def _do(conn):
            c = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            c.execute(
                "UPDATE delivery_jobs SET status=?, card_id=?, error=?, updated_at=? "
                "WHERE chat_id=? AND item_id=?",
                (status, card_id, error, now, chat_id, item_id)
            )
        try:
            self._run_with_retry(_do)
        except Exception as e:
            logger.error(f"更新发货任务失败: {e}")

    def reset_delivery_job(self, chat_id, item_id):
        """重置发货任务状态为 pending，允许重新发货"""
        def _do(conn):
            c = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            c.execute(
                "UPDATE delivery_jobs SET status='pending', error=NULL, updated_at=? "
                "WHERE chat_id=? AND item_id=? AND status IN ('failed', 'pending')",
                (now, chat_id, item_id)
            )
        try:
            self._run_with_retry(_do)
            return True
        except Exception as e:
            logger.error(f"重置发货任务失败: {e}")
            return False

    # ── 消耗（后续自动回复集成用） ─────────────────

    def _get_item_lock(self, item_id):
        """获取商品级别的锁，防止并发发货"""
        with self._locks_mutex:
            if item_id not in self._claim_locks:
                self._claim_locks[item_id] = threading.Lock()
            return self._claim_locks[item_id]

    def claim_one(self, item_id, chat_id):
        """原子取一张未用卡密并标记已用。固定模式直接返回内容。"""
        mode = self._get_delivery_mode(item_id)
        if mode == "fixed":
            return {"mode": "fixed", "content": self._get_fixed_content(item_id)}
        # 使用商品级锁防止并发重复发货
        with self._get_item_lock(item_id):
            try:
                with self._txn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE cards SET used = 1, chat_id = ?, used_at = ?, delivery_status = 0 "
                        "WHERE id = (SELECT id FROM cards WHERE item_id = ? AND used = 0 LIMIT 1) "
                        "RETURNING id, fields",
                        (chat_id, datetime.now(timezone.utc).isoformat(), item_id)
                    )
                    row = cursor.fetchone()
                    if row:
                        return {"mode": "stock", "id": row[0], "fields": json.loads(row[1])}
                    return None
            except Exception as e:
                logger.error(f"取卡失败: {e}")
                raise

    def _get_delivery_mode(self, item_id):
        try:
            with self._read_txn() as conn:
                c = conn.cursor()
                c.execute("SELECT delivery_mode FROM virtual_items WHERE item_id = ?", (item_id,))
                r = c.fetchone()
                if not r:
                    raise ValueError(f"商品 {item_id} 未注册为虚拟商品")
                return r[0] or "stock"
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"获取发货模式失败: {e}")
            raise ValueError(f"无法获取商品 {item_id} 的发货模式: {e}") from e

    def _get_fixed_content(self, item_id):
        try:
            with self._read_txn() as conn:
                c = conn.cursor()
                c.execute("SELECT fixed_content FROM virtual_items WHERE item_id = ?", (item_id,))
                r = c.fetchone()
                return r[0] if r else ""
        except Exception:
            return ""

    def update_fixed_content(self, item_id, content):
        data = (content or "").encode("utf-8")
        if len(data) > self.MAX_FIXED_CONTENT_BYTES:
            raise ValueError(f"固定内容过长 ({len(data)} 字节，上限 {self.MAX_FIXED_CONTENT_BYTES})")
        try:
            with self._txn() as conn:
                c = conn.cursor()
                c.execute("UPDATE virtual_items SET fixed_content = ? WHERE item_id = ?",
                          (content or "", item_id))
            return {"status": "updated", "item_id": item_id}
        except Exception as e:
            logger.error(f"更新固定内容失败: {e}")
            raise

    # ── 发货记录 ────────────────────────────────────

    def mark_delivery_status(self, card_id, ok):
        try:
            with self._txn() as conn:
                c = conn.cursor()
                c.execute("UPDATE cards SET delivery_status = ? WHERE id = ?",
                          (1 if ok else -1, card_id))
        except Exception as e:
            logger.error(f"更新发货状态失败: {e}")

    def has_delivered(self, chat_id, item_id):
        """检查该 chat_id 对 item_id 是否已有发货成功或发货中的记录"""
        try:
            with self._read_txn() as conn:
                c = conn.cursor()
                c.execute(
                    "SELECT COUNT(*) FROM cards WHERE chat_id = ? AND item_id = ? AND used = 1 AND delivery_status IN (0, 1)",
                    (chat_id, item_id)
                )
                return c.fetchone()[0] > 0
        except Exception as e:
            logger.error(f"检查发货记录失败: {e}")
            return False

    def recover_stuck_deliveries(self, timeout_minutes=10):
        """恢复卡在发货中状态的记录，标记为失败并返回需要加入人工队列的信息"""
        try:
            from datetime import datetime, timedelta
            cutoff = (datetime.now() - timedelta(minutes=timeout_minutes)).isoformat()
            with self._txn() as conn:
                c = conn.cursor()
                # 查找超时的发货中记录
                c.execute(
                    "SELECT id, item_id, chat_id, used_at FROM cards "
                    "WHERE used = 1 AND delivery_status = 0 AND used_at < ?",
                    (cutoff,)
                )
                stuck_records = c.fetchall()
                if not stuck_records:
                    return []
                # 标记为失败
                stuck_ids = [r[0] for r in stuck_records]
                placeholders = ",".join("?" * len(stuck_ids))
                c.execute(
                    f"UPDATE cards SET delivery_status = -1 WHERE id IN ({placeholders})",
                    stuck_ids
                )
                logger.warning(f"已将 {len(stuck_records)} 条卡死的发货记录标记为失败")
                return [
                    {"card_id": r[0], "item_id": r[1], "chat_id": r[2], "used_at": r[3]}
                    for r in stuck_records
                ]
        except Exception as e:
            logger.error(f"恢复卡死发货记录失败: {e}")
            return []

    def record_fixed_delivery(self, item_id, chat_id, ok):
        """记录 fixed 模式的发货记录，插入一条 used=1 的记录用于追踪"""
        try:
            with self._txn() as conn:
                c = conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                c.execute(
                    "INSERT INTO cards (item_id, fields, used, chat_id, used_at, delivery_status) "
                    "VALUES (?, ?, 1, ?, ?, ?)",
                    (item_id, '{"mode":"fixed"}', chat_id, now, 1 if ok else -1)
                )
        except Exception as e:
            logger.error(f"记录 fixed 模式发货状态失败: {e}")

    def mark_refund_status(self, card_id, refunded=True):
        """标记退款状态"""
        try:
            with self._txn() as conn:
                c = conn.cursor()
                c.execute(
                    "UPDATE cards SET refund_status = ? WHERE id = ?",
                    (1 if refunded else 0, card_id)
                )
        except Exception as e:
            logger.error(f"更新退款状态失败: {e}")

    def get_refund_stats(self, days=30):
        """获取退款统计"""
        try:
            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            with self._read_txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) as total, "
                    "SUM(CASE WHEN refund_status = 1 THEN 1 ELSE 0 END) as refunded "
                    "FROM cards WHERE used = 1 AND used_at >= ?",
                    (cutoff,)
                )
                totals = cursor.fetchone()
                return {
                    "total_delivered": totals[0] or 0,
                    "total_refunded": totals[1] or 0,
                }
        except Exception as e:
            logger.error(f"获取退款统计失败: {e}")
            return {"total_delivered": 0, "total_refunded": 0}

    def list_delivery_log(self, item_id=None, limit=50):
        try:
            with self._read_txn() as conn:
                cursor = conn.cursor()
                if item_id:
                    cursor.execute(
                        "SELECT c.id, c.item_id, v.label, c.chat_id, v.delivery_mode, "
                        "c.delivery_status, c.used_at "
                        "FROM cards c "
                        "LEFT JOIN virtual_items v ON c.item_id = v.item_id "
                        "WHERE c.used = 1 AND c.item_id = ? "
                        "ORDER BY c.used_at DESC LIMIT ?",
                        (item_id, limit)
                    )
                else:
                    cursor.execute(
                        "SELECT c.id, c.item_id, v.label, c.chat_id, v.delivery_mode, "
                        "c.delivery_status, c.used_at "
                        "FROM cards c "
                        "LEFT JOIN virtual_items v ON c.item_id = v.item_id "
                        "WHERE c.used = 1 "
                        "ORDER BY c.used_at DESC LIMIT ?",
                        (limit,)
                    )
                rows = cursor.fetchall()
                return {
                    "items": [
                        {
                            "id": row[0],
                            "item_id": row[1],
                            "label": row[2] or "",
                            "chat_id": row[3],
                            "delivery_mode": row[4] or "stock",
                            "delivery_ok": row[5] == 1,
                            "used_at": row[6],
                        }
                        for row in rows
                    ]
                }
        except Exception as e:
            logger.error(f"获取发货记录失败: {e}")
            return {"items": []}

    def get_delivery_stats(self, days=30):
        """获取发货统计，包括每日发货量和成功率"""
        try:
            from datetime import datetime, timedelta, timezone
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            with self._read_txn() as conn:
                cursor = conn.cursor()
                # 每日发货统计
                cursor.execute(
                    "SELECT DATE(used_at) as date, COUNT(*) as total, "
                    "SUM(CASE WHEN delivery_status = 1 THEN 1 ELSE 0 END) as success, "
                    "SUM(CASE WHEN delivery_status = -1 THEN 1 ELSE 0 END) as failed "
                    "FROM cards WHERE used = 1 AND used_at >= ? "
                    "GROUP BY DATE(used_at) ORDER BY date",
                    (cutoff,)
                )
                daily_stats = cursor.fetchall()
                # 总体统计
                cursor.execute(
                    "SELECT COUNT(*) as total, "
                    "SUM(CASE WHEN delivery_status = 1 THEN 1 ELSE 0 END) as success, "
                    "SUM(CASE WHEN delivery_status = -1 THEN 1 ELSE 0 END) as failed "
                    "FROM cards WHERE used = 1 AND used_at >= ?",
                    (cutoff,)
                )
                totals = cursor.fetchone()
                # 补全空日期
                today = datetime.now(timezone.utc).date()
                daily_map = {row[0]: {"total": row[1], "success": row[2], "failed": row[3]} for row in daily_stats}
                filled_daily = []
                for i in range(days):
                    date = (today - timedelta(days=days-1-i)).isoformat()
                    stats = daily_map.get(date, {"total": 0, "success": 0, "failed": 0})
                    filled_daily.append({"date": date, **stats})
                return {
                    "daily": filled_daily,
                    "totals": {
                        "total": totals[0] or 0,
                        "success": totals[1] or 0,
                        "failed": totals[2] or 0,
                    }
                }
        except Exception as e:
            logger.error(f"获取发货统计失败: {e}")
            return {"daily": [], "totals": {"total": 0, "success": 0, "failed": 0}}
