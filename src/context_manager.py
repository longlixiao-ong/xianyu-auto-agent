import sqlite3
import os
import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from loguru import logger


class ChatContextManager:
    """
    聊天上下文管理器
    
    负责存储和检索用户与商品之间的对话历史，使用SQLite数据库进行持久化存储。
    支持按会话ID检索对话历史，以及议价次数统计。
    """
    
    def __init__(self, max_history=100, db_path="data/chat_history.db"):
        """
        初始化聊天上下文管理器
        
        Args:
            max_history: 每个对话保留的最大消息数
            db_path: SQLite数据库文件路径
        """
        self.max_history = max_history
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """初始化数据库表结构"""
        # 确保数据库目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        conn = sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA journal_mode=WAL")
        
        # 创建消息表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            chat_id TEXT
        )
        ''')
        
        # 检查是否需要添加chat_id字段（兼容旧数据库）
        cursor.execute("PRAGMA table_info(messages)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'chat_id' not in columns:
            cursor.execute('ALTER TABLE messages ADD COLUMN chat_id TEXT')
            logger.info("已为messages表添加chat_id字段")
        
        # 创建索引以加速查询
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_user_item ON messages (user_id, item_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_chat_id ON messages (chat_id)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_timestamp ON messages (timestamp)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_chat_timestamp ON messages (chat_id, timestamp)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_item_chat ON messages (item_id, chat_id)
        ''')
        
        # 创建基于会话ID的议价次数表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_bargain_counts (
            chat_id TEXT PRIMARY KEY,
            count INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 创建商品信息表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            item_id TEXT PRIMARY KEY,
            data TEXT NOT NULL,
            price REAL,
            description TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # 创建会话级图片观察结果表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_image_observations (
            chat_id TEXT PRIMARY KEY,
            observation TEXT NOT NULL,
            image_urls TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat_runtime_states (
            chat_id TEXT PRIMARY KEY,
            item_id TEXT,
            purchase_status TEXT DEFAULT 'not_purchased',
            purchase_confidence TEXT DEFAULT 'low',
            reply_decision TEXT,
            last_reason TEXT,
            details TEXT,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS manual_review_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            item_id TEXT,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            details TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_manual_review_active
        ON manual_review_queue (chat_id, reason, status)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_manual_review_status_updated
        ON manual_review_queue (status, updated_at DESC)
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"聊天历史数据库初始化完成: {self.db_path}")
        
    def _connect(self):
        return sqlite3.connect(self.db_path, timeout=10.0, check_same_thread=False)

    @contextmanager
    def _txn(self, retries=3):
        conn = self._connect()
        for attempt in range(retries):
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < retries - 1:
                    conn.rollback()
                    time.sleep(0.1 * (attempt + 1))
                else:
                    conn.rollback()
                    raise
        finally:
            conn.close()
        

            
    def save_item_info(self, item_id, item_data):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                price = float(item_data.get('soldPrice', 0))
                description = item_data.get('desc', '')
                data_json = json.dumps(item_data, ensure_ascii=False)
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    INSERT INTO items (item_id, data, price, description, last_updated) 
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(item_id) 
                    DO UPDATE SET data = ?, price = ?, description = ?, last_updated = ?
                    """,
                    (
                        item_id, data_json, price, description, now,
                        data_json, price, description, now
                    )
                )
            logger.debug(f"商品信息已保存: {item_id}")
        except Exception as e:
            logger.error(f"保存商品信息时出错: {e}")
    
    def get_item_info(self, item_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data FROM items WHERE item_id = ?", (item_id,))
                result = cursor.fetchone()
                return json.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"获取商品信息时出错: {e}")
            return None

    def add_message_by_chat(self, chat_id, user_id, item_id, role, content):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO messages (user_id, item_id, role, content, timestamp, chat_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, item_id, role, content, datetime.now(timezone.utc).isoformat(), chat_id)
                )
                cursor.execute(
                    """
                    SELECT id FROM messages 
                    WHERE chat_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT ?, 1
                    """, 
                    (chat_id, self.max_history)
                )
                oldest_to_keep = cursor.fetchone()
                if oldest_to_keep:
                    cursor.execute(
                        "DELETE FROM messages WHERE chat_id = ? AND id < ?",
                        (chat_id, oldest_to_keep[0])
                    )
        except Exception as e:
            logger.error(f"添加消息到数据库时出错: {e}")

    def get_context_by_chat(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT role, content FROM messages 
                    WHERE chat_id = ? 
                    ORDER BY timestamp ASC
                    LIMIT ?
                    """, 
                    (chat_id, self.max_history)
                )
                messages = [{"role": role, "content": content} for role, content in cursor.fetchall()]
            bargain_count = self.get_bargain_count_by_chat(chat_id)
            if bargain_count > 0:
                messages.append({"role": "system", "content": f"议价次数: {bargain_count}"})
            return messages
        except Exception as e:
            logger.error(f"获取对话历史时出错: {e}")
            return []

    def increment_bargain_count_by_chat(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    INSERT INTO chat_bargain_counts (chat_id, count, last_updated)
                    VALUES (?, 1, ?)
                    ON CONFLICT(chat_id) 
                    DO UPDATE SET count = count + 1, last_updated = ?
                    """,
                    (chat_id, now, now)
                )
            logger.debug(f"会话 {chat_id} 议价次数已增加")
        except Exception as e:
            logger.error(f"增加议价次数时出错: {e}")

    def get_bargain_count_by_chat(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT count FROM chat_bargain_counts WHERE chat_id = ?", (chat_id,))
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"获取议价次数时出错: {e}")
            return 0

    def save_image_observation_by_chat(self, chat_id, observation, image_urls=None):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                image_urls_json = json.dumps(image_urls or [], ensure_ascii=False)
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    INSERT INTO chat_image_observations (chat_id, observation, image_urls, last_updated)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id)
                    DO UPDATE SET observation = ?, image_urls = ?, last_updated = ?
                    """,
                    (chat_id, observation, image_urls_json, now, observation, image_urls_json, now)
                )
        except Exception as e:
            logger.error(f"保存图片观察结果时出错: {e}")

    def get_image_observation_by_chat(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT observation, image_urls, last_updated
                    FROM chat_image_observations
                    WHERE chat_id = ?
                    """,
                    (chat_id,)
                )
                result = cursor.fetchone()
                if not result:
                    return None
                observation, image_urls_json, last_updated = result
                return {
                    "observation": observation,
                    "image_urls": json.loads(image_urls_json or "[]"),
                    "last_updated": last_updated,
                }
        except Exception as e:
            logger.error(f"获取图片观察结果时出错: {e}")
            return None

    def clear_image_observation_by_chat(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM chat_image_observations WHERE chat_id = ?", (chat_id,))
        except Exception as e:
            logger.error(f"清除图片观察结果时出错: {e}")

    def upsert_chat_runtime_state(
        self, chat_id, item_id, purchase_status="not_purchased",
        purchase_confidence="low", reply_decision=None, last_reason=None, details=None,
    ):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                details_json = json.dumps(details or {}, ensure_ascii=False)
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    INSERT INTO chat_runtime_states (
                        chat_id, item_id, purchase_status, purchase_confidence,
                        reply_decision, last_reason, details, last_updated
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        item_id = excluded.item_id,
                        purchase_status = excluded.purchase_status,
                        purchase_confidence = excluded.purchase_confidence,
                        reply_decision = excluded.reply_decision,
                        last_reason = excluded.last_reason,
                        details = excluded.details,
                        last_updated = excluded.last_updated
                    """,
                    (chat_id, item_id, purchase_status, purchase_confidence,
                     reply_decision, last_reason, details_json, now)
                )
        except Exception as e:
            logger.error(f"更新会话运行态时出错: {e}")

    def get_chat_runtime_state(self, chat_id):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT item_id, purchase_status, purchase_confidence, reply_decision,
                           last_reason, details, last_updated
                    FROM chat_runtime_states
                    WHERE chat_id = ?
                    """,
                    (chat_id,)
                )
                result = cursor.fetchone()
                if not result:
                    return None
                return {
                    "item_id": result[0],
                    "purchase_status": result[1],
                    "purchase_confidence": result[2],
                    "reply_decision": result[3],
                    "last_reason": result[4],
                    "details": json.loads(result[5] or "{}"),
                    "last_updated": result[6],
                }
        except Exception as e:
            logger.error(f"获取会话运行态时出错: {e}")
            return None

    def enqueue_manual_review(self, chat_id, item_id, reason, details=None, status="pending"):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                details_json = json.dumps(details or {}, ensure_ascii=False)
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    """
                    SELECT id FROM manual_review_queue
                    WHERE chat_id = ? AND reason = ? AND status = 'pending'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (chat_id, reason)
                )
                existing = cursor.fetchone()
                if existing:
                    cursor.execute(
                        """
                        UPDATE manual_review_queue
                        SET item_id = ?, details = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (item_id, details_json, now, existing[0])
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO manual_review_queue
                        (chat_id, item_id, reason, status, details, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (chat_id, item_id, reason, status, details_json, now, now)
                    )
        except Exception as e:
            logger.error(f"加入人工待处理队列时出错: {e}")

    def get_manual_review_items(self, status="pending"):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT chat_id, item_id, reason, status, details, created_at, updated_at
                    FROM manual_review_queue
                    WHERE status = ?
                    ORDER BY updated_at DESC
                    """,
                    (status,)
                )
                rows = cursor.fetchall()
                return [
                    {
                        "chat_id": row[0], "item_id": row[1], "reason": row[2],
                        "status": row[3], "details": json.loads(row[4] or "{}"),
                        "created_at": row[5], "updated_at": row[6],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"读取人工待处理队列时出错: {e}")
            return []

    def update_manual_review_status(self, review_id, new_status):
        """更新人工队列项的状态"""
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                now = datetime.now(timezone.utc).isoformat()
                cursor.execute(
                    "UPDATE manual_review_queue SET status = ?, updated_at = ? WHERE id = ?",
                    (new_status, now, review_id)
                )
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"更新人工队列状态时出错: {e}")
            return False

    def list_chat_runtime_states(self, limit=50):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT chat_id, item_id, purchase_status, purchase_confidence,
                           reply_decision, last_reason, details, last_updated
                    FROM chat_runtime_states
                    ORDER BY last_updated DESC
                    LIMIT ?
                    """,
                    (limit,)
                )
                rows = cursor.fetchall()
                return [
                    {
                        "chat_id": row[0], "item_id": row[1], "purchase_status": row[2],
                        "purchase_confidence": row[3], "reply_decision": row[4],
                        "last_reason": row[5], "details": json.loads(row[6] or "{}"),
                        "last_updated": row[7],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"读取最近会话运行态时出错: {e}")
            return []

    def list_recent_image_observations(self, limit=50):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT chat_id, observation, image_urls, last_updated
                    FROM chat_image_observations
                    ORDER BY last_updated DESC
                    LIMIT ?
                    """,
                    (limit,)
                )
                rows = cursor.fetchall()
                return [
                    {
                        "chat_id": row[0], "observation": row[1],
                        "image_urls": json.loads(row[2] or "[]"), "last_updated": row[3],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"读取最近图片观察记录时出错: {e}")
            return []

    # ── 消息记录查询 ──────────────────────────────────

    def list_conversations(self, item_id=None, limit=50, offset=0):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                if item_id:
                    cursor.execute(
                        "SELECT m.chat_id, m.item_id, MAX(m.timestamp) as last_ts, COUNT(*) as msg_count, "
                        "MAX(CASE WHEN dc.delivered > 0 THEN 1 ELSE 0 END) as auto_delivered "
                        "FROM messages m "
                        "LEFT JOIN ("
                        "  SELECT chat_id, COUNT(*) as delivered "
                        "  FROM cards WHERE used = 1 AND delivery_status = 1 GROUP BY chat_id"
                        ") dc ON m.chat_id = dc.chat_id "
                        "WHERE m.item_id = ? "
                        "GROUP BY m.chat_id ORDER BY last_ts DESC LIMIT ? OFFSET ?",
                        (item_id, limit, offset)
                    )
                else:
                    cursor.execute(
                        "SELECT m.chat_id, m.item_id, MAX(m.timestamp) as last_ts, COUNT(*) as msg_count, "
                        "MAX(CASE WHEN dc.delivered > 0 THEN 1 ELSE 0 END) as auto_delivered "
                        "FROM messages m "
                        "LEFT JOIN ("
                        "  SELECT chat_id, COUNT(*) as delivered "
                        "  FROM cards WHERE used = 1 AND delivery_status = 1 GROUP BY chat_id"
                        ") dc ON m.chat_id = dc.chat_id "
                        "GROUP BY m.chat_id ORDER BY last_ts DESC LIMIT ? OFFSET ?",
                        (limit, offset)
                    )
                rows = cursor.fetchall()
                return [
                    {
                        "chat_id": row[0],
                        "item_id": row[1] or "",
                        "last_message_at": row[2],
                        "message_count": row[3],
                        "auto_delivered": bool(row[4]),
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"获取会话列表失败: {e}")
            return []

    def get_conversation_detail(self, chat_id, limit=200, offset=0):
        try:
            with self._txn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT role, content, timestamp FROM messages "
                    "WHERE chat_id = ? ORDER BY timestamp ASC LIMIT ? OFFSET ?",
                    (chat_id, limit, offset)
                )
                rows = cursor.fetchall()
                return [
                    {"role": row[0], "content": row[1], "timestamp": row[2]}
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"获取会话详情失败: {e}")
            return []
