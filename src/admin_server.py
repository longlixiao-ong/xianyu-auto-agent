import json
import os
import traceback
from http import HTTPStatus
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlparse

from loguru import logger


MAX_QUERY_LIMIT = 500

# 不需要鉴权的接口
PUBLIC_ENDPOINTS = {"/api/status/overview", "/api/status/ping"}


class AdminWSGIApp:
    """管理后台 WSGI 应用"""

    def __init__(self, service, static_dir):
        self.service = service
        self.static_dir = Path(static_dir)

    def __call__(self, environ, start_response):
        """WSGI 应用入口"""
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        query_string = environ.get("QUERY_STRING", "")
        query = parse_qs(query_string)

        try:
            # OPTIONS 预检请求不鉴权
            if method == "OPTIONS":
                status, headers, body = self._handle_options()
            # 所有 GET 请求不鉴权（本地 127.0.0.1 已足够安全）
            elif method == "GET":
                status, headers, body = self._handle_get(path, query)
            # PUT/POST 写操作需要鉴权
            elif method in ("PUT", "POST"):
                ok, code, msg = self._check_auth(environ)
                if not ok:
                    status, headers, body = self._json_response(
                        {"error": code, "message": msg}, HTTPStatus.UNAUTHORIZED
                    )
                elif method == "PUT":
                    payload = self._read_body(environ)
                    status, headers, body = self._handle_put(path, payload)
                else:
                    payload = self._read_body(environ)
                    status, headers, body = self._handle_post(path, payload)
            else:
                status, headers, body = self._json_response(
                    {"error": "method_not_allowed"}, HTTPStatus.METHOD_NOT_ALLOWED
                )
        except Exception as exc:
            logger.error(f"后台 {method} {path} 失败: {exc}\n{traceback.format_exc()}")
            status, headers, body = self._json_response(
                {"error": "server_error", "message": str(exc)},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        start_response(status, headers)
        return [body]

    def _read_body(self, environ):
        """读取请求体"""
        content_length = int(environ.get("CONTENT_LENGTH", 0))
        if content_length <= 0:
            return {}
        raw = environ["wsgi.input"].read(content_length).decode("utf-8")
        return json.loads(raw) if raw.strip() else {}

    def _json_response(self, payload, status_code=HTTPStatus.OK):
        """构建 JSON 响应"""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        status = f"{status_code.value} {status_code.phrase}"
        headers = [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
        ]
        return status, headers, body

    def _html_response(self, html_text, status_code=HTTPStatus.OK):
        """构建 HTML 响应"""
        body = html_text.encode("utf-8")
        status = f"{status_code.value} {status_code.phrase}"
        headers = [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ]
        return status, headers, body

    def _handle_options(self):
        """处理 CORS 预检请求"""
        status = "204 No Content"
        headers = [
            ("Access-Control-Allow-Origin", "*"),
            ("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS"),
            ("Access-Control-Allow-Headers", "Content-Type, Authorization"),
            ("Content-Length", "0"),
        ]
        return status, headers, b""

    def _check_auth(self, environ):
        """检查授权"""
        expected = (os.environ.get("ADMIN_API_TOKEN") or "").strip()
        if not expected:
            return (
                False,
                "admin_token_missing",
                "未配置环境变量 ADMIN_API_TOKEN，已拒绝操作。请在 .env 中设置 ADMIN_API_TOKEN 后重启进程。",
            )
        auth = (environ.get("HTTP_AUTHORIZATION") or "").strip()
        if not auth.lower().startswith("bearer "):
            return (
                False,
                "admin_auth_required",
                "需要请求头 Authorization: Bearer <与 .env 中 ADMIN_API_TOKEN 一致的值>",
            )
        if auth[7:].strip() != expected:
            return (False, "admin_auth_forbidden", "管理接口令牌无效")
        return (True, "", "")

    def _parse_limit(self, query, key="limit", default=50, maximum=MAX_QUERY_LIMIT):
        """解析分页参数"""
        try:
            raw = (query.get(key) or [str(default)])[0]
            limit = int(raw)
        except (ValueError, TypeError, IndexError):
            logger.warning("limit 参数解析失败，使用默认值 {}", default)
            return default
        return min(max(limit, 1), maximum)

    def _handle_get(self, path, query):
        """处理 GET 请求"""
        service = self.service

        if path == "/":
            index_path = self.static_dir / "index.html"
            return self._html_response(index_path.read_text(encoding="utf-8"))

        if path == "/api/status/overview":
            return self._json_response(service.get_overview())
        if path == "/api/status/logs":
            limit = self._parse_limit(query, default=120)
            return self._json_response(service.get_logs(limit=limit))
        if path == "/api/config/models":
            return self._json_response(service.get_model_config())
        if path == "/api/config/secrets":
            return self._json_response(service.get_secret_config())
        if path == "/api/config/runtime":
            return self._json_response(service.get_runtime_config())
        if path == "/api/prompts":
            return self._json_response(service.get_all_prompts())
        if path == "/api/review/manual-review":
            status = (query.get("status") or ["pending"])[0]
            return self._json_response(service.get_manual_review(status=status))
        if path == "/api/review/runtime-states":
            limit = self._parse_limit(query)
            return self._json_response(service.get_runtime_states(limit=limit))
        if path == "/api/review/image-observations":
            limit = self._parse_limit(query)
            return self._json_response(service.get_recent_image_observations(limit=limit))
        if path == "/api/cards/virtual-items":
            return self._json_response(service.get_virtual_items())
        if path == "/api/cards/stock":
            item_id = (query.get("item_id") or [None])[0]
            return self._json_response(service.get_cards_stock(item_id=item_id))
        if path == "/api/cards/list":
            item_id = (query.get("item_id") or [None])[0]
            used_raw = (query.get("used") or [None])[0]
            used = int(used_raw) if used_raw is not None else None
            limit = self._parse_limit(query, default=200)
            return self._json_response(service.get_cards_list(item_id=item_id, used=used, limit=limit))
        if path == "/api/cards/delivery-log":
            item_id = (query.get("item_id") or [None])[0]
            limit = self._parse_limit(query, default=50)
            return self._json_response(service.get_delivery_log(item_id=item_id, limit=limit))
        if path == "/api/cards/delivery-stats":
            try:
                days = int((query.get("days") or ["30"])[0])
            except (ValueError, TypeError):
                days = 30
            return self._json_response(service.get_delivery_stats(days=days))
        if path == "/api/cards/refund-stats":
            try:
                days = int((query.get("days") or ["30"])[0])
            except (ValueError, TypeError):
                days = 30
            return self._json_response(service.get_refund_stats(days=days))
        if path == "/api/messages/conversations":
            item_id = (query.get("item_id") or [None])[0]
            limit = self._parse_limit(query, default=50)
            try:
                offset = int((query.get("offset") or ["0"])[0])
            except (ValueError, TypeError):
                offset = 0
            return self._json_response(service.get_conversations(item_id=item_id, limit=limit, offset=offset))
        if path == "/api/messages/detail":
            chat_id = (query.get("chat_id") or [""])[0]
            if not chat_id:
                return self._json_response({"error": "chat_id_required"}, HTTPStatus.BAD_REQUEST)
            limit = self._parse_limit(query, default=200)
            try:
                offset = int((query.get("offset") or ["0"])[0])
            except (ValueError, IndexError):
                offset = 0
            return self._json_response(service.get_conversation_detail(chat_id, limit=limit, offset=offset))

        return self._json_response({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    def _handle_put(self, path, payload):
        """处理 PUT 请求"""
        service = self.service
        if path == "/api/config/models":
            return self._json_response(service.update_model_config(payload))
        if path == "/api/config/runtime":
            return self._json_response(service.update_runtime_config(payload))
        if path == "/api/config/secrets":
            return self._json_response(service.update_cookie_config(payload))
        if path.startswith("/api/prompts/"):
            prompt_name = path.rsplit("/", 1)[-1]
            return self._json_response(service.update_prompt(prompt_name, payload.get("content", "")))

        return self._json_response({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    def _handle_post(self, path, payload):
        """处理 POST 请求"""
        service = self.service
        if path == "/api/ops/reload-prompts":
            return self._json_response(service.reload_prompts())
        if path == "/api/ops/reload-runtime":
            return self._json_response(service.reload_runtime())
        if path == "/api/ops/refresh-items":
            return self._json_response(service.refresh_items())
        if path == "/api/ops/manual-mode":
            chat_id = str(payload.get("chat_id", "")).strip()
            if not chat_id:
                return self._json_response({"error": "chat_id_required"}, HTTPStatus.BAD_REQUEST)
            return self._json_response(service.toggle_manual_mode(chat_id))
        if path == "/api/ops/service/start":
            return self._json_response(service.start_service())
        if path == "/api/ops/service/stop":
            return self._json_response(service.stop_service())
        if path == "/api/cards/virtual-items":
            return self._json_response(service.register_virtual_item(payload))
        if path == "/api/cards/virtual-items/delete":
            return self._json_response(service.unregister_virtual_item(payload))
        if path == "/api/cards/import":
            return self._json_response(service.import_cards(payload))
        if path == "/api/cards/fixed-content":
            return self._json_response(service.update_fixed_content(payload))
        if path == "/api/review/update-status":
            review_id = payload.get("id")
            new_status = str(payload.get("status", "")).strip()
            if not review_id or not new_status:
                return self._json_response({"error": "id_and_status_required"}, HTTPStatus.BAD_REQUEST)
            return self._json_response(service.update_manual_review_status(review_id, new_status))

        return self._json_response({"error": "not_found"}, HTTPStatus.NOT_FOUND)


def start_admin_server(service, host="127.0.0.1", port=18061, static_dir="admin_static"):
    """启动管理后台服务器（使用 waitress）"""
    app = AdminWSGIApp(service, static_dir)

    def run_server():
        from waitress import serve
        serve(app, host=host, port=port, _quiet=True, threads=4)

    thread = Thread(target=run_server, name="xianyu-admin-server", daemon=True)
    thread.start()
    logger.info(f"本地后台已启动: http://{host}:{port}")
    return None, thread
