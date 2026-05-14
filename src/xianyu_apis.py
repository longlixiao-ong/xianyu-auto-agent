import time
import os
import sys
import json
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger
from selling_items import get_selling_items_via_browser
from utils.xianyu_utils import generate_sign, MTOP_APP_KEY, REG_APP_KEY
from admin_runtime import ENV_FILE_LOCK, safe_update_env
from admin_service import RUNTIME_CONFIG_DEFAULTS


class RiskControlError(Exception):
    """闲鱼风控导致无法继续获取 token。"""


class CookieInvalidError(Exception):
    """Cookie 失效或无法继续获取 token，应由上层处理而非退出进程。"""


class XianyuApis:
    def __init__(self):
        self.url = 'https://h5api.m.goofish.com/h5/mtop.taobao.idlemessage.pc.login.token/1.0/'
        self.runtime_status_path = os.getenv("RUNTIME_STATUS_PATH", os.path.join("data", "runtime_status.json"))
        self.risk_control_retry_interval = int(os.getenv("RISK_CONTROL_RETRY_INTERVAL", RUNTIME_CONFIG_DEFAULTS["RISK_CONTROL_RETRY_INTERVAL"]))
        self.session = requests.Session()
        # Ignore host-level proxy env (e.g. 127.0.0.1:9) for direct API calls.
        self.session.trust_env = False
        self.session.headers.update({
            'accept': 'application/json',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'no-cache',
            'origin': 'https://www.goofish.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.goofish.com/',
            'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        })

    @staticmethod
    def _normalize_item_list_card(card):
        if not isinstance(card, dict):
            return None

        # 新版 API 返回结构: card 被包装在 cardData 里
        data = card.get("cardData") or card.get("data") or card
        detail = data.get("detailParams") or data.get("detail") or {}

        item_id = str(data.get("itemId") or data.get("item_id") or data.get("id")
                       or detail.get("itemId") or detail.get("item_id") or "").strip()
        href = str(
            data.get("itemUrl")
            or data.get("jumpUrl")
            or data.get("targetUrl")
            or data.get("url")
            or ""
        ).strip()
        if href.startswith("/"):
            href = f"https://www.goofish.com{href}"
        if not href and item_id:
            href = f"https://www.goofish.com/item?id={item_id}"

        title = str(data.get("title") or data.get("itemTitle") or data.get("mainTitle")
                      or detail.get("title") or "(无标题)").strip() or "(无标题)"
        price = str(data.get("price") or data.get("itemPrice") or data.get("displayPrice")
                     or data.get("soldPrice") or detail.get("soldPrice")
                     or detail.get("price") or "").strip()
        image_url = str(data.get("imageUrl") or data.get("mainPic") or data.get("picUrl")
                        or detail.get("picUrl") or data.get("image") or "").strip()

        if image_url.startswith("//"):
            image_url = f"https:{image_url}"
        elif image_url.startswith("/"):
            image_url = f"https://www.goofish.com{image_url}"

        if not item_id and not href:
            return None

        return {
            "title": title[:120],
            "price": price[:40],
            "href": href,
            "item_id": item_id or None,
            "status_key": "selling",
            "status_label": "在售",
            "image_url": image_url,
        }

    def _session_cookie_string(self):
        return '; '.join([f"{cookie.name}={cookie.value}" for cookie in self.session.cookies])

    def _call_current_account_item_list_api(self, page_number=1, page_size=20):
        cookie_string = self._session_cookie_string()
        token_raw = str(self.session.cookies.get("_m_h5_tk") or "").strip()
        user_id = str(self.session.cookies.get("unb") or "").strip()
        if "_" not in token_raw:
            raise RuntimeError("缺少 _m_h5_tk，无法调用商品列表 API")
        if not user_id:
            raise RuntimeError("缺少 unb，无法调用商品列表 API")

        token = token_raw.split("_", 1)[0]
        params = {
            'jsv': '2.7.2',
            'appKey': MTOP_APP_KEY,
            't': str(int(time.time() * 1000)),
            'sign': '',
            'v': '1.0',
            'type': 'originaljson',
            'accountSite': 'xianyu',
            'dataType': 'json',
            'timeout': '20000',
            'api': 'mtop.idle.web.xyh.item.list',
            'sessionOption': 'AutoLoginOnly',
            'spm_cnt': 'a21ybx.personal.0.0',
        }
        data_val = json.dumps(
            {
                "needGroupInfo": True,
                "pageNumber": max(1, int(page_number)),
                "pageSize": max(1, int(page_size)),
                "userId": user_id,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        params['sign'] = hashlib.md5(
            f"{token}&{params['t']}&{MTOP_APP_KEY}&{data_val}".encode("utf-8")
        ).hexdigest()

        response = self.session.post(
            'https://h5api.m.goofish.com/h5/mtop.idle.web.xyh.item.list/1.0/',
            params=params,
            data={'data': data_val},
            headers={
                'origin': 'https://www.goofish.com',
                'referer': f'https://www.goofish.com/personal?userId={user_id}',
                'x-requested-with': 'XMLHttpRequest',
                'cookie': cookie_string,
            },
        )
        logger.debug(f"[商品列表API] 状态码: {response.status_code}, URL: {response.url}")
        logger.debug(f"[商品列表API] 请求参数: data={data_val[:200]}")
        res_json = response.json()
        body_preview = json.dumps(res_json, ensure_ascii=False)[:2000]
        logger.debug(f"[商品列表API] 响应体(前2000): {body_preview}")
        ret_value = res_json.get('ret', []) if isinstance(res_json, dict) else []
        if not any('SUCCESS::调用成功' in ret for ret in ret_value):
            raise RuntimeError(f"商品列表API调用失败: {ret_value}")
        data_payload = res_json.get("data") or {}
        if not isinstance(data_payload, dict):
            raise RuntimeError("商品列表API返回 data 结构异常")
        return data_payload

    def get_my_items_via_live_api(self, output_path=None, page_size=20):
        data_payload = self._call_current_account_item_list_api(page_number=1, page_size=page_size)
        logger.info(f"[商品列表] data_payload type={type(data_payload).__name__}, keys={list(data_payload.keys()) if isinstance(data_payload, dict) else 'N/A'}")
        group_list = data_payload.get("itemGroupList") or []
        selling_count = 0
        for group in group_list:
            if not isinstance(group, dict):
                continue
            if str(group.get("groupName") or "").strip() == "在售":
                try:
                    selling_count = int(group.get("itemNumber") or 0)
                except (TypeError, ValueError):
                    selling_count = 0
                break

        items = []
        raw_cards = data_payload.get("cardList") or []
        logger.info(f"[商品列表API] cardList 长度={len(raw_cards)}, totalCount={data_payload.get('totalCount')}")
        if raw_cards:
            sample = raw_cards[0]
            if isinstance(sample, dict):
                top_keys = list(sample.keys())
                cd = sample.get("cardData") or sample.get("data")
                cd_keys = list(cd.keys()) if isinstance(cd, dict) else f"not dict ({type(cd).__name__})"
                dp = sample.get("detailParams") or (cd.get("detailParams") if isinstance(cd, dict) else None)
                dp_keys = list(dp.keys()) if isinstance(dp, dict) else "N/A"
                logger.info(f"[商品列表API] 首张卡片结构: top={top_keys}, cardData={cd_keys}, detailParams={dp_keys}")
        for card in raw_cards:
            if not isinstance(card, dict):
                logger.warning(f"[商品列表API] cardList 中包含非 dict 条目: {type(card)}")
                continue
            normalized = self._normalize_item_list_card(card)
            if normalized:
                items.append(normalized)

        payload = {
            "fetched_at": datetime.now().isoformat(),
            "item_count": len(items),
            "items": items,
            "section_counts": {"selling": selling_count, "offline": 0, "draft": 0},
            "metadata": {
                "source": "personal_page_api",
                "user_id": str(self.session.cookies.get("unb") or "").strip(),
                "total_count": data_payload.get("totalCount"),
                "next_page": data_payload.get("nextPage"),
                "next_page_model": data_payload.get("nextPageModel"),
                "next_page_num": data_payload.get("nextPageNum"),
            },
        }

        if output_path:
            parent = os.path.dirname(output_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")

        return payload

    def _is_interactive(self):
        try:
            return sys.stdin is not None and sys.stdin.isatty()
        except Exception:
            return False

    def write_runtime_status(self, status, message, extra=None):
        try:
            parent = os.path.dirname(self.runtime_status_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            payload = {
                "status": status,
                "message": message,
                "updated_at": datetime.now().isoformat(),
            }
            if extra:
                payload["details"] = extra
            with open(self.runtime_status_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
        except Exception as e:
            logger.warning(f"写入运行状态失败: {e}")
        
    def clear_duplicate_cookies(self):
        """清理重复的cookies"""
        # 创建一个新的CookieJar
        new_jar = requests.cookies.RequestsCookieJar()
        
        # 记录已经添加过的cookie名称
        added_cookies = set()
        
        # 按照cookies列表的逆序遍历（最新的通常在后面）
        cookie_list = list(self.session.cookies)
        cookie_list.reverse()
        
        for cookie in cookie_list:
            # 如果这个cookie名称还没有添加过，就添加到新jar中
            if cookie.name not in added_cookies:
                new_jar.set_cookie(cookie)
                added_cookies.add(cookie.name)
                
        # 替换session的cookies
        self.session.cookies = new_jar
        
        # 更新完cookies后，更新.env文件
        self.update_env_cookies()
        
    def update_env_cookies(self):
        """更新.env文件中的COOKIES_STR"""
        try:
            cookie_str = '; '.join([f"{cookie.name}={cookie.value}" for cookie in self.session.cookies])
            env_path = Path(os.getcwd()) / '.env'
            if not env_path.exists():
                logger.warning(".env文件不存在，无法更新COOKIES_STR")
                return
            safe_update_env(env_path, {"COOKIES_STR": cookie_str})
            logger.debug("已更新.env文件中的COOKIES_STR")
        except Exception as e:
            logger.warning(f"更新.env文件失败: {str(e)}")
        
    def hasLogin(self, retry_count=0):
        """调用hasLogin.do接口进行登录状态检查"""
        if retry_count >= 2:
            logger.error("Login检查失败，重试次数过多")
            return False
            
        try:
            url = 'https://passport.goofish.com/newlogin/hasLogin.do'
            params = {
                'appName': 'xianyu',
                'fromSite': '77'
            }
            data = {
                'hid': self.session.cookies.get('unb', ''),
                'ltl': 'true',
                'appName': 'xianyu',
                'appEntrance': 'web',
                '_csrf_token': self.session.cookies.get('XSRF-TOKEN', ''),
                'umidToken': '',
                'hsiz': self.session.cookies.get('cookie2', ''),
                'bizParams': 'taobaoBizLoginFrom=web',
                'mainPage': 'false',
                'isMobile': 'false',
                'lang': 'zh_CN',
                'returnUrl': '',
                'fromSite': '77',
                'isIframe': 'true',
                'documentReferer': 'https://www.goofish.com/',
                'defaultView': 'hasLogin',
                'umidTag': 'SERVER',
                'deviceId': self.session.cookies.get('cna', '')
            }
            
            response = self.session.post(url, params=params, data=data)
            res_json = response.json()
            
            if res_json.get('content', {}).get('success'):
                logger.debug("Login成功")
                # 清理和更新cookies
                self.clear_duplicate_cookies()
                self.write_runtime_status("login_ok", "闲鱼网页登录态有效")
                return True
            else:
                logger.warning(f"Login失败: {res_json}")
                self.write_runtime_status("login_failed", "闲鱼网页登录态无效", {"response": res_json})
                time.sleep(0.5)
                return self.hasLogin(retry_count + 1)
                
        except Exception as e:
            logger.error(f"Login请求异常: {str(e)}")
            self.write_runtime_status("login_error", "闲鱼登录检查异常", {"error": str(e)})
            time.sleep(0.5)
            return self.hasLogin(retry_count + 1)

    def get_token(self, device_id, retry_count=0):
        if retry_count >= 2:  # 最多重试3次
            logger.warning("获取token失败，尝试重新登陆")
            # 尝试通过hasLogin重新登录
            if self.hasLogin():
                logger.info("重新登录成功，重新尝试获取token")
                return self.get_token(device_id, 0)  # 重置重试次数
            else:
                logger.error("重新登录失败，Cookie已失效")
                logger.error("🔴 程序即将退出，请更新.env文件中的COOKIES_STR后重新启动")
                self.write_runtime_status(
                    "cookie_invalid",
                    "重新登录失败，Cookie 已失效，请更新 .env 中的 COOKIES_STR",
                    {"device_id": device_id}
                )
                raise CookieInvalidError(
                    "重新登录失败，Cookie 已失效，请更新 .env 中的 COOKIES_STR 后重试"
                )

        params = {
            'jsv': '2.7.2',
            'appKey': MTOP_APP_KEY,
            't': str(int(time.time()) * 1000),
            'sign': '',
            'v': '1.0',
            'type': 'originaljson',
            'accountSite': 'xianyu',
            'dataType': 'json',
            'timeout': '20000',
            'api': 'mtop.taobao.idlemessage.pc.login.token',
            'sessionOption': 'AutoLoginOnly',
            'spm_cnt': 'a21ybx.im.0.0',
        }
        data_val = '{"appKey":"' + REG_APP_KEY + '","deviceId":"' + device_id + '"}'
        data = {
            'data': data_val,
        }
        
        # 简单获取token，信任cookies已清理干净
        token = self.session.cookies.get('_m_h5_tk', '').split('_')[0]
        
        sign = generate_sign(params['t'], token, data_val)
        params['sign'] = sign
        
        try:
            response = self.session.post('https://h5api.m.goofish.com/h5/mtop.taobao.idlemessage.pc.login.token/1.0/', params=params, data=data)
            res_json = response.json()
            
            if isinstance(res_json, dict):
                ret_value = res_json.get('ret', [])
                # 检查ret是否包含成功信息
                if not any('SUCCESS::调用成功' in ret for ret in ret_value):
                    # 检测风控/限流错误
                    error_msg = str(ret_value)
                    if 'RGV587_ERROR' in error_msg or '被挤爆啦' in error_msg:
                        logger.error(f"❌ 触发风控: {ret_value}")
                        logger.error("🔴 系统目前无法自动解决，请进入闲鱼网页版-点击消息-过滑块-复制最新的Cookie")
                        self.write_runtime_status(
                            "risk_control_blocked",
                            "闲鱼风控拦截，请在闲鱼网页版完成滑块并更新 COOKIES_STR",
                            {
                                "device_id": device_id,
                                "ret": ret_value,
                                "retry_after_seconds": self.risk_control_retry_interval,
                                "action": "打开闲鱼网页版 -> 点击消息 -> 通过滑块 -> 更新 .env 中 COOKIES_STR",
                            }
                        )

                        if self._is_interactive():
                            print("\n" + "="*50)
                            new_cookie_str = input("请输入新的Cookie字符串 (复制浏览器中的完整cookie，直接回车则退出程序): ").strip()
                            print("="*50 + "\n")
                            
                            if new_cookie_str:
                                try:
                                    from http.cookies import SimpleCookie
                                    cookie = SimpleCookie()
                                    cookie.load(new_cookie_str)
                                    
                                    self.session.cookies.clear()
                                    for key, morsel in cookie.items():
                                        self.session.cookies.set(key, morsel.value, domain='.goofish.com')
                                    
                                    logger.success("✅ Cookie已更新，正在尝试重连...")
                                    self.update_env_cookies()
                                    return self.get_token(device_id, 0)
                                except Exception as e:
                                    logger.error(f"Cookie解析失败: {e}")
                                    raise CookieInvalidError(f"Cookie 解析失败: {e}") from e
                            else:
                                logger.info("用户取消输入，未更新 Cookie")
                                raise CookieInvalidError("用户取消输入，未更新 Cookie")

                        raise RiskControlError("闲鱼风控拦截，当前为后台模式，无法交互输入 Cookie")

                    logger.warning(f"Token API调用失败，错误信息: {ret_value}")
                    # 处理响应中的Set-Cookie
                    if 'Set-Cookie' in response.headers:
                        logger.debug("检测到Set-Cookie，更新cookie")  # 降级为DEBUG并简化
                        self.clear_duplicate_cookies()
                    time.sleep(0.5)
                    return self.get_token(device_id, retry_count + 1)
                else:
                    logger.info("Token获取成功")
                    self.write_runtime_status("token_ok", "闲鱼 token 获取成功")
                    return res_json
            else:
                logger.error(f"Token API返回格式异常: {res_json}")
                return self.get_token(device_id, retry_count + 1)
                
        except RiskControlError:
            raise
        except CookieInvalidError:
            raise
        except Exception as e:
            logger.error(f"Token API请求异常: {str(e)}")
            self.write_runtime_status("token_error", "闲鱼 token 请求异常", {"error": str(e)})
            time.sleep(0.5)
            return self.get_token(device_id, retry_count + 1)

    def get_item_info(self, item_id, retry_count=0):
        """获取商品信息，自动处理token失效的情况"""
        if retry_count >= 3:  # 最多重试3次
            logger.error("获取商品信息失败，重试次数过多")
            return {"error": "获取商品信息失败，重试次数过多"}
            
        params = {
            'jsv': '2.7.2',
            'appKey': MTOP_APP_KEY,
            't': str(int(time.time()) * 1000),
            'sign': '',
            'v': '1.0',
            'type': 'originaljson',
            'accountSite': 'xianyu',
            'dataType': 'json',
            'timeout': '20000',
            'api': 'mtop.taobao.idle.pc.detail',
            'sessionOption': 'AutoLoginOnly',
            'spm_cnt': 'a21ybx.im.0.0',
        }
        
        data_val = '{"itemId":"' + item_id + '"}'
        data = {
            'data': data_val,
        }
        
        # 简单获取token，信任cookies已清理干净
        token = self.session.cookies.get('_m_h5_tk', '').split('_')[0]
        
        sign = generate_sign(params['t'], token, data_val)
        params['sign'] = sign
        
        try:
            response = self.session.post(
                'https://h5api.m.goofish.com/h5/mtop.taobao.idle.pc.detail/1.0/', 
                params=params, 
                data=data
            )
            
            res_json = response.json()
            # 检查返回状态
            if isinstance(res_json, dict):
                ret_value = res_json.get('ret', [])
                # 检查ret是否包含成功信息
                if not any('SUCCESS::调用成功' in ret for ret in ret_value):
                    logger.warning(f"商品信息API调用失败，错误信息: {ret_value}")
                    # 处理响应中的Set-Cookie
                    if 'Set-Cookie' in response.headers:
                        logger.debug("检测到Set-Cookie，更新cookie")
                        self.clear_duplicate_cookies()
                    time.sleep(0.5)
                    return self.get_item_info(item_id, retry_count + 1)
                else:
                    logger.debug(f"商品信息获取成功: {item_id}")
                    return res_json
            else:
                logger.error(f"商品信息API返回格式异常: {res_json}")
                return self.get_item_info(item_id, retry_count + 1)
                
        except Exception as e:
            logger.error(f"商品信息API请求异常: {str(e)}")
            time.sleep(0.5)
            return self.get_item_info(item_id, retry_count + 1)

    def get_my_items_via_seed_item(self, seed_item_id):
        """
        通过一个已知商品ID调用详情接口，提取 sellerDO.sellerItems 作为当前账号商品列表。
        """
        if not seed_item_id:
            raise ValueError("seed_item_id 不能为空")

        detail = self.get_item_info(seed_item_id)
        if not isinstance(detail, dict):
            raise RuntimeError("商品详情返回格式异常")
        ret_value = detail.get("ret", [])
        if not any("SUCCESS::调用成功" in ret for ret in ret_value):
            raise RuntimeError(f"详情接口调用失败: {ret_value}")

        data = detail.get("data", {}) or {}
        seller = data.get("sellerDO", {}) or {}
        cards = seller.get("sellerItems", []) or []
        section_counts = {"selling": len(cards)}
        items = []
        for card in cards:
            href = (card or {}).get("itemUrl") or (card or {}).get("jumpUrl") or ""
            if href and href.startswith("/"):
                href = f"https://www.goofish.com{href}"
            item_id = str((card or {}).get("itemId") or (card or {}).get("id") or "").strip()
            if not href and item_id:
                href = f"https://www.goofish.com/item?id={item_id}"
            title = (card or {}).get("title") or (card or {}).get("itemTitle") or "(无标题)"
            price = (
                (card or {}).get("price")
                or (card or {}).get("itemPrice")
                or (card or {}).get("soldPrice")
                or ""
            )
            items.append(
                {
                    "title": str(title)[:60],
                    "price": str(price),
                    "href": href,
                    "item_id": item_id or None,
                    "status_key": "selling",
                    "status_label": "在售",
                }
            )
        payload = {
            "fetched_at": datetime.now().isoformat(),
            "item_count": len(items),
            "items": items,
            "section_counts": section_counts,
            "metadata": {
                "source": "seed_item_detail",
                "seed_item_id": str(seed_item_id),
                "seller_item_count": seller.get("itemCount"),
            },
        }
        return payload

    def get_my_items(self, output_path=None, headless=True, fetcher=None, sections=None, seed_item_id=None):
        """
        获取当前账号商品列表（在售/已下架/草稿），并可选写入本地 JSON 快照。

        Args:
            output_path: 可选，写入 JSON 快照的路径。
            headless: 是否使用无头浏览器模式。
            fetcher: 测试注入点。默认使用浏览器采集实现。
            sections: 可选，抓取的状态分区配置。

        Returns:
            dict: 包含 item_count/items/metadata 的结构化结果。
        """
        cookie_string = self._session_cookie_string()
        if not cookie_string:
            raise ValueError("当前 session 中没有可用 Cookie，无法获取商品列表。")

        try:
            result = self.get_my_items_via_live_api(output_path=output_path)
        except Exception as api_error:
            import traceback
            logger.warning(f"实时商品列表 API 获取失败，尝试浏览器采集: {api_error}")
            logger.warning(f"[商品列表] 异常堆栈:\n{traceback.format_exc()}")
            if fetcher is None:
                fetcher = get_selling_items_via_browser
            try:
                result = fetcher(
                    cookie_string=cookie_string,
                    output_path=output_path,
                    headless=headless,
                    sections=sections,
                )
            except Exception as fetch_error:
                fallback_seed_item_id = seed_item_id or os.getenv("SELLING_ITEMS_SEED_ITEM_ID", "").strip()
                if not fallback_seed_item_id:
                    raise
                logger.warning(f"浏览器采集失败，降级到 seed-item 方案: {fetch_error}")
                result = self.get_my_items_via_seed_item(fallback_seed_item_id)
                if output_path:
                    parent = os.path.dirname(output_path)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                        f.write("\n")
        logger.info(f"商品列表获取完成，共 {result.get('item_count', 0)} 条")
        return result

    def get_my_selling_items(self, output_path=None, headless=True, fetcher=None, sections=None, seed_item_id=None):
        """
        兼容旧调用名。当前实现已升级为抓取全部商品状态。
        """
        return self.get_my_items(
            output_path=output_path,
            headless=headless,
            fetcher=fetcher,
            sections=sections,
            seed_item_id=seed_item_id,
        )
