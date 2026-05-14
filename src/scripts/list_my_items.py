import argparse
import sys
from pathlib import Path
from http.cookies import SimpleCookie

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from selling_items import format_selling_items_text
from xianyu_apis import XianyuApis


DEFAULT_OUTPUT_PATH = Path("data") / "my_items.json"


def load_cookie_string(env_path: Path) -> str:
    if not env_path.exists():
        raise FileNotFoundError(f".env 文件不存在: {env_path}")

    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.startswith("COOKIES_STR="):
            return line.split("=", 1)[1].strip()

    raise ValueError(f"未在 {env_path} 中找到 COOKIES_STR")


def main():
    parser = argparse.ArgumentParser(description="列出当前闲鱼账号所有商品（在售、已下架、草稿）")
    parser.add_argument("--env-path", default=".env", help="环境变量文件路径，默认 .env")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help=f"JSON 输出路径，默认 {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="使用有头浏览器模式运行采集",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="仅输出结果，不写 JSON 文件",
    )
    parser.add_argument(
        "--seed-item-id",
        default="",
        help="可选：当浏览器采集失败时，用该商品ID走详情降级模式（建议传你自己的在售商品ID）",
    )
    args = parser.parse_args()

    env_path = Path(args.env_path).resolve()
    cookie_string = load_cookie_string(env_path)
    output_path = None if args.no_write else Path(args.output).resolve()
    cookie = SimpleCookie()
    cookie.load(cookie_string)

    apis = XianyuApis()
    apis.session.cookies.clear()
    for key, morsel in cookie.items():
        apis.session.cookies.set(key, morsel.value, domain=".goofish.com")

    payload = apis.get_my_items(
        output_path=str(output_path) if output_path else None,
        headless=not args.headed,
        seed_item_id=args.seed_item_id.strip() or None,
    )

    print(format_selling_items_text(payload["items"]))
    if payload.get("section_counts"):
        print(f"\n分组统计：{payload['section_counts']}")
    if output_path:
        print(f"\nJSON 已写入：{output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"获取商品列表失败：{exc}", file=sys.stderr)
        raise
