import argparse
import asyncio
import sys
from tv_flow.core import TvFlowEngine

def main():
    parser = argparse.ArgumentParser(description="tv-flow: 智能家庭电视源自愈、多源测活与自动分发管道")
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # 子命令: run
    run_parser = subparsers.add_parser("run", help="执行全网多源抓取、并发测活并生成自愈 M3U 文件")
    run_parser.add_argument("-c", "--config", default="config/config.yaml", help="配置文件路径")
    run_parser.add_argument("--concurrency", type=int, default=None, help="并发探测协程数")

    args = parser.parse_args()

    if args.command == "run" or args.command is None:
        cfg = getattr(args, "config", "config/config.yaml")
        concurrency = getattr(args, "concurrency", None)
        engine = TvFlowEngine(config_path=cfg)
        asyncio.run(engine.run(max_concurrent=concurrency))
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
