import asyncio
import aiohttp
import os
import re
import time
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class StreamItem:
    def __init__(self, raw_extinf: str, url: str, channel_name: str, group_title: str = "", tvg_logo: str = ""):
        self.raw_extinf = raw_extinf
        self.url = url
        self.channel_name = channel_name
        self.group_title = group_title
        self.tvg_logo = tvg_logo
        self.latency_ms: float = 999999.0
        self.is_alive: bool = False

    def clean_name(self) -> str:
        name = self.channel_name.strip()
        # 标准化 CCTV 名称 (如 CCTV1, CCTV-1 综合 -> CCTV-1; CCTV5+ -> CCTV-5+)
        cctv_match = re.search(r"CCTV[-_ ]?([0-9]+)(\+?)", name, re.IGNORECASE)
        if cctv_match:
            num = cctv_match.group(1)
            plus = cctv_match.group(2)
            return f"CCTV-{num}{plus}"
        if re.search(r"CCTV[-_ ]?4K", name, re.IGNORECASE):
            return "CCTV-4K"
        if re.search(r"CCTV[-_ ]?8K", name, re.IGNORECASE):
            return "CCTV-8K"

        # 卫视清洗 (如 湖南卫视高清, [IPv6]浙江卫视 -> 湖南卫视, 浙江卫视)
        ws_match = re.search(r"([\u4e00-\u9fa5]+卫视)", name)
        if ws_match:
            return ws_match.group(1)

        # 移除常见的“高清”、“超清”、“[IPv6]”等干扰词
        cleaned = re.sub(r"\[.*?\]|\(.*?\)|高清|超清|标清|1080P|4K|50fps|HD|「.*?」", "", name).strip()
        return cleaned

class TvFlowEngine:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = Path(config_path)
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

    @staticmethod
    def parse_m3u(content: str) -> List[StreamItem]:
        items = []
        lines = content.splitlines()
        extinf = ""
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#EXTINF:"):
                extinf = line
            elif line.startswith("http") and extinf:
                name_match = re.search(r',([^,]+)$', extinf)
                name = name_match.group(1).strip() if name_match else ""

                group_match = re.search(r'group-title="([^"]+)"', extinf)
                group = group_match.group(1).strip() if group_match else ""

                logo_match = re.search(r'tvg-logo="([^"]+)"', extinf)
                logo = logo_match.group(1).strip() if logo_match else ""

                items.append(StreamItem(extinf, line, name, group, logo))
                extinf = ""
        return items

    @staticmethod
    async def probe_stream(session: aiohttp.ClientSession, item: StreamItem, timeout_sec: float) -> bool:
        start_time = time.time()
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        try:
            async with session.get(item.url, headers=headers, timeout=timeout, allow_redirects=True) as resp:
                if resp.status == 200:
                    _ = await resp.content.read(1024)
                    item.latency_ms = (time.time() - start_time) * 1000
                    item.is_alive = True
                    return True
        except Exception:
            pass
        item.is_alive = False
        return False

    async def fetch_upstream(self, session: aiohttp.ClientSession, url: str) -> List[StreamItem]:
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
                    return self.parse_m3u(text)
        except Exception as e:
            print(f"[Warn] Failed to fetch upstream {url}: {e}")
        return []

    async def run(self, max_concurrent: Optional[int] = None) -> Tuple[int, int]:
        concurrency = max_concurrent or self.config.get("probe", {}).get("concurrency", 50)
        timeout_sec = self.config.get("probe", {}).get("timeout_seconds", 3.5)
        mom_whitelist = self.config.get("mom_whitelist", [])
        output_dir = Path(self.config.get("output", {}).get("dir", "output"))
        output_dir.mkdir(parents=True, exist_ok=True)

        print("[tv-flow] Starting upstream harvesting...")
        all_raw_items: List[StreamItem] = []

        connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
        async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:
            # 1. 抓取所有启用的上游
            for src in self.config.get("upstreams", []):
                if src.get("enabled", True):
                    items = await self.fetch_upstream(session, src["url"])
                    print(f"  -> {src['name']}: {len(items)} streams fetched.")
                    all_raw_items.extend(items)

            # 2. 按标准化频道聚合去重
            channel_map: Dict[str, List[StreamItem]] = {}
            for item in all_raw_items:
                cname = item.clean_name()
                if not cname:
                    continue
                if cname not in channel_map:
                    channel_map[cname] = []
                if item.url not in [x.url for x in channel_map[cname]]:
                    channel_map[cname].append(item)

            print(f"[tv-flow] Total unique channels found: {len(channel_map)}. Starting concurrent probing...")

            sem = asyncio.Semaphore(concurrency)

            async def sem_probe(it: StreamItem):
                async with sem:
                    return await self.probe_stream(session, it, timeout_sec)

            # 收集待测试流：优先白名单，其余频道测试前 2 个候选
            tasks = []
            for cname, stream_list in channel_map.items():
                is_mom_channel = any(target.lower() == cname.lower() for target in mom_whitelist)
                sample_count = 6 if is_mom_channel else 2
                for it in stream_list[:sample_count]:
                    tasks.append(sem_probe(it))

            await asyncio.gather(*tasks)

        # 3. 结果构建
        mom_results: List[StreamItem] = []
        pro_results: List[StreamItem] = []

        # 构建长辈专属版本 (严格按照白名单顺序排序输出)
        for target_name in mom_whitelist:
            for cname, items in channel_map.items():
                if target_name.lower() == cname.lower():
                    alive = [x for x in items if x.is_alive]
                    if alive:
                        alive.sort(key=lambda x: x.latency_ms)
                        mom_results.append(alive[0])
                    elif items:
                        # 容灾兜底：当运行环境缺少 IPv6 路由导致探针受限时，保底注入首选权威流
                        mom_results.append(items[0])
                    break

        # 构建全量高可用版本
        pro_max = self.config.get("probe", {}).get("pro_max_streams_per_channel", 2)
        for cname, items in channel_map.items():
            alive = [x for x in items if x.is_alive]
            if alive:
                alive.sort(key=lambda x: x.latency_ms)
                pro_results.extend(alive[:pro_max])
            elif items:
                pro_results.extend(items[:pro_max])

        # 4. 导出 M3U 文件
        mom_file = output_dir / self.config.get("output", {}).get("mom_file", "mom-live.m3u")
        pro_file = output_dir / self.config.get("output", {}).get("pro_file", "pro-live.m3u")

        with open(mom_file, "w", encoding="utf-8") as f:
            f.write('#EXTM3U name="Mom-Live-AutoHeal" x-tvg-url="https://live.fanmingming.com/e.xml"\n')
            for it in mom_results:
                f.write(f'{it.raw_extinf}\n{it.url}\n')

        with open(pro_file, "w", encoding="utf-8") as f:
            f.write('#EXTM3U name="Pro-Live-All" x-tvg-url="https://live.fanmingming.com/e.xml"\n')
            for it in pro_results:
                f.write(f'{it.raw_extinf}\n{it.url}\n')

        # 导出 VOD 配置模板
        vod_file = output_dir / self.config.get("output", {}).get("vod_template", "vod-config.json")
        vod_content = """{
  "spider": "https://mirror.ghproxy.com/https://raw.githubusercontent.com/FongMi/TV/release/jar/custom_spider.jar",
  "wallpaper": "https://picsum.photos/1920/1080",
  "lives": [
    {
      "name": "自愈IPv6直播",
      "type": 0,
      "url": "./mom-live.m3u",
      "epg": "https://live.fanmingming.com/e.xml",
      "logo": "https://live.fanmingming.com/tv/{name}.png"
    }
  ],
  "sites": [
    {
      "key": "fantaiying",
      "name": "🚀 饭太硬 | 原画云盘",
      "type": 3,
      "api": "csp_Config",
      "ext": "http://饭太硬.top/tv"
    }
  ]
}"""
        with open(vod_file, "w", encoding="utf-8") as f:
            f.write(vod_content)

        print(f"[tv-flow] Completed. Mom Channels: {len(mom_results)}, Pro Streams: {len(pro_results)}")
        return len(mom_results), len(pro_results)
