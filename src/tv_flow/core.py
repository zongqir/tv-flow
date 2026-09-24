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
                url = line.strip()
                # 过滤明显非直播流或广告短视频（如 mp4/flv/kwimgs/bdstatic 等假流）
                url_clean = url.lower().split("?")[0]
                if any(url_clean.endswith(ext) for ext in [".mp4", ".flv", ".mkv", ".avi"]):
                    extinf = ""
                    continue
                if any(bad in url.lower() for bad in ["kwimgs.com", "bdstatic.com", "douyinvod.com"]):
                    extinf = ""
                    continue

                name_match = re.search(r',([^,]+)$', extinf)
                name = name_match.group(1).strip() if name_match else ""

                group_match = re.search(r'group-title="([^"]+)"', extinf)
                group = group_match.group(1).strip() if group_match else ""

                logo_match = re.search(r'tvg-logo="([^"]+)"', extinf)
                logo = logo_match.group(1).strip() if logo_match else ""

                items.append(StreamItem(extinf, url, name, group, logo))
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
                    chunk = await resp.content.read(1024)
                    # 严格要求 HLS 播放列表特征 (#EXTM3U 或 #EXT-X-)，杜绝伪装成 200 的 HTML 报错页或非法数据
                    c_type = resp.headers.get("content-type", "").lower()
                    if b"#EXTM3U" in chunk or b"#EXT-X-" in chunk or "mpegurl" in c_type:
                        latency = (time.time() - start_time) * 1000
                        # 境外/GitHub 代理流增加延迟惩罚，确保免梯环境国内原生 IPv6/CDN 优先胜出
                        if "github.io" in item.url or "githubusercontent.com" in item.url:
                            latency += 600.0
                        item.latency_ms = latency
                        item.is_alive = True
                        return True
        except Exception:
            pass
        item.is_alive = False
        return False

    async def fetch_upstream(self, session: aiohttp.ClientSession, url: str) -> List[StreamItem]:
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            proxy = os.environ.get("https_proxy") or os.environ.get("http_proxy")
            if not proxy and "github" in url.lower():
                proxy = "http://127.0.0.1:7890"

            async with session.get(url, headers=headers, timeout=timeout, proxy=proxy) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
                    return self.parse_m3u(text)
        except Exception as e:
            print(f"[Warn] Failed to fetch upstream {url}: {repr(e)}")
        return []

    async def run(self, max_concurrent: Optional[int] = None) -> Tuple[int, int]:
        concurrency = max_concurrent or self.config.get("probe", {}).get("concurrency", 50)
        timeout_sec = self.config.get("probe", {}).get("timeout_seconds", 3.5)
        mom_whitelist = self.config.get("mom_whitelist", [])
        output_dir_str = self.config.get("output", {}).get("dir", "output")
        output_dir = Path(output_dir_str)
        if not output_dir.is_absolute():
            output_dir = (self.config_path.parent.parent / output_dir).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()
        def _silent_handler(l, ctx):
            pass
        loop.set_exception_handler(_silent_handler)

        print("[tv-flow] Starting upstream harvesting...")
        all_raw_items: List[StreamItem] = []

        # 1. 抓取上游源（使用 AsyncResolver + 自动代理支持 GitHub）
        fetch_connector = aiohttp.TCPConnector(limit=concurrency, ssl=False, resolver=aiohttp.AsyncResolver())
        async with aiohttp.ClientSession(connector=fetch_connector, trust_env=True) as fetch_session:
            for src in self.config.get("upstreams", []):
                if src.get("enabled", True):
                    items = await self.fetch_upstream(fetch_session, src["url"])
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

        # 3. 测活探针（必须直连，绝不走境外代理，以便直通国内运营商 IPv6 骨干网）
        # 使用 aiodns AsyncResolver 杜绝 glibc getaddrinfo 线程池阻塞悬挂
        resolver = aiohttp.AsyncResolver()
        probe_connector = aiohttp.TCPConnector(limit=concurrency, ssl=False, resolver=resolver)
        async with aiohttp.ClientSession(connector=probe_connector, trust_env=False) as probe_session:
            sem = asyncio.Semaphore(concurrency)

            async def sem_probe(it: StreamItem):
                async with sem:
                    return await self.probe_stream(probe_session, it, timeout_sec)

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

        # 导出 VOD / 电视聚合配置模板
        vod_file = output_dir / self.config.get("output", {}).get("vod_template", "vod-config.json")
        vod_content = """{
  "wallpaper": "https://picsum.photos/1920/1080",
  "lives": [
    {
      "name": "长辈精选 (自愈IPv6)",
      "type": 0,
      "url": "https://cdn.jsdelivr.net/gh/zongqir/tv-flow@main/output/mom-live.m3u",
      "epg": "https://live.fanmingming.com/e.xml",
      "logo": "https://live.fanmingming.com/tv/{name}.png"
    },
    {
      "name": "资深全量 (千路高可用)",
      "type": 0,
      "url": "https://cdn.jsdelivr.net/gh/zongqir/tv-flow@main/output/pro-live.m3u",
      "epg": "https://live.fanmingming.com/e.xml",
      "logo": "https://live.fanmingming.com/tv/{name}.png"
    }
  ]
}"""
        with open(vod_file, "w", encoding="utf-8") as f:
            f.write(vod_content)

        print(f"[tv-flow] Completed. Mom Channels: {len(mom_results)}, Pro Streams: {len(pro_results)}")
        return len(mom_results), len(pro_results)
