# tv-flow

家庭智能电视（Android TV）流媒体源自愈、多源测活与自动分发管道。

## 解决的痛点

1. **源时效性与死链雪崩**：网上随便复制的 M3U 或影视仓接口平均几周到数月即失效，人工维护成本极高；
2. **长辈交互灾难**：动辄几百个包含购物台、失效台、高延迟节点的杂乱列表，长辈极易点错或卡顿；
3. **国内外网络隔离**：GitHub Actions 负责海外云端并发抓取与深度测活，清洗后自动镜像至国内 Gitee 仓库，国内电视直连秒开。

## 核心特性

- **多源异构抓取**：聚合 `fanmingming/live`、`Guovin/iptv-api`、`YanG` 等权威公网源，互为兜底；
- **异步高并发测活**：`aiohttp` 协程级毫秒级探针，测试真实可播放状态与响应延时，按延迟升序重排；
- **长辈/极客分级交付**：
  - `mom-live.m3u`：严格锁定央视与核心卫视 25 频道白名单，每个频道只留 1 条响应最快的优质流；
  - `pro-live.m3u`：全量 4K、高刷、超清备用流；
- **GitHub Actions 无人值守**：每天北京时间凌晨 04:00 定时执行，自动测活、自动更新并推送到 Gitee。

## 本地使用

```bash
# 1. 运行自愈管道
tv-flow run

# 2. 调整并发度执行
tv-flow run --concurrency 50
```

## 云端自动化部署

在 GitHub 仓库中配置以下 Repository Secrets：
- `GITEE_USER`: 你的 Gitee 用户名
- `GITEE_TOKEN`: Gitee 私人令牌 (Private Token，需具备 projects 权限)
- `GITEE_REPO`: Gitee 目标仓库名 (如 `tv-flow`)

电视端（My-TV-0 / FongMi）直接订阅 Gitee Raw 链接：
`https://gitee.com/<GITEE_USER>/<GITEE_REPO>/raw/main/output/mom-live.m3u`
