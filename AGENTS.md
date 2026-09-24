# tv-flow: 智能家庭电视源自愈、多源测活与自动分发管道

`tv-flow` 是为家庭智能电视打造的流媒体生命周期治理管道：多上游抓取、异步并发测活、长辈/极客分级修剪、自动生成高可用 M3U 与点播配置，并结合 GitHub Actions 实现无人值守每日自愈与 Gitee 国内极速镜像推送。

---

## 核心命令与场景

| 场景 | 命令 | 说明 |
| :--- | :--- | :--- |
| **全量抓取与测活** | `tv-flow run` | 并发拉取上游源，异步测活各频道流地址，剔除死链并生成 `mom-live.m3u` 与 `pro-live.m3u`。 |
| **指定并发度执行** | `tv-flow run --concurrency 30` | 调整并发协程数量以适应不同的网络带宽条件。 |
| **指定自定义配置** | `tv-flow run -c <path>` | 指定外部 YAML 配置文件路径。 |

---

## 架构原则与输出规范

1. **分级修剪原则（长辈与极客物理隔离）**：
   - **长辈版 (`output/mom-live.m3u`)**：
     - 严格受控于 `mom_whitelist`（央视 CCTV-1~16、4K 与核心省级卫视）；
     - 频道数量严格限制在 20~25 个；
     - 每个频道仅保留当前响应延时最低的 1 个健康流节点，杜绝线路选择与杂乱垃圾频道。
   - **资深全量版 (`output/pro-live.m3u`)**：
     - 包含全量 4K、超清体育、地方台；
     - 每个频道保留 2 条高可用备用流。
2. **多上游冗余与死链淘汰**：
   - 依赖至少 3 个权威上游源互为备份（`fanmingming`、`Ftindy`、`YanG`），杜绝单一项目维护者停更带来的单点故障。
   - 3.5 秒无响应或 HTTP 状态非 200 的流一律丢弃。
3. **云端 CI/CD 自动化闭环**：
   - 由 `.github/workflows/update.yml` 每天凌晨 04:00 自动触发；
   - 测活重构后自动提交并推送至 Gitee，国内电视直连零代理。

---

## 目录结构

```text
tool/tv-flow/
├── .github/workflows/update.yml # GitHub Actions 定时自愈与 Gitee 镜像推流
├── AGENTS.md                    # 本规范指南
├── README.md                    # 项目说明
├── bin/tv-flow                  # argc 规范全局 CLI 包装器
├── config/config.yaml           # 上游源与长辈白名单配置
├── output/                      # 生成的 M3U / JSON 文件
│   ├── mom-live.m3u
│   ├── pro-live.m3u
│   └── vod-config.json
├── pyproject.toml               # Python 依赖与项目配置
└── src/tv_flow/                 # 核心引擎代码
    ├── cli.py
    └── core.py
```
