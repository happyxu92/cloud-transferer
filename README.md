# Cloud Transferer

百度网盘 → 夸克网盘 **自动迁移工具**。基于 AList 的多网盘抽象 + 本服务器中转下载/上传（即「方案 A」）。

> 适用场景：拥有百度 SVIP（解除下载限速），有一台带宽 ≥ 30 Mbps 的小服务器（本项目按 2C4G/60G SSD/30Mbps 调优）。

## 特性

- 📦 完全 Docker 化部署：`docker compose up -d` 一把梭
- 🔁 增量同步：按「路径 + 文件大小」判断目标是否已存在，已存在自动跳过
- 🧱 文件级任务：每个文件独立入库 + 重试 + 失败结构化日志
- 🪫 磁盘哨兵：AList 临时目录占用过高时自动暂停下发新任务（防止 60G 系统盘被撑爆）
- ⏰ 内置 APScheduler，支持 cron 周期同步
- 🧰 友好 CLI（`ct`），可手动添加 / 启停 / 重试任务
- 🚫 不删除百度源文件（按你的要求）

## 架构

```
┌──────────────────────────────────┐
│  本服务器  (Docker)               │
│                                  │
│  ┌──────────┐    REST    ┌─────┐ │
│  │ migrator │ ─────────► │AList│ │
│  │  CLI/Job │ ◄───────── │5244 │ │
│  └──────────┘            └─────┘ │
│       │                    │  │  │
│       │                    ▼  ▼  │
│       │              百度盘  夸克盘│
│   /data SQLite              │    │
│   + logs                    ▼    │
│                  AList temp(下载)│
│                  → 上传到夸克     │
└──────────────────────────────────┘
```

## 快速开始

### 1. 准备

```bash
git clone https://github.com/happyxu92/cloud-transferer.git
cd cloud-transferer
cp .env.example .env
# 编辑 .env，至少修改 ALIST_PASSWORD
```

### 2. 启动

```bash
docker compose up -d
docker compose logs -f alist     # 等待 "Start HTTP server @ :5244"
```

首次启动后查看 AList 的 admin 初始密码（若未在 .env 设过）：

```bash
docker compose exec alist ./alist admin
```

### 3. 在 AList 中配置网盘 driver（一次性，使用浏览器）

打开 `http://你的服务器IP:5244`，登录后进入「存储」：

#### 百度网盘
- 类型：`BaiduNetdisk`
- 挂载路径：`/baidu`
- refresh_token：参考 https://alist.nn.ci/zh/guide/drivers/baidu.html 用 alist 官方网页换取
- 下载 API：SVIP 用 `official`，普通账号用 `crack`
- 排序、缓存过期等保留默认

#### 夸克网盘
- 类型：`Quark`
- 挂载路径：`/quark`
- Cookie：浏览器登录 https://pan.quark.cn 后，F12 → Application → Cookies，复制全部内容（必须含 `__pus`）
- 上传分片大小：默认即可

配置完后，在 AList 文件管理中分别浏览 `/baidu`、`/quark`，确认能列出文件。

### 4. 验证连通性

```bash
docker compose exec migrator ct doctor
```

期望输出：
```
✓ AList 登录成功
✓ 百度 根目录 /baidu 可访问，共 N 项
✓ 夸克 根目录 /quark 可访问，共 M 项
```

### 5. 创建并执行迁移任务

```bash
# 一次性任务：把百度盘 /电影 整个迁到夸克盘 /归档/电影
docker compose exec migrator ct job add movies /baidu/电影 /quark/归档/电影

# 立即执行（前台运行，可以 Ctrl+C 中断；状态会持久化，下次接着跑）
docker compose exec migrator ct job run 1

# 查看任务进度
docker compose exec migrator ct task stats --job 1
docker compose exec migrator ct task list --job 1 --status failed
docker compose exec migrator ct task progress --job 1
```

### 6. 周期同步（可选）

方式 A：直接命令行加 cron job：

```bash
docker compose exec migrator ct job add daily_sync /baidu/迁移 /quark/归档 --cron "0 3 * * *"
```

`migrator` 容器内的 `ct run` 长驻进程会按时执行。

方式 B：在 `.env` 中设置：
```env
DEFAULT_JOB_CRON=0 3 * * *
DEFAULT_JOB_SRC=/baidu/迁移
DEFAULT_JOB_DST=/quark/归档
```
启动时自动注册（仅首次）。

## CLI 速查

```text
ct doctor                                  连通性检查
ct run                                     启动调度器（容器入口默认就是它）
ct job add NAME SRC DST [--cron "..."]     新建任务
ct job list                                列出任务
ct job run ID                              立即执行
ct job enable/disable ID                   启停 cron
ct job rm ID                               删除任务及其文件记录

ct task list   [--job ID] [--status XXX]   文件级任务列表
ct task stats  [--job ID]                  按状态汇总（含容量）
ct task progress [--job ID]                查看 copying 任务实时进度
ct task retry  [--job ID] [--include-oversize]  把 failed 重置为 pending
```

任务状态： `pending | copying | success | failed | skipped | oversize`

## 关键参数（.env）

| 变量 | 默认 | 说明 |
|---|---|---|
| `MAX_CONCURRENCY` | 2 | 同时进行的 copy 任务数（≤2 防 OOM）|
| `MAX_FILE_GB` | 20 | 单文件超过则不下载（标记 oversize）|
| `EXISTS_POLICY` | skip | 目的已存在策略 |
| `MAX_RETRY` | 3 | 单文件失败重试次数 |
| `TASK_TIMEOUT_SEC` | 14400 | AList 复制任务最长 4h |
| `DISK_PAUSE_PERCENT` | 80 | 磁盘占用阈值，超过暂停新任务 |
| `ALIST_TEMP_DIR` | /alist-data/temp | 哨兵监控的目录 |

## 工作原理（方案 A 简述）

1. `ct job run ID` 触发 `migrator.run_job`
2. 通过 AList `/api/fs/list` 递归扫描源目录，与目的目录做 diff 入库
3. 并发（默认 2）调用 `/api/fs/copy`，AList 内部：
   - 用百度 driver 下载到容器内 `temp/` 目录
   - 用夸克 driver 上传到目的路径
4. migrator 轮询 `/api/admin/task/copy/{undone,done}` 直到完成
5. 校验目的文件大小一致，标记 success；否则按 `MAX_RETRY` 重试

> 百度账号必须为 SVIP 才能跑满 30Mbps，否则下载阶段会成为瓶颈。

## 故障排查

| 现象 | 原因 / 处理 |
|---|---|
| `doctor` 显示百度目录失败 | refresh_token 失效，去 AList web 重新生成 |
| `doctor` 显示夸克目录失败 | Cookie 失效，重新从浏览器复制并更新 driver |
| 任务状态长期 `copying` | 查 `docker compose logs alist`，可能百度限速或被风控；可重启 alist 容器 |
| 想看当前复制进度 | 运行 `docker compose exec migrator ct task progress --job ID` |
| `ct job run` 被中断后仍显示 `running/copying` | 直接重新执行 `docker compose exec migrator ct job run ID`，会自动恢复或重置中断的任务 |
| `磁盘占用 X% 暂停新任务` | AList temp 没有及时清理，先停 migrator，进 alist 容器删除 `/opt/alist/data/temp/*` |
| 大量 `failed` 任务 | `ct task list --status failed`，常见为夸克 cookie 过期；处理后 `ct task retry --job ID` |
| `oversize` 任务 | 调大 `MAX_FILE_GB` 或单独处理 |

## 目录结构

```
cloud-transferer/
├── docker-compose.yml
├── .env.example
├── alist/data/                 AList 持久化（卷）
└── app/
    ├── Dockerfile
    ├── pyproject.toml
    └── src/cloud_transferer/
        ├── cli.py              CLI 入口
        ├── config.py           pydantic-settings
        ├── alist_client.py     AList HTTP API 封装
        ├── db.py / models.py   SQLite + SQLModel
        ├── migrator.py         核心迁移逻辑
        ├── scheduler.py        APScheduler 长驻
        ├── disk_guard.py       磁盘哨兵
        └── logging_setup.py    loguru 配置
```

## License

MIT

See `LICENSE` for the full text.
