# QQ 群消息持久化服务

仅接收 QQ 官方机器人所在群的新消息并保存，不做 issue 整理，也不发送群消息。当前为单机采集器，尚未实现跨主机租约和自动主备切换。

## 运行与数据位置

支持 Python 3.11；`service.sh` 和 LaunchAgent 部署方式适用于 macOS。

设计文档：

- [采集与主备高可用需求](docs/collection-and-ha-requirements.md)
- [QQ Bot 能力清单](docs/bot-capabilities.md)

```sh
cd /path/to/qq-collector
./service.sh status
./service.sh logs
./service.sh stop
./service.sh start
./service.sh restart
```

- `data/messages.sqlite3`：SQLite 数据库，WAL 模式，消息与恢复位置在同一事务提交，synchronous=FULL。
- `data/media/`：已下载图片和语音，以安全哈希命名；原文件名、类型和相对路径保存在 attachments 表。
- `backups/messages-YYYY-MM-DD.sqlite3`：每天首次维护检查时做数据库快照，按 UTC 日期命名，保留最近 7 份。附件不重复备份；这些同机备份不提供异机灾备。
- `logs/collector.log`：连接、重试等运行日志，单文件 5 MiB，保留 3 个轮转文件。
- `config/credentials.json`：本机私有凭证，权限 0600，不提交版本库。
- `~/Library/LaunchAgents/com.cherry.qq-collector.plist`：macOS LaunchAgent，用户登录时启动，进程退出后自动拉起，最短重启间隔 30 秒。

无需保持终端或本次会话打开。电脑睡眠、关机、用户退出登录或断网时不能保证接收；长期运行应放在持续在线的机器。没有普通 QQ 群历史消息补拉功能，Resume 只在 QQ 服务端仍保留会话时有效。

## 接收范围

- `GROUP_MESSAGE_CREATE`、`GROUP_AT_MESSAGE_CREATE` 都存储，以 `(group_openid, message_id)` 去重。
- 保存正文、原发送时间、接收时间、发送者 openid/昵称/角色、消息类型、引用及转发结构。
- 保留清理传输凭据后的群事件 JSON；不保存单聊消息。
- 群通过入群事件或第一条群消息自动发现，异步查询群资料；不内置任何实际群标识。
- 每小时刷新群资料和机器人接收设置。群内必须开启“获取群内全部消息”；只 @ 模式无法收到普通消息。
- 顶层及递归 `msg_elements` 中的图片、语音/音频自动下载，单文件上限 5 MiB。
- 视频、普通文件、大于上限的图片/音频保留元数据，状态为 skipped。附件下载失败短重试 3 次后标记 failed。
- 临时签名 URL 只在待下载任务中暂存，完成或最终失败后移除；长期事件和附件 JSON 清理 URL 中的 rkey/auth_token 等传输凭据。消息正文仍按原文保存，不是通用敏感信息脱敏器。SQLite WAL 和历史快照可能暂时保留曾经入库的下载 URL，所有数据文件都应按私有数据保护。
- 单机进程锁防止本项目多开；不要用相同 bot 凭证另开 WebSocket 监听器，以免互相挤掉。

## 表结构与读取

`groups`：群资料与最近接收设置；`messages`：消息；`attachments`：附件及保存结果；`events`：群事件及会话事件；`state`：连接恢复与运行状态。

只读查看最近消息（该命令会显示聊天正文，只在需要时执行）：

```sh
sqlite3 -readonly -header -column data/messages.sqlite3 \
  'SELECT g.group_name,m.timestamp,m.author_name,m.content FROM messages m JOIN groups g USING(group_openid) ORDER BY m.received_at DESC LIMIT 20;'
```

附件本地位置是项目目录加上 `attachments.local_path`，只有 `status=ok` 代表已成功保存文件。文件可通过 `sha256` 校验。

运行状态查询展示连接状态、最新心跳时间、消息计数与附件状态；判定在线时同时检查心跳和进程存活时间是否持续更新。数据库时间采用 UTC，原消息 timestamp 保留 QQ 传入的时区。

若状态为 `connection=blocked`、`operator_action_required=true`，网关因权限、协议或账号状态错误停止重连；先检查 `gateway_close_code` 并解决原因，再手动重启。此时维护和已入队附件任务仍可运行，不能仅凭进程存活判断采集在线。

## 维护

```sh
# 依赖已锁定；仅首次安装/维护时执行
uv sync --locked
.venv/bin/python -m unittest -v
```

Python 固定 3.11.14，依赖使用独立 `.venv`。更换凭证时编辑私有 config 文件后 restart。迁移时重建虚拟环境并重新生成 launchd 中的绝对路径；不要直接复制 `.venv`。

首次部署（先安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)）：

```sh
uv sync --locked
cp config/credentials.example.json config/credentials.json
chmod 600 config/credentials.json
# 编辑 config/credentials.json，填入自己的 QQ Bot 凭证
mkdir -p data logs
# 前台试运行；确认成功后按 Ctrl+C 退出，再安装 LaunchAgent
.venv/bin/python collector.py run
```

凭证文件、运行数据库、附件、日志和备份均不应上传 GitHub。不要同时运行前台进程和 LaunchAgent。

macOS 自动启动：在项目目录执行下列命令，按实际路径生成模板（自动处理 XML 转义），然后启动服务。

```sh
.venv/bin/python - <<'PYTHON'
from pathlib import Path
from xml.sax.saxutils import escape
root = Path.cwd().resolve()
text = (root / "deploy/com.cherry.qq-collector.plist.template").read_text()
target = Path.home() / "Library/LaunchAgents/com.cherry.qq-collector.plist"
target.parent.mkdir(parents=True, exist_ok=True)
if target.exists():
    raise SystemExit("LaunchAgent 已存在；请检查现有配置后再更新")
target.write_text(text.replace("__PROJECT_ROOT__", escape(str(root))))
PYTHON
./service.sh start
./service.sh status
```

安装前需要创建 `logs/`，否则 launchd 无法打开标准输出/错误文件。服务启动后的网络与接收权限需在自己的环境验证。

数据库在线备份请使用 SQLite backup API，不要在运行中仅复制主 sqlite 文件而遗漏 WAL。恢复备份前先停止服务并保留当前数据库及附件。消息与附件不会自动按天删除，应定期检查磁盘空间。

## 测试与验证边界

```sh
uv sync --locked
.venv/bin/python -m unittest -v
sh -n service.sh
```

测试使用临时数据库、本地模拟网关和模拟附件，不读取正式凭证、不发送群消息。覆盖事务与恢复位置、消息去重、递归附件、下载限制与哈希、凭据清理、首次启动、token 刷新、心跳超时和会话恢复。

本仓库不包含真实群聊天、凭证或原环境运行回执。自动化测试通过不代表当前 QQ Bot 权限、全量消息接收开关或生产网络已验证；部署者应按自己的账号实际验收。
