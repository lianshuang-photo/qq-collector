# QQ Bot 能力清单

## 当前实现范围

当前项目使用 QQ 官方 Bot API v2 接收事件。实际能力由部署者的机器人权限、平台策略和群内设置决定。

### 入站采集

- WebSocket 接收 `GROUP_MESSAGE_CREATE` 和 `GROUP_AT_MESSAGE_CREATE`。
- 接收入群、退群和群消息接收设置变化事件。
- 自动发现新群，查询 `/v2/groups/{group_openid}/info` 和 `/bot_state`。
- 保存正文、原始时间、接收时间、发送者群内 openid、昵称、角色、消息类型、引用/转发嵌套结构。
- 顶层及递归 `msg_elements` 中的图片、语音和附件元数据进入数据库。
- 图片和语音按策略下载；视频、普通文件和大文件默认只存元数据。

### 运行能力

- App Access Token 自动刷新。
- WebSocket 心跳、Resume、Invalid Session 后重新 Identify。
- 断线指数退避重连、单进程锁和 macOS LaunchAgent 自动拉起。
- SQLite WAL、事务落库、每日快照和状态查询。

### 出站消息

当前采集器没有实现出站消息。后续扩展可以参考官方接口，权限和配额需在部署时核验：

```http
POST https://api.bot.qq.com/v2/groups/{group_openid}/messages
```

纯文本请求的最小结构：

```json
{
  "msg_type": 0,
  "content": "文本内容"
}
```

支持的主要消息类型包括：

- `msg_type=0`：纯文本。
- `msg_type=2`：Markdown，可配内嵌键盘。
- `msg_type=7`：富媒体，需先通过上传接口取得 `file_info`。

被动回复可以在收到事件后 5 分钟内使用 `msg_id` 或 `event_id`。`GROUP_ADD_ROBOT` 应使用 WebSocket 事件最外层的 `id` 作为 `event_id`，不能误用 `d.id`。同一个被动回复的 `msg_seq` 需要递增且避免重复。

主动消息不带 `msg_id` 或 `event_id`，需要机器人具备主动发言权限，并受 Bot、单群和每日配额限制。是否允许主动发言应查询当前机器人的实际接收/发言设置。

官方文档：[群消息发送接口](https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_groups_group_openid_messages.post.html)

## 适合后续增加的功能

### 1. 机器人自动欢迎

触发 `GROUP_ADD_ROBOT` 后：

1. 先落库事件。
2. 查询群资料和接收设置。
3. 在事件有效期内用 `event_id` 回复欢迎语。
4. 发送结果写入 `outbound_messages`，按事件幂等，避免重复欢迎。

欢迎语应明确说明：公开群消息和附件会被保存为社区反馈素材；用户可以用 `【问题】`、`【需求】` 标记内容；不要发送密码、Token 等敏感信息。

### 2. 轻量交互

可以支持：

- `@机器人 帮助`：返回提交反馈格式。
- `【需求】`、`【问题】`：只增加可检索标签，不自动创建 issue。
- 管理员查询最近消息数、附件数和采集状态。
- 管理员手动发送公告或重新发送欢迎语。

### 3. 富媒体和管理消息

可以增加图片/文件上传后发送、引用回复、机器人自己消息撤回和 Markdown/键盘菜单。所有出站操作都应记录目标群、调用原因、请求幂等键、响应状态和 QQ 返回 ID。

### 4. 统计和导出

可以增加按群、时间、消息类型、附件类型统计，以及导出脱敏 JSON/CSV。导出功能需要单独的权限控制，不能把真实 token、临时媒体 URL 或 `auth_token` 带出。

## 当前明确不支持或不应承诺

- 没有普通 QQ 群历史消息拉取接口，无法补齐机器人离线前或长期离线期间的消息。
- 没有可靠的普通 QQ 群列表接口，群只能通过入群事件或消息逐步发现。
- 不能获取完整群成员名单。
- 不能获取用户真实 QQ 号，只能保存按 Bot 隔离的 `member_openid`。
- 不能依靠多个同时运行的 WebSocket 连接提高稳定性；本实现按 `shard=[0,1]` 建连，应采用单主连接和故障接管。
- 当前没有自动 issue 分类、相似问题合并或外部工单同步。

## 数据和隐私边界

- 临时媒体 URL 带有短期签名，下载成功后从长期附件记录中移除。
- 日志不应打印 AppSecret、Access Token、`auth_token` 或完整聊天正文。
- 部署者应确保有权采集目标群的消息和附件，并向群成员说明保存行为。
- 任何未来的自动回复或外部系统写入，都要增加权限、幂等和审计记录。
