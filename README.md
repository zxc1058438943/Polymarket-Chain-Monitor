# Polymarket Chain Monitor

Polymarket 多钱包链上监控面板：监听 Polygon 交易事件、聚合钱包活动信号，并通过本地 Web UI 查看监控结果。仓库同时包含一个独立的公开持仓查询 CLI，以及可选的 BTC 5 分钟市场分析模块。

> **安全边界：** 当前仓库并未包含 `trade-executor.js`，因此不具备完整的真实下单实现。交易接口会明确报告执行器不可用。不要把缺失的执行器当成可正常实盘的功能，也不要为了通过检查打开真实交易开关。

## 功能和边界

- Polygon WebSocket 事件监控，解析支持的订单成交及代币转账事件。
- 多钱包信号聚合与本地 Web UI。
- 可选的 QQ Bot 消息通知。
- 可选的 BTC 5 分钟市场分析和技术指标。
- 通过 Polymarket Data API 查询公开钱包持仓；只读、不签名、不下单。
- 统一项目准入检查、离线单元测试和 GitHub Actions 工作流。

## 环境要求

- Node.js 18 或更高版本。
- npm。
- Polygon WebSocket RPC。根据使用的功能，可再设置 HTTP RPC、代理和通知配置。

## 安装

```bash
git clone https://github.com/zxc1058438943/Polymarket-Chain-Monitor.git
cd Polymarket-Chain-Monitor
npm ci
```

Windows PowerShell：

```powershell
Copy-Item .env.example .env
```

macOS / Linux：

```bash
cp .env.example .env
```

编辑本机 `.env`，填写自己的 `POLYGON_WSS` 等配置。不要提交真实的 `.env`、私钥、API 密钥、令牌或私人监控数据。初次启动请使用只读配置和模拟模式。

## 启动与验证

启动监控服务与 Web UI：

```bash
npm start
```

默认 UI 地址：`http://127.0.0.1:3001`。需在本机 `.env` 中填写 `POLYGON_WSS`，否则主进程会拒绝启动。端口可通过 `UI_PORT` 修改。

运行项目准入自检：

```bash
npm run check
```

运行单元测试：

```bash
npm test
```

自检覆盖 JavaScript 语法、HTML 内嵌脚本语法、package/lock 依赖一致性、README 本地链接、环境模板安全默认值、运行日志忽略规则和钱包 JSON 格式。单元测试覆盖钱包地址参数校验、持仓字段规范化、API 请求参数、HTTP 异常、响应格式异常与分页上限。

### 查询公开持仓

```bash
npm run positions -- 0xYourWalletAddress
```

也可以设置 `PM_FUNDER_ADDRESS` 或 `WATCH_WALLET`。输出 JSON：

```bash
npm run positions -- 0xYourWalletAddress --json
```

默认最多读取 10 页，每页最多 500 条记录。达到上限时 CLI 会输出提示，允许通过 `POSITIONS_MAX_PAGES` 提高上限。遇到网络异常、非 2xx HTTP 响应或错误 JSON 时，程序会返回非零退出状态。

## 文件结构

| 文件 | 作用 |
| --- | --- |
| `ws-bridge.js` | 主监控服务、HTTP API、WebSocket 桥接与通知 |
| `index.html` | 本地 Web UI |
| `monitor-polymarket-ws.js` | 旧版单钱包监听脚本，不应与主服务同时运行 |
| `btc-5m-analyzer.js` | 可选 BTC 5 分钟分析模块，默认关闭 |
| `get-positions.js` | 只读持仓查询 CLI |
| `auto-trade-wallets.example.json` | 空的跟单配置示例，不含真实钱包地址 |
| `.env.example` | 不含凭据的安全配置模板 |
| `.gitignore` | 排除本地环境和运行产物 |
| `scripts/check-repo.js` | 项目结构、语法、依赖与安全配置自检 |
| `test/repo.test.js` | 持仓 CLI 的离线单元测试 |
| `.github/workflows/quality.yml` | 推送和 PR 时触发的自动检查 |

## 运行时数据与输出

主服务会在本地创建或更新 `watch-wallets.json`、`tracked-projects.json` 和 `auto-trade-wallets.json`。这些文件属于本地运行状态，已加入 `.gitignore`，不应提交到仓库。交易日志（若对应模块启用）写入 `trade-log.jsonl`，BTC 5 分钟分析日志写入 `btc5m-logs/events.jsonl`。持仓 CLI 默认将结果输出到终端；使用 `--json` 时输出 JSON，不会自动写入文件。

## 交易和隐私安全

服务首次启动会为跟单配置创建本地 `auto-trade-wallets.json`；仓库只保留空模板 `auto-trade-wallets.example.json`，避免把个人监控名单和启用实盘的配置公开。示例配置默认设置 `AUTO_TRADE_ENABLED=false`、`AUTO_TRADE_MODE=paper`、`AUTO_TRADE_DRY_RUN=true`、`AUTO_TRADE_ALLOW_LIVE_AUTO=false`、`AUTO_EXIT_ENABLED=false`、`QQ_TRADE_COMMANDS_ENABLED=false` 和 `KILL_SWITCH=true`。除非你已在独立环境审查交易执行器、密钥权限和风险限额，否则不要启用实盘功能。

目前 `trade-executor.js` 不在仓库文件列表中，交易连接、凭证派生、下单和交易退出功能不能视为已经验证可用。账户公开持仓查询与真实交易执行是两件不同的事。

原仓库曾公开过。即便将它改为私有，也不能满足“从未公开过”的仓库准入条件。若需要用于要求私有且从未公开的开发过程评测，应选用符合条件的真实私有项目，而不是试图通过改名或补交提交改变历史事实。

## 设计取舍与验证策略

1. **持仓查询与交易执行分离。** 查询 CLI 只读访问公开 Data API，不依赖交易私钥或 CLOB 凭证，便于离线单测，也减少误触真实订单的风险。
2. **分页有明确上限。** 避免 API 数据异常时无边界请求；到达上限时提示数据可能被截断，调用方可主动增加 `POSITIONS_MAX_PAGES`。
3. **外部数据需规范化。** Data API 的数值字段统一转为有限数字，布尔字段用明确的真值列表解析，防止字符串 `"false"` 被 JavaScript 当作真。
4. **交易执行器缺失时快速失败。** 状态接口报告不可用，连接、凭证派生、测试下单和紧急停止写操作接口返回明确错误，而不是抛出难以理解的 500。
5. **验证不依赖外网市场响应。** 单元测试使用模拟 fetch 响应，覆盖常规结果、空值/布尔字段、错误状态、错误响应结构以及分页边界。部署前仍需对真实 Polygon RPC 与 Data API 做只读连接验收。

## 后续开发 TODO

1. **补齐交易执行器的可审计接口。** 当前 `ws-bridge.js` 仍有余额、跟单、退出与交易 API 调用依赖 `trade-executor.js`，但执行器文件不在仓库中；在恢复实盘能力前，需要先确定模块契约、失败语义、模拟模式与账户权限边界。
2. **给主 HTTP API 增加统一鉴权与请求限制。** `UI_SECRET` 目前用于 UI 请求头，需逐条审计所有改变钱包列表、项目列表、配置和交易状态的路由，补未授权请求测试及速率限制，避免本机端口意外暴露时被调用。
3. **为 Polygon WebSocket 增加可复现的日志回放测试。** 目前事件解析和聚合逻辑与长期连接、重连行为交织；应把代表性的 V1/V2 成交及 ERC 转账日志整理为脱敏 fixture，断言断线重连和重复日志不会重复发信号。
4. **解决持仓查询分页截断的可观测性。** CLI 当前用页数上限保护 API；后续可增加明确的分页元数据、重试退避和请求 ID，以便在限流或 API 数据量增长时区分“持仓为空”和“只读到部分结果”。
5. **为 BTC 5 分钟分析建立离线回放基准。** `btc-5m-analyzer.js` 依赖实时市场发现、行情和 AI 服务；应保存脱敏的市场快照与 K 线 fixture，测试超时、市场轮换、重复调度及结算日志，且在没有交易执行器时始终保持纯分析模式。
6. **为本地运行产物建立独立的数据目录。** 主服务会读写监控钱包、跟踪项目和自动跟单 JSON；应统一通过可配置的数据目录加载和保存这些文件，并在启动时验证备份、格式迁移与写入失败的行为。
7. **完善 PR/CI 的失败诊断。** 当前托管 Actions 的运行没有提供 runner 步骤或日志，需确认仓库的 Actions 可用性后，再确保 `npm ci`、`npm run check` 和 `npm test` 在真实 Node.js runner 上执行，并保存测试报告供审阅。

## 许可

当前仓库没有明确的开源许可证。未经代码所有者明确授权，不应假设允许重新发布或商业使用。
