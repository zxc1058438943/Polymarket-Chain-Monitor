# Polymarket Chain Monitor

Polymarket 多钱包链上监控面板，包含 Polygon 事件监听、钱包信号聚合、Web UI，以及可选的 BTC 5 分钟行情分析模块。

> **当前仓库的交易执行边界：** 当前版本没有包含 `trade-executor.js`，因此不要将本仓库当作可直接使用的实盘自动交易程序。默认只用于监控和研究；示例环境会关闭自动交易并打开紧急停止开关。不要把私钥、API 密钥或真实的 `.env` 文件提交到 Git。

## 功能

- Polygon WebSocket 链上事件监听，解析相关交易与代币转移日志。
- 多钱包监控与信号聚合，可通过 Web UI 查看监控状态和信号。
- 可选 QQ Bot 通知（需要自行填写应用凭证）。
- 可选 BTC 5 分钟市场发现、Binance K 线与技术指标分析；需要时再自行配置 AI API。
- CLI 持仓查询工具，通过 Polymarket Data API 查询指定钱包公开持仓。
- 准入自检、Node.js 单元测试和 GitHub Actions 自动检查。

## 环境要求

- Node.js 18 或更高版本。
- npm。
- 如所在网络不能直接访问 Polymarket / Binance API，可配置本机代理。

## 安装

```bash
git clone https://github.com/zxc1058438943/Polymarket-Chain-Monitor.git
cd Polymarket-Chain-Monitor
npm ci
```

Windows PowerShell 创建本地配置：

```powershell
Copy-Item .env.example .env
```

macOS / Linux：

```bash
cp .env.example .env
```

编辑本地 `.env`，至少填写你自己使用的 Polygon RPC。配置模板中的交易相关设置是安全默认值，不要为了通过检查而开启实盘开关。

## 启动和检查

启动 Web UI 与链上监控：

```bash
npm start
```

默认界面地址为 `http://127.0.0.1:3001`。若端口已占用，可在本地 `.env` 中修改 `UI_PORT`。

运行代码与项目结构准入检查：

```bash
npm run check
```

运行单元测试：

```bash
npm test
```

查询某个钱包的持仓：

```bash
npm run positions -- 0xYourWalletAddress
```

也可设置 `PM_FUNDER_ADDRESS` 或 `WATCH_WALLET` 后运行 `npm run positions`。增加 `--json` 参数可输出 JSON 格式：

```bash
npm run positions -- 0xYourWalletAddress --json
```

持仓查询只读取公开 Data API，不签名、不下单。

## 项目结构

| 路径 | 用途 |
| --- | --- |
| `ws-bridge.js` | 主服务：链上监听、HTTP 服务、WebSocket 桥接与可选通知 |
| `index.html` | Web UI |
| `monitor-polymarket-ws.js` | 旧版单钱包监听脚本，仅用于兼容/参考 |
| `btc-5m-analyzer.js` | 可选 BTC 5 分钟行情分析模块 |
| `get-positions.js` | 查询公开钱包持仓的 CLI |
| `auto-trade-wallets.json` | 钱包跟踪/跟单配置数据 |
| `.env.example` | 不含凭证的本地配置模板 |
| `.gitignore` | 忽略本地配置、依赖和运行日志 |
| `scripts/check-repo.js` | 检查语法、依赖锁定、README 本地链接和安全默认值 |
| `test/repo.test.js` | 持仓查询工具的离线单元测试 |
| `.github/workflows/quality.yml` | 推送和 PR 时自动运行检查与测试 |

## 环境配置与安全

- **不要提交真实的 `.env` 文件。** `.env.example` 只放占位符；API 凭证应留在自己的本地环境。
- **默认不交易。** 保持 `AUTO_TRADE_ENABLED=false`、`AUTO_TRADE_MODE=paper`、`AUTO_TRADE_DRY_RUN=true`、`AUTO_TRADE_ALLOW_LIVE_AUTO=false`。
- 首次运行建议仅使用只读 RPC 和公开 Data API。不要把真实私钥粘贴到 Issue、日志或聊天中。
- 钱包地址本身是公开链上标识，但仍建议不要把私人监控名单及账户配置提交到公开仓库。
- `trade-executor.js` 未包含在当前仓库中；涉及真实下单、自动退出或止盈止损的功能不能仅凭 README 声明视为已经可用。

## 模块说明

### `ws-bridge.js`

主监控服务。需要在本地 `.env` 中配置 Polygon WebSocket RPC（`POLYGON_WSS`），并按需设置 `POLYGON_HTTP`、`WATCH_WALLET`、`PROXY_URL` 等变量。

### `btc-5m-analyzer.js`

用于可选的 BTC 5 分钟市场分析。通过 `BTC5M_ENABLED` 显式启用前，请先配置并检查数据源；如需要 AI 分析，再填写 `BTC5M_AI_BASE_URL`、`BTC5M_AI_API_KEY` 和 `BTC5M_AI_MODEL`。该模块的分析输出不代表盈利保证。

## 已知边界

- 旧版 `monitor-polymarket-ws.js` 与主服务逻辑可能不同步，请不要同时启动。
- 自动交易执行器没有随当前仓库提供；不要使用真实资金测试缺失的交易执行流程。
- 公开 API、合约与市场响应格式可能发生变化；运行前请在模拟/只读环境确认连接和数据是否正常。

## 许可

仓库当前未提供明确的开源许可证。除非仓库所有者另行授权，不应假定可以将代码重新发布或用于商业用途。
