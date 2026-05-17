# Polymarket Chain Monitor

Polymarket 多钱包链上监控 + 智能跟单 + BTC 5分钟 AI 预测交易平台。

## 功能概览

| 模块 | 说明 |
|------|------|
| **链上监控** | Polygon WebSocket 实时订阅 OrderFilled / Transfer 事件，解码 V1/V2 合约日志 |
| **多钱包跟踪** | 同时监控多个钱包，聚合信号并评分 |
| **智能跟单** | 自动复制优质钱包的买入操作，支持 paper/sign/live 三种模式 |
| **退出检测** | 双系统退出监控（活动轮询 + 持仓缩水检测），支持选定项目退出提醒 |
| **自动止盈止损** | 追踪止盈、固定止盈、止损、价格地板、自动退出 |
| **BTC 5分钟预测** | 自动发现 BTC Up/Down 市场，Binance K线 + 20+ 技术指标 + AI 方向预测 |
| **Web UI** | 暗色主题仪表盘，实时信号、持仓、交易控制、账户余额 |
| **QQ Bot** | 高质量信号推送，支持 QQ 查询持仓 / 卖出指令 |
| **资金面板** | 定期推送账户余额变化到 QQ |

## 快速开始

### 环境要求

- Node.js >= 18
- 网络代理（访问 Binance / Polymarket API）

### 安装

```bash
git clone <repo-url>
cd polymarket-chain-monitor
npm install
```

### 配置

复制并编辑环境配置：

```bash
cp .env.example .env
```

最小配置（仅监控，不交易）：

```env
POLYGON_WSS=wss://polygon-mainnet.g.alchemy.com/v2/<YOUR_KEY>
POLYGON_HTTP=https://polygon-mainnet.g.alchemy.com/v2/<YOUR_KEY>
WATCH_WALLET=0x你的钱包地址
PROXY_URL=http://127.0.0.1:7890
UI_PORT=3001
```

### 启动

```bash
npm start
```

打开浏览器访问 `http://127.0.0.1:3001`。

### 查询持仓（CLI）

```bash
npm run positions
```

## 项目结构

```
├── ws-bridge.js              # 主进程：链上监控 / HTTP服务 / QQ Bot / 信号聚合
├── trade-executor.js         # 交易执行：CLOB下单 / 止盈止损 / 资金管理
├── btc-5m-analyzer.js        # BTC 5分钟：市场发现 / K线分析 / AI预测 / 自动交易
├── get-positions.js          # CLI 持仓查询工具
├── index.html                # Web UI 仪表盘
├── .env                      # 环境配置
├── watch-wallets.json        # 监控钱包列表
├── auto-trade-wallets.json   # 自动跟单配置
├── tracked-projects.json     # 选定退出监控项目
├── trade-log.jsonl           # 交易日志（JSONL）
└── btc5m-logs/
    └── events.jsonl          # BTC 5分钟分析日志
```

## 核心模块

### ws-bridge.js — 主进程

启动流程：
1. 加载钱包列表 / 选定项目 / 跟单配置
2. 初始化 BTC 5分钟模块（如已启用）
3. 启动 HTTP 服务器 + WebSocket 桥接
4. 连接 Polygon WebSocket RPC，订阅链上事件
5. 连接 QQ Bot Gateway
6. 启动退出监控 + 账户余额推送

### trade-executor.js — 交易引擎

- **三种模式：** `paper`（模拟）→ `sign`（签名不提交）→ `live`（真实交易）
- **安全控制：** 紧急停止开关、每日支出上限、重复信号冷却、价格偏差过滤、最低深度要求
- **自动退出：** 跟踪钱包退出时自动卖出
- **止盈止损：** 追踪止盈（从高点回撤卖出）、固定止盈、止损、价格地板

### btc-5m-analyzer.js — BTC 5分钟 AI 预测

- 每 5 分钟自动发现新的 BTC Up/Down 市场
- 从 Binance 获取 1分钟 K线（60根）
- 计算技术指标：RSI(7/14)、EMA(5/13/21)、MACD、VCP、Darvas Box、枢轴点、ATR、Second Entry
- **代码预计算评分**（趋势/结构/量能/形态/RR 各0-2分），AI 仅做最终确认
- 支持 OpenAI 兼容 API（Gemini / Qwen / DeepSeek 等）
- 每次分析 75 秒超时，最多 2 次重试

## 配置说明

### 必填项

| 变量 | 说明 |
|------|------|
| `POLYGON_WSS` | Polygon WebSocket RPC |
| `POLYGON_HTTP` | Polygon HTTP RPC |
| `WATCH_WALLET` | 监控的钱包地址 |
| `PROXY_URL` | 网络代理地址 |

### 交易配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTO_TRADE_ENABLED` | false | 启用自动跟单 |
| `AUTO_TRADE_MODE` | paper | paper / sign / live |
| `AUTO_TRADE_DRY_RUN` | true | live 模式下仍阻止真实下单 |
| `AUTO_TRADE_USD_PER_ORDER` | 1 | 每单金额 (USD) |
| `AUTO_TRADE_DAILY_MAX_USD` | 50 | 每日支出上限 |

### BTC 5分钟 AI 配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `BTC5M_ENABLED` | false | 启用 BTC 5分钟模块 |
| `BTC5M_AI_BASE_URL` | - | AI API 地址（OpenAI 兼容） |
| `BTC5M_AI_API_KEY` | - | AI API 密钥 |
| `BTC5M_AI_MODEL` | - | 模型名称 |
| `BTC5M_DEFAULT_TRADE_USD` | 1 | 默认交易金额 |

支持的 AI 模型（OpenAI 兼容格式）：
- Google Gemini（gemini-2.0-flash 等）
- 阿里 Qwen（qwen-turbo 等）
- DeepSeek（deepseek-chat 等）
- 小米 MIMO（mimo-v2-omni 等）

### 安全控制

| 变量 | 说明 |
|------|------|
| `KILL_SWITCH` | 设为 true 立即停止所有自动交易 |
| `BALANCE_ALERT_USD` | 余额低于此值时发出警告 |
| `AUTO_TRADE_MAX_ACTIVE_PROJECTS` | 最大同时持仓项目数 |
| `AUTO_TRADE_MIN_FREE_CASH_USD` | 最低保留现金 |

完整配置项参见 [.env](.env) 文件。

## Web UI

启动后访问 `http://127.0.0.1:<UI_PORT>`，功能包括：

- 实时信号流（链上交易 + 订单簿深度）
- 钱包管理（添加 / 删除 / 查看评分）
- 持仓总览 + 盈亏统计
- BTC 5分钟分析面板（K线指标 / AI 预测 / 交易执行）
- 账户余额实时监控
- 交易控制（买入 / 卖出 / 紧急停止）

## QQ Bot

配置 `QQ_APP_ID` 和 `QQ_CLIENT_SECRET` 后，系统会自动：

- 推送高质量买入/退出信号
- 定期推送账户余额变化
- 支持 QQ 内查询持仓、按编号卖出

## 日志

- `trade-log.jsonl` — 所有交易操作记录（JSONL 格式）
- `btc5m-logs/events.jsonl` — BTC 5分钟分析结果 + 结算记录

## 技术栈

- **运行时：** Node.js (CommonJS)
- **链交互：** Polygon WebSocket RPC + ethers.js
- **交易 SDK：** @polymarket/clob-client-v2
- **HTTP 客户端：** node-fetch + https-proxy-agent
- **WebSocket：** ws（Polygon / CLOB 价格流 / QQ Bot）
- **AI：** OpenAI 兼容 API
- **前端：** 单文件 HTML + 原生 JS

## 许可

私人项目，未公开授权。
