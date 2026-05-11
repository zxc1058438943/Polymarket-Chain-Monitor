olymarket Chain Monitor - 完整功能详解
1. 核心功能模块
1.1 多钱包链上监控
监控多个钱包: 从 watch-wallets.json 读取监控钱包列表
实时事件监听: 通过 Polygon WebSocket 监听链上事件：
OrderFilled (V1/V2) - 订单成交
Transfer - pUSD 转账
TransferSingle - ERC1155 持仓变动
钱包发现: 自动从 Polymarket Leaderboard 发现高评分钱包
1.2 信号聚合与分类
从原始链上交易中提取结构化信号：

信号类型	说明
open	开仓 - 新买入某档位
add	加仓 - 继续买入已有档位
reduce	减仓 - 部分卖出
close	平仓 - 全部卖出
activity_exit	从活动检测到的退出
position_exit	从持仓变化检测到的退出
1.3 自动跟单交易
当被监控钱包买入时，自动在你的账户下单：

触发条件:
信号类型为 taker 买入
价格在 [0.02, 0.95] 范围内
深度 > $50
钱包评分 ≥ 65
参数可调: 每单金额、最大价差、订单类型
1.4 自动平仓 (Auto-Exit)
当被跟随钱包卖出时，自动卖出你的对应持仓：

触发: 检测到 activity_exit 或 position_exit 信号
执行: 以最佳 bid 价卖出（扣除滑点）
冷却: 防止重复平仓
1.5 自动止盈 (Auto Take-Profit)
盈利达到阈值时自动卖出：

默认配置: PnL ≥ 350% 时触发
防止 pyramiding: 已持仓不再加仓
1.6 追踪止损 (Trailing Stop)
激活条件: PnL 达到 30% 后开始追踪最高价
回撤止损: 从高点下跌 30% 时自动卖出
1.7 止损 (Stop Loss)
配置: PnL 跌破 -50% 时自动卖出
价格地板: 价格跌破 $0.05 时卖出
2. 安全机制
机制	说明
Kill Switch	紧急停止 KILL_SWITCH=true
三重模式	paper → sign → live
日额度限制	AUTO_TRADE_DAILY_MAX_USD=50
重复冷却	同信号 60 秒内不重复下单
余额告警	低于阈值时警告
持仓限制	最大活跃项目数、最小现金
3. 启动方法
前置要求

npm install
配置 .env

# 区块链连接
POLYGON_WSS=wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYGON_HTTP=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
WATCH_WALLET=0xYourWallet

# 代理（可选）
PROXY_URL=http://127.0.0.1:7890

# UI 端口
UI_PORT=3001

# 交易私钥
PM_PRIVATE_KEY=0xYourPrivateKey
PM_FUNDER_ADDRESS=0xYourFunderAddress
启动方式
方式一：Web UI + 实时监控（推荐）

npm start
# 访问 http://localhost:3001
自动启动 WebSocket 桥接服务 + 浏览器 UI

方式二：仅查看持仓

npm run positions
方式三：命令行监控（已弃用）

npm run monitor
方式四：测试交易连接

npm run trade:check
4. UI 功能说明
打开 http://localhost:3001 后：

功能区	说明
状态栏	连接状态、被监控钱包地址
指标卡	信号总数、各类型计数
信号日志	实时滚动，可按类型筛选
钱包管理	添加/移除监控钱包
项目追踪	标记跟随的项目，触发自动平仓
QQ Bot 状态	Telegram 通知推送状态
5. QQ 机器人推送
配置后自动推送：

高质量买入信号
减仓/平仓信号
账户余额变化
6. 配置文件
文件	用途
watch-wallets.json	监控钱包列表
tracked-projects.json	追踪的项目（自动平仓用）
trade-log.jsonl	交易日志
.pm-clob-creds.json	CLOB API 凭证
