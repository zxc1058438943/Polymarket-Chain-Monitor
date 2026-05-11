tracked-projects_example.json/**
 * Polymarket multi-wallet monitor
 *
 * Features:
 * - Monitor one or more wallets from watch-wallets.json
 * - Add/remove wallets from the web UI
 * - Aggregate a tx into one wallet-level follow signal
 * - Classify action as open / add / reduce / close when possible
 * - Show current positions + settlement status in UI
 * - Optional QQ Bot push with Gateway keepalive
 */

require("dotenv").config();

const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");
const { ethers } = require("ethers");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { URL } = require("url");

let tradeExecutor = null;
try {
  tradeExecutor = require("./trade-executor");
} catch (err) {
  console.warn(`[交易模块] trade-executor.js 未加载，交易账户/下单测试功能不可用: ${err.message}`);
}
function requireTradeExecutor() {
  if (!tradeExecutor) {
    throw new Error("交易模块不可用：请确认 trade-executor.js 存在，并已运行 npm install @polymarket/clob-client-v2 viem");
  }
  return tradeExecutor;
}

function cleanEnvValue(value) {
  // dotenv treats unquoted # as a comment in many cases, but keep this guard so
  // copied .env lines like WATCH_WALLET=0xabc # note never break ethers.isAddress.
  return String(value || "").replace(/\s+#.*$/, "").trim();
}
function envBool(name, fallback = false) {
  const v = cleanEnvValue(process.env[name]).toLowerCase();
  if (!v) return fallback;
  return ["1", "true", "yes", "on"].includes(v);
}
function envNumber(name, fallback) {
  const raw = cleanEnvValue(process.env[name]);
  if (raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}
function truthyLike(value) {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return false;
  if (typeof value === "number") return Number.isFinite(value) && value !== 0;
  const s = String(value).trim().toLowerCase();
  if (!s) return false;
  return ["1", "true", "yes", "y", "on", "closed", "resolved", "redeemable", "claimable"].includes(s);
}

// ─── Config ────────────────────────────────────────────────────────────────
const WSS_URL = cleanEnvValue(process.env.POLYGON_WSS);
const POLYGON_HTTP = cleanEnvValue(process.env.POLYGON_HTTP);
const DEFAULT_WALLET = cleanEnvValue(process.env.WATCH_WALLET).toLowerCase();
const PROXY_URL = cleanEnvValue(process.env.PROXY_URL);
const TX_FLUSH_DELAY_MS = envNumber("TX_FLUSH_DELAY_MS", 3500);
const UI_PORT = envNumber("UI_PORT", 3001);
const HOST = cleanEnvValue(process.env.UI_HOST) || "127.0.0.1";

const QQ_APP_ID = (process.env.QQ_APP_ID || "").trim();
const QQ_CLIENT_SECRET = (process.env.QQ_CLIENT_SECRET || "").trim();
const QQBOT_TARGET_TYPE = (process.env.QQBOT_TARGET_TYPE || "c2c").trim().toLowerCase();
const QQBOT_TARGET_ID = (
  process.env.QQBOT_TARGET_ID ||
  process.env.QQBOT_HOME_CHANNEL ||
  process.env.QQ_HOME_CHANNEL ||
  ""
).trim();
const QQ_SANDBOX = envBool("QQ_SANDBOX", false);
const QQ_API_BASE = QQ_SANDBOX ? "https://sandbox.api.sgroup.qq.com" : "https://api.sgroup.qq.com";
const QQ_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken";
const QQ_GATEWAY_INTENTS = 1 << 25; // C2C_MESSAGE_CREATE

// ─── Follow-quality filters ────────────────────────────────────────────────
// UI 会显示所有信号；QQ 默认只推送通过这些过滤条件的高质量“可跟”信号。
const QQ_PUSH_HIGH_QUALITY_ONLY = !["0", "false", "no", "off"].includes(cleanEnvValue(process.env.QQ_PUSH_HIGH_QUALITY_ONLY).toLowerCase());
// true = QQ 只推送：主动买入订单 + 减仓/平仓订单；UI 仍显示所有信号。
const QQ_PUSH_ACTIVE_BUY_EXIT_ONLY = envBool("QQ_PUSH_ACTIVE_BUY_EXIT_ONLY", true);
const SIGNAL_MIN_WALLET_SCORE = envNumber("SIGNAL_MIN_WALLET_SCORE", 65);
const SIGNAL_MIN_NOTIONAL_USD = envNumber("SIGNAL_MIN_NOTIONAL_USD", 20);
const SIGNAL_MAX_PRICE_DIFF = envNumber("SIGNAL_MAX_PRICE_DIFF", 0.03); // 3 cents
const SIGNAL_MIN_DEPTH_USD = envNumber("SIGNAL_MIN_DEPTH_USD", 50);
const SIGNAL_FOLLOW_SAMPLE_USD = envNumber("SIGNAL_FOLLOW_SAMPLE_USD", 50);
const SIGNAL_ONLY_TAKER_OPEN_ADD = !["0", "false", "no", "off"].includes(cleanEnvValue(process.env.SIGNAL_ONLY_TAKER_OPEN_ADD).toLowerCase());



// ─── Wallet discovery ─────────────────────────────────────────────────────
const DISCOVERY_DEFAULT_CATEGORY = (process.env.DISCOVERY_DEFAULT_CATEGORY || "OVERALL").trim().toUpperCase();
const DISCOVERY_DEFAULT_PERIOD = (process.env.DISCOVERY_DEFAULT_PERIOD || "MONTH").trim().toUpperCase();
const DISCOVERY_DEFAULT_ORDER_BY = (process.env.DISCOVERY_DEFAULT_ORDER_BY || "PNL").trim().toUpperCase();
const DISCOVERY_DEFAULT_LIMIT = envNumber("DISCOVERY_DEFAULT_LIMIT", 25);
const DISCOVERY_MIN_PNL = envNumber("DISCOVERY_MIN_PNL", 0);
const DISCOVERY_MIN_VOL = envNumber("DISCOVERY_MIN_VOL", 0);
const DISCOVERY_MIN_SCORE = envNumber("DISCOVERY_MIN_SCORE", 65);
const FETCH_TIMEOUT_MS = envNumber("FETCH_TIMEOUT_MS", 12000);
const DISCOVERY_ANALYZE_TIMEOUT_MS = envNumber("DISCOVERY_ANALYZE_TIMEOUT_MS", 6000);
const DISCOVERY_CONCURRENCY = Math.max(1, envNumber("DISCOVERY_CONCURRENCY", 5));

const ENABLE_CHAIN_BALANCE_CHECK = envBool("ENABLE_CHAIN_BALANCE_CHECK", false);
const BALANCE_QUERY_TIMEOUT_MS = envNumber("BALANCE_QUERY_TIMEOUT_MS", 12000);

// Cache and reliability knobs.
const MARKET_CACHE_TTL_MS = envNumber("MARKET_CACHE_TTL_MS", 30 * 60_000);
const ORDERBOOK_CACHE_TTL_MS = envNumber("ORDERBOOK_CACHE_TTL_MS", 45_000);
const ACTIVITY_DEFAULT_LIMIT = Math.max(1, envNumber("ACTIVITY_DEFAULT_LIMIT", 100));
const ACTIVITY_MAX_LIMIT = Math.max(20, envNumber("ACTIVITY_MAX_LIMIT", 250));
const ACTIVITY_CACHE_TTL_MS = envNumber("ACTIVITY_CACHE_TTL_MS", 20_000);

// Exit watchdogs. These are deliberately separate from the Polygon WS listener:
// - Activity polling catches sells/redeems that the WS listener missed, and exits that happened via API-visible trades.
// - Position polling catches a position shrinking/disappearing even if the activity endpoint is delayed.
const EXIT_ACTIVITY_WATCH_ENABLED = envBool("EXIT_ACTIVITY_WATCH_ENABLED", true);
const EXIT_ACTIVITY_POLL_MS = envNumber("EXIT_ACTIVITY_POLL_MS", 20_000);
const EXIT_ACTIVITY_LOOKBACK_LIMIT = Math.max(5, Math.min(ACTIVITY_MAX_LIMIT, envNumber("EXIT_ACTIVITY_LOOKBACK_LIMIT", 80)));
const EXIT_ACTIVITY_STARTUP_RECENT_MINUTES = envNumber("EXIT_ACTIVITY_STARTUP_RECENT_MINUTES", 20);
const EXIT_POSITION_WATCH_ENABLED = envBool("EXIT_POSITION_WATCH_ENABLED", true);
const EXIT_POSITION_POLL_MS = envNumber("EXIT_POSITION_POLL_MS", 30_000);
const EXIT_POSITION_MIN_SIZE_DELTA = envNumber("EXIT_POSITION_MIN_SIZE_DELTA", 0.01);
// Share-delta guard: Data API can jitter by tiny fractions while UI rounds to 2 decimals.
// A true reduce alert should show a visible share decrease, not only a value/price change.
const EXIT_POSITION_MIN_SHARE_DELTA = envNumber("EXIT_POSITION_MIN_SHARE_DELTA", 0.1);
const EXIT_POSITION_MIN_SIZE_DELTA_PCT = envNumber("EXIT_POSITION_MIN_SIZE_DELTA_PCT", 0.001); // 0.1% of previous size
const EXIT_POSITION_REQUIRE_DISPLAY_SHARE_DROP = envBool("EXIT_POSITION_REQUIRE_DISPLAY_SHARE_DROP", true);
const EXIT_POSITION_MIN_VALUE_DELTA_USD = envNumber("EXIT_POSITION_MIN_VALUE_DELTA_USD", 1);
// Prevent high-volume wallets from flooding the UI/QQ when Data API pages shift or old near-zero positions disappear.
const EXIT_POSITION_TRACK_MIN_VALUE_USD = envNumber("EXIT_POSITION_TRACK_MIN_VALUE_USD", 5);
const EXIT_POSITION_ALERT_ON_MISSING = envBool("EXIT_POSITION_ALERT_ON_MISSING", false);
const EXIT_POSITION_MISSING_CONFIRM_POLLS = Math.max(1, envNumber("EXIT_POSITION_MISSING_CONFIRM_POLLS", 2));
const EXIT_POSITION_MAX_SIGNALS_PER_WALLET_POLL = Math.max(1, envNumber("EXIT_POSITION_MAX_SIGNALS_PER_WALLET_POLL", 5));
const EXIT_ACTIVITY_MIN_USD = envNumber("EXIT_ACTIVITY_MIN_USD", 1);
const EXIT_ACTIVITY_MAX_SIGNALS_PER_WALLET_POLL = Math.max(1, envNumber("EXIT_ACTIVITY_MAX_SIGNALS_PER_WALLET_POLL", 8));
const EXIT_BATCH_ENABLED = envBool("EXIT_BATCH_ENABLED", true);
const EXIT_BATCH_MAX_ITEMS = Math.max(1, envNumber("EXIT_BATCH_MAX_ITEMS", 8));
const EXIT_ALERT_COOLDOWN_MS = envNumber("EXIT_ALERT_COOLDOWN_MS", 10 * 60_000);
// true = 退出/减仓/平仓只针对用户在 UI 里手动选择过的项目；主动买入仍照常显示/推送。
const TRACKED_EXIT_ONLY = envBool("TRACKED_EXIT_ONLY", true);
const SEEN_LOG_LIMIT = envNumber("SEEN_LOG_LIMIT", 10_000);
const SEEN_LOG_PRUNE_TO = envNumber("SEEN_LOG_PRUNE_TO", 7_000);
const QQ_RETRY_MAX = envNumber("QQ_RETRY_MAX", 3);
const QQ_RETRY_DELAY_MS = envNumber("QQ_RETRY_DELAY_MS", 5_000);

// Auto-copy trading controls. Global live safety still lives in trade-executor.js;
// these settings choose which watched wallets are allowed to trigger automatic copy orders.
const AUTO_TRADE_DEFAULT_AMOUNT_USD = envNumber("AUTO_TRADE_USD_PER_ORDER", 1);
const AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF = envNumber("AUTO_TRADE_MAX_PRICE_DIFF", 0.05);
const AUTO_TRADE_DEFAULT_MODE = cleanEnvValue(process.env.AUTO_TRADE_MODE) || "paper";
const AUTO_TRADE_MIN_SECONDS_BETWEEN_WALLET_TRADES = envNumber("AUTO_TRADE_MIN_SECONDS_BETWEEN_WALLET_TRADES", 3);
// When an automatic buy is actually executed/simulated, add that token to the selected-project list
// so later exit reminders can trigger auto-exit for the same exact outcome token.
const AUTO_TRACK_PROJECT_AFTER_BUY = envBool("AUTO_TRACK_PROJECT_AFTER_BUY", true);
// Optional account-level guard before live/sign auto copy. Prevents the bot from
// opening new positions when cash is too low or too many active/settlement
// positions are already occupying the small account.
const AUTO_TRADE_ACCOUNT_GUARD_ENABLED = envBool("AUTO_TRADE_ACCOUNT_GUARD_ENABLED", true);
const AUTO_TRADE_MIN_FREE_CASH_USD = envNumber("AUTO_TRADE_MIN_FREE_CASH_USD", 0);
const AUTO_TRADE_MAX_ACTIVE_PROJECTS = envNumber("AUTO_TRADE_MAX_ACTIVE_PROJECTS", 0);
const AUTO_TRADE_MAX_TOTAL_PROJECTS = envNumber("AUTO_TRADE_MAX_TOTAL_PROJECTS", 0);
const AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS = envNumber("AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS", 0);
const AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD = envNumber("AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD", 0);
const AUTO_TRADE_BLOCK_ON_ACCOUNT_STATUS_ERROR = envBool("AUTO_TRADE_BLOCK_ON_ACCOUNT_STATUS_ERROR", false);
// Same-token guard: if your trading account already holds the exact outcome token,
// skip new auto-buy signals for that token. This prevents accidental pyramiding/add-ons
// when the leader wallet buys the same market several times.
const AUTO_TRADE_SKIP_IF_HOLDING_SAME_TOKEN = envBool("AUTO_TRADE_SKIP_IF_HOLDING_SAME_TOKEN", true);
const AUTO_TRADE_SAME_TOKEN_MIN_SHARES = envNumber("AUTO_TRADE_SAME_TOKEN_MIN_SHARES", 0.000001);

// Auto take-profit: when your own active position reaches this PnL%, sell it even
// without a followed-wallet exit signal. +350 means profit is +350%, not price 350%.
const AUTO_TAKE_PROFIT_ENABLED = envBool("AUTO_TAKE_PROFIT_ENABLED", false);
const AUTO_TAKE_PROFIT_PCT = envNumber("AUTO_TAKE_PROFIT_PCT", 350);
const AUTO_TAKE_PROFIT_MAX_PER_POLL = Math.max(1, envNumber("AUTO_TAKE_PROFIT_MAX_PER_POLL", 3));
const AUTO_TAKE_PROFIT_MIN_VALUE_USD = envNumber("AUTO_TAKE_PROFIT_MIN_VALUE_USD", 0.50);

// Trailing high-water-mark stop: after PnL reaches ACTIVATE_PCT, sell if price
// drops DRAWDOWN_PCT from its all-time peak during this session.
const AUTO_TRAILING_STOP_ENABLED = envBool("AUTO_TRAILING_STOP_ENABLED", false);
const AUTO_TRAILING_STOP_ACTIVATE_PCT = envNumber("AUTO_TRAILING_STOP_ACTIVATE_PCT", 30);
const AUTO_TRAILING_STOP_DRAWDOWN_PCT = envNumber("AUTO_TRAILING_STOP_DRAWDOWN_PCT", 30);
const AUTO_TRAILING_STOP_MAX_PER_POLL = Math.max(1, envNumber("AUTO_TRAILING_STOP_MAX_PER_POLL", 3));
const AUTO_TRAILING_STOP_MIN_VALUE_USD = envNumber("AUTO_TRAILING_STOP_MIN_VALUE_USD", 0.50);

// Stop-loss: sell when unrealised PnL% drops below -AUTO_STOP_LOSS_PCT.
const AUTO_STOP_LOSS_ENABLED = envBool("AUTO_STOP_LOSS_ENABLED", false);
const AUTO_STOP_LOSS_PCT = envNumber("AUTO_STOP_LOSS_PCT", 50);
const AUTO_STOP_LOSS_MAX_PER_POLL = Math.max(1, envNumber("AUTO_STOP_LOSS_MAX_PER_POLL", 3));
const AUTO_STOP_LOSS_MIN_VALUE_USD = envNumber("AUTO_STOP_LOSS_MIN_VALUE_USD", 0.30);

// Price-floor stop: sell when token price falls below this absolute threshold,
// regardless of PnL (catches near-zero probability collapse).
const AUTO_PRICE_FLOOR_ENABLED = envBool("AUTO_PRICE_FLOOR_ENABLED", false);
const AUTO_PRICE_FLOOR_PRICE = envNumber("AUTO_PRICE_FLOOR_PRICE", 0.05);
const AUTO_PRICE_FLOOR_MAX_PER_POLL = Math.max(1, envNumber("AUTO_PRICE_FLOOR_MAX_PER_POLL", 3));
const AUTO_PRICE_FLOOR_MIN_VALUE_USD = envNumber("AUTO_PRICE_FLOOR_MIN_VALUE_USD", 0.30);

// Account balance dashboard + periodic QQ summary.
const ACCOUNT_BALANCE_POLL_MS = envNumber("ACCOUNT_BALANCE_POLL_MS", 60_000);
const ACCOUNT_BALANCE_PUSH_ENABLED = envBool("ACCOUNT_BALANCE_PUSH_ENABLED", true);
const ACCOUNT_BALANCE_PUSH_INTERVAL_MS = envNumber("ACCOUNT_BALANCE_PUSH_INTERVAL_MS", 10 * 60_000);
const ACCOUNT_BALANCE_PUSH_MIN_DELTA_USD = envNumber("ACCOUNT_BALANCE_PUSH_MIN_DELTA_USD", 0);
// Compact QQ account summary + private QQ trade commands.
const ACCOUNT_BALANCE_PUSH_COMPACT = envBool("ACCOUNT_BALANCE_PUSH_COMPACT", true);
const ACCOUNT_BALANCE_PUSH_MAX_POSITIONS = Math.max(1, envNumber("ACCOUNT_BALANCE_PUSH_MAX_POSITIONS", 8));
const ACCOUNT_BALANCE_PUSH_INCLUDE_DUST = envBool("ACCOUNT_BALANCE_PUSH_INCLUDE_DUST", false);
const ACCOUNT_QQ_POSITION_MIN_VALUE_USD = envNumber("ACCOUNT_QQ_POSITION_MIN_VALUE_USD", 0.01);
const QQ_TRADE_COMMANDS_ENABLED = envBool("QQ_TRADE_COMMANDS_ENABLED", true);
const QQ_SELL_COMMAND_MODE = cleanEnvValue(process.env.QQ_SELL_COMMAND_MODE) || "live"; // paper | sign | live
const QQ_SELL_COMMAND_ORDER_TYPE = (cleanEnvValue(process.env.QQ_SELL_COMMAND_ORDER_TYPE) || cleanEnvValue(process.env.AUTO_EXIT_ORDER_TYPE) || "FAK").toUpperCase();
const QQ_SELL_COMMAND_MAX_SLIPPAGE = envNumber("QQ_SELL_COMMAND_MAX_SLIPPAGE", envNumber("AUTO_EXIT_MAX_SLIPPAGE", 0.05));
const QQ_SELL_COMMAND_MIN_BID = envNumber("QQ_SELL_COMMAND_MIN_BID", envNumber("AUTO_EXIT_MIN_BID", 0.01));
// Account position classification. Data API may keep zero-value/lost outcome tokens
// in /positions even after a market is closed. These should not be treated as
// active holdings or the largest position.
const ACCOUNT_POSITION_DUST_USD = envNumber("ACCOUNT_POSITION_DUST_USD", envNumber("AUTO_TRADE_POSITION_DUST_USD", 0.10));
const ACCOUNT_POSITION_DUST_PRICE = envNumber("ACCOUNT_POSITION_DUST_PRICE", 0.005);
const ACCOUNT_SETTLEMENT_END_GRACE_MS = envNumber("ACCOUNT_SETTLEMENT_END_GRACE_MS", 10 * 60_000);

// Optional API protection. If set, access the UI as http://127.0.0.1:3001/?token=YOUR_SECRET.
const UI_SECRET = cleanEnvValue(process.env.UI_SECRET);

// Official current Polymarket collateral token is pUSD CollateralToken proxy.
// If you intentionally monitor legacy USDC.e flows, override this in .env.
const POLYMARKET_COLLATERAL_TOKEN = cleanEnvValue(process.env.POLYMARKET_COLLATERAL_TOKEN) || "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";

const WALLET_FILE = path.join(__dirname, "watch-wallets.json");
const TRACKED_PROJECT_FILE = path.join(__dirname, "tracked-projects.json");
const AUTO_TRADE_WALLET_FILE = path.join(__dirname, "auto-trade-wallets.json");

if (!WSS_URL) {
  console.error("缺少 POLYGON_WSS，请检查 .env");
  process.exit(1);
}
if (DEFAULT_WALLET && !ethers.isAddress(DEFAULT_WALLET)) {
  console.error("WATCH_WALLET 不是有效地址");
  process.exit(1);
}

const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;
// Use raw JSON-RPC for Polygon HTTP so requests can go through PROXY_URL.
// ethers.JsonRpcProvider does not use HttpsProxyAgent here and was causing socket hang up / network detection failures.
const provider = null;

// ─── Contracts ─────────────────────────────────────────────────────────────
const CONTRACTS = {
  CTF_EXCHANGE_V2: "0xE111180000d2663C0091e4f400237545B87B996B",
  NEG_RISK_CTF_EXCHANGE_V2: "0xe2222d279d744050d28e00520010520000310F59",
  CTF_EXCHANGE_V1: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
  NEG_RISK_CTF_EXCHANGE_V1: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
  CONDITIONAL_TOKENS: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
  PUSD: POLYMARKET_COLLATERAL_TOKEN,
};
const EXCHANGE_ADDRESSES = [
  CONTRACTS.CTF_EXCHANGE_V2,
  CONTRACTS.NEG_RISK_CTF_EXCHANGE_V2,
  CONTRACTS.CTF_EXCHANGE_V1,
  CONTRACTS.NEG_RISK_CTF_EXCHANGE_V1,
];

const iface = new ethers.Interface([
  "event OrderFilled(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint8 side,uint256 tokenId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee,bytes32 builder,bytes32 metadata)",
  "event OrderFilledV1(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint256 makerAssetId,uint256 takerAssetId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee)",
  "event Transfer(address indexed from,address indexed to,uint256 value)",
  "event TransferSingle(address indexed operator,address indexed from,address indexed to,uint256 id,uint256 value)",
  "event TransferBatch(address indexed operator,address indexed from,address indexed to,uint256[] ids,uint256[] values)",
  "function balanceOf(address account,uint256 id) view returns (uint256)",
]);

const TOPICS = {
  ORDER_FILLED_V2: ethers.id("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"),
  ORDER_FILLED_V1: ethers.id("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"),
  ERC20_TRANSFER: ethers.id("Transfer(address,address,uint256)"),
  ERC1155_TRANSFER_SINGLE: ethers.id("TransferSingle(address,address,address,uint256,uint256)"),
  ERC1155_TRANSFER_BATCH: ethers.id("TransferBatch(address,address,address,uint256[],uint256[])"),
};

// ─── State ─────────────────────────────────────────────────────────────────
let wallets = [];
let walletMap = new Map();
let walletTopicSet = new Set();
let selectedWallet = null;
let trackedProjects = [];
let trackedProjectMap = new Map();
let autoTradeWallets = [];
let autoTradeWalletMap = new Map();
let accountStatusCache = null;
// Per-token peak price tracking for trailing stop (in-memory, reset on restart).
const positionPeakPrice = new Map(); // tokenId -> peakPrice (number)
let accountBalanceTimer = null;
let accountBalancePushTimer = null;
let lastPushedAccountStatus = null;
const autoTradeWalletCooldown = new Map();

let polygonWs = null;
let polygonRpcId = 1;
let reconnectTimer = null;
let restartTimer = null;
let polygonReconnectDelay = 3000;          // exponential backoff start
const POLYGON_RECONNECT_MAX_DELAY = 60000; // cap at 60 s
const pendingSubRequests = new Map();
const activeSubscriptions = new Map();
const seenLogs = new Set();
const txBuffers = new Map();

let qqWs = null;
let qqSeq = null;
let qqHeartbeatTimer = null;
let qqReconnectTimer = null;
let qqAccessToken = "";
let qqTokenExpireAt = 0;
let qqMsgSeq = 1;
let lastC2CMsgId = "";
let lastC2COpenId = "";
let polygonPingTimer = null;
const qqRetryQueue = [];

const marketCacheByToken = new Map();
const marketCacheByCondition = new Map();
const balanceCache = new Map();
const orderBookCache = new Map();
const walletAnalysisCache = new Map();
const walletActivityCache = new Map();
const seenActivityExitKeys = new Set();
const positionSnapshots = new Map();
const trackedProjectPositionStates = new Map();
const positionMissingCounts = new Map();
const exitAlertCooldown = new Map();
let activityWatchTimer = null;
let positionWatchTimer = null;
let activityWatchBootstrapped = false;
let positionWatchBootstrapped = false;

// ─── Basic helpers ─────────────────────────────────────────────────────────
function now() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function normalizeAddress(addr) { return String(addr || "").trim().toLowerCase(); }
function shortAddr(addr) { return addr ? `${addr.slice(0, 6)}...${addr.slice(-4)}` : ""; }
function txUrl(hash) { return `https://polygonscan.com/tx/${hash}`; }
function contractName(address) {
  const a = String(address || "").toLowerCase();
  for (const [name, addr] of Object.entries(CONTRACTS)) {
    if (addr.toLowerCase() === a) return name;
  }
  return address;
}
function topicForWallet(address) { return ethers.zeroPadValue(address, 32).toLowerCase(); }
function topicToAddress(topic) { return ethers.getAddress("0x" + String(topic).slice(26)).toLowerCase(); }
function safeJsonParse(text, fallback = null) { try { return JSON.parse(text); } catch { return fallback; } }
function parseMaybeJson(v) { return Array.isArray(v) ? v : (typeof v === "string" ? safeJsonParse(v, null) : null); }
function amount6ToNumber(v) { try { return Number(ethers.formatUnits(v, 6)); } catch { return Number(v); } }
function moneyText(n) { const v = Number(n); return Number.isFinite(v) ? `$${v.toFixed(2)}` : "-"; }
function priceText(n) { const v = Number(n); return Number.isFinite(v) ? `${(v * 100).toFixed(1)}%` : "-"; }
function sharesText(n) { const v = Number(n); return Number.isFinite(v) ? `${v.toLocaleString("en-US", { maximumFractionDigits: 2 })} shares` : "-"; }
function pctText(n) { const v = Number(n); return Number.isFinite(v) ? `${(v * 100).toFixed(1)}%` : "-"; }
function roundShareDisplay(n) {
  const v = Number(n);
  return Number.isFinite(v) ? Math.round(v * 100) / 100 : 0;
}
function analyzeShareReduction(beforeSize, afterSize) {
  const before = Number(beforeSize || 0);
  const after = Number(afterSize || 0);
  const rawDelta = before - after;
  const displayBefore = roundShareDisplay(before);
  const displayAfter = roundShareDisplay(after);
  const displayDelta = displayBefore - displayAfter;
  const pctThreshold = Math.abs(before) * EXIT_POSITION_MIN_SIZE_DELTA_PCT;
  const threshold = Math.max(EXIT_POSITION_MIN_SIZE_DELTA, EXIT_POSITION_MIN_SHARE_DELTA, pctThreshold);
  const visibleDropOk = !EXIT_POSITION_REQUIRE_DISPLAY_SHARE_DROP || displayDelta > 0;
  return {
    before,
    after,
    rawDelta,
    displayBefore,
    displayAfter,
    displayDelta,
    threshold,
    ok: Number.isFinite(rawDelta) && rawDelta >= threshold && visibleDropOk,
    reason: `rawDelta=${rawDelta.toFixed(8)}, threshold=${threshold.toFixed(8)}, display ${displayBefore.toFixed(2)} -> ${displayAfter.toFixed(2)}`,
  };
}
function shortValue(v, head = 8, tail = 6) {
  const str = String(v || "");
  return str.length > head + tail + 3 ? `${str.slice(0, head)}...${str.slice(-tail)}` : str;
}
function qqConfigured() { return !!(QQ_APP_ID && QQ_CLIENT_SECRET && QQBOT_TARGET_ID); }
function withProxyOptions(options = {}) { return PROXY_URL ? { ...options, agent } : options; }
function cacheGet(map, key, ttlMs) {
  const hit = map.get(key);
  if (!hit) return undefined;
  if (hit && typeof hit === "object" && Object.prototype.hasOwnProperty.call(hit, "ts")) {
    if (Date.now() - hit.ts <= ttlMs) return hit.value;
    map.delete(key);
    return undefined;
  }
  // Backward compatibility for old cache entries.
  map.delete(key);
  return undefined;
}
function cacheSet(map, key, value) {
  map.set(key, { ts: Date.now(), value });
  return value;
}
function isApiRequest(pathname) { return pathname === "/api" || pathname.startsWith("/api/"); }
function requestAuthorized(req, urlObj) {
  if (!UI_SECRET || !isApiRequest(urlObj.pathname)) return true;
  const headerSecret = req.headers["x-ui-secret"] || "";
  const bearer = String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  const querySecret = urlObj.searchParams.get("token") || "";
  return headerSecret === UI_SECRET || bearer === UI_SECRET || querySecret === UI_SECRET;
}

async function fetchText(url, options = {}) {
  const fetch = (await import("node-fetch")).default;
  const timeoutMs = Number(options.timeoutMs || FETCH_TIMEOUT_MS);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`请求超时 ${timeoutMs}ms: ${url}`)), timeoutMs);
  try {
    const merged = withProxyOptions({ ...options, signal: controller.signal });
    delete merged.timeoutMs;
    return await fetch(url, merged);
  } catch (err) {
    if (err?.name === "AbortError" || String(err?.message || "").includes("aborted")) {
      throw new Error(`请求超时 ${timeoutMs}ms: ${url}`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}
function friendlyError(err) {
  const msg = String(err?.message || err || "未知错误");
  if (msg.includes("请求超时") || msg.includes("aborted")) return "Polymarket/API 请求超时，请检查代理或稍后重试";
  if (msg.includes("socket hang up")) return "网络连接被断开 socket hang up，请检查代理/RPC";
  if (msg.includes("JsonRpcProvider failed")) return "Polygon HTTP RPC 无法连接";
  if (msg.includes("HTTP 429")) return "API 限流，请降低并发后重试";
  return msg.slice(0, 300);
}
async function apiJson(url, options = {}) {
  const res = await fetchText(url, { ...options, headers: { accept: "application/json", ...(options.headers || {}) } });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}\n${text.slice(0, 500)}`);
  const json = safeJsonParse(text, null);
  if (json === null) throw new Error(`接口返回不是 JSON: ${text.slice(0, 200)}`);
  return json;
}
function sendJson(res, status, obj) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(obj));
}
async function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    let oversized = false;
    req.on("data", chunk => {
      data += chunk;
      if (data.length > 1e6) {
        oversized = true;
        req.destroy(new Error("请求体超过 1MB 限制"));
      }
    });
    req.on("end", () => {
      if (oversized) return; // error 事件会触发 reject
      resolve(data ? safeJsonParse(data, {}) : {});
    });
    req.on("error", reject);
  });
}

// ─── Wallet storage ────────────────────────────────────────────────────────
function canonicalWallet(w) {
  const address = normalizeAddress(w.address);
  if (!ethers.isAddress(address)) throw new Error(`无效钱包地址: ${w.address}`);
  return {
    name: String(w.name || shortAddr(address)).trim() || shortAddr(address),
    address,
    enabled: w.enabled !== false,
    tags: Array.isArray(w.tags) ? w.tags : [],
    minUsd: Number(w.minUsd || 0),
    note: String(w.note || ""),
    createdAt: w.createdAt || new Date().toISOString(),
  };
}
function saveWallets() {
  fs.writeFileSync(WALLET_FILE, JSON.stringify(wallets, null, 2), "utf8");
  rebuildWalletMaps();
}
function rebuildWalletMaps() {
  walletMap = new Map(wallets.map(w => [w.address, w]));
  walletTopicSet = new Set(wallets.filter(w => w.enabled).map(w => topicForWallet(w.address)));
  if (!selectedWallet || !walletMap.has(selectedWallet)) {
    selectedWallet = wallets.find(w => w.enabled)?.address || wallets[0]?.address || null;
  }
}
function loadWallets() {
  let list = [];
  if (fs.existsSync(WALLET_FILE)) {
    list = safeJsonParse(fs.readFileSync(WALLET_FILE, "utf8"), []);
  }
  if (!Array.isArray(list)) list = [];
  if (!list.length && DEFAULT_WALLET && ethers.isAddress(DEFAULT_WALLET)) {
    list = [{ name: "主钱包", address: DEFAULT_WALLET, enabled: true, tags: ["default"] }];
  }
  const seen = new Set();
  wallets = [];
  for (const raw of list) {
    try {
      const w = canonicalWallet(raw);
      if (seen.has(w.address)) continue;
      seen.add(w.address);
      wallets.push(w);
    } catch (err) {
      console.warn(`[钱包配置跳过] ${err.message}`);
    }
  }
  saveWallets();
}
function publicWallets() { return wallets.map(w => ({ ...w, short: shortAddr(w.address) })); }



// ─── Auto-copy wallet storage ─────────────────────────────────────────────
function normalizeTradeMode(mode) {
  const m = String(mode || "global").trim().toLowerCase();
  return ["global", "paper", "sign", "live"].includes(m) ? m : "global";
}
function canonicalAutoTradeWallet(input = {}) {
  const wallet = normalizeAddress(input.wallet || input.address || "");
  if (!ethers.isAddress(wallet)) throw new Error(`无效自动跟单钱包地址: ${input.wallet || input.address}`);
  const amountUsd = Math.max(0, Number(input.amountUsd ?? input.usdPerOrder ?? AUTO_TRADE_DEFAULT_AMOUNT_USD));
  const maxPriceDiff = Math.max(0, Number(input.maxPriceDiff ?? AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF));
  const mode = normalizeTradeMode(input.mode);
  return {
    wallet,
    walletName: walletLabel(wallet),
    enabled: input.enabled === true,
    mode,
    amountUsd: Number.isFinite(amountUsd) && amountUsd > 0 ? amountUsd : AUTO_TRADE_DEFAULT_AMOUNT_USD,
    maxPriceDiff: Number.isFinite(maxPriceDiff) && maxPriceDiff > 0 ? maxPriceDiff : AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF,
    note: String(input.note || ""),
    updatedAt: input.updatedAt || new Date().toISOString(),
    createdAt: input.createdAt || new Date().toISOString(),
  };
}
function rebuildAutoTradeWalletMap() {
  autoTradeWalletMap = new Map(autoTradeWallets.map(x => [x.wallet, x]));
}
function loadAutoTradeWallets() {
  let list = [];
  if (fs.existsSync(AUTO_TRADE_WALLET_FILE)) list = safeJsonParse(fs.readFileSync(AUTO_TRADE_WALLET_FILE, "utf8"), []);
  if (!Array.isArray(list)) list = [];
  const seen = new Set();
  autoTradeWallets = [];
  for (const raw of list) {
    try {
      const x = canonicalAutoTradeWallet(raw);
      if (seen.has(x.wallet)) continue;
      seen.add(x.wallet);
      autoTradeWallets.push(x);
    } catch (err) {
      console.warn(`[自动跟单钱包配置跳过] ${err.message}`);
    }
  }
  saveAutoTradeWallets(false);
}
function saveAutoTradeWallets(doBroadcast = true) {
  autoTradeWallets.sort((a, b) => String(a.walletName || "").localeCompare(String(b.walletName || "")));
  fs.writeFileSync(AUTO_TRADE_WALLET_FILE, JSON.stringify(autoTradeWallets, null, 2), "utf8");
  rebuildAutoTradeWalletMap();
  if (doBroadcast) broadcast({ type: "autoTradeWallets", wallets: publicAutoTradeWallets() });
}
function publicAutoTradeWallets() {
  return autoTradeWallets.map(x => ({
    ...x,
    walletName: walletLabel(x.wallet),
    shortWallet: shortAddr(x.wallet),
    maxPriceDiffText: `${(Number(x.maxPriceDiff || 0) * 100).toFixed(1)}¢`,
  }));
}
function getAutoTradeWalletConfig(address) {
  const wallet = normalizeAddress(address);
  const cfg = autoTradeWalletMap.get(wallet);
  return cfg && cfg.enabled ? cfg : null;
}
function upsertAutoTradeWallet(input) {
  const next = canonicalAutoTradeWallet({ ...input, updatedAt: new Date().toISOString() });
  const idx = autoTradeWallets.findIndex(x => x.wallet === next.wallet);
  if (idx >= 0) autoTradeWallets[idx] = { ...autoTradeWallets[idx], ...next, createdAt: autoTradeWallets[idx].createdAt || next.createdAt };
  else autoTradeWallets.push(next);
  saveAutoTradeWallets();
  return next;
}
function deleteAutoTradeWallet(address) {
  const wallet = normalizeAddress(address);
  const before = autoTradeWallets.length;
  autoTradeWallets = autoTradeWallets.filter(x => x.wallet !== wallet);
  if (autoTradeWallets.length !== before) saveAutoTradeWallets();
  return autoTradeWallets.length !== before;
}

// ─── Selected project / exit watchlist storage ────────────────────────────
function makeTrackedProjectId(p) {
  const base = [normalizeAddress(p.wallet), p.tokenId || "", p.conditionId || "", p.outcome || "", p.marketQuestion || ""].join(":");
  return Buffer.from(base).toString("base64url").slice(0, 80);
}
function canonicalTrackedProject(input = {}) {
  const wallet = normalizeAddress(input.wallet || input.address || selectedWallet || "");
  if (!ethers.isAddress(wallet)) throw new Error(`无效钱包地址: ${input.wallet || input.address}`);
  const tokenId = String(input.tokenId || input.asset || "").trim();
  const conditionId = String(input.conditionId || input.condition_id || "").trim();
  const marketQuestion = String(input.marketQuestion || input.title || input.question || "").trim();
  const outcome = String(input.outcome || "").trim();
  if (!tokenId && !conditionId && !marketQuestion) throw new Error("缺少 tokenId / conditionId / marketQuestion，无法选择项目");
  const p = {
    id: String(input.id || ""),
    wallet,
    walletName: walletLabel(wallet),
    tokenId,
    conditionId,
    marketQuestion: marketQuestion || "未知市场",
    outcome: outcome || "-",
    marketUrl: String(input.marketUrl || input.openUrl || input.url || ""),
    source: String(input.source || "manual"),
    note: String(input.note || ""),
    enabled: input.enabled !== false,
    selectedAt: input.selectedAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  p.id = p.id || makeTrackedProjectId(p);
  return p;
}
function rebuildTrackedProjectMap() {
  trackedProjectMap = new Map(trackedProjects.map(p => [p.id, p]));
}
function loadTrackedProjects() {
  let list = [];
  if (fs.existsSync(TRACKED_PROJECT_FILE)) list = safeJsonParse(fs.readFileSync(TRACKED_PROJECT_FILE, "utf8"), []);
  if (!Array.isArray(list)) list = [];
  const seen = new Set();
  trackedProjects = [];
  for (const raw of list) {
    try {
      const p = canonicalTrackedProject(raw);
      if (seen.has(p.id)) continue;
      seen.add(p.id);
      trackedProjects.push(p);
    } catch (err) {
      console.warn(`[选择项目配置跳过] ${err.message}`);
    }
  }
  saveTrackedProjects(false);
}
function saveTrackedProjects(doBroadcast = true) {
  trackedProjects.sort((a, b) => String(b.selectedAt || "").localeCompare(String(a.selectedAt || "")));
  fs.writeFileSync(TRACKED_PROJECT_FILE, JSON.stringify(trackedProjects, null, 2), "utf8");
  rebuildTrackedProjectMap();
  if (doBroadcast) broadcast({ type: "trackedProjects", projects: publicTrackedProjects() });
}
function publicTrackedProjects() {
  return trackedProjects.map(p => ({ ...p, walletName: walletLabel(p.wallet), shortWallet: shortAddr(p.wallet) }));
}
function normalizeComparableText(v) {
  return String(v || "").trim().toLowerCase().replace(/\s+/g, " ");
}
function trackedProjectsForWallet(address) {
  const wallet = normalizeAddress(address);
  return trackedProjects.filter(p => p.enabled !== false && p.wallet === wallet);
}
function getExitWatchWallets() {
  if (!TRACKED_EXIT_ONLY) return getEnabledWallets();
  const walletsWithProjects = new Set(trackedProjects.filter(p => p.enabled !== false).map(p => p.wallet));
  return getEnabledWallets().filter(w => walletsWithProjects.has(w.address));
}
function findTrackedProjectForSignal(event) {
  if (!event) return null;
  const wallet = normalizeAddress(event.wallet || event.address || "");
  if (!wallet) return null;
  const tokenId = String(event.tokenId || event.asset || "").trim();
  const conditionId = String(event.conditionId || event.condition_id || "").trim();
  const title = normalizeComparableText(event.marketQuestion || event.title || event.question || "");
  const outcome = normalizeComparableText(event.outcome || "");
  for (const p of trackedProjectsForWallet(wallet)) {
    if (p.tokenId && tokenId && String(p.tokenId) === tokenId) return p;
    if (p.conditionId && conditionId && String(p.conditionId).toLowerCase() === conditionId.toLowerCase()) {
      if (!p.outcome || !outcome || normalizeComparableText(p.outcome) === outcome) return p;
    }
    if (p.marketQuestion && title && normalizeComparableText(p.marketQuestion) === title) {
      if (!p.outcome || !outcome || normalizeComparableText(p.outcome) === outcome) return p;
    }
  }
  return null;
}
function isExitSignalForProjectTracking(event) {
  if (!event) return false;
  if (["activity_exit", "position_exit", "position_closed", "exit_batch"].includes(String(event.type || ""))) return true;
  if (event.positionChangeGroup === "exit") return true;
  if (["SELL", "REDEEM"].includes(String(event.side || "").toUpperCase())) return true;
  return ["减仓", "平仓", "卖出", "赎回", "退出汇总"].includes(String(event.positionChange || ""));
}
function shouldEmitEventByTracking(event) {
  if (!TRACKED_EXIT_ONLY || !isExitSignalForProjectTracking(event)) return true;
  if (event?.type === "exit_batch" && Array.isArray(event.items) && event.items.length) return true;
  const matched = findTrackedProjectForSignal(event);
  if (matched) {
    event.trackedProjectId = matched.id;
    event.trackedProjectName = matched.marketQuestion;
    event.trackedProjectMatched = true;
    return true;
  }
  console.log(`[退出监控跳过] ${walletLabel(event.wallet)} ${event.marketQuestion || event.title || "未知市场"} 未选择该项目`);
  return false;
}
function upsertTrackedProject(input) {
  const p = canonicalTrackedProject(input);
  const idx = trackedProjects.findIndex(x => x.id === p.id);
  if (idx >= 0) trackedProjects[idx] = { ...trackedProjects[idx], ...p, selectedAt: trackedProjects[idx].selectedAt || p.selectedAt };
  else trackedProjects.push(p);
  saveTrackedProjects();
  return p;
}
function deleteTrackedProject(id) {
  const before = trackedProjects.length;
  trackedProjects = trackedProjects.filter(p => p.id !== id);
  if (trackedProjects.length !== before) saveTrackedProjects();
  return trackedProjects.length !== before;
}
function getEnabledWallets() { return wallets.filter(w => w.enabled); }
function walletLabel(address) {
  const w = walletMap.get(normalizeAddress(address));
  return w ? w.name : shortAddr(address);
}
function isWatched(address) { return walletMap.has(normalizeAddress(address)); }
function watchedWalletsFromAddresses(addrs) {
  const out = [];
  const seen = new Set();
  for (const addr of addrs.map(normalizeAddress)) {
    if (!addr || !walletMap.has(addr) || seen.has(addr)) continue;
    const w = walletMap.get(addr);
    if (!w.enabled) continue;
    out.push(addr);
    seen.add(addr);
  }
  return out;
}

// ─── Market and position APIs ──────────────────────────────────────────────
function buildPolymarketUrl(market, tokenId) {
  const eventSlug = market?.events?.[0]?.slug || market?.eventSlug;
  const slug = market?.slug;
  const base = eventSlug
    ? `https://polymarket.com/event/${eventSlug}`
    : slug
      ? `https://polymarket.com/market/${slug}`
      : null;
  return base ? `${base}${tokenId ? `?tid=${encodeURIComponent(tokenId)}` : ""}` : null;
}
function firstMarketFromGammaResponse(data) {
  if (Array.isArray(data)) return data[0] || null;
  return data?.markets?.[0] || data?.data?.[0] || data?.results?.[0] || null;
}

async function getGammaMarketByQuery(paramsList) {
  for (const params of paramsList) {
    const url = new URL("https://gamma-api.polymarket.com/markets");
    url.searchParams.set("limit", "1");
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null || value === "") continue;
      url.searchParams.set(key, String(value));
    }
    try {
      const data = await apiJson(url.toString());
      const market = firstMarketFromGammaResponse(data);
      if (market) return market;
    } catch (err) {
      // Try the next accepted Gamma query shape. Polymarket has used both
      // condition_ids and clob_token_ids as array-like filters over time.
    }
  }
  return null;
}

function tokenIdsFromMarketLike(market, tokenInfo = null) {
  const fromClob = parseMaybeJson(market?.clobTokenIds) || parseMaybeJson(market?.clob_token_ids) || null;
  if (Array.isArray(fromClob) && fromClob.length) return fromClob.map(String);
  const tokens = Array.isArray(market?.tokens) ? market.tokens : [];
  const fromTokens = tokens.map(t => t.token_id || t.tokenId || t.id || t.asset_id).filter(Boolean).map(String);
  if (fromTokens.length) return fromTokens;
  return [tokenInfo?.primary_token_id, tokenInfo?.secondary_token_id, tokenInfo?.primaryTokenId, tokenInfo?.secondaryTokenId]
    .filter(Boolean)
    .map(String);
}

function outcomesFromMarketLike(market, tokenInfo = null) {
  const parsed = parseMaybeJson(market?.shortOutcomes) || parseMaybeJson(market?.outcomes) || parseMaybeJson(market?.outcome_names);
  if (Array.isArray(parsed) && parsed.length) return parsed.map(String);
  const tokens = Array.isArray(market?.tokens) ? market.tokens : [];
  const fromTokens = tokens.map(t => t.outcome || t.name || t.label).filter(Boolean).map(String);
  if (fromTokens.length) return fromTokens;
  if (tokenInfo?.outcomes && Array.isArray(tokenInfo.outcomes)) return tokenInfo.outcomes.map(String);
  return ["YES", "NO"];
}

async function getMarketByCondition(conditionId) {
  if (!conditionId) return null;
  conditionId = String(conditionId);
  const cached = cacheGet(marketCacheByCondition, conditionId, MARKET_CACHE_TTL_MS);
  if (cached !== undefined) return cached;
  try {
    const market = await getGammaMarketByQuery([
      { condition_ids: conditionId },
      { "condition_ids[]": conditionId },
      { conditionId },
      { condition_id: conditionId },
    ]);
    return cacheSet(marketCacheByCondition, conditionId, market);
  } catch (err) {
    console.warn(`[市场状态查询失败] condition=${conditionId} ${friendlyError(err)}`);
    return cacheSet(marketCacheByCondition, conditionId, null);
  }
}

async function getMarketByToken(tokenId) {
  tokenId = String(tokenId || "");
  if (!tokenId) return null;
  const cached = cacheGet(marketCacheByToken, tokenId, MARKET_CACHE_TTL_MS);
  if (cached !== undefined) return cached;

  const empty = { tokenId, outcome: "Outcome Token", marketQuestion: `未知市场 / token ${shortValue(tokenId)}`, marketUrl: null, conditionId: null, resolveError: "market_not_resolved" };
  let tokenInfo = null;
  let conditionId = null;
  let market = null;

  // 1) Fast path: CLOB token -> condition id. This endpoint is official, but it
  // can return 404 for stale/negative-risk/older tokens, so it must not be the
  // only path. Docs: GET /markets-by-token/{token_id}.
  try {
    tokenInfo = await apiJson(`https://clob.polymarket.com/markets-by-token/${encodeURIComponent(tokenId)}`, { timeoutMs: Math.min(FETCH_TIMEOUT_MS, 12_000) });
    conditionId = tokenInfo?.condition_id || tokenInfo?.conditionId || null;
  } catch (err) {
    console.warn(`[CLOB token映射失败，尝试Gamma兜底] tokenId=${shortValue(tokenId)} ${friendlyError(err)}`);
  }

  // 2) Robust fallback: Gamma can filter directly by clob_token_ids. This is the
  // key fix for "未知市场" when the CLOB helper endpoint cannot map the token.
  try {
    const queries = [];
    if (conditionId) {
      queries.push({ condition_ids: conditionId }, { "condition_ids[]": conditionId }, { conditionId }, { condition_id: conditionId });
    }
    queries.push({ clob_token_ids: tokenId }, { "clob_token_ids[]": tokenId }, { clobTokenIds: tokenId }, { token_id: tokenId });
    market = await getGammaMarketByQuery(queries);
  } catch (err) {
    console.warn(`[Gamma token映射失败] tokenId=${shortValue(tokenId)} ${friendlyError(err)}`);
  }

  if (!conditionId && market) conditionId = market.conditionId || market.condition_id || null;
  if (conditionId && market !== null) cacheSet(marketCacheByCondition, String(conditionId), market);

  const tokenIds = tokenIdsFromMarketLike(market, tokenInfo);
  const outcomes = outcomesFromMarketLike(market, tokenInfo);
  let idx = tokenIds.findIndex(id => String(id) === tokenId);
  if (idx < 0 && tokenInfo && String(tokenInfo.primary_token_id || tokenInfo.primaryTokenId) === tokenId) idx = 0;
  if (idx < 0 && tokenInfo && String(tokenInfo.secondary_token_id || tokenInfo.secondaryTokenId) === tokenId) idx = 1;

  const marketQuestion = market?.question || market?.title || tokenInfo?.question || tokenInfo?.market_question || null;
  const resolved = {
    tokenId,
    outcome: outcomes[idx] || (idx === 0 ? "YES" : idx === 1 ? "NO" : "Outcome Token"),
    marketQuestion: marketQuestion || `未知市场 / token ${shortValue(tokenId)}`,
    marketSlug: market?.slug || null,
    eventSlug: market?.events?.[0]?.slug || market?.eventSlug || null,
    marketUrl: buildPolymarketUrl(market, tokenId),
    conditionId,
    market,
    tokenInfo,
    resolveSource: market ? (conditionId ? "gamma_condition_or_token" : "gamma_token") : (tokenInfo ? "clob_only" : "unresolved"),
  };

  if (!market && !tokenInfo) {
    console.warn(`[市场信息补全失败] tokenId=${shortValue(tokenId)} CLOB 与 Gamma 均未命中`);
  }
  return cacheSet(marketCacheByToken, tokenId, resolved);
}
async function polygonRpc(method, params = [], timeoutMs = BALANCE_QUERY_TIMEOUT_MS) {
  if (!POLYGON_HTTP) throw new Error("缺少 POLYGON_HTTP");
  const res = await fetchText(POLYGON_HTTP, {
    method: "POST",
    timeoutMs,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: Date.now(), method, params }),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`Polygon RPC HTTP ${res.status}: ${text.slice(0, 300)}`);
  const data = safeJsonParse(text, null);
  if (!data) throw new Error(`Polygon RPC 返回非 JSON: ${text.slice(0, 200)}`);
  if (data.error) throw new Error(`Polygon RPC ${method} 错误: ${JSON.stringify(data.error).slice(0, 300)}`);
  return data.result;
}
async function ethCall(to, data, timeoutMs = BALANCE_QUERY_TIMEOUT_MS) {
  return polygonRpc("eth_call", [{ to, data }, "latest"], timeoutMs);
}
async function getErc1155Balance(address, tokenId) {
  if (!ENABLE_CHAIN_BALANCE_CHECK || !POLYGON_HTTP || !address || !tokenId) return null;
  const key = `${normalizeAddress(address)}:${tokenId}`;
  const cached = cacheGet(balanceCache, key, 15_000);
  if (cached !== undefined) return cached;
  try {
    const erc1155Iface = new ethers.Interface(["function balanceOf(address,uint256) view returns (uint256)"]);
    const data = erc1155Iface.encodeFunctionData("balanceOf", [address, tokenId]);
    const raw = await ethCall(CONTRACTS.CONDITIONAL_TOKENS, data);
    const [bal] = erc1155Iface.decodeFunctionResult("balanceOf", raw);
    const n = amount6ToNumber(bal);
    return cacheSet(balanceCache, key, n);
  } catch (err) {
    console.warn(`[余额查询失败] ${shortAddr(address)} token=${shortValue(tokenId)} ${friendlyError(err)}`);
    return null;
  }
}

async function getErc20Balance(address, tokenAddress, decimals = 6) {
  if (!POLYGON_HTTP || !address || !tokenAddress) return null;
  try {
    const erc20 = new ethers.Interface(["function balanceOf(address) view returns (uint256)"]);
    const data = erc20.encodeFunctionData("balanceOf", [address]);
    const raw = await ethCall(tokenAddress, data, BALANCE_QUERY_TIMEOUT_MS);
    const [bal] = erc20.decodeFunctionResult("balanceOf", raw);
    return Number(ethers.formatUnits(bal, decimals));
  } catch (err) {
    console.warn(`[账户余额查询失败] ${shortAddr(address)} ${friendlyError(err)}`);
    return null;
  }
}
function getTradingFunderAddress() {
  const fromEnv = normalizeAddress(process.env.PM_FUNDER_ADDRESS || "");
  if (ethers.isAddress(fromEnv)) return fromEnv;
  return DEFAULT_WALLET && ethers.isAddress(DEFAULT_WALLET) ? DEFAULT_WALLET : null;
}
function accountPositionUrl(p) {
  if (!p) return null;
  const tokenId = p.asset || p.tokenId || p.token_id || p.clobTokenId || p.clob_token_id || p.outcomeTokenId || "";
  if (p.eventSlug) return `https://polymarket.com/event/${p.eventSlug}${tokenId ? `?tid=${encodeURIComponent(tokenId)}` : ""}`;
  if (p.slug) return `https://polymarket.com/market/${p.slug}${tokenId ? `?tid=${encodeURIComponent(tokenId)}` : ""}`;
  if (p.conditionId) return `https://polymarket.com/?search=${encodeURIComponent(p.title || p.conditionId)}`;
  return null;
}
function normalizeAccountPosition(p) {
  const size = Number(p?.size || 0);
  const curPrice = Number(p?.curPrice ?? p?.currPrice ?? p?.price ?? 0);
  const currentValue = Number(p?.currentValue ?? (size * curPrice) ?? 0) || 0;
  const initialValue = Number(p?.initialValue || 0) || 0;
  const cashPnl = Number(p?.cashPnl || 0) || 0;
  const percentPnl = Number(p?.percentPnl);
  const tokenId = String(p?.asset || p?.tokenId || p?.token_id || p?.clobTokenId || p?.clob_token_id || p?.outcomeTokenId || "");
  const title = String(p?.title || p?.market?.question || p?.question || "未知市场");
  const outcome = String(p?.outcome || p?.outcomeName || p?.assetName || "-");
  const rawStatus = String(p?.status || p?.marketStatus || p?.state || p?.market?.status || "").toLowerCase();
  const endDateRaw = p?.endDate || p?.end_date || p?.market?.endDate || p?.market?.end_date || p?.eventEndDate || p?.event_end_date || "";
  const endTs = endDateRaw ? Date.parse(endDateRaw) : 0;
  const endDateIso = Number.isFinite(endTs) && endTs > 0 ? new Date(endTs).toISOString() : String(endDateRaw || "");
  const nowTs = Date.now();
  const endPassed = Number.isFinite(endTs) && endTs > 0 && nowTs > endTs + ACCOUNT_SETTLEMENT_END_GRACE_MS;
  const closedLike = truthyLike(p?.closed) || truthyLike(p?.market?.closed) || truthyLike(p?.event?.closed) || rawStatus.includes("closed");
  const resolvedLike = truthyLike(p?.resolved) || truthyLike(p?.market?.resolved) || truthyLike(p?.event?.resolved) || rawStatus.includes("resolved") || rawStatus.includes("final");
  const redeemableLike = truthyLike(p?.redeemable) || truthyLike(p?.canRedeem) || truthyLike(p?.claimable) || rawStatus.includes("redeem");
  const winningOutcome = String(p?.winningOutcome || p?.winning_outcome || p?.winner || p?.result || p?.resolvedOutcome || p?.resolved_outcome || "").trim();
  const outcomeNorm = normalizeComparableText(outcome);
  const winnerNorm = normalizeComparableText(winningOutcome);
  const outcomeWon = !!winnerNorm && (winnerNorm === outcomeNorm || winnerNorm.includes(outcomeNorm) || outcomeNorm.includes(winnerNorm));
  const outcomeLost = !!winnerNorm && !outcomeWon;
  const zeroValue = currentValue <= ACCOUNT_POSITION_DUST_USD;
  const nearZeroPrice = Number.isFinite(curPrice) && curPrice <= ACCOUNT_POSITION_DUST_PRICE;
  const likelyLost = outcomeLost || (zeroValue && (closedLike || resolvedLike || endPassed || nearZeroPrice || (initialValue > 0 && cashPnl <= -initialValue * 0.8)));
  let bucket = "active";
  let bucketLabel = "活跃";
  let bucketReason = "仍有有效估值，按活跃仓位统计";
  if (likelyLost) {
    bucket = "lost";
    bucketLabel = "已输/归零";
    bucketReason = "仓位价值接近 0，且市场已结束/已归零，不再占用活跃名额";
  } else if (zeroValue) {
    bucket = "dust";
    bucketLabel = "灰尘仓";
    bucketReason = `估值 <= ${moneyText(ACCOUNT_POSITION_DUST_USD)}，不计入活跃仓位`;
  } else if (closedLike || resolvedLike || redeemableLike || endPassed) {
    bucket = "settlement";
    bucketLabel = "待结算";
    bucketReason = "市场已结束/可结算，但仍有估值，等待卖出或 Redeem";
  }
  return {
    tokenId,
    conditionId: String(p?.conditionId || p?.condition_id || ""),
    title,
    outcome,
    size,
    sizeText: sharesText(size),
    curPrice: Number.isFinite(curPrice) ? curPrice : null,
    curPriceText: Number.isFinite(curPrice) ? priceText(curPrice) : "-",
    currentValue,
    currentValueText: moneyText(currentValue),
    initialValue,
    initialValueText: moneyText(initialValue),
    cashPnl,
    cashPnlText: `${cashPnl >= 0 ? "+" : ""}${moneyText(cashPnl)}`,
    percentPnl: Number.isFinite(percentPnl) ? percentPnl : null,
    percentPnlText: Number.isFinite(percentPnl) ? `${percentPnl >= 0 ? "+" : ""}${percentPnl.toFixed(2)}%` : "-",
    marketUrl: accountPositionUrl(p),
    endDate: endDateIso,
    endPassed,
    closedLike,
    resolvedLike,
    redeemableLike,
    winningOutcome,
    outcomeWon,
    outcomeLost,
    bucket,
    bucketLabel,
    bucketReason,
  };
}
function sumAccountPositionValue(items) {
  return items.reduce((sum, p) => sum + Number(p.currentValue || 0), 0);
}
function sortPositionsByValue(items) {
  return [...items].sort((a, b) => Number(b.currentValue || 0) - Number(a.currentValue || 0));
}
async function getTradingAccountStatus({ noCache = false } = {}) {
  const address = getTradingFunderAddress();
  if (!address) throw new Error("缺少 PM_FUNDER_ADDRESS / WATCH_WALLET，无法查询交易账户资金");
  if (!noCache && accountStatusCache && Date.now() - accountStatusCache.ts < 20_000) return accountStatusCache.value;
  const [pusdBalance, positions] = await Promise.all([
    getErc20Balance(address, POLYMARKET_COLLATERAL_TOKEN, 6),
    getCurrentPositions(address, { limit: 500 }).catch(err => {
      console.warn(`[账户持仓查询失败] ${shortAddr(address)} ${friendlyError(err)}`);
      return [];
    }),
  ]);
  let positionsValue = 0;
  let positionsInitial = 0;
  let cashPnl = 0;
  const normalizedPositions = [];
  for (const p of positions || []) {
    const normalized = normalizeAccountPosition(p);
    normalizedPositions.push(normalized);
    positionsValue += normalized.currentValue || 0;
    positionsInitial += normalized.initialValue || 0;
    cashPnl += normalized.cashPnl || 0;
  }
  const activePositions = sortPositionsByValue(normalizedPositions.filter(p => p.bucket === "active"));
  const settlementPositions = sortPositionsByValue(normalizedPositions.filter(p => p.bucket === "settlement"));
  const dustPositions = sortPositionsByValue(normalizedPositions.filter(p => p.bucket === "dust" || p.bucket === "lost"));
  const lostPositions = sortPositionsByValue(normalizedPositions.filter(p => p.bucket === "lost"));
  const activeValue = sumAccountPositionValue(activePositions);
  const settlementValue = sumAccountPositionValue(settlementPositions);
  const dustValue = sumAccountPositionValue(dustPositions);
  const lostValue = sumAccountPositionValue(lostPositions);
  const topActivePosition = activePositions[0] || null;
  const topSettlementPosition = settlementPositions[0] || null;
  const topDustPosition = dustPositions[0] || null;
  const equity = Number(pusdBalance || 0) + positionsValue;
  const effectiveEquity = Number(pusdBalance || 0) + activeValue + settlementValue;
  const value = {
    ok: true,
    address,
    shortAddress: shortAddr(address),
    collateralToken: POLYMARKET_COLLATERAL_TOKEN,
    pusdBalance,
    pusdBalanceText: moneyText(pusdBalance || 0),
    // positionsCount/rawPositionsCount is the raw Data API count. It can include
    // zero-value/lost/dust residual tokens and should not be used for trading limits.
    positionsCount: Array.isArray(positions) ? positions.length : 0,
    rawPositionsCount: Array.isArray(positions) ? positions.length : 0,
    allPositions: normalizedPositions.slice(0, 500),
    effectivePositionsCount: activePositions.length + settlementPositions.length,
    effectivePositionsValue: activeValue + settlementValue,
    effectivePositionsValueText: moneyText(activeValue + settlementValue),
    positionsValue,
    positionsValueText: moneyText(positionsValue),
    activePositionsCount: activePositions.length,
    activePositionsValue: activeValue,
    activePositionsValueText: moneyText(activeValue),
    settlementPositionsCount: settlementPositions.length,
    settlementPositionsValue: settlementValue,
    settlementPositionsValueText: moneyText(settlementValue),
    dustPositionsCount: dustPositions.length,
    dustPositionsValue: dustValue,
    dustPositionsValueText: moneyText(dustValue),
    lostPositionsCount: lostPositions.length,
    lostPositionsValue: lostValue,
    lostPositionsValueText: moneyText(lostValue),
    topPosition: topActivePosition,
    topActivePosition,
    topSettlementPosition,
    topDustPosition,
    topPositions: activePositions.slice(0, 3),
    activePositions: activePositions.slice(0, 20),
    settlementPositions: settlementPositions.slice(0, 20),
    dustPositions: dustPositions.slice(0, 20),
    positionsInitial,
    positionsInitialText: moneyText(positionsInitial),
    cashPnl,
    cashPnlText: `${cashPnl >= 0 ? "+" : ""}${moneyText(cashPnl)}`,
    equity,
    equityText: moneyText(equity),
    effectiveEquity,
    effectiveEquityText: moneyText(effectiveEquity),
    classification: {
      dustUsd: ACCOUNT_POSITION_DUST_USD,
      dustPrice: ACCOUNT_POSITION_DUST_PRICE,
      note: "活跃/有效持仓不包含已结束归零/灰尘仓；这些仍可能由 Data API 返回，但不会占用自动跟单名额。",
    },
    takeProfit: {
      enabled: AUTO_TAKE_PROFIT_ENABLED,
      pct: AUTO_TAKE_PROFIT_PCT,
      minValueUsd: AUTO_TAKE_PROFIT_MIN_VALUE_USD,
      maxPerPoll: AUTO_TAKE_PROFIT_MAX_PER_POLL,
    },
    updatedAt: new Date().toISOString(),
  };
  value.commandPositions = accountCommandPositionList(value);
  accountStatusCache = { ts: Date.now(), value };
  return value;
}
function positionSummaryLine(prefix, p) {
  if (!p) return `${prefix}：无`;
  return `${prefix}：${p.currentValueText} · ${p.outcome} · ${p.title}`;
}
function positionDetailLine(prefix, p) {
  if (!p) return null;
  return `${prefix}详情：${p.sizeText} @ ${p.curPriceText} / PnL ${p.cashPnlText}${p.percentPnlText && p.percentPnlText !== "-" ? ` (${p.percentPnlText})` : ""}`;
}
function compactTitleForQQ(title, maxLen = 42) {
  const s = String(title || "未知市场").replace(/\s+/g, " ").trim();
  return s.length > maxLen ? `${s.slice(0, maxLen - 1)}…` : s;
}
function formatAvgToNow(p) {
  const size = Number(p?.size || 0);
  const initial = Number(p?.initialValue || 0);
  const avg = size > 0 && initial > 0 ? initial / size : null;
  const avgText = Number.isFinite(avg) ? `${(avg * 100).toFixed(1)}¢` : "-";
  const nowText = Number.isFinite(Number(p?.curPrice)) ? `${(Number(p.curPrice) * 100).toFixed(1)}¢` : "-";
  return `${avgText}→${nowText}`;
}
function accountCommandPositionList(status) {
  if (!status) return [];
  const active = Array.isArray(status.activePositions) ? status.activePositions : [];
  const settlement = Array.isArray(status.settlementPositions) ? status.settlementPositions : [];
  const dust = ACCOUNT_BALANCE_PUSH_INCLUDE_DUST && Array.isArray(status.dustPositions) ? status.dustPositions : [];
  const seen = new Set();
  const out = [];
  for (const p of [...active, ...settlement, ...dust]) {
    const tokenId = String(p?.tokenId || "");
    if (!tokenId || seen.has(tokenId)) continue;
    const value = Number(p?.currentValue || 0);
    if (value < ACCOUNT_QQ_POSITION_MIN_VALUE_USD && p?.bucket !== "settlement") continue;
    seen.add(tokenId);
    out.push({ ...p, qqIndex: out.length + 1 });
  }
  return out.slice(0, ACCOUNT_BALANCE_PUSH_MAX_POSITIONS);
}
function qqPositionLine(p) {
  const idx = Number(p?.qqIndex || 0);
  const pnl = p?.percentPnlText && p.percentPnlText !== "-" ? p.percentPnlText : (p?.cashPnlText || "-");
  return `${idx}. ${compactTitleForQQ(p?.title)} | ${p?.outcome || "-"} | ${formatAvgToNow(p)} | ${p?.currentValueText || "-"} | ${pnl} | ${p?.bucketLabel || "活跃"}`;
}
function formatAccountBalancePushCompact(status, previous = null) {
  const items = accountCommandPositionList(status);
  const deltaEquity = previous ? Number(status.equity || 0) - Number(previous.equity || 0) : 0;
  const deltaText = previous ? `  ${deltaEquity >= 0 ? "▲" : "▼"}${moneyText(Math.abs(deltaEquity))}` : "";
  const lines = [
    `💼 持仓快报`,
    `资产 ${status.equityText || "-"}${deltaText}  ·  现金 ${status.pusdBalanceText || "-"}`,
    `活跃 ${status.activePositionsCount || 0}  /  待结算 ${status.settlementPositionsCount || 0}` + (status.dustPositionsCount ? `  ·  灰尘 ${status.dustPositionsCount}` : ""),
    `──────────────`,
    ...(items.length ? items.map(qqPositionLine) : ["无可操作持仓"]),
    `──────────────`,
    `sell 1 卖出  ·  positions 刷新`,
  ];
  return lines.join("\n");
}
function formatAccountBalancePush(status, previous = null) {
  if (ACCOUNT_BALANCE_PUSH_COMPACT) return formatAccountBalancePushCompact(status, previous);
  const deltaEquity = previous ? Number(status.equity || 0) - Number(previous.equity || 0) : 0;
  const deltaCash = previous ? Number(status.pusdBalance || 0) - Number(previous.pusdBalance || 0) : 0;
  const deltaPos = previous ? Number(status.positionsValue || 0) - Number(previous.positionsValue || 0) : 0;
  return [
    "💰 Polymarket 账户资金变化",
    `账户：${status.shortAddress || shortAddr(status.address)}`,
    `可用 pUSD：${status.pusdBalanceText}`,
    `活跃持仓：${status.activePositionsValueText}（${status.activePositionsCount} 个）`,
    `待结算：${status.settlementPositionsValueText}（${status.settlementPositionsCount} 个）`,
    `已输/灰尘：${status.dustPositionsValueText}（${status.dustPositionsCount} 个）`,
    status.takeProfit?.enabled ? `自动止盈：ON，PnL ≥ ${status.takeProfit.pct}% 自动卖出` : `自动止盈：OFF`,
    positionSummaryLine("最大活跃持仓", status.topActivePosition),
    positionDetailLine("最大活跃持仓", status.topActivePosition),
    status.topActivePosition?.marketUrl ? `最大活跃持仓链接：${status.topActivePosition.marketUrl}` : null,
    status.topSettlementPosition ? positionSummaryLine("最大待结算", status.topSettlementPosition) : null,
    `总权益：${status.equityText}`,
    previous ? `10分钟变化：权益 ${deltaEquity >= 0 ? "+" : ""}${moneyText(deltaEquity)} / 现金 ${deltaCash >= 0 ? "+" : ""}${moneyText(deltaCash)} / 持仓 ${deltaPos >= 0 ? "+" : ""}${moneyText(deltaPos)}` : "首次资金快照",
    `时间：${new Date(status.updatedAt || Date.now()).toLocaleString("zh-CN")}`,
  ].filter(Boolean).join("\n");
}
function findExistingSameTokenPosition(status, tokenId) {
  if (!AUTO_TRADE_SKIP_IF_HOLDING_SAME_TOKEN || !status || !tokenId) return null;
  const tid = String(tokenId);
  const candidates = Array.isArray(status.allPositions)
    ? status.allPositions
    : [
        ...(status.activePositions || []),
        ...(status.settlementPositions || []),
        ...(status.dustPositions || []),
      ];
  return candidates.find(p =>
    String(p?.tokenId || "") === tid &&
    Number(p?.size || 0) >= AUTO_TRADE_SAME_TOKEN_MIN_SHARES
  ) || null;
}
function sameTokenHoldingSkipReason(status, event) {
  const existing = findExistingSameTokenPosition(status, event?.tokenId);
  if (!existing) return null;
  return `已持有同档位仓位：${existing.title || event?.marketQuestion || "未知市场"} · ${existing.outcome || event?.outcome || "-"} · ${existing.sizeText || sharesText(existing.size || 0)} · ${existing.currentValueText || moneyText(existing.currentValue || 0)}，本次属于加仓，已按配置跳过`;
}
function takeProfitCandidatePositions(status) {
  if (!AUTO_TAKE_PROFIT_ENABLED || !status || !Array.isArray(status.activePositions)) return [];
  return status.activePositions
    .filter(p => Number(p.percentPnl) >= AUTO_TAKE_PROFIT_PCT && Number(p.currentValue || 0) >= AUTO_TAKE_PROFIT_MIN_VALUE_USD && p.tokenId)
    .sort((a, b) => Number(b.percentPnl || 0) - Number(a.percentPnl || 0))
    .slice(0, AUTO_TAKE_PROFIT_MAX_PER_POLL);
}

// Keep per-position peak price up to date every poll cycle.
function updatePositionPeakPrices(status) {
  if (!status?.activePositions) return;
  const activeIds = new Set();
  for (const p of status.activePositions) {
    if (!p.tokenId || !Number.isFinite(p.curPrice) || p.curPrice <= 0) continue;
    activeIds.add(p.tokenId);
    const prev = positionPeakPrice.get(p.tokenId) ?? 0;
    if (p.curPrice > prev) positionPeakPrice.set(p.tokenId, p.curPrice);
  }
  // Remove tokens no longer in active positions.
  for (const id of positionPeakPrice.keys()) {
    if (!activeIds.has(id)) positionPeakPrice.delete(id);
  }
}

function trailingStopCandidates(status) {
  if (!AUTO_TRAILING_STOP_ENABLED || !status?.activePositions) return [];
  return status.activePositions.filter(p => {
    if (!p.tokenId || !Number.isFinite(p.curPrice) || !Number.isFinite(p.percentPnl)) return false;
    if (Number(p.currentValue || 0) < AUTO_TRAILING_STOP_MIN_VALUE_USD) return false;
    if (Number(p.percentPnl) < AUTO_TRAILING_STOP_ACTIVATE_PCT) return false;
    const peak = positionPeakPrice.get(p.tokenId);
    if (!peak || peak <= 0) return false;
    const drawdown = (peak - p.curPrice) / peak;
    return drawdown >= AUTO_TRAILING_STOP_DRAWDOWN_PCT / 100;
  }).sort((a, b) => {
    const ddA = (() => { const pk = positionPeakPrice.get(a.tokenId) || a.curPrice; return pk > 0 ? (pk - a.curPrice) / pk : 0; })();
    const ddB = (() => { const pk = positionPeakPrice.get(b.tokenId) || b.curPrice; return pk > 0 ? (pk - b.curPrice) / pk : 0; })();
    return ddB - ddA;
  }).slice(0, AUTO_TRAILING_STOP_MAX_PER_POLL);
}

function stopLossCandidates(status) {
  if (!AUTO_STOP_LOSS_ENABLED || !status?.activePositions) return [];
  return status.activePositions.filter(p => {
    if (!p.tokenId || !Number.isFinite(p.percentPnl)) return false;
    if (Number(p.currentValue || 0) < AUTO_STOP_LOSS_MIN_VALUE_USD) return false;
    return Number(p.percentPnl) <= -AUTO_STOP_LOSS_PCT;
  }).sort((a, b) => Number(a.percentPnl || 0) - Number(b.percentPnl || 0))
    .slice(0, AUTO_STOP_LOSS_MAX_PER_POLL);
}

function priceFloorCandidates(status) {
  if (!AUTO_PRICE_FLOOR_ENABLED || !status?.activePositions) return [];
  return status.activePositions.filter(p => {
    if (!p.tokenId || !Number.isFinite(p.curPrice)) return false;
    if (Number(p.currentValue || 0) < AUTO_PRICE_FLOOR_MIN_VALUE_USD) return false;
    return p.curPrice < AUTO_PRICE_FLOOR_PRICE;
  }).sort((a, b) => (a.curPrice || 0) - (b.curPrice || 0))
    .slice(0, AUTO_PRICE_FLOOR_MAX_PER_POLL);
}

async function maybeAutoProtectStatus(status) {
  if (!tradeExecutor || !status) return;
  updatePositionPeakPrices(status);

  // Collect candidates from all three triggers, deduplicate by tokenId.
  const seen = new Set();
  const queue = [];
  for (const p of trailingStopCandidates(status)) {
    if (!seen.has(p.tokenId)) { seen.add(p.tokenId); queue.push({ p, trigger: "trailing_stop" }); }
  }
  for (const p of stopLossCandidates(status)) {
    if (!seen.has(p.tokenId)) { seen.add(p.tokenId); queue.push({ p, trigger: "stop_loss" }); }
  }
  for (const p of priceFloorCandidates(status)) {
    if (!seen.has(p.tokenId)) { seen.add(p.tokenId); queue.push({ p, trigger: "price_floor" }); }
  }
  if (queue.length === 0) return;

  for (const { p, trigger } of queue) {
    const peak = positionPeakPrice.get(p.tokenId);
    const drawdownPct = (peak && peak > 0) ? ((peak - p.curPrice) / peak * 100) : 0;
    const triggerDesc =
      trigger === "trailing_stop" ? `追踪止盈触发 高点 ${priceText(peak || p.curPrice)} → 现 ${priceText(p.curPrice)}，回撤 ${drawdownPct.toFixed(1)}%` :
      trigger === "stop_loss"     ? `止损触发 PnL ${Number(p.percentPnl || 0).toFixed(1)}% ≤ -${AUTO_STOP_LOSS_PCT}%` :
                                    `价格地板触发 ${priceText(p.curPrice)} < ${priceText(AUTO_PRICE_FLOOR_PRICE)}`;
    const triggerLabel =
      trigger === "trailing_stop" ? "📉 追踪止盈" :
      trigger === "stop_loss"     ? "🛑 止损" :
                                    "🔻 价格地板";
    try {
      const result = await tradeExecutor.maybeAutoTakeProfitPosition({
        ...p,
        autoTakeProfitMode: "global",
        autoTakeProfitOrderType: "FAK",
      });
      if (!result) continue;
      console.log(`[${triggerLabel}] ${p.title || "未知市场"} ${triggerDesc} ${result.message || result.reason || ""}`);
      broadcast({
        type: "tradeResult",
        signalId: `${trigger}:${p.tokenId || ""}`,
        wallet: "",
        marketQuestion: p.title || "",
        outcome: p.outcome || "",
        result,
        time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      });
      sendHermesQQBot([
        result.skipped ? `🤖 ${triggerLabel}跳过` : result.ok ? `${triggerLabel}完成` : `⚠️ ${triggerLabel}未执行`,
        `${p.outcome || ""} · ${p.title || "-"}`,
        `──────────────`,
        triggerDesc,
        `PnL ${Number(p.percentPnl || 0).toFixed(1)}%  ·  ${p.currentValueText || moneyText(p.currentValue || 0)}`,
        result.bestBid ? `出价 ${(result.bestBid * 100).toFixed(1)}¢  ·  限价 ${result.limitPrice ? `${(result.limitPrice * 100).toFixed(1)}¢` : "-"}` : null,
        result.message || result.reason || null,
        p.marketUrl || null,
      ].filter(Boolean).join("\n")).catch(err => console.error(`[${trigger}QQ推送异常]`, err.message));
      if (result.ok && !result.skipped) {
        positionPeakPrice.delete(p.tokenId);
        accountStatusCache = null;
      }
    } catch (err) {
      const error = friendlyError(err);
      console.warn(`[${triggerLabel}失败] ${error}`);
      sendHermesQQBot([`⚠️ ${triggerLabel}失败`, `${p.outcome || ""} · ${p.title || "-"}`, `错误：${error}`].join("\n")).catch(() => {});
    }
  }
}
async function maybeAutoTakeProfitStatus(status) {
  if (!tradeExecutor || !AUTO_TAKE_PROFIT_ENABLED || !status) return [];
  const candidates = takeProfitCandidatePositions(status);
  const results = [];
  for (const p of candidates) {
    try {
      const result = await tradeExecutor.maybeAutoTakeProfitPosition({
        ...p,
        autoTakeProfitMode: "global",
        autoTakeProfitOrderType: "FAK",
      });
      if (!result) continue;
      results.push({ position: p, result });
      const payload = {
        type: "tradeResult",
        signalId: `take-profit:${p.tokenId || ""}`,
        wallet: "",
        marketQuestion: p.title || "",
        outcome: p.outcome || "",
        result,
        time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      };
      console.log(`[自动止盈] ${result.mode || "unknown"} ${p.title || "未知市场"} PnL=${Number(p.percentPnl || 0).toFixed(2)}% ${result.message || result.reason || ""}`);
      broadcast(payload);
      sendHermesQQBot([
        result.skipped ? "🤖 止盈跳过" : result.ok ? "💰 止盈完成" : "⚠️ 止盈未执行",
        `${p.outcome || ""} · ${p.title || "-"}`,
        `──────────────`,
        `PnL ${Number(p.percentPnl || 0).toFixed(1)}%  /  阈值 ${AUTO_TAKE_PROFIT_PCT}%`,
        `${p.sizeText || sharesText(p.size || 0)}  ·  ${p.currentValueText || moneyText(p.currentValue || 0)}`,
        result.bestBid ? `出价 ${(result.bestBid * 100).toFixed(1)}¢  ·  限价 ${result.limitPrice ? `${(result.limitPrice * 100).toFixed(1)}¢` : "-"}` : null,
        result.message || result.reason || null,
        p.marketUrl || null,
      ].filter(Boolean).join("\n")).catch(err => console.error("[自动止盈QQ推送异常]", err.message));
      if (result.ok && !result.skipped) accountStatusCache = null;
    } catch (err) {
      const error = friendlyError(err);
      console.warn(`[自动止盈失败] ${error}`);
      broadcast({
        type: "tradeResult",
        signalId: `take-profit:${p.tokenId || ""}`,
        marketQuestion: p.title || "",
        outcome: p.outcome || "",
        error,
        time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      });
      sendHermesQQBot(["⚠️ 止盈失败", `${p.outcome || ""} · ${p.title || "-"}`, `PnL ${Number(p.percentPnl || 0).toFixed(1)}%`, `错误：${error}`].join("\n")).catch(() => {});
    }
  }
  return results;
}
function accountGuardSkipReason(status, plannedAmountUsd) {
  if (!AUTO_TRADE_ACCOUNT_GUARD_ENABLED || !status) return null;
  const cash = Number(status.pusdBalance || 0);
  const amount = Number(plannedAmountUsd || AUTO_TRADE_DEFAULT_AMOUNT_USD || 0);
  const reserve = Number(AUTO_TRADE_MIN_FREE_CASH_USD || 0);
  if (reserve > 0 && cash < amount + reserve) {
    return `可用 pUSD ${moneyText(cash)} < 本次 ${moneyText(amount)} + 预留 ${moneyText(reserve)}`;
  }
  if (AUTO_TRADE_MAX_ACTIVE_PROJECTS > 0 && Number(status.activePositionsCount || 0) >= AUTO_TRADE_MAX_ACTIVE_PROJECTS) {
    return `活跃持仓 ${status.activePositionsCount} 个 >= 上限 ${AUTO_TRADE_MAX_ACTIVE_PROJECTS} 个`;
  }
  const effectiveCount = Number(status.effectivePositionsCount ?? (Number(status.activePositionsCount || 0) + Number(status.settlementPositionsCount || 0)));
  if (AUTO_TRADE_MAX_TOTAL_PROJECTS > 0 && effectiveCount >= AUTO_TRADE_MAX_TOTAL_PROJECTS) {
    return `有效持仓 ${effectiveCount} 个 >= 上限 ${AUTO_TRADE_MAX_TOTAL_PROJECTS} 个（活跃 ${status.activePositionsCount || 0} + 待结算 ${status.settlementPositionsCount || 0}，API原始 ${status.rawPositionsCount ?? status.positionsCount ?? 0}）`;
  }
  if (AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS > 0 && Number(status.settlementPositionsCount || 0) >= AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS) {
    return `待结算 ${status.settlementPositionsCount} 个 >= 上限 ${AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS} 个`;
  }
  if (AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD > 0 && Number(status.settlementPositionsValue || 0) >= AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD) {
    return `待结算估值 ${moneyText(status.settlementPositionsValue)} >= 上限 ${moneyText(AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD)}`;
  }
  return null;
}
async function pollAccountStatusOnce({ push = false } = {}) {
  if (!tradeExecutor) return null;
  const status = await getTradingAccountStatus({ noCache: true });
  broadcast({ type: "accountStatus", status });
  await maybeAutoTakeProfitStatus(status).catch(err => console.warn("[自动止盈监控]", friendlyError(err)));
  await maybeAutoProtectStatus(status).catch(err => console.warn("[追踪止盈/止损监控]", friendlyError(err)));
  if (push && ACCOUNT_BALANCE_PUSH_ENABLED && qqConfigured()) {
    const previous = lastPushedAccountStatus;
    const delta = previous ? Math.abs(Number(status.equity || 0) - Number(previous.equity || 0)) : Infinity;
    if (!previous || delta >= ACCOUNT_BALANCE_PUSH_MIN_DELTA_USD || ACCOUNT_BALANCE_PUSH_MIN_DELTA_USD <= 0) {
      await sendHermesQQBot(formatAccountBalancePush(status, previous)).catch(err => console.error("[账户资金QQ推送异常]", err.message));
      lastPushedAccountStatus = status;
    }
  }
  return status;
}
function startAccountStatusWatcher() {
  if (accountBalanceTimer) clearInterval(accountBalanceTimer);
  if (accountBalancePushTimer) clearInterval(accountBalancePushTimer);
  pollAccountStatusOnce({ push: false }).catch(err => console.warn("[账户资金监控]", friendlyError(err)));
  accountBalanceTimer = setInterval(() => pollAccountStatusOnce({ push: false }).catch(err => console.warn("[账户资金监控]", friendlyError(err))), Math.max(15_000, ACCOUNT_BALANCE_POLL_MS));
  if (ACCOUNT_BALANCE_PUSH_ENABLED) {
    accountBalancePushTimer = setInterval(() => pollAccountStatusOnce({ push: true }).catch(err => console.warn("[账户资金推送]", friendlyError(err))), Math.max(60_000, ACCOUNT_BALANCE_PUSH_INTERVAL_MS));
  }
}

function parseQQTradeCommand(text) {
  const s = String(text || "").trim().toLowerCase();
  if (!s) return null;
  if (["positions", "position", "pos", "持仓", "仓位", "账户"].includes(s)) return { type: "positions" };
  const m = s.match(/^(?:sell|卖出|平仓)\s*#?\s*(\d+)$/i);
  if (m) return { type: "sell", index: Number(m[1]) };
  return null;
}
function isAuthorizedQQCommandAuthor(openid) {
  const actual = String(openid || "").trim();
  if (!QQBOT_TARGET_ID) return false;
  return actual === QQBOT_TARGET_ID;
}
async function sendQQAccountPositions() {
  const status = await getTradingAccountStatus({ noCache: true });
  await sendHermesQQBot(formatAccountBalancePushCompact(status, lastPushedAccountStatus));
  return status;
}
async function sellAccountPositionByQQIndex(index) {
  if (!QQ_TRADE_COMMANDS_ENABLED) throw new Error("QQ 交易命令未启用：QQ_TRADE_COMMANDS_ENABLED=false");
  const ex = requireTradeExecutor();
  const status = await getTradingAccountStatus({ noCache: true });
  const items = accountCommandPositionList(status);
  const p = items.find(x => Number(x.qqIndex) === Number(index));
  if (!p) {
    return { ok: false, skipped: true, reason: `没有找到编号 ${index}。发送 positions 查看最新编号。`, status };
  }
  const shares = Number(p.size || 0);
  if (!p.tokenId || !Number.isFinite(shares) || shares <= 0) {
    return { ok: false, skipped: true, reason: `编号 ${index} 没有可卖 token/shares`, position: p, status };
  }
  const book = await getOrderBook(p.tokenId);
  const bestBid = Number(book?.bids?.[0]?.price);
  if (!Number.isFinite(bestBid) || bestBid < QQ_SELL_COMMAND_MIN_BID) {
    return { ok: false, skipped: true, reason: `编号 ${index} 当前 bid 不足：${Number.isFinite(bestBid) ? `${(bestBid * 100).toFixed(1)}¢` : "无 bid"}`, position: p, status };
  }
  const limitPrice = Math.max(QQ_SELL_COMMAND_MIN_BID, Number((bestBid - QQ_SELL_COMMAND_MAX_SLIPPAGE).toFixed(4)));
  const mode = String(QQ_SELL_COMMAND_MODE || "live").toLowerCase();
  const result = await ex.createSignedOrderOrPost({
    mode,
    tokenId: p.tokenId,
    side: "SELL",
    price: limitPrice,
    amountUsd: shares * limitPrice,
    shares,
    orderType: QQ_SELL_COMMAND_ORDER_TYPE,
    confirmLive: "I_UNDERSTAND",
    source: "qq_sell_command",
  });
  accountStatusCache = null;
  return { ok: true, index, position: p, bestBid, limitPrice, result, status };
}
async function handleQQTradeCommand(text, authorOpenId) {
  const cmd = parseQQTradeCommand(text);
  if (!cmd) return false;
  if (!isAuthorizedQQCommandAuthor(authorOpenId)) {
    console.warn(`[QQ命令拒绝] 非授权 openid=${shortValue(authorOpenId)}`);
    return true;
  }
  try {
    if (cmd.type === "positions") {
      await sendQQAccountPositions();
      return true;
    }
    if (cmd.type === "sell") {
      const out = await sellAccountPositionByQQIndex(cmd.index);
      if (!out.ok || out.skipped) {
        await sendHermesQQBot(["⚠️ 卖出未执行", `编号 ${cmd.index}`, `原因：${out.reason || "未知"}`, "发送 positions 查看最新编号"].join("\n"));
        return true;
      }
      const p = out.position;
      const r = out.result || {};
      await sendHermesQQBot([
        "✅ 卖出已提交",
        `${p.outcome || "-"} · ${p.title || "未知市场"}`,
        `──────────────`,
        `${p.sizeText || sharesText(p.size || 0)}  ·  ${(out.bestBid * 100).toFixed(1)}¢ → ${(out.limitPrice * 100).toFixed(1)}¢`,
        r.message || (r.ok ? "订单已提交" : "已处理"),
      ].join("\n"));
      return true;
    }
  } catch (err) {
    await sendHermesQQBot(["❌ QQ交易命令失败", `命令：${String(text || "").trim()}`, `错误：${friendlyError(err)}`].join("\n")).catch(() => {});
    return true;
  }
  return false;
}

async function getCurrentPositions(address, opts = {}) {
  const params = new URLSearchParams({
    user: normalizeAddress(address),
    sizeThreshold: opts.sizeThreshold || "0.01",
    limit: String(opts.limit || 500),
    offset: String(opts.offset || 0),
    sortBy: opts.sortBy || "CURRENT",
    sortDirection: opts.sortDirection || "DESC",
  });
  const data = await apiJson(`https://data-api.polymarket.com/positions?${params.toString()}`);
  return Array.isArray(data) ? data : [];
}
async function getClosedPositions(address) {
  const endpoints = [
    `https://data-api.polymarket.com/closed-positions?user=${encodeURIComponent(normalizeAddress(address))}&limit=500&offset=0`,
    `https://data-api.polymarket.com/positions?user=${encodeURIComponent(normalizeAddress(address))}&closed=true&limit=500&offset=0`,
  ];
  for (const url of endpoints) {
    try {
      const data = await apiJson(url);
      return Array.isArray(data) ? data : [];
    } catch (_) {}
  }
  return [];
}

function extractArrayPayload(data) {
  if (Array.isArray(data)) return data;
  for (const key of ["activity", "activities", "trades", "data", "results", "items"]) {
    if (Array.isArray(data?.[key])) return data[key];
  }
  return [];
}
function activityTimestamp(item) {
  const raw = item.timestamp || item.createdAt || item.created_at || item.time || item.updatedAt || item.date;
  if (!raw) return 0;
  const d = new Date(raw);
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}
function activityUrl(item) {
  if (item.eventSlug) return `https://polymarket.com/event/${item.eventSlug}`;
  if (item.slug) return `https://polymarket.com/market/${item.slug}`;
  if (item.marketSlug) return `https://polymarket.com/market/${item.marketSlug}`;
  if (item.conditionId) return `https://polymarket.com/?search=${encodeURIComponent(item.title || item.conditionId)}`;
  return item.transactionHash ? txUrl(item.transactionHash) : null;
}
function normalizeActivityItem(item, address) {
  const rawType = String(item.type || item.activityType || item.action || item.name || "").toUpperCase();
  let side = String(item.side || item.direction || item.orderSide || "").toUpperCase();
  const rawOutcome = item.outcome || item.outcomeName || item.assetName || item.answer || "-";
  const title = item.title || item.question || item.market || item.marketQuestion || item.eventTitle || "未知市场";
  const conditionId = item.conditionId || item.condition_id || item.condition || item.marketConditionId || "";
  const tokenId = item.asset || item.tokenId || item.token_id || item.clobTokenId || item.clob_token_id || item.outcomeTokenId || "";
  const price = Number(item.price ?? item.avgPrice ?? item.averagePrice ?? item.outcomePrice ?? item.valuePrice);
  const size = Number(item.size ?? item.amount ?? item.shares ?? item.quantity ?? item.outcomeTokens ?? 0);
  const usdcSize = Number(item.usdcSize ?? item.notional ?? item.value ?? item.cashAmount ?? item.amountUsd ?? (Number.isFinite(price) && Number.isFinite(size) ? price * size : 0));
  const maker = normalizeAddress(item.maker || item.makerAddress || "");
  const taker = normalizeAddress(item.taker || item.takerAddress || "");
  const wallet = normalizeAddress(address);
  const role = maker === wallet ? "maker" : taker === wallet ? "taker" : (String(item.role || "").toLowerCase() || "-");
  if (!side) {
    if (rawType.includes("BUY")) side = "BUY";
    else if (rawType.includes("SELL")) side = "SELL";
    else if (rawType.includes("REDEEM")) side = "REDEEM";
  }
  let actionGroup = "other";
  if (rawType.includes("REDEEM") || side === "REDEEM") actionGroup = "redeem";
  else if (side === "BUY") actionGroup = "buy";
  else if (side === "SELL") actionGroup = "sell";
  else if (rawType.includes("MERGE") || rawType.includes("SPLIT")) actionGroup = "convert";
  const actionText = actionGroup === "buy" ? "买入" : actionGroup === "sell" ? "卖出/可能减仓" : actionGroup === "redeem" ? "Redeem/赎回" : actionGroup === "convert" ? "拆分/合并" : (rawType || "活动");
  const ts = activityTimestamp(item);
  return {
    id: item.id || item.transactionHash || `${wallet}-${ts}-${Math.random()}`,
    wallet,
    walletName: walletLabel(wallet),
    type: rawType || actionGroup,
    action: actionText,
    actionGroup,
    side: side || "-",
    role,
    title,
    outcome: rawOutcome,
    conditionId: conditionId ? String(conditionId) : "",
    tokenId: tokenId ? String(tokenId) : "",
    price: Number.isFinite(price) ? price : null,
    priceText: Number.isFinite(price) ? priceText(price) : "-",
    size: Number.isFinite(size) ? size : 0,
    sizeText: Number.isFinite(size) ? sharesText(size) : "-",
    usdcSize: Number.isFinite(usdcSize) ? usdcSize : 0,
    usdcSizeText: Number.isFinite(usdcSize) ? moneyText(usdcSize) : "-",
    timestamp: ts ? new Date(ts).toISOString() : null,
    timeText: ts ? new Date(ts).toLocaleString("zh-CN") : "-",
    transactionHash: item.transactionHash || item.txHash || item.hash || "",
    txUrl: item.transactionHash || item.txHash || item.hash ? txUrl(item.transactionHash || item.txHash || item.hash) : null,
    marketUrl: activityUrl(item),
    raw: item,
  };
}
async function getWalletActivity(address, opts = {}) {
  address = normalizeAddress(address);
  const limit = Math.max(1, Math.min(ACTIVITY_MAX_LIMIT, Number(opts.limit || ACTIVITY_DEFAULT_LIMIT)));
  const offset = Math.max(0, Number(opts.offset || 0));
  const cacheKey = `${address}:${limit}:${offset}`;
  if (!opts.noCache) {
    const cached = cacheGet(walletActivityCache, cacheKey, ACTIVITY_CACHE_TTL_MS);
    if (cached !== undefined) return cached;
  }

  const endpoints = [
    `https://data-api.polymarket.com/activity?user=${encodeURIComponent(address)}&limit=${limit}&offset=${offset}`,
    `https://data-api.polymarket.com/activity?proxyWallet=${encodeURIComponent(address)}&limit=${limit}&offset=${offset}`,
    `https://data-api.polymarket.com/trades?user=${encodeURIComponent(address)}&limit=${limit}&offset=${offset}`,
    `https://data-api.polymarket.com/trades?proxyWallet=${encodeURIComponent(address)}&limit=${limit}&offset=${offset}`,
  ];
  let lastError = null;
  for (const url of endpoints) {
    try {
      const data = await apiJson(url, { timeoutMs: Math.min(FETCH_TIMEOUT_MS, 20_000) });
      const items = extractArrayPayload(data).map(x => normalizeActivityItem(x, address));
      items.sort((a, b) => (new Date(b.timestamp || 0).getTime()) - (new Date(a.timestamp || 0).getTime()));
      return cacheSet(walletActivityCache, cacheKey, { address, walletName: walletLabel(address), items, endpoint: url, limit, offset, updatedAt: new Date().toISOString() });
    } catch (err) {
      lastError = err;
    }
  }
  throw new Error(`历史 Activity 查询失败：${friendlyError(lastError)}`);
}

function activityUniqueKey(item) {
  const hash = item.transactionHash || item.txHash || item.hash || "";
  if (hash) return `${item.wallet}:tx:${hash}:${item.actionGroup}:${item.side}:${item.outcome}:${item.size}:${item.usdcSize}`;
  return `${item.wallet}:activity:${item.timestamp || ""}:${item.actionGroup}:${item.title}:${item.outcome}:${item.price}:${item.size}:${item.usdcSize}`;
}
function isActivityExit(item) {
  const group = String(item?.actionGroup || "").toLowerCase();
  const side = String(item?.side || "").toUpperCase();
  const typ = String(item?.type || "").toUpperCase();
  return group === "sell" || group === "redeem" || side === "SELL" || side === "REDEEM" || typ.includes("SELL") || typ.includes("REDEEM") || typ.includes("CLAIM");
}
function activityExitToSignal(item, source = "activity_poll") {
  const isRedeem = String(item.actionGroup).toLowerCase() === "redeem" || String(item.side).toUpperCase() === "REDEEM";
  const action = isRedeem ? "历史赎回/已退出" : "历史卖出/可能减仓";
  const positionChange = isRedeem ? "赎回" : "卖出";
  return {
    type: "activity_exit",
    label: "历史退出提醒",
    source,
    wallet: item.wallet,
    walletName: item.walletName || walletLabel(item.wallet),
    side: isRedeem ? "REDEEM" : "SELL",
    role: item.role || "api",
    action,
    positionChange,
    positionChangeGroup: "exit",
    positionChangeConfidence: "medium",
    positionChangeDetail: isRedeem ? "Data API 发现 Redeem/赎回动作" : "Data API 发现卖出动作；可能是减仓或平仓",
    outcome: item.outcome || "-",
    marketQuestion: item.title || "未知市场",
    marketUrl: item.marketUrl || null,
    tokenId: item.tokenId || "",
    conditionId: item.conditionId || "",
    openUrl: item.marketUrl || item.txUrl || null,
    estimatedPrice: item.price,
    estimatedPriceText: item.priceText || "-",
    estimatedSize: item.size,
    estimatedSizeText: item.sizeText || "-",
    estimatedNotional: item.usdcSize,
    estimatedNotionalText: item.usdcSizeText || "-",
    txHash: item.transactionHash || "",
    txUrl: item.txUrl || null,
    followVerdict: "退出提醒",
    followPass: true,
    followReasonsText: "历史 Activity 轮询发现退出动作",
    signalTitle: `${isRedeem ? "♻️" : "🟠"} ${walletLabel(item.wallet)} ${action} ${item.outcome || ""}`,
    signalSummary: `${walletLabel(item.wallet)} · ${action} · ${item.title || "未知市场"} · ${item.usdcSizeText || "-"}`,
    time: item.timeText || new Date().toLocaleTimeString("zh-CN"),
    rawActivity: item.raw || null,
  };
}
function positionKey(p) {
  return String(p.asset || p.tokenId || `${p.conditionId || ""}:${p.outcome || ""}:${p.title || ""}`);
}
function makePositionSnapshot(positions) {
  const m = new Map();
  for (const p of positions || []) {
    const key = positionKey(p);
    if (!key) continue;
    m.set(key, positionLikeToRow(p, key));
  }
  return m;
}
function positionLikeToRow(p, key = null) {
  const size = Number(p.size ?? p.shares ?? p.quantity ?? p.outcomeTokens ?? p.totalShares ?? 0);
  const price = p.curPrice ?? p.currPrice ?? p.price ?? p.avgPrice ?? null;
  const valueFallback = Number.isFinite(Number(price)) && Number.isFinite(size) ? Number(price) * size : 0;
  return {
    key: key || positionKey(p),
    title: p.title || p.market?.question || p.question || p.marketQuestion || p.eventTitle || "未知市场",
    outcome: p.outcome || p.outcomeName || p.assetName || p.answer || "-",
    size,
    currentValue: Number(p.currentValue ?? p.amountWon ?? p.value ?? p.usdcSize ?? valueFallback ?? 0),
    curPrice: p.curPrice ?? p.currPrice ?? p.price ?? p.avgPrice ?? null,
    avgPrice: p.avgPrice ?? p.averagePrice ?? null,
    conditionId: p.conditionId || p.condition_id || null,
    tokenId: p.asset || p.tokenId || p.token_id || p.assetId || null,
    url: positionUrl(p),
    raw: p,
  };
}
function rowMatchesTrackedProject(row, project) {
  if (!row || !project) return false;
  const rowToken = String(row.tokenId || row.asset || "").trim();
  const rowCondition = String(row.conditionId || row.condition_id || "").trim().toLowerCase();
  const rowTitle = normalizeComparableText(row.title || row.marketQuestion || "");
  const rowOutcome = normalizeComparableText(row.outcome || "");
  const projectToken = String(project.tokenId || "").trim();
  const projectCondition = String(project.conditionId || "").trim().toLowerCase();
  const projectTitle = normalizeComparableText(project.marketQuestion || "");
  const projectOutcome = normalizeComparableText(project.outcome || "");
  const outcomeOk = !projectOutcome || projectOutcome === "-" || !rowOutcome || rowOutcome === "-" || rowOutcome === projectOutcome;
  if (projectToken && rowToken && projectToken === rowToken) return true;
  if (projectCondition && rowCondition && projectCondition === rowCondition && outcomeOk) return true;
  if (projectTitle && rowTitle && projectTitle === rowTitle && outcomeOk) return true;
  return false;
}
function aggregateTrackedPositionRows(rows, project) {
  const matched = (rows || []).filter(row => rowMatchesTrackedProject(row, project));
  if (!matched.length) return null;
  const base = matched[0];
  return {
    ...base,
    key: project.id || base.key,
    title: project.marketQuestion || base.title,
    outcome: project.outcome || base.outcome,
    size: matched.reduce((s, x) => s + Number(x.size || 0), 0),
    currentValue: matched.reduce((s, x) => s + Number(x.currentValue || 0), 0),
    tokenId: project.tokenId || base.tokenId,
    conditionId: project.conditionId || base.conditionId,
    url: project.marketUrl || base.url,
    matchedCount: matched.length,
  };
}
function findTrackedPositionInSnapshot(snapshot, project) {
  return aggregateTrackedPositionRows([...snapshot.values()], project);
}
function findTrackedProjectInClosedPositions(closedPositions, project) {
  const rows = (closedPositions || []).map((p, i) => positionLikeToRow(p, positionKey(p) || `closed-${i}`));
  return aggregateTrackedPositionRows(rows, project);
}
function trackedProjectClosedToSignal(wallet, project, closedRow, source = "closed_position_watch") {
  const raw = closedRow?.raw || {};
  const amountWon = Number(raw.amountWon ?? raw.currentValue ?? raw.value ?? closedRow?.currentValue ?? 0);
  const totalTraded = Number(raw.totalTraded ?? raw.totalBought ?? raw.initialValue ?? raw.usdcSize ?? 0);
  const pnl = Number(raw.cashPnl ?? raw.realizedPnl ?? raw.pnl ?? (Number.isFinite(amountWon) && Number.isFinite(totalTraded) ? amountWon - totalTraded : 0));
  const detailParts = [];
  if (Number.isFinite(totalTraded) && totalTraded > 0) detailParts.push(`交易额 ${moneyText(totalTraded)}`);
  if (Number.isFinite(amountWon) && amountWon > 0) detailParts.push(`赢得 ${moneyText(amountWon)}`);
  if (Number.isFinite(pnl) && pnl !== 0) detailParts.push(`盈亏 ${pnl >= 0 ? "+" : ""}${moneyText(pnl)}`);
  return {
    type: "position_closed",
    label: "项目已关闭提醒",
    source,
    wallet,
    walletName: walletLabel(wallet),
    side: "CLOSED",
    role: "api",
    action: "项目已关闭",
    positionChange: "Closed",
    positionChangeGroup: "exit",
    positionChangeConfidence: "high",
    positionChangeDetail: detailParts.length ? detailParts.join(" · ") : "该项目已经出现在目标钱包 Closed positions 中",
    outcome: project?.outcome || closedRow?.outcome || "-",
    marketQuestion: project?.marketQuestion || closedRow?.title || "未知市场",
    marketUrl: project?.marketUrl || closedRow?.url || null,
    openUrl: project?.marketUrl || closedRow?.url || null,
    tokenId: project?.tokenId || closedRow?.tokenId || null,
    conditionId: project?.conditionId || closedRow?.conditionId || null,
    estimatedPrice: closedRow?.avgPrice ?? closedRow?.curPrice ?? null,
    estimatedPriceText: pctText(closedRow?.avgPrice ?? closedRow?.curPrice),
    estimatedSize: closedRow?.size || 0,
    estimatedSizeText: sharesText(closedRow?.size || 0),
    estimatedNotional: Number.isFinite(amountWon) && amountWon > 0 ? amountWon : Math.max(0, pnl),
    estimatedNotionalText: Number.isFinite(amountWon) && amountWon > 0 ? moneyText(amountWon) : moneyText(Math.max(0, pnl)),
    followVerdict: "已关闭",
    followPass: true,
    followReasonsText: "目标钱包 Closed positions 中已出现该项目",
    signalTitle: `✅ ${walletLabel(wallet)} 项目已关闭 ${project?.outcome || closedRow?.outcome || ""}`,
    signalSummary: `${walletLabel(wallet)} · 项目已关闭 · ${project?.marketQuestion || closedRow?.title || "未知市场"} · ${detailParts.join(" · ") || "Closed"}`,
    trackedProjectId: project?.id,
    trackedProjectName: project?.marketQuestion,
    trackedProjectMatched: true,
    time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  };
}
function positionExitToSignal(wallet, before, after, source = "position_watch") {
  const beforeSize = Number(before?.size || 0);
  const afterSize = Number(after?.size || 0);
  const reduction = analyzeShareReduction(beforeSize, afterSize);
  const sizeDelta = Math.max(0, beforeSize - afterSize);
  const beforeValue = Number(before?.currentValue || 0);
  const afterValue = Number(after?.currentValue || 0);
  const valueDelta = Math.max(0, beforeValue - afterValue);
  const closed = afterSize <= EXIT_POSITION_MIN_SIZE_DELTA;
  const positionChange = closed ? "平仓" : "减仓";
  return {
    type: "position_exit",
    label: "持仓变化提醒",
    source,
    wallet,
    walletName: walletLabel(wallet),
    side: "SELL",
    role: "api",
    action: `持仓${positionChange}`,
    positionChange,
    positionChangeGroup: "exit",
    positionChangeConfidence: "medium",
    positionChangeDetail: `${sharesText(beforeSize)} → ${sharesText(afterSize)}（减少 ${sharesText(sizeDelta)}）`,
    rawSizeDelta: sizeDelta,
    rawValueDelta: valueDelta,
    reductionDebug: reduction.reason,
    outcome: before?.outcome || after?.outcome || "-",
    marketQuestion: before?.title || after?.title || "未知市场",
    marketUrl: before?.url || after?.url || null,
    openUrl: before?.url || after?.url || null,
    tokenId: before?.tokenId || after?.tokenId || null,
    conditionId: before?.conditionId || after?.conditionId || null,
    estimatedPrice: before?.curPrice ?? after?.curPrice ?? null,
    estimatedPriceText: pctText(before?.curPrice ?? after?.curPrice),
    estimatedSize: sizeDelta,
    estimatedSizeText: sharesText(sizeDelta),
    estimatedNotional: valueDelta,
    estimatedNotionalText: moneyText(valueDelta),
    followVerdict: "退出提醒",
    followPass: true,
    followReasonsText: "持仓轮询发现仓位减少/消失",
    signalTitle: `🟠 ${walletLabel(wallet)} ${positionChange} ${before?.outcome || ""}`,
    signalSummary: `${walletLabel(wallet)} · ${positionChange} · ${before?.title || "未知市场"} · ${sharesText(beforeSize)} → ${sharesText(afterSize)}`,
    time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  };
}
function exitSignalCooldownKey(signal) {
  const wallet = normalizeAddress(signal?.wallet);
  // Key on wallet + specific token/condition only (ignoring source type).
  // This prevents the same exit from firing via chain WS, activity poll AND
  // position-snapshot poll all within the same cooldown window.
  const market = signal?.tokenId || signal?.conditionId || signal?.marketQuestion || signal?.title || "unknown";
  const outcome = signal?.outcome || "";
  return `exit:${wallet}:${market}:${outcome}`;
}
function canEmitExitSignal(signal, key = null) {
  const k = key || exitSignalCooldownKey(signal);
  const last = exitAlertCooldown.get(k) || 0;
  if (Date.now() - last < EXIT_ALERT_COOLDOWN_MS) return false;
  exitAlertCooldown.set(k, Date.now());
  if (exitAlertCooldown.size > 5000) {
    const cutoff = Date.now() - EXIT_ALERT_COOLDOWN_MS * 2;
    for (const [oldKey, ts] of exitAlertCooldown.entries()) {
      if (ts < cutoff) exitAlertCooldown.delete(oldKey);
    }
  }
  return true;
}
function buildExitBatchSignal(wallet, signals, source = "exit_batch") {
  const list = signals.slice(0, EXIT_BATCH_MAX_ITEMS);
  const totalUsd = signals.reduce((sum, x) => sum + Number(x.estimatedNotional || 0), 0);
  const hasMore = signals.length > list.length;
  const title = `${walletLabel(wallet)} 退出提醒汇总`;
  const summaryItems = list.map(x => `${x.positionChange || x.action || "退出"} ${x.outcome || ""} · ${x.marketQuestion || "未知市场"} · ${x.estimatedNotionalText || "-"}`);
  return {
    type: "exit_batch",
    label: "退出提醒汇总",
    source,
    wallet,
    walletName: walletLabel(wallet),
    side: "SELL",
    role: "api",
    action: `退出汇总 ${signals.length} 条`,
    positionChange: "退出汇总",
    positionChangeGroup: "exit",
    positionChangeConfidence: "medium",
    positionChangeDetail: summaryItems.join("；") + (hasMore ? `；另有 ${signals.length - list.length} 条已折叠` : ""),
    outcome: "",
    marketQuestion: `${signals.length} 个退出/减仓动作已合并`,
    estimatedNotional: totalUsd,
    estimatedNotionalText: moneyText(totalUsd),
    followVerdict: "退出汇总",
    followPass: true,
    followReasonsText: `同一轮轮询发现 ${signals.length} 条退出，为避免刷屏已合并`,
    signalTitle: `🟠 ${title}：${signals.length} 条`,
    signalSummary: `${walletLabel(wallet)} · ${signals.length} 条退出提醒 · 合计约 ${moneyText(totalUsd)}`,
    items: list.map(x => ({
      action: x.action,
      positionChange: x.positionChange,
      outcome: x.outcome,
      marketQuestion: x.marketQuestion,
      estimatedNotionalText: x.estimatedNotionalText,
      estimatedSizeText: x.estimatedSizeText,
      estimatedPriceText: x.estimatedPriceText,
      openUrl: x.openUrl,
      source: x.source,
    })),
    foldedCount: Math.max(0, signals.length - list.length),
    time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
  };
}
async function emitExitSignals(wallet, signals, source = "exit_batch") {
  const filtered = signals.filter(Boolean).filter(sig => canEmitExitSignal(sig));
  if (!filtered.length) return;
  if (EXIT_BATCH_ENABLED && filtered.length > 1) {
    await emitUiEvent(buildExitBatchSignal(wallet, filtered, source));
    return;
  }
  for (const sig of filtered) await emitUiEvent(sig);
}
async function pollActivityExitsOnce() {
  if (!EXIT_ACTIVITY_WATCH_ENABLED) return;
  const enabled = getExitWatchWallets();
  if (TRACKED_EXIT_ONLY && !enabled.length) return;
  const startupRecentCutoff = Date.now() - EXIT_ACTIVITY_STARTUP_RECENT_MINUTES * 60_000;
  await mapLimit(enabled, Math.min(3, DISCOVERY_CONCURRENCY || 1), async (w) => {
    try {
      const data = await getWalletActivity(w.address, { limit: EXIT_ACTIVITY_LOOKBACK_LIMIT, offset: 0, noCache: true });
      const exits = (data.items || [])
        .filter(isActivityExit)
        .filter(x => Number(x.usdcSize || 0) >= EXIT_ACTIVITY_MIN_USD || String(x.actionGroup || "").toLowerCase() === "redeem")
        .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
      const signals = [];
      for (const item of exits) {
        const key = activityUniqueKey(item);
        if (seenActivityExitKeys.has(key)) continue;
        seenActivityExitKeys.add(key);
        // 防止长时间运行后 Set 无限增长（每 20s 一次 × 多个钱包 × 80 条）
        if (seenActivityExitKeys.size > 50_000) {
          const pruneCount = Math.max(1, seenActivityExitKeys.size - 35_000);
          let i = 0;
          for (const oldKey of seenActivityExitKeys) {
            seenActivityExitKeys.delete(oldKey);
            if (++i >= pruneCount) break;
          }
        }
        const ts = item.timestamp ? new Date(item.timestamp).getTime() : 0;
        if (!activityWatchBootstrapped && (!ts || ts < startupRecentCutoff)) continue;
        const sig = activityExitToSignal(item);
        if (TRACKED_EXIT_ONLY && !findTrackedProjectForSignal(sig)) continue;
        signals.push(sig);
        if (signals.length >= EXIT_ACTIVITY_MAX_SIGNALS_PER_WALLET_POLL) break;
      }
      await emitExitSignals(w.address, signals, "activity_poll_batch");
    } catch (err) {
      console.warn(`[Activity退出监控失败] ${w.name} ${shortAddr(w.address)} ${friendlyError(err)}`);
    }
  });
  activityWatchBootstrapped = true;
}
async function pollTrackedProjectPositionExitsForWallet(w) {
  const projects = trackedProjectsForWallet(w.address);
  if (!projects.length) return;

  const [positions, closedPositions] = await Promise.all([
    getCurrentPositions(w.address, { limit: 500 }),
    getClosedPositions(w.address).catch(() => []),
  ]);
  const next = makePositionSnapshot(positions);
  let stateMap = trackedProjectPositionStates.get(w.address);
  if (!stateMap) {
    stateMap = new Map();
    trackedProjectPositionStates.set(w.address, stateMap);
  }

  const signals = [];
  for (const project of projects) {
    const current = findTrackedPositionInSnapshot(next, project);
    const closed = findTrackedProjectInClosedPositions(closedPositions, project);
    const prev = stateMap.get(project.id);
    const status = closed ? "closed" : current ? "active" : "absent";
    const row = current || closed || null;

    // First poll only establishes baseline. This avoids immediately alerting on
    // projects that were already closed before the user selected them or before
    // the script was restarted.
    if (!prev) {
      stateMap.set(project.id, { status, row, ts: Date.now() });
      continue;
    }

    if (closed && prev.status !== "closed") {
      signals.push(trackedProjectClosedToSignal(w.address, project, closed));
    } else if (prev.status === "active" && prev.row && current) {
      const before = prev.row;
      const after = current;
      const reduction = analyzeShareReduction(before.size, after.size);
      const valueDelta = Number(before.currentValue || 0) - Number(after.currentValue || 0);
      if (reduction.ok && valueDelta >= EXIT_POSITION_MIN_VALUE_DELTA_USD) {
        const sig = positionExitToSignal(w.address, before, after, "tracked_position_reduce_watch");
        sig.trackedProjectId = project.id;
        sig.trackedProjectName = project.marketQuestion;
        sig.trackedProjectMatched = true;
        sig.followReasonsText = `已选择项目 Active position shares 明显减少；${reduction.reason}`;
        signals.push(sig);
      } else if (valueDelta >= EXIT_POSITION_MIN_VALUE_DELTA_USD && !reduction.ok) {
        console.log(`[减仓检测忽略] ${w.name} ${project.marketQuestion} shares未明显减少，只是估值/价格变化：${reduction.reason}, valueDelta=${valueDelta.toFixed(4)}`);
      }
    } else if (prev.status === "active" && !current && closed) {
      signals.push(trackedProjectClosedToSignal(w.address, project, closed));
    } else if (prev.status === "active" && !current && !closed && EXIT_POSITION_ALERT_ON_MISSING) {
      const sig = positionExitToSignal(w.address, prev.row, { ...prev.row, size: 0, currentValue: 0 }, "tracked_position_missing_watch");
      sig.trackedProjectId = project.id;
      sig.trackedProjectName = project.marketQuestion;
      sig.trackedProjectMatched = true;
      sig.followReasonsText = "已选择项目从 Active positions 消失，但 Closed API 暂未确认";
      signals.push(sig);
    }

    stateMap.set(project.id, { status, row, ts: Date.now() });
    if (signals.length >= EXIT_POSITION_MAX_SIGNALS_PER_WALLET_POLL) break;
  }

  await emitExitSignals(w.address, signals, "tracked_project_position_watch_batch");
}

async function pollPositionExitsOnce() {
  if (!EXIT_POSITION_WATCH_ENABLED) return;
  const enabled = getExitWatchWallets();
  if (TRACKED_EXIT_ONLY && !enabled.length) return;
  await mapLimit(enabled, Math.min(3, DISCOVERY_CONCURRENCY || 1), async (w) => {
    try {
      if (TRACKED_EXIT_ONLY) {
        await pollTrackedProjectPositionExitsForWallet(w);
        return;
      }

      const positions = await getCurrentPositions(w.address, { limit: 500 });
      const next = makePositionSnapshot(positions);
      const prev = positionSnapshots.get(w.address);
      const signals = [];

      if (prev) {
        // If a high-volume wallet suddenly returns an empty/near-empty page, treat it as API/page churn, not mass exits.
        if (prev.size > 10 && next.size === 0) {
          console.warn(`[持仓退出监控跳过] ${w.name} ${shortAddr(w.address)} 当前快照为空，疑似 Data API 波动，未生成退出提醒`);
        } else {
          for (const [key, before] of prev.entries()) {
            const beforeValue = Number(before.currentValue || 0);
            if (beforeValue < EXIT_POSITION_TRACK_MIN_VALUE_USD) continue;

            const after = next.get(key);
            if (!after) {
              const missKey = `${w.address}:${key}`;
              const missCount = (positionMissingCounts.get(missKey) || 0) + 1;
              positionMissingCounts.set(missKey, missCount);
              if (!EXIT_POSITION_ALERT_ON_MISSING || missCount < EXIT_POSITION_MISSING_CONFIRM_POLLS) continue;
              const sig = positionExitToSignal(w.address, before, { ...before, size: 0, currentValue: 0 }, "position_missing_watch");
              if (!TRACKED_EXIT_ONLY || findTrackedProjectForSignal(sig)) signals.push(sig);
            } else {
              positionMissingCounts.delete(`${w.address}:${key}`);
              const reduction = analyzeShareReduction(before.size, after.size);
              const valueDelta = Number(before.currentValue || 0) - Number(after.currentValue || 0);
              if (reduction.ok && valueDelta >= EXIT_POSITION_MIN_VALUE_DELTA_USD) {
                const sig = positionExitToSignal(w.address, before, after);
                if (!TRACKED_EXIT_ONLY || findTrackedProjectForSignal(sig)) signals.push(sig);
              } else if (valueDelta >= EXIT_POSITION_MIN_VALUE_DELTA_USD && !reduction.ok) {
                console.log(`[减仓检测忽略] ${w.name} ${before.title || before.key} shares未明显减少，只是估值/价格变化：${reduction.reason}, valueDelta=${valueDelta.toFixed(4)}`);
              }
            }
            if (signals.length >= EXIT_POSITION_MAX_SIGNALS_PER_WALLET_POLL) break;
          }
        }
      }

      // Track only meaningful positions. Low-value/0-value tails from whales are too noisy and page in/out frequently.
      const trackedNext = new Map();
      for (const [key, row] of next.entries()) {
        if (Number(row.currentValue || 0) >= EXIT_POSITION_TRACK_MIN_VALUE_USD) trackedNext.set(key, row);
      }
      positionSnapshots.set(w.address, trackedNext);
      await emitExitSignals(w.address, signals, "position_watch_batch");
    } catch (err) {
      console.warn(`[持仓退出监控失败] ${w.name} ${shortAddr(w.address)} ${friendlyError(err)}`);
    }
  });
  positionWatchBootstrapped = true;
}
function startExitWatchers() {
  if (activityWatchTimer) clearInterval(activityWatchTimer);
  if (positionWatchTimer) clearInterval(positionWatchTimer);
  if (EXIT_ACTIVITY_WATCH_ENABLED) {
    pollActivityExitsOnce().catch(err => console.warn("[Activity退出监控]", friendlyError(err)));
    activityWatchTimer = setInterval(() => pollActivityExitsOnce().catch(err => console.warn("[Activity退出监控]", friendlyError(err))), Math.max(5_000, EXIT_ACTIVITY_POLL_MS));
  }
  if (EXIT_POSITION_WATCH_ENABLED) {
    pollPositionExitsOnce().catch(err => console.warn("[持仓退出监控]", friendlyError(err)));
    positionWatchTimer = setInterval(() => pollPositionExitsOnce().catch(err => console.warn("[持仓退出监控]", friendlyError(err))), Math.max(10_000, EXIT_POSITION_POLL_MS));
  }
}
function restartExitWatchers() {
  positionSnapshots.clear();
  trackedProjectPositionStates.clear();
  positionMissingCounts.clear();
  startExitWatchers();
}
function positionUrl(p) {
  if (p.eventSlug) return `https://polymarket.com/event/${p.eventSlug}`;
  if (p.slug) return `https://polymarket.com/market/${p.slug}`;
  if (p.conditionId) return `https://polymarket.com/?search=${encodeURIComponent(p.title || p.conditionId)}`;
  return null;
}
function parseEndDate(p, market) {
  const raw = market?.endDateIso || market?.endDate || p.endDate || p.endDateIso;
  const d = raw ? new Date(raw) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
}
function inferSettlementStatus(p, market) {
  const status = String(market?.umaResolutionStatus || market?.resolutionStatus || "").toLowerCase();
  const end = parseEndDate(p, market);
  const nowTs = Date.now();
  const outcome = p.outcome || "-";
  const closed = market?.closed === true || p.closed === true;
  const active = market?.active !== false && !closed;
  const resolved = Boolean(
    market?.resolved === true ||
    market?.resolutionData ||
    market?.winningOutcome ||
    market?.winner ||
    status.includes("resolved")
  );

  if (status.includes("dispute")) {
    return { code: "disputed", label: "争议中", badge: "争议", eta: "可能 4–6 天或更久", detail: status || "disputed" };
  }
  if (resolved || status === "resolved") {
    return { code: "redeemable", label: `${outcome} 已赢/已结算`, badge: "可 Redeem", eta: "可检查 Redeem", detail: status || "resolved" };
  }
  if (status.includes("proposed") || market?.umaResolutionStatus === "proposed") {
    return { code: "proposed", label: `${outcome} 已提案，等待 UMA 挑战期`, badge: "UMA proposed", eta: "无争议通常约 2 小时内", detail: status || "proposed" };
  }
  if (end && nowTs >= end.getTime()) {
    return { code: "expired", label: "已到期，等待结果提案", badge: "待提案", eta: "等待提案 + 挑战期", detail: "expired" };
  }
  if (active) {
    const diff = end ? Math.max(0, end.getTime() - nowTs) : null;
    return { code: "active", label: "交易中", badge: "Active", eta: diff === null ? "未知到期时间" : humanDuration(diff), detail: "active" };
  }
  return { code: "unknown", label: "状态未知", badge: "Unknown", eta: "请打开市场页确认", detail: status || "unknown" };
}
function humanDuration(ms) {
  const sec = Math.floor(ms / 1000);
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (d > 0) return `约 ${d}天${h}小时 后到期`;
  if (h > 0) return `约 ${h}小时${m}分钟 后到期`;
  return `约 ${m}分钟 后到期`;
}
function buildPositionRow(p, market = null, statusError = null) {
  const status = statusError
    ? { code: "status_unavailable", label: "状态暂不可用", badge: "状态失败", eta: "持仓数据已显示，市场结算状态查询失败", detail: statusError }
    : inferSettlementStatus(p, market);
  const endDate = parseEndDate(p, market);
  return {
    title: p.title || market?.question || "未知市场",
    outcome: p.outcome || "-",
    size: Number(p.size || 0),
    sizeText: sharesText(p.size),
    avgPrice: p.avgPrice,
    avgPriceText: pctText(p.avgPrice),
    curPrice: p.curPrice ?? p.currPrice,
    curPriceText: pctText(p.curPrice ?? p.currPrice),
    currentValue: Number(p.currentValue || 0),
    currentValueText: moneyText(p.currentValue),
    cashPnl: Number(p.cashPnl || 0),
    cashPnlText: `${Number(p.cashPnl || 0) >= 0 ? "+" : ""}${moneyText(p.cashPnl)}`,
    percentPnl: p.percentPnl,
    initialValue: Number(p.initialValue || 0),
    conditionId: p.conditionId,
    tokenId: p.asset,
    endDate: endDate?.toISOString() || null,
    endDateText: endDate?.toLocaleString("zh-CN") || "-",
    url: positionUrl(p) || buildPolymarketUrl(market, p.asset),
    status,
    statusError,
  };
}
async function enrichPositions(address) {
  let positions = [];
  try {
    positions = await getCurrentPositions(address);
  } catch (err) {
    throw new Error(`当前持仓 Data API 查询失败：${friendlyError(err)}`);
  }

  const enriched = await Promise.allSettled(positions.map(async (p) => {
    let market = null;
    let statusError = null;
    if (p.conditionId) {
      try {
        market = await getMarketByCondition(p.conditionId);
      } catch (err) {
        statusError = friendlyError(err);
      }
    }
    return buildPositionRow(p, market, statusError);
  }));

  return enriched.map((r, i) => {
    if (r.status === "fulfilled") return r.value;
    return buildPositionRow(positions[i], null, friendlyError(r.reason));
  });
}
async function analyzeWallet(address) {
  address = normalizeAddress(address);
  const w = walletMap.get(address) || { name: shortAddr(address), address };
  const positions = await enrichPositions(address);
  const totalValue = positions.reduce((s, p) => s + Number(p.currentValue || 0), 0);
  const unrealizedPnl = positions.reduce((s, p) => s + Number(p.cashPnl || 0), 0);
  const redeemableValue = positions
    .filter(p => ["redeemable", "proposed"].includes(p.status.code))
    .reduce((s, p) => s + Number(p.currentValue || 0), 0);

  let closedPositions = [];
  try { closedPositions = await getClosedPositions(address); } catch (_) {}
  const closedPnl = closedPositions.reduce((s, p) => s + Number(p.cashPnl || p.realizedPnl || 0), 0);
  const closedCount = closedPositions.length;
  const winCount = closedPositions.filter(p => Number(p.cashPnl || p.realizedPnl || 0) > 0).length;
  const winRate = closedCount ? winCount / closedCount : null;
  const roiBase = positions.reduce((s, p) => s + Number(p.initialValue || 0), 0);
  const currentRoi = roiBase > 0 ? unrealizedPnl / roiBase : null;

  let score = 45;
  if (closedCount === 0) score -= 18;
  else if (closedCount < 5) score -= 10;
  else score += 5;
  if (closedCount >= 10) score += 8;
  if (closedCount >= 30) score += 8;
  score += ((winRate ?? 0.5) - 0.5) * 30;
  if (currentRoi !== null) score += Math.max(-15, Math.min(20, currentRoi * 50));
  score += Math.min(10, Math.log10(totalValue + 1) * 3);
  score = Math.max(0, Math.min(100, score));

  return {
    name: w.name,
    address,
    short: shortAddr(address),
    enabled: w.enabled !== false,
    score: Math.round(score),
    totalValue,
    totalValueText: moneyText(totalValue),
    unrealizedPnl,
    unrealizedPnlText: `${unrealizedPnl >= 0 ? "+" : ""}${moneyText(unrealizedPnl)}`,
    redeemableValue,
    redeemableValueText: moneyText(redeemableValue),
    positionCount: positions.length,
    proposedOrRedeemableCount: positions.filter(p => ["proposed", "redeemable"].includes(p.status.code)).length,
    closedCount,
    closedPnl,
    closedPnlText: `${closedPnl >= 0 ? "+" : ""}${moneyText(closedPnl)}`,
    winRate,
    winRateText: winRate === null ? "样本不足" : `${(winRate * 100).toFixed(1)}%`,
    currentRoi,
    currentRoiText: currentRoi === null ? "-" : `${(currentRoi * 100).toFixed(1)}%`,
    positions,
    note: "评分为基础参考：当前持仓、已关闭样本、胜率和ROI综合。真正跟单还需看盘口流动性。",
  };
}


// ─── Wallet discovery helpers ─────────────────────────────────────────────
function clamp(n, min, max) { return Math.max(min, Math.min(max, Number(n) || 0)); }
function parseLeaderboardWallet(item) {
  const address = normalizeAddress(item?.proxyWallet || item?.address || item?.wallet || item?.user || "");
  if (!ethers.isAddress(address)) return null;
  return {
    rank: Number(item.rank || 0),
    address,
    userName: String(item.userName || item.username || item.name || shortAddr(address)),
    vol: Number(item.vol || item.volume || 0),
    pnl: Number(item.pnl || item.profit || 0),
    profileImage: item.profileImage || "",
    xUsername: item.xUsername || "",
    verifiedBadge: Boolean(item.verifiedBadge),
    raw: item,
  };
}
function discoveryScore(item, analysis) {
  const walletScore = Number(analysis?.score || 0);
  const pnl = Number(item.pnl || 0);
  const vol = Number(item.vol || 0);
  const winRate = Number(analysis?.winRate);
  const closedCount = Number(analysis?.closedCount || 0);
  const pnlScore = pnl <= 0 ? clamp(40 + pnl / 1000, 0, 50) : clamp(50 + Math.log10(pnl + 1) * 12, 50, 100);
  const volScore = vol <= 0 ? 35 : clamp(35 + Math.log10(vol + 1) * 10, 35, 100);
  const winScore = Number.isFinite(winRate) ? clamp(winRate * 100, 0, 100) : 50;
  const samplePenalty = closedCount < 5 ? 12 : closedCount < 15 ? 6 : 0;
  return Math.round(clamp(walletScore * 0.55 + pnlScore * 0.20 + volScore * 0.10 + winScore * 0.15 - samplePenalty, 0, 100));
}
async function getLeaderboardCandidates(options = {}) {
  const category = String(options.category || DISCOVERY_DEFAULT_CATEGORY || "OVERALL").toUpperCase();
  const timePeriod = String(options.timePeriod || DISCOVERY_DEFAULT_PERIOD || "MONTH").toUpperCase();
  const orderBy = String(options.orderBy || DISCOVERY_DEFAULT_ORDER_BY || "PNL").toUpperCase();
  const limit = clamp(options.limit || DISCOVERY_DEFAULT_LIMIT, 1, 50);
  const offset = clamp(options.offset || 0, 0, 1000);
  const url = new URL("https://data-api.polymarket.com/v1/leaderboard");
  url.searchParams.set("category", category);
  url.searchParams.set("timePeriod", timePeriod);
  url.searchParams.set("orderBy", orderBy);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  const data = await apiJson(url.toString());
  const arr = Array.isArray(data) ? data : (data?.rankings || data?.leaderboard || data?.data || []);
  return arr.map(parseLeaderboardWallet).filter(Boolean);
}
async function withTimeoutValue(promise, ms, fallback) {
  let settled = false;
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        resolve(fallback);
      }
    }, ms);
    Promise.resolve(promise)
      .then((value) => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          resolve(value);
        }
      })
      .catch(() => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          resolve(fallback);
        }
      });
  });
}

async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, async () => {
    while (idx < items.length) {
      const current = idx++;
      out[current] = await fn(items[current], current);
    }
  });
  await Promise.all(workers);
  return out;
}

async function analyzeWalletLight(address) {
  address = normalizeAddress(address);

  const positionsPromise = withTimeoutValue(
    getCurrentPositions(address, { limit: 80 }),
    DISCOVERY_ANALYZE_TIMEOUT_MS,
    []
  );
  const closedPromise = withTimeoutValue(
    getClosedPositions(address),
    DISCOVERY_ANALYZE_TIMEOUT_MS,
    []
  );

  const [positions, closedPositions] = await Promise.all([positionsPromise, closedPromise]);

  const totalValue = positions.reduce((s, p) => s + Number(p.currentValue || 0), 0);
  const unrealizedPnl = positions.reduce((s, p) => s + Number(p.cashPnl || 0), 0);
  const redeemableValue = positions
    .filter(p => p.redeemable === true || p.resolved === true)
    .reduce((s, p) => s + Number(p.currentValue || 0), 0);
  const closedPnl = closedPositions.reduce((s, p) => s + Number(p.cashPnl || p.realizedPnl || 0), 0);
  const closedCount = closedPositions.length;
  const winCount = closedPositions.filter(p => Number(p.cashPnl || p.realizedPnl || 0) > 0).length;
  const winRate = closedCount ? winCount / closedCount : null;
  const initialValue = positions.reduce((s, p) => s + Number(p.initialValue || 0), 0);
  const currentRoi = initialValue > 0 ? unrealizedPnl / initialValue : null;

  let score = 45;
  if (closedCount === 0) score -= 18;
  else if (closedCount < 5) score -= 10;
  else score += 5;
  if (closedCount >= 10) score += 8;
  if (closedCount >= 30) score += 8;
  score += ((winRate ?? 0.5) - 0.5) * 30;
  if (currentRoi !== null) score += Math.max(-15, Math.min(20, currentRoi * 50));
  score += Math.min(10, Math.log10(totalValue + 1) * 3);
  score = Math.round(clamp(score, 0, 100));

  return {
    address,
    short: shortAddr(address),
    score,
    totalValue,
    totalValueText: moneyText(totalValue),
    unrealizedPnl,
    unrealizedPnlText: `${unrealizedPnl >= 0 ? "+" : ""}${moneyText(unrealizedPnl)}`,
    redeemableValue,
    redeemableValueText: moneyText(redeemableValue),
    positionCount: positions.length,
    closedCount,
    closedPnl,
    closedPnlText: `${closedPnl >= 0 ? "+" : ""}${moneyText(closedPnl)}`,
    winRate,
    winRateText: winRate === null ? "样本不足" : `${(winRate * 100).toFixed(1)}%`,
    currentRoi,
    currentRoiText: currentRoi === null ? "-" : `${(currentRoi * 100).toFixed(1)}%`,
    note: "快速评分：发现钱包时只做轻量分析，避免卡住。加入监控后可在钱包评分里做更完整分析。",
  };
}

function leaderboardOnlyScore(item) {
  const pnl = Number(item.pnl || 0);
  const vol = Number(item.vol || 0);
  const pnlScore = pnl <= 0 ? clamp(40 + pnl / 1000, 0, 50) : clamp(50 + Math.log10(pnl + 1) * 12, 50, 100);
  const volScore = vol <= 0 ? 35 : clamp(35 + Math.log10(vol + 1) * 10, 35, 100);
  return Math.round(clamp(pnlScore * 0.75 + volScore * 0.25, 0, 100));
}

async function discoverWalletCandidates(options = {}) {
  const minPnl = Number(options.minPnl ?? DISCOVERY_MIN_PNL);
  const minVol = Number(options.minVol ?? DISCOVERY_MIN_VOL);
  const minScore = Number(options.minScore ?? DISCOVERY_MIN_SCORE);
  const analyze = options.analyze !== false;

  const base = (await withTimeoutValue(
    getLeaderboardCandidates(options),
    Number(process.env.DISCOVERY_LEADERBOARD_TIMEOUT_MS || 12000),
    []
  ))
    .filter(x => Number(x.pnl || 0) >= minPnl && Number(x.vol || 0) >= minVol);

  if (!base.length) return [];

  const results = await mapLimit(base, DISCOVERY_CONCURRENCY, async (item) => {
    let analysis = null;
    if (analyze) {
      analysis = await withTimeoutValue(
        analyzeWalletLight(item.address),
        DISCOVERY_ANALYZE_TIMEOUT_MS + 1500,
        null
      );
    }

    const score = analysis ? discoveryScore(item, analysis) : leaderboardOnlyScore(item);
    const common = {
      ...item,
      analysis,
      discoveryScore: score,
      discoveryScoreText: analysis ? `${score}/100` : `${score}/100 快速`,
      alreadyWatched: walletMap.has(item.address),
    };

    if (score < minScore) {
      return { ...common, filteredOut: true, filterReason: `发现评分 ${score} < ${minScore}` };
    }
    return { ...common, filteredOut: false, filterReason: analysis ? "通过" : "通过（快速评分）" };
  });

  results.sort((a, b) => (Number(b.discoveryScore || 0) - Number(a.discoveryScore || 0)) || (Number(b.pnl || 0) - Number(a.pnl || 0)));
  return results;
}
function addDiscoveredWallets(candidates, options = {}) {
  const added = [];
  const updated = [];
  const maxAdd = Number(options.maxAdd || 50);
  for (const c of candidates.slice(0, maxAdd)) {
    const address = normalizeAddress(c.address || c.proxyWallet);
    if (!ethers.isAddress(address)) continue;
    const name = String(c.name || c.userName || c.username || `lb_${shortAddr(address)}`).trim();
    const tags = Array.from(new Set([...(Array.isArray(c.tags) ? c.tags : []), "discovered", String(c.category || "leaderboard").toLowerCase()].filter(Boolean)));
    const existing = wallets.findIndex(w => w.address === address);
    const wallet = canonicalWallet({ name, address, enabled: c.enabled !== false, tags, note: c.note || `自动发现：PNL ${moneyText(c.pnl || 0)} / VOL ${moneyText(c.vol || 0)} / score ${c.discoveryScore ?? "-"}` });
    if (existing >= 0) {
      wallets[existing] = { ...wallets[existing], name: wallets[existing].name || wallet.name, tags: Array.from(new Set([...(wallets[existing].tags || []), ...tags])), enabled: wallets[existing].enabled !== false };
      updated.push(wallets[existing]);
    } else {
      wallets.push(wallet);
      added.push(wallet);
    }
  }
  if (added.length || updated.length) saveWallets();
  return { added, updated, wallets: publicWallets() };
}

// ─── Follow signal classification ─────────────────────────────────────────

async function analyzeWalletCached(address) {
  address = normalizeAddress(address);
  const cached = cacheGet(walletAnalysisCache, address, 10 * 60_000);
  if (cached !== undefined) return cached;
  try {
    const data = await analyzeWallet(address);
    cacheSet(walletAnalysisCache, address, data);
    return data;
  } catch (err) {
    console.warn(`[钱包评分失败] ${shortAddr(address)} ${err.message}`);
    return { address, score: 0, error: err.message };
  }
}

function parseBookSide(items, side) {
  const arr = Array.isArray(items) ? items : [];
  const out = arr
    .map(x => ({
      price: Number(x.price ?? x.p ?? x[0]),
      size: Number(x.size ?? x.s ?? x[1]),
    }))
    .filter(x => Number.isFinite(x.price) && Number.isFinite(x.size) && x.price > 0 && x.size > 0);
  out.sort((a, b) => side === "ask" ? a.price - b.price : b.price - a.price);
  return out;
}

async function getOrderBook(tokenId) {
  tokenId = String(tokenId || "");
  if (!tokenId) return null;
  const cached = cacheGet(orderBookCache, tokenId, ORDERBOOK_CACHE_TTL_MS);
  if (cached !== undefined) return cached;
  const attempts = [
    `https://clob.polymarket.com/book?token_id=${encodeURIComponent(tokenId)}`,
    `https://clob.polymarket.com/orderbook?token_id=${encodeURIComponent(tokenId)}`,
  ];
  for (const url of attempts) {
    try {
      const raw = await apiJson(url);
      const book = Array.isArray(raw) ? raw[0] : raw;
      const parsed = {
        tokenId,
        bids: parseBookSide(book?.bids || book?.buy || book?.orders?.bids, "bid"),
        asks: parseBookSide(book?.asks || book?.sell || book?.orders?.asks, "ask"),
        raw: book,
      };
      return cacheSet(orderBookCache, tokenId, parsed);
    } catch (err) {
      // Try the next endpoint shape.
    }
  }
  return null;
}

function simulateMarketFill(levels, side, targetUsd) {
  const usd = Number(targetUsd || 0);
  if (!Array.isArray(levels) || !levels.length || usd <= 0) {
    return { ok: false, avgPrice: null, shares: 0, cost: 0, reason: "盘口为空" };
  }
  let remainingUsd = usd;
  let shares = 0;
  let cost = 0;
  for (const level of levels) {
    const price = Number(level.price);
    const availableShares = Number(level.size);
    if (!Number.isFinite(price) || !Number.isFinite(availableShares) || price <= 0 || availableShares <= 0) continue;
    const levelUsd = availableShares * price;
    const takeUsd = Math.min(remainingUsd, levelUsd);
    const takeShares = takeUsd / price;
    shares += takeShares;
    cost += takeUsd;
    remainingUsd -= takeUsd;
    if (remainingUsd <= 1e-9) break;
  }
  return {
    ok: remainingUsd <= 1e-6,
    avgPrice: shares > 0 ? cost / shares : null,
    shares,
    cost,
    reason: remainingUsd > 1e-6 ? `盘口不足，仅可成交 ${moneyText(cost)}` : "ok",
  };
}

function depthNearTarget(levels, side, targetPrice, maxDiff) {
  if (!Array.isArray(levels) || !levels.length || !Number.isFinite(Number(targetPrice))) return 0;
  const p = Number(targetPrice);
  const diff = Number(maxDiff || 0);
  let usd = 0;
  for (const level of levels) {
    if (side === "BUY" && level.price <= p + diff) usd += level.price * level.size;
    if (side === "SELL" && level.price >= Math.max(0, p - diff)) usd += level.price * level.size;
  }
  return usd;
}

async function analyzeFollowability(signal, walletAnalysis) {
  const reasons = [];
  const tokenId = signal?.tokenId;
  const side = signal?.side;
  const targetPrice = Number(signal?.estimatedPrice);
  const score = Number(walletAnalysis?.score || 0);
  const notional = Number(signal?.estimatedNotional || 0);
  const isEntry = ["开仓", "加仓"].includes(signal?.positionChange) || (!ENABLE_CHAIN_BALANCE_CHECK && signal?.side === "BUY" && signal?.positionChange === "买入");
  const isTaker = signal?.role === "taker";

  if (QQ_PUSH_HIGH_QUALITY_ONLY && score < SIGNAL_MIN_WALLET_SCORE) reasons.push(`钱包评分 ${score} < ${SIGNAL_MIN_WALLET_SCORE}`);
  if (SIGNAL_ONLY_TAKER_OPEN_ADD && (!isTaker || !isEntry || side !== "BUY")) reasons.push(ENABLE_CHAIN_BALANCE_CHECK ? "不是主动开仓/加仓买入信号" : "不是主动买入信号，或链上余额校验关闭导致无法确认开/加仓");
  if (Number.isFinite(notional) && notional < SIGNAL_MIN_NOTIONAL_USD) reasons.push(`成交金额 ${moneyText(notional)} < ${moneyText(SIGNAL_MIN_NOTIONAL_USD)}`);

  const book = await getOrderBook(tokenId);
  if (!book) {
    reasons.push("无法读取盘口");
    return {
      pass: reasons.length === 0,
      verdict: reasons.length === 0 ? "可跟" : "不推",
      reasons,
      walletScore: score,
      liquidity: null,
    };
  }

  const levels = side === "SELL" ? book.bids : book.asks;
  const best = levels?.[0]?.price ?? null;
  const sampleFill = simulateMarketFill(levels, side, SIGNAL_FOLLOW_SAMPLE_USD);
  // Use avg fill price instead of best price — better reflects true execution cost
  // when the order is larger than the top-of-book level.
  const effectivePrice = (sampleFill.ok && Number.isFinite(sampleFill.avgPrice)) ? sampleFill.avgPrice : best;
  const priceDiff = Number.isFinite(effectivePrice) && Number.isFinite(targetPrice)
    ? (side === "SELL" ? targetPrice - effectivePrice : effectivePrice - targetPrice)
    : null;
  const depthUsd = depthNearTarget(levels, side, targetPrice, SIGNAL_MAX_PRICE_DIFF);
  const avgDiff = Number.isFinite(sampleFill.avgPrice) && Number.isFinite(targetPrice)
    ? (side === "SELL" ? targetPrice - sampleFill.avgPrice : sampleFill.avgPrice - targetPrice)
    : null;

  if (!Number.isFinite(best)) reasons.push("没有可成交盘口");
  if (Number.isFinite(priceDiff) && priceDiff > SIGNAL_MAX_PRICE_DIFF) reasons.push(`当前均价价差 ${(priceDiff * 100).toFixed(1)}¢ > ${(SIGNAL_MAX_PRICE_DIFF * 100).toFixed(1)}¢`);
  if (depthUsd < SIGNAL_MIN_DEPTH_USD) reasons.push(`目标价附近深度 ${moneyText(depthUsd)} < ${moneyText(SIGNAL_MIN_DEPTH_USD)}`);
  if (!sampleFill.ok) reasons.push(`样本单 ${moneyText(SIGNAL_FOLLOW_SAMPLE_USD)} 盘口不足`);

  const pass = reasons.length === 0;
  return {
    pass,
    verdict: pass ? "可跟" : (isEntry && isTaker ? "谨慎" : "不推"),
    reasons,
    walletScore: score,
    orderbook: {
      bestPrice: best,
      bestPriceText: priceText(best),
      effectivePrice,
      effectivePriceText: priceText(effectivePrice),
      targetPrice,
      targetPriceText: priceText(targetPrice),
      priceDiff,
      priceDiffText: Number.isFinite(priceDiff) ? `${(priceDiff * 100).toFixed(1)}¢` : "-",
      depthUsd,
      depthUsdText: moneyText(depthUsd),
      sampleUsd: SIGNAL_FOLLOW_SAMPLE_USD,
      sampleAvgPrice: sampleFill.avgPrice,
      sampleAvgPriceText: priceText(sampleFill.avgPrice),
      sampleShares: sampleFill.shares,
      sampleSharesText: sharesText(sampleFill.shares),
      sampleOk: sampleFill.ok,
      sampleReason: sampleFill.reason,
      avgDiff,
      avgDiffText: Number.isFinite(avgDiff) ? `${(avgDiff * 100).toFixed(1)}¢` : "-",
      bids: book.bids.slice(0, 10),
      asks: book.asks.slice(0, 10),
    },
  };
}

function isActiveBuyOrderSignal(event) {
  // 钱包视角：taker + BUY = 目标钱包正在主动买入 outcome token。
  return event?.type === "order" && event?.role === "taker" && event?.side === "BUY";
}

function isReduceOrCloseOrderSignal(event) {
  // 减仓/平仓也作为重要订单信号推送。链上余额校验关闭时，卖出只能低置信度标成“卖出”，也必须推送，避免错过退出。
  if (event?.type !== "order") return false;
  const pc = String(event?.positionChange || "");
  return ["减仓", "平仓", "卖出"].includes(pc) || event?.positionChangeGroup === "exit" || event?.side === "SELL";
}
function isExitWatchSignal(event) {
  return ["activity_exit", "position_exit", "position_closed"].includes(String(event?.type || "")) || event?.positionChangeGroup === "exit";
}

function shouldPushSignal(event) {
  const isTrackedExit = !TRACKED_EXIT_ONLY || !isExitSignalForProjectTracking(event) || (event?.type === "exit_batch" && Array.isArray(event.items) && event.items.length) || !!findTrackedProjectForSignal(event);
  if (QQ_PUSH_ACTIVE_BUY_EXIT_ONLY) {
    if (isActiveBuyOrderSignal(event)) return true;
    if ((isReduceOrCloseOrderSignal(event) || isExitWatchSignal(event)) && isTrackedExit) return true;
    return false;
  }
  if (!QQ_PUSH_HIGH_QUALITY_ONLY) return isTrackedExit;
  return event?.followCheck?.pass === true || (isExitWatchSignal(event) && isTrackedExit);
}

function qqSkipReason(event) {
  if (TRACKED_EXIT_ONLY && isExitSignalForProjectTracking(event) && !findTrackedProjectForSignal(event)) return "退出信号未命中已选择项目";
  if (QQ_PUSH_ACTIVE_BUY_EXIT_ONLY) {
    if (isActiveBuyOrderSignal(event)) return "";
    if (isReduceOrCloseOrderSignal(event)) return "";
    if (isExitWatchSignal(event)) return "";
    if (event?.type !== "order") return "不是主动买入/退出提醒";
    return "不是主动买入，也不是退出/卖出信号";
  }
  return event?.followReasonsText || "未通过过滤";
}

function activeText(role) { return role === "taker" ? "主动" : role === "maker" ? "被动" : "相关"; }
function inferWalletSideFromOrder(order) {
  if (!order) return "BUY";
  // Polymarket 链上 OrderFilled 的 side 字段是 maker 视角（maker 的买卖方向）。
  // taker 方向与 maker 相反：maker BUY → taker SELL（taker 卖出 outcome token 给 maker）。
  // 此函数返回的是 "钱包自身" 的买卖方向，所以 taker 时需要反转。
  // 注意：这里只作为 fallback，主路径由 inferWalletSideFromFlows 通过实际净流量判断。
  if (order.role === "taker") return order.side === "SELL" ? "BUY" : "SELL";
  if (order.role === "maker") return order.side;
  return order.side;
}
function roleTextFromOrders(orderEvents) {
  if (orderEvents.some(o => o.role === "taker")) return "taker";
  if (orderEvents.some(o => o.role === "maker")) return "maker";
  return "related";
}
function estimateOrderNumbers(event) {
  const maker = amount6ToNumber(event.makerFilled);
  const taker = amount6ToNumber(event.takerFilled);
  let size = null;
  let notional = null;
  // OrderFilled amounts are maker-asset and taker-asset amounts. For V2 orders:
  // BUY order: maker asset is collateral, taker asset is outcome token.
  // SELL order: maker asset is outcome token, taker asset is collateral.
  // The tx-level net pUSD/token flow remains the source of truth; this is only a fallback.
  if (Number.isFinite(maker) && Number.isFinite(taker) && maker > 0 && taker > 0) {
    if (event.side === "BUY") { notional = maker; size = taker; }
    else if (event.side === "SELL") { notional = taker; size = maker; }
    else { notional = Math.min(maker, taker); size = Math.max(maker, taker); }
  }
  const price = size && size > 0 && notional && notional > 0 ? notional / size : null;
  const ratio = maker && taker ? maker / taker : null;
  return { maker, taker, ratio, price, size, notional };
}
function addFlow(map, tokenId, delta) {
  if (!tokenId || !Number.isFinite(delta)) return;
  const key = String(tokenId);
  const cur = map.get(key) || { tokenId: key, net: 0, in: 0, out: 0 };
  cur.net += delta;
  if (delta >= 0) cur.in += delta;
  else cur.out += Math.abs(delta);
  map.set(key, cur);
}
function collectTokenFlows(events, wallet) {
  const flows = new Map();
  for (const ev of events) {
    if (ev.type !== "erc1155") continue;
    if (ev.tokens && Array.isArray(ev.tokens)) {
      for (const t of ev.tokens) {
        const qty = amount6ToNumber(t.valueRaw || t.value);
        const delta = ev.to === wallet ? qty : ev.from === wallet ? -qty : 0;
        addFlow(flows, t.id, delta);
      }
    } else if (ev.tokenId) {
      const qty = amount6ToNumber(ev.valueRaw || ev.value);
      const delta = ev.to === wallet ? qty : ev.from === wallet ? -qty : 0;
      addFlow(flows, ev.tokenId, delta);
    }
  }
  return flows;
}
function collectNetPusd(events, wallet) {
  let net = 0;
  for (const ev of events) {
    if (ev.type !== "erc20") continue;
    const amount = amount6ToNumber(ev.amountRaw || ev.amount);
    if (ev.from === wallet) net -= amount;
    if (ev.to === wallet) net += amount;
  }
  return net;
}
function chooseTokenId(orderEvents, tokenFlows) {
  const orderIds = [...new Set(orderEvents.map(o => String(o.tokenId || "")).filter(Boolean))];
  let best = null;
  for (const id of orderIds) {
    const flow = tokenFlows.get(id);
    const score = flow ? Math.abs(flow.net) + 1 : 0.1;
    if (!best || score > best.score) best = { id, score };
  }
  for (const [id, flow] of tokenFlows.entries()) {
    const score = Math.abs(flow.net);
    if (!best || score > best.score) best = { id, score };
  }
  return best?.id || orderIds[0] || null;
}
function choosePrimaryOrder(orderEvents, tokenId) {
  return orderEvents.find(o => o.tokenId === tokenId && o.role === "taker")
    || orderEvents.find(o => o.tokenId === tokenId)
    || orderEvents.find(o => o.role === "taker")
    || orderEvents[0];
}
function inferWalletSideFromFlows(netPusd, tokenNet, fallbackOrder) {
  const eps = 1e-9;
  if (tokenNet > eps && netPusd < -eps) return "BUY";
  if (tokenNet < -eps && netPusd > eps) return "SELL";
  if (tokenNet > eps) return "BUY";
  if (tokenNet < -eps) return "SELL";
  return inferWalletSideFromOrder(fallbackOrder);
}
async function inferPositionChange(wallet, tokenId, side, size) {
  const post = await getErc1155Balance(wallet, tokenId);
  if (!Number.isFinite(post) || !Number.isFinite(size)) {
    const positionChange = side === "BUY" ? "买入" : "卖出";
    return {
      positionChange,
      positionChangeGroup: side === "BUY" ? "entry" : "exit",
      positionChangeConfidence: "low",
      preBalance: null,
      postBalance: post,
      detail: ENABLE_CHAIN_BALANCE_CHECK ? "链上余额校验失败，无法判断开仓/平仓" : "未启用链上余额校验，仅判断买入/卖出",
    };
  }
  const eps = 0.01;
  let pre;
  let positionChange;
  if (side === "BUY") {
    pre = Math.max(0, post - size);
    positionChange = pre <= eps ? "开仓" : "加仓";
  } else {
    pre = post + size;
    positionChange = post <= eps ? "平仓" : "减仓";
  }
  return {
    positionChange,
    positionChangeGroup: side === "BUY" ? "entry" : "exit",
    positionChangeConfidence: "high",
    preBalance: pre,
    postBalance: post,
    detail: `${sharesText(pre)} → ${sharesText(post)}`,
  };
}
async function buildAggregatedFollowSignal(wallet, events) {
  const orderEvents = events.filter(ev => ev.type === "order");
  if (!orderEvents.length) return null;

  const tokenFlows = collectTokenFlows(events, wallet);
  const tokenId = chooseTokenId(orderEvents, tokenFlows);
  const primary = choosePrimaryOrder(orderEvents, tokenId);
  const role = roleTextFromOrders(orderEvents);
  const netPusd = collectNetPusd(events, wallet);
  const tokenFlow = tokenId ? tokenFlows.get(String(tokenId)) : null;
  const tokenNet = tokenFlow ? tokenFlow.net : 0;
  const side = inferWalletSideFromFlows(netPusd, tokenNet, primary);
  const marketInfo = await getMarketByToken(tokenId);
  const fallbackNums = primary ? estimateOrderNumbers(primary) : {};
  const size = Math.abs(tokenNet) > 0 ? Math.abs(tokenNet) : fallbackNums.size;
  const notional = Math.abs(netPusd) > 0 ? Math.abs(netPusd) : fallbackNums.notional;
  const price = (Number.isFinite(size) && size > 0 && Number.isFinite(notional) && notional > 0) ? notional / size : fallbackNums.price;
  const pos = await inferPositionChange(wallet, tokenId, side, size);

  const outcome = marketInfo?.outcome || "Outcome Token";
  const verb = side === "SELL" ? "卖出" : "买入";
  const action = `${activeText(role)}${pos.positionChange}${verb}`;
  const txHash = primary?.txHash || events[0]?.txHash;
  const versions = [...new Set(orderEvents.map(o => o.version).filter(Boolean))].join("+") || primary?.version || "-";
  const block = Math.max(...events.map(e => Number(e.block || 0)).filter(Boolean));
  const priority = role === "taker" ? "高：目标钱包主动吃单" : role === "maker" ? "低：目标钱包挂单被动成交" : "中：相关成交";
  const walletAnalysis = await analyzeWalletCached(wallet);
  const followCheck = await analyzeFollowability({ wallet, side, role, positionChange: pos.positionChange, tokenId, estimatedPrice: price, estimatedNotional: notional }, walletAnalysis);

  return {
    type: "order",
    label: "跟单信号",
    isAggregated: true,
    wallet,
    walletName: walletLabel(wallet),
    side,
    role,
    action,
    positionChange: pos.positionChange,
    positionChangeGroup: pos.positionChangeGroup,
    positionChangeConfidence: pos.positionChangeConfidence,
    preBalance: pos.preBalance,
    postBalance: pos.postBalance,
    positionChangeDetail: pos.detail,
    outcome,
    version: versions,
    tokenId,
    contract: primary?.contract,
    marketQuestion: marketInfo?.marketQuestion || "未知市场",
    marketUrl: marketInfo?.marketUrl || null,
    conditionId: marketInfo?.conditionId || null,
    estimatedPrice: price,
    estimatedPriceText: priceText(price),
    estimatedSize: size,
    estimatedSizeText: sharesText(size),
    estimatedNotional: notional,
    estimatedNotionalText: moneyText(notional),
    netPusd,
    netPusdText: moneyText(Math.abs(netPusd)),
    tokenNet,
    tokenNetText: sharesText(Math.abs(tokenNet)),
    orderCount: orderEvents.length,
    rawOrderSides: orderEvents.map(o => `${o.role}:${o.side}`).join(", "),
    followPriority: priority,
    signalTitle: `🚨 ${action} ${outcome}`,
    signalSummary: `${walletLabel(wallet)} · ${action} ${outcome} @ ${priceText(price)} — ${marketInfo?.marketQuestion || "未知市场"}`,
    openUrl: marketInfo?.marketUrl || txUrl(txHash),
    txHash,
    txUrl: txUrl(txHash),
    block,
    walletScore: walletAnalysis?.score || 0,
    walletScoreText: `${walletAnalysis?.score || 0}/100`,
    followCheck,
    followVerdict: followCheck?.verdict || "未知",
    followPass: followCheck?.pass === true,
    currentFollowPrice: followCheck?.orderbook?.effectivePrice ?? followCheck?.orderbook?.bestPrice ?? null,
    currentFollowPriceText: followCheck?.orderbook?.effectivePriceText || followCheck?.orderbook?.bestPriceText || "-",
    followPriceDiffText: followCheck?.orderbook?.priceDiffText || "-",
    liquidityDepthText: followCheck?.orderbook?.depthUsdText || "-",
    sampleFillText: followCheck?.orderbook ? `${moneyText(followCheck.orderbook.sampleUsd)} → ${followCheck.orderbook.sampleSharesText} @ ${followCheck.orderbook.sampleAvgPriceText}` : "-",
    followReasonsText: followCheck?.reasons?.length ? followCheck.reasons.join("；") : "通过",
    orders: orderEvents.map(o => ({ role: o.role, side: o.side, version: o.version, tokenId: o.tokenId, makerFilled: o.makerFilled, takerFilled: o.takerFilled })),
  };
}
function formatSignalText(event) {
  if (event?.type === "exit_batch") {
    const w = event.walletName || shortAddr(event.wallet);
    const cnt = event.items?.length || 0;
    const extra = event.foldedCount ? ` +${event.foldedCount}折叠` : "";
    const lines = [
      `🟠 退出汇总  ${w}  ×${cnt}${extra}`,
      `──────────────`,
      ...(event.items || []).map((x, i) =>
        `${i + 1}. ${x.outcome || ""} · ${x.marketQuestion || "未知市场"} · ${x.estimatedNotionalText || x.positionChange || "-"}`
      ),
    ];
    if (event.estimatedNotionalText) lines.push(``, `合计 ${event.estimatedNotionalText}`);
    return lines.join("\n");
  }
  if (event && ["activity_exit", "position_exit", "position_closed"].includes(String(event.type || ""))) {
    const w = event.walletName || shortAddr(event.wallet);
    const valueParts = [event.estimatedNotionalText, event.estimatedPriceText, event.estimatedSizeText].filter(Boolean);
    return [
      `🟠 退出提醒  ${w}`,
      `──────────────`,
      `${event.positionChange || event.action || "退出"} · ${event.outcome || ""}`,
      event.marketQuestion || "未知市场",
      ``,
      valueParts.length ? valueParts.join(" · ") : null,
      event.positionChangeDetail || event.followReasonsText || null,
      ``,
      event.openUrl || event.txUrl ? (event.openUrl || event.txUrl) : null,
    ].filter(x => x !== null && x !== undefined).join("\n");
  }
  if (!event || event.type !== "order") {
    return JSON.stringify(event, null, 2);
  }
  const w = event.walletName || shortAddr(event.wallet);
  const valueParts = [event.estimatedNotionalText, event.estimatedPriceText, event.estimatedSizeText].filter(Boolean);
  const metaParts = [
    event.walletScoreText ? `评分 ${event.walletScoreText}` : null,
    event.followVerdict || null,
    event.liquidityDepthText ? `深度 ${event.liquidityDepthText}` : null,
  ].filter(Boolean);
  const priceParts = [
    event.currentFollowPriceText ? `跟价 ${event.currentFollowPriceText}` : null,
    event.followPriceDiffText ? `价差 ${event.followPriceDiffText}` : null,
    event.sampleFillText ? `样本 ${event.sampleFillText}` : null,
  ].filter(Boolean);
  return [
    `🚨 跟单信号  ${w}`,
    `──────────────`,
    `${event.action || "买入"} · ${event.outcome || ""}`,
    event.marketQuestion || "未知市场",
    ``,
    valueParts.length ? valueParts.join(" · ") : null,
    metaParts.length ? metaParts.join(" · ") : null,
    priceParts.length ? priceParts.join(" · ") : null,
    event.followReasonsText || null,
    ``,
    event.openUrl || event.txUrl ? (event.openUrl || event.txUrl) : null,
  ].filter(x => x !== null && x !== undefined).join("\n");
}

// ─── QQ Bot ────────────────────────────────────────────────────────────────
async function getQQAccessToken() {
  if (qqAccessToken && Date.now() < qqTokenExpireAt - 60_000) return qqAccessToken;
  const res = await fetchText(QQ_TOKEN_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ appId: QQ_APP_ID, clientSecret: QQ_CLIENT_SECRET }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) throw new Error(`获取 QQ access_token 失败: HTTP ${res.status} ${JSON.stringify(data)}`);
  qqAccessToken = data.access_token;
  qqTokenExpireAt = Date.now() + Number(data.expires_in || 7200) * 1000;
  return qqAccessToken;
}
function qqHeaders(token) {
  return {
    "content-type": "application/json",
    authorization: `QQBot ${token}`,
    "x-union-appid": QQ_APP_ID,
  };
}
async function getQQGatewayUrl(token) {
  const data = await apiJson(`${QQ_API_BASE}/gateway`, { headers: { authorization: `QQBot ${token}` } });
  if (!data.url) throw new Error("QQ Gateway URL 为空");
  return data.url;
}
async function sendQQNow(text) {
  if (!qqConfigured()) return;
  const token = await getQQAccessToken();
  let url;
  const bodies = [];

  if (QQBOT_TARGET_TYPE === "c2c") {
    url = `${QQ_API_BASE}/v2/users/${QQBOT_TARGET_ID}/messages`;
    // First try true proactive C2C push. If QQ rejects it, fall back to the latest reply context.
    bodies.push({ msg_type: 0, content: text, msg_seq: qqMsgSeq++ });
    if (lastC2CMsgId && lastC2COpenId === QQBOT_TARGET_ID) {
      bodies.push({ msg_type: 0, content: text, msg_id: lastC2CMsgId, msg_seq: qqMsgSeq++ });
    }
  } else if (QQBOT_TARGET_TYPE === "group") {
    url = `${QQ_API_BASE}/v2/groups/${QQBOT_TARGET_ID}/messages`;
    bodies.push({ msg_type: 0, content: text, msg_seq: qqMsgSeq++ });
  } else if (QQBOT_TARGET_TYPE === "guild") {
    url = `${QQ_API_BASE}/channels/${QQBOT_TARGET_ID}/messages`;
    bodies.push({ content: text });
  } else {
    throw new Error(`未知 QQBOT_TARGET_TYPE: ${QQBOT_TARGET_TYPE}`);
  }

  let lastError = "";
  for (const body of bodies) {
    const res = await fetchText(url, { method: "POST", headers: qqHeaders(token), body: JSON.stringify(body) });
    const result = await res.text();
    if (res.ok) return result;
    lastError = `HTTP ${res.status} ${result}`;
  }
  throw new Error(lastError || "QQ 推送失败");
}
function enqueueQQRetry(text, attempt = 1, reason = "") {
  if (!qqConfigured() || attempt > QQ_RETRY_MAX) {
    if (reason) console.error(`[Hermes QQ Bot 推送失败] ${reason}`);
    return;
  }
  // 用闭包捕获 retryItem，而不是用 shift() 取队首，避免多条重试并发时互相错位
  const retryItem = { text, attempt, reason, queuedAt: Date.now() };
  qqRetryQueue.push(retryItem);
  setTimeout(async () => {
    const idx = qqRetryQueue.indexOf(retryItem);
    if (idx < 0) return; // 已被其他逻辑取走
    qqRetryQueue.splice(idx, 1);
    try {
      await sendQQNow(retryItem.text);
      console.log(`[Hermes QQ Bot] 重试成功 attempt=${retryItem.attempt}`);
    } catch (err) {
      console.warn(`[Hermes QQ Bot] 重试失败 attempt=${retryItem.attempt}: ${friendlyError(err)}`);
      enqueueQQRetry(retryItem.text, retryItem.attempt + 1, friendlyError(err));
    }
  }, QQ_RETRY_DELAY_MS * attempt).unref?.();
}
async function sendHermesQQBot(text) {
  if (!qqConfigured()) return;
  try {
    await sendQQNow(text);
    console.log("[Hermes QQ Bot] 推送成功");
  } catch (err) {
    const msg = friendlyError(err);
    console.error(`[Hermes QQ Bot 推送失败，已入队重试] ${msg}`);
    enqueueQQRetry(text, 1, msg);
  }
}
async function connectQQGateway() {
  if (!qqConfigured()) return;
  if (qqReconnectTimer) { clearTimeout(qqReconnectTimer); qqReconnectTimer = null; }
  try {
    const token = await getQQAccessToken();
    const url = await getQQGatewayUrl(token);
    console.log("[Hermes QQ Bot] 连接 QQ Gateway...");
    qqWs = new WebSocket(url, { agent });

    qqWs.on("message", raw => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      if (typeof msg.s === "number") qqSeq = msg.s;
      if (msg.op === 10) {
        const interval = msg.d?.heartbeat_interval || 41250;
        qqWs.send(JSON.stringify({ op: 2, d: { token: `QQBot ${token}`, intents: QQ_GATEWAY_INTENTS, shard: [0, 1], properties: { $os: "windows", $browser: "polymarket-monitor", $device: "polymarket-monitor" } } }));
        if (qqHeartbeatTimer) clearInterval(qqHeartbeatTimer);
        qqHeartbeatTimer = setInterval(() => {
          if (qqWs?.readyState === WebSocket.OPEN) qqWs.send(JSON.stringify({ op: 1, d: qqSeq }));
        }, interval);
      }
      if (msg.op === 0) {
        if (msg.t === "READY") console.log("[Hermes QQ Bot] Gateway READY");
        if (msg.t === "C2C_MESSAGE_CREATE") {
          if (msg.d?.author?.bot) return;
          lastC2COpenId = msg.d?.author?.user_openid || msg.d?.author?.id || "";
          lastC2CMsgId = msg.d?.id || "";
          const content = String(msg.d?.content || "").trim();
          console.log(`[Hermes QQ Bot] 收到私聊，已记录回复上下文 ${shortValue(lastC2COpenId)}${content ? ` 内容=${content.slice(0, 60)}` : ""}`);
          if (content) {
            handleQQTradeCommand(content, lastC2COpenId).catch(err => console.error("[QQ命令处理异常]", err.message));
          }
        }
      }
    });
    qqWs.on("error", err => console.error("[Hermes QQ Bot] Gateway 错误:", err.message));
    qqWs.on("close", () => {
      if (qqHeartbeatTimer) clearInterval(qqHeartbeatTimer);
      qqReconnectTimer = setTimeout(connectQQGateway, 5000);
    });
  } catch (err) {
    console.error("[Hermes QQ Bot] Gateway 连接失败:", err.message);
    qqReconnectTimer = setTimeout(connectQQGateway, 10000);
  }
}

// ─── Parse chain events ────────────────────────────────────────────────────
function parseRawLog(log) {
  const topic0 = log.topics[0].toLowerCase();

  if (topic0 === TOPICS.ORDER_FILLED_V2.toLowerCase()) {
    const d = iface.decodeEventLog("OrderFilled", log.data, log.topics);
    const maker = d.maker.toLowerCase();
    const taker = d.taker.toLowerCase();
    const walletsInEvent = watchedWalletsFromAddresses([maker, taker]);
    if (!walletsInEvent.length) return [];
    const makerAmountFilled = d.makerAmountFilled.toString();
    const takerAmountFilled = d.takerAmountFilled.toString();
    const side = Number(d.side) === 0 ? "BUY" : "SELL";
    return walletsInEvent.map(wallet => ({
      type: "order", version: "V2", label: "OrderFilled V2", wallet, walletName: walletLabel(wallet),
      role: maker === wallet ? "maker" : taker === wallet ? "taker" : "related",
      side, contract: contractName(log.address), tokenId: d.tokenId.toString(), maker: d.maker, taker: d.taker,
      makerFilled: makerAmountFilled, takerFilled: takerAmountFilled, fee: d.fee.toString(), orderHash: d.orderHash,
      block: Number(log.blockNumber), txHash: log.transactionHash, txUrl: txUrl(log.transactionHash),
    }));
  }

  if (topic0 === TOPICS.ORDER_FILLED_V1.toLowerCase()) {
    // V1 data layout after indexed orderHash/maker/taker:
    // [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]
    const decoded = ethers.AbiCoder.defaultAbiCoder().decode(["uint256", "uint256", "uint256", "uint256", "uint256"], log.data);
    if (decoded.length < 5) throw new Error("V1 OrderFilled data 解码字段不足");
    const maker = topicToAddress(log.topics[2]);
    const taker = topicToAddress(log.topics[3]);
    const walletsInEvent = watchedWalletsFromAddresses([maker, taker]);
    if (!walletsInEvent.length) return [];
    const makerAssetId = decoded[0];
    const takerAssetId = decoded[1];
    const side = makerAssetId === 0n ? "BUY" : "SELL";
    const tokenId = makerAssetId === 0n ? takerAssetId.toString() : makerAssetId.toString();
    return walletsInEvent.map(wallet => ({
      type: "order", version: "V1", label: "OrderFilled V1", wallet, walletName: walletLabel(wallet),
      role: maker === wallet ? "maker" : taker === wallet ? "taker" : "related",
      side, contract: contractName(log.address), tokenId, maker, taker,
      makerFilled: decoded[2].toString(), takerFilled: decoded[3].toString(), fee: decoded[4].toString(), orderHash: log.topics[1],
      block: Number(log.blockNumber), txHash: log.transactionHash, txUrl: txUrl(log.transactionHash),
    }));
  }

  if (topic0 === TOPICS.ERC20_TRANSFER.toLowerCase()) {
    const from = topicToAddress(log.topics[1]);
    const to = topicToAddress(log.topics[2]);
    const walletsInEvent = watchedWalletsFromAddresses([from, to]);
    if (!walletsInEvent.length) return [];
    const value = ethers.AbiCoder.defaultAbiCoder().decode(["uint256"], log.data)[0];
    return walletsInEvent.map(wallet => ({
      type: "erc20", label: "pUSD 转账", wallet, walletName: walletLabel(wallet), dir: from === wallet ? "out" : "in",
      contract: contractName(log.address), from, to, amount: ethers.formatUnits(value, 6), amountRaw: value.toString(),
      block: Number(log.blockNumber), txHash: log.transactionHash, txUrl: txUrl(log.transactionHash),
    }));
  }

  if (topic0 === TOPICS.ERC1155_TRANSFER_SINGLE.toLowerCase()) {
    const d = iface.decodeEventLog("TransferSingle", log.data, log.topics);
    const from = d.from.toLowerCase();
    const to = d.to.toLowerCase();
    const walletsInEvent = watchedWalletsFromAddresses([from, to]);
    if (!walletsInEvent.length) return [];
    return walletsInEvent.map(wallet => ({
      type: "erc1155", label: "TransferSingle", wallet, walletName: walletLabel(wallet), dir: from === wallet ? "out" : "in",
      contract: contractName(log.address), operator: d.operator, from, to, tokenId: d.id.toString(), value: d.value.toString(), valueRaw: d.value.toString(),
      block: Number(log.blockNumber), txHash: log.transactionHash, txUrl: txUrl(log.transactionHash),
    }));
  }

  if (topic0 === TOPICS.ERC1155_TRANSFER_BATCH.toLowerCase()) {
    const d = iface.decodeEventLog("TransferBatch", log.data, log.topics);
    const from = d.from.toLowerCase();
    const to = d.to.toLowerCase();
    const walletsInEvent = watchedWalletsFromAddresses([from, to]);
    if (!walletsInEvent.length) return [];
    const tokens = d.ids.map((id, i) => ({ id: id.toString(), value: d.values[i].toString(), valueRaw: d.values[i].toString() }));
    return walletsInEvent.map(wallet => ({
      type: "erc1155", label: "TransferBatch", wallet, walletName: walletLabel(wallet), dir: from === wallet ? "out" : "in",
      contract: contractName(log.address), operator: d.operator, from, to, tokens,
      block: Number(log.blockNumber), txHash: log.transactionHash, txUrl: txUrl(log.transactionHash),
    }));
  }

  return [];
}
async function maybeAutoTradeEvent(event) {
  if (!tradeExecutor || !event) return;
  const autoCfg = getAutoTradeWalletConfig(event.wallet);
  if (!autoCfg) return;
  if (event.type !== "order" || event.role !== "taker" || event.side !== "BUY") return;
  if (event.followPass !== true) {
    console.log(`[自动跟单跳过] ${walletLabel(event.wallet)} 信号未通过可跟性过滤`);
    return;
  }

  const targetPrice = Number(event.estimatedPrice);
  const currentPrice = Number(event.currentFollowPrice ?? event.estimatedPrice);
  const priceDiff = Number.isFinite(targetPrice) && Number.isFinite(currentPrice) ? currentPrice - targetPrice : null;
  const maxPriceDiff = Number(autoCfg.maxPriceDiff ?? AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF);
  if (Number.isFinite(priceDiff) && Number.isFinite(maxPriceDiff) && priceDiff > maxPriceDiff) {
    const payload = {
      type: "tradeResult",
      signalId: event.id || "",
      wallet: event.wallet || "",
      marketQuestion: event.marketQuestion || "",
      outcome: event.outcome || "",
      result: { ok: false, skipped: true, reason: `价差 ${(priceDiff * 100).toFixed(1)}¢ > ${(maxPriceDiff * 100).toFixed(1)}¢`, mode: autoCfg.mode },
      time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    };
    console.log(`[自动跟单跳过] ${payload.result.reason}`);
    broadcast(payload);
    return;
  }

  if (AUTO_TRADE_ACCOUNT_GUARD_ENABLED) {
    try {
      const plannedAmountUsd = Number(autoCfg.amountUsd ?? AUTO_TRADE_DEFAULT_AMOUNT_USD);
      const status = await getTradingAccountStatus({ noCache: false });
      const sameTokenReason = sameTokenHoldingSkipReason(status, event);
      const guardReason = sameTokenReason || accountGuardSkipReason(status, plannedAmountUsd);
      if (guardReason) {
        const payload = {
          type: "tradeResult",
          signalId: event.id || "",
          wallet: event.wallet || "",
          marketQuestion: event.marketQuestion || "",
          outcome: event.outcome || "",
          result: { ok: false, skipped: true, reason: `账户风控拦截：${guardReason}`, mode: autoCfg.mode, accountGuard: true },
          time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        };
        console.log(`[自动跟单跳过] ${payload.result.reason}`);
        broadcast(payload);
        return;
      }
    } catch (guardErr) {
      const msg = friendlyError(guardErr);
      console.warn(`[账户风控检查失败] ${msg}`);
      if (AUTO_TRADE_BLOCK_ON_ACCOUNT_STATUS_ERROR) {
        broadcast({
          type: "tradeResult",
          signalId: event.id || "",
          wallet: event.wallet || "",
          marketQuestion: event.marketQuestion || "",
          outcome: event.outcome || "",
          result: { ok: false, skipped: true, reason: `账户资金状态查询失败，已按配置跳过：${msg}`, mode: autoCfg.mode, accountGuard: true },
          time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        });
        return;
      }
    }
  }

  const cooldownKey = `${event.wallet}:${event.tokenId || ""}`;
  const nowTs = Date.now();
  const lastTs = autoTradeWalletCooldown.get(cooldownKey) || 0;
  if (nowTs - lastTs < AUTO_TRADE_MIN_SECONDS_BETWEEN_WALLET_TRADES * 1000) {
    console.log(`[自动跟单跳过] ${walletLabel(event.wallet)} ${shortValue(event.tokenId)} 冷却中`);
    return;
  }
  autoTradeWalletCooldown.set(cooldownKey, nowTs);

  try {
    const mode = autoCfg.mode && autoCfg.mode !== "global" ? autoCfg.mode : AUTO_TRADE_DEFAULT_MODE;
    const result = await tradeExecutor.maybeAutoTradeSignal({
      ...event,
      autoTradeWalletEnabled: true,
      autoTradeAmountUsd: autoCfg.amountUsd,
      autoTradeMaxPriceDiff: maxPriceDiff,
      autoTradeMode: mode,
      autoTradeOrderType: "FAK",
    });
    if (!result) return;
    const payload = {
      type: "tradeResult",
      signalId: event.id || "",
      wallet: event.wallet || "",
      marketQuestion: event.marketQuestion || "",
      outcome: event.outcome || "",
      result,
      time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    };
    console.log(`[自动跟单] ${result.mode || "unknown"} ${result.message || result.reason || ""}`);
    broadcast(payload);
    if (AUTO_TRACK_PROJECT_AFTER_BUY && result.ok && !result.skipped && event.tokenId) {
      try {
        upsertTrackedProject({
          wallet: event.wallet,
          walletName: walletLabel(event.wallet),
          tokenId: event.tokenId,
          conditionId: event.conditionId || "",
          marketQuestion: event.marketQuestion || "未知市场",
          outcome: event.outcome || "-",
          marketUrl: event.marketUrl || event.openUrl || "",
          source: "auto_trade",
          note: `自动跟单 ${result.mode || mode} 后自动加入退出监控`,
          enabled: true,
        });
      } catch (trackErr) {
        console.warn(`[自动加入退出监控失败] ${trackErr.message}`);
      }
    }
    sendHermesQQBot([
      result.skipped ? "🤖 跟单跳过" : result.ok ? "🤖 跟单完成" : "🤖 跟单未执行",
      `${event.outcome || ""} · ${event.marketQuestion || "-"}`,
      `──────────────`,
      `限价 ${event.currentFollowPriceText || event.estimatedPriceText || "-"}  ·  金额 ${result.plan?.amountUsd ? `$${result.plan.amountUsd}` : `$${autoCfg.amountUsd}`}`,
      Number.isFinite(priceDiff) ? `价差 ${(priceDiff * 100).toFixed(1)}¢  /  上限 ${(maxPriceDiff * 100).toFixed(1)}¢` : null,
      result.message || result.reason || null,
    ].filter(Boolean).join("\n")).catch(err => console.error("[自动跟单QQ推送异常]", err.message));
    pollAccountStatusOnce({ push: false }).catch(() => {});
  } catch (err) {
    const error = friendlyError(err);
    console.warn(`[自动跟单跳过/失败] ${error}`);
    broadcast({
      type: "tradeResult",
      signalId: event.id || "",
      wallet: event.wallet || "",
      marketQuestion: event.marketQuestion || "",
      outcome: event.outcome || "",
      error,
      time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    });
    sendHermesQQBot(["⚠️ 跟单失败", walletLabel(event.wallet), `${event.marketQuestion || "-"}`, `错误：${error}`].join("\n")).catch(() => {});
  }
}

async function maybeAutoExitEvent(event) {
  if (!tradeExecutor || !event) return;
  if (!isExitWatchSignal(event) && !isReduceOrCloseOrderSignal(event)) return;
  if (event.type === "exit_batch") return;
  // Auto-exit is project-driven, not wallet-config-driven.
  // Require a matched tracked project; wallet config affects mode only.
  const matched = findTrackedProjectForSignal(event);
  if (!matched) {
    console.log(`[自动平仓跳过] ${walletLabel(event.wallet)} 未命中已选择项目`);
    return;
  }
  const autoCfg = getAutoTradeWalletConfig(event.wallet);

  try {
    const mode = (autoCfg?.mode && autoCfg.mode !== "global") ? autoCfg.mode : AUTO_TRADE_DEFAULT_MODE;
    const result = await tradeExecutor.maybeAutoExitSignal({
      ...event,
      autoTradeMode: mode,
      autoExitMode: mode,
      autoExitOrderType: "FAK",
      trackedProjectId: matched.id,
      trackedProjectName: matched.marketQuestion,
    });
    if (!result) return;
    const payload = {
      type: "tradeResult",
      signalId: event.id || "",
      wallet: event.wallet || "",
      marketQuestion: event.marketQuestion || "",
      outcome: event.outcome || "",
      result,
      time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    };
    console.log(`[自动平仓] ${result.mode || "unknown"} ${result.message || result.reason || ""}`);
    broadcast(payload);
    sendHermesQQBot([
      result.skipped ? "🤖 平仓跳过" : result.ok ? "🤖 平仓完成" : "🤖 平仓未执行",
      `${event.outcome || ""} · ${event.marketQuestion || "-"}`,
      `──────────────`,
      `份额 ${result.ownShares ? Number(result.ownShares).toFixed(4) : result.plan?.shares || "-"}`,
      result.bestBid ? `出价 ${(result.bestBid * 100).toFixed(1)}¢  ·  限价 ${result.limitPrice ? `${(result.limitPrice * 100).toFixed(1)}¢` : "-"}` : null,
      result.message || result.reason || null,
    ].filter(Boolean).join("\n")).catch(err => console.error("[自动平仓QQ推送异常]", err.message));
    pollAccountStatusOnce({ push: false }).catch(() => {});
  } catch (err) {
    const error = friendlyError(err);
    console.warn(`[自动平仓失败] ${error}`);
    broadcast({
      type: "tradeResult",
      signalId: event.id || "",
      wallet: event.wallet || "",
      marketQuestion: event.marketQuestion || "",
      outcome: event.outcome || "",
      error,
      autoExit: true,
      time: new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
    });
    sendHermesQQBot(["⚠️ 平仓失败", walletLabel(event.wallet), `${event.marketQuestion || "-"}`, `错误：${error}`].join("\n")).catch(() => {});
  }
}

async function emitUiEvent(event) {
  if (!event) return;
  if (!shouldEmitEventByTracking(event)) return;
  event.id = `${event.wallet || "wallet"}-${event.txHash || Date.now()}-${Math.random()}`;
  event.time = new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const text = formatSignalText(event);
  console.log("\n" + "=".repeat(90));
  console.log(text);
  console.log("=".repeat(90));
  broadcast({ type: "event", event });
  if (shouldPushSignal(event)) {
    sendHermesQQBot(text).catch(err => console.error("[QQ 推送异常]", err.message));
  } else {
    console.log(`[QQ 推送跳过] ${qqSkipReason(event)}`);
  }
  maybeAutoTradeEvent(event).catch(err => console.error("[自动跟单异常]", err.message));
  maybeAutoExitEvent(event).catch(err => console.error("[自动平仓异常]", err.message));
}
function queueWalletTxEvent(event) {
  const key = `${event.wallet}:${event.txHash}`;
  let buf = txBuffers.get(key);
  if (!buf) {
    buf = { wallet: event.wallet, txHash: event.txHash, events: [], timer: null };
    txBuffers.set(key, buf);
  }
  buf.events.push(event);
  if (buf.timer) clearTimeout(buf.timer);
  buf.timer = setTimeout(() => flushTx(key).catch(err => console.error("[聚合交易失败]", err.message)), TX_FLUSH_DELAY_MS);
}
async function flushTx(key) {
  const buf = txBuffers.get(key);
  if (!buf) return;
  txBuffers.delete(key);
  const orderEvents = buf.events.filter(ev => ev.type === "order");
  if (orderEvents.length) {
    const signal = await buildAggregatedFollowSignal(buf.wallet, buf.events);
    await emitUiEvent(signal || orderEvents[0]);
    return;
  }
  for (const ev of buf.events) await emitUiEvent(ev);
}
function handleLog(raw) {
  const uniqueKey = `${raw.transactionHash}:${raw.logIndex}`;
  if (seenLogs.has(uniqueKey)) return;
  seenLogs.add(uniqueKey);
  if (seenLogs.size > SEEN_LOG_LIMIT) {
    const pruneCount = Math.max(1, seenLogs.size - SEEN_LOG_PRUNE_TO);
    let i = 0;
    for (const key of seenLogs) {
      seenLogs.delete(key);
      if (++i >= pruneCount) break;
    }
  }
  try {
    const events = parseRawLog(raw);
    for (const event of events) queueWalletTxEvent(event);
  } catch (err) {
    console.error("[解析日志失败]", err.message);
  }
}

// ─── Polygon subscriptions ─────────────────────────────────────────────────
function rpc(method, params) {
  const id = polygonRpcId++;
  polygonWs.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
  return id;
}
function subscribeLogs(name, filter) {
  const id = rpc("eth_subscribe", ["logs", filter]);
  pendingSubRequests.set(id, { name, filter });
}
function setupSubscriptions() {
  const enabled = getEnabledWallets();
  if (!enabled.length) {
    console.warn("[订阅] 当前没有启用的钱包");
    return;
  }
  const walletTopics = enabled.map(w => topicForWallet(w.address));
  console.log(`[订阅] 启用钱包 ${enabled.length} 个`);

  subscribeLogs("V2/V1 OrderFilled - maker", { address: EXCHANGE_ADDRESSES, topics: [[TOPICS.ORDER_FILLED_V2, TOPICS.ORDER_FILLED_V1], null, walletTopics] });
  subscribeLogs("V2/V1 OrderFilled - taker", { address: EXCHANGE_ADDRESSES, topics: [[TOPICS.ORDER_FILLED_V2, TOPICS.ORDER_FILLED_V1], null, null, walletTopics] });
  subscribeLogs("pUSD Transfer - from", { address: CONTRACTS.PUSD, topics: [TOPICS.ERC20_TRANSFER, walletTopics] });
  subscribeLogs("pUSD Transfer - to", { address: CONTRACTS.PUSD, topics: [TOPICS.ERC20_TRANSFER, null, walletTopics] });
  subscribeLogs("CTF TransferSingle - from", { address: CONTRACTS.CONDITIONAL_TOKENS, topics: [TOPICS.ERC1155_TRANSFER_SINGLE, null, walletTopics] });
  subscribeLogs("CTF TransferSingle - to", { address: CONTRACTS.CONDITIONAL_TOKENS, topics: [TOPICS.ERC1155_TRANSFER_SINGLE, null, null, walletTopics] });
  subscribeLogs("CTF TransferBatch - from", { address: CONTRACTS.CONDITIONAL_TOKENS, topics: [TOPICS.ERC1155_TRANSFER_BATCH, null, walletTopics] });
  subscribeLogs("CTF TransferBatch - to", { address: CONTRACTS.CONDITIONAL_TOKENS, topics: [TOPICS.ERC1155_TRANSFER_BATCH, null, null, walletTopics] });
}
function restartPolygon(reason = "wallet list changed") {
  if (restartTimer) clearTimeout(restartTimer);
  restartTimer = setTimeout(() => {
    console.log(`[Polygon] 重启订阅: ${reason}`);
    if (polygonWs && polygonWs.readyState === WebSocket.OPEN) polygonWs.close(4000, reason);
    else connectPolygon();
  }, 300);
}
function connectPolygon() {
  if (polygonPingTimer) { clearInterval(polygonPingTimer); polygonPingTimer = null; }
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  if (polygonWs && [WebSocket.OPEN, WebSocket.CONNECTING].includes(polygonWs.readyState)) return;
  console.log(`[${now()}] 连接 Polygon WebSocket...`);
  polygonWs = new WebSocket(WSS_URL, { agent, handshakeTimeout: 20_000 });

  polygonWs.on("open", () => {
    polygonReconnectDelay = 3000; // reset on successful connect
    console.log(`[${now()}] Polygon WS 已连接`);
    broadcast({ type: "connected", wallets: publicWallets() });
    pendingSubRequests.clear();
    activeSubscriptions.clear();
    setupSubscriptions();
  });

  polygonWs.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.id && msg.result) {
      const sub = pendingSubRequests.get(msg.id);
      if (sub) {
        pendingSubRequests.delete(msg.id);
        activeSubscriptions.set(msg.result, sub);
        console.log(`[订阅] ${sub.name} -> ${msg.result}`);
        broadcast({ type: "subscribed", name: sub.name, subId: msg.result });
      }
      return;
    }
    if (msg.method === "eth_subscription" && msg.params?.result) handleLog(msg.params.result);
    if (msg.error) console.error("[RPC Error]", msg.error);
  });

  polygonWs.on("error", err => {
    console.error(`[${now()}] Polygon WS 错误:`, err.message);
    broadcast({ type: "error", message: err.message });
  });
  polygonWs.on("close", (code, reason) => {
    if (polygonPingTimer) { clearInterval(polygonPingTimer); polygonPingTimer = null; }
    console.error(`[${now()}] Polygon WS 断开: ${code} ${reason || ""} — ${polygonReconnectDelay / 1000}s 后重连`);
    pendingSubRequests.clear();
    activeSubscriptions.clear();
    broadcast({ type: "disconnected" });
    reconnectTimer = setTimeout(() => {
      polygonReconnectDelay = Math.min(polygonReconnectDelay * 2, POLYGON_RECONNECT_MAX_DELAY);
      connectPolygon();
    }, polygonReconnectDelay);
  });

  polygonPingTimer = setInterval(() => {
    if (!polygonWs || polygonWs.readyState !== WebSocket.OPEN) { clearInterval(polygonPingTimer); polygonPingTimer = null; return; }
    try { polygonWs.ping(); } catch {}
  }, 30_000);
}

// ─── HTTP and UI ───────────────────────────────────────────────────────────
const INDEX_FILE = path.join(__dirname, "index.html");
let indexHtmlCache = null;
function loadIndexHtml() {
  if (indexHtmlCache !== null) return indexHtmlCache;
  indexHtmlCache = fs.existsSync(INDEX_FILE) ? fs.readFileSync(INDEX_FILE, "utf8") : null;
  return indexHtmlCache;
}
const httpServer = http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    if (isApiRequest(u.pathname) && !requestAuthorized(req, u)) {
      sendJson(res, 401, { error: "unauthorized: missing or invalid UI token" });
      return;
    }
    if (req.method === "GET" && (u.pathname === "/" || u.pathname === "/index.html")) {
      const html = loadIndexHtml();
      if (html !== null) {
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
        res.end(html);
      } else {
        res.writeHead(404); res.end("index.html not found");
      }
      return;
    }

    if (u.pathname === "/api/wallets" && req.method === "GET") {
      sendJson(res, 200, { wallets: publicWallets(), selectedWallet, qqbot: qqConfigured() });
      return;
    }
    if (u.pathname === "/api/wallets" && req.method === "POST") {
      const body = await readBody(req);
      const w = canonicalWallet({ name: body.name, address: body.address, enabled: body.enabled !== false, tags: body.tags || [] });
      const idx = wallets.findIndex(x => x.address === w.address);
      if (idx >= 0) wallets[idx] = { ...wallets[idx], ...w, createdAt: wallets[idx].createdAt };
      else wallets.push(w);
      selectedWallet = w.address;
      saveWallets();
      broadcast({ type: "wallets", wallets: publicWallets(), selectedWallet });
      restartPolygon("wallet added");
      restartExitWatchers();
      sendJson(res, 200, { ok: true, wallet: w, wallets: publicWallets() });
      return;
    }
    if (u.pathname.startsWith("/api/wallets/") && req.method === "DELETE") {
      const address = normalizeAddress(decodeURIComponent(u.pathname.split("/").pop()));
      const before = wallets.length;
      wallets = wallets.filter(w => w.address !== address);
      trackedProjects = trackedProjects.filter(p => p.wallet !== address);
      saveWallets();
      saveTrackedProjects();
      broadcast({ type: "wallets", wallets: publicWallets(), selectedWallet });
      if (wallets.length !== before) { restartPolygon("wallet removed"); restartExitWatchers(); }
      sendJson(res, 200, { ok: true, wallets: publicWallets(), selectedWallet });
      return;
    }
    if (u.pathname.startsWith("/api/wallets/") && u.pathname.endsWith("/toggle") && req.method === "POST") {
      const parts = u.pathname.split("/");
      const address = normalizeAddress(decodeURIComponent(parts[3]));
      const body = await readBody(req);
      const w = wallets.find(x => x.address === address);
      if (!w) { sendJson(res, 404, { error: "wallet not found" }); return; }
      w.enabled = body.enabled !== undefined ? Boolean(body.enabled) : !w.enabled;
      saveWallets();
      broadcast({ type: "wallets", wallets: publicWallets(), selectedWallet });
      restartPolygon("wallet toggled");
      restartExitWatchers();
      sendJson(res, 200, { ok: true, wallet: w });
      return;
    }
    if (u.pathname === "/api/select-wallet" && req.method === "POST") {
      const body = await readBody(req);
      const address = normalizeAddress(body.address);
      if (!walletMap.has(address)) { sendJson(res, 404, { error: "wallet not found" }); return; }
      selectedWallet = address;
      broadcast({ type: "selectedWallet", selectedWallet });
      sendJson(res, 200, { ok: true, selectedWallet });
      return;
    }
    if (u.pathname === "/api/positions" && req.method === "GET") {
      const address = normalizeAddress(u.searchParams.get("address") || selectedWallet || DEFAULT_WALLET);
      if (!ethers.isAddress(address)) { sendJson(res, 400, { error: "invalid address" }); return; }
      const positions = await enrichPositions(address);
      sendJson(res, 200, { address, walletName: walletLabel(address), positions, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/analyze-wallet" && req.method === "GET") {
      const address = normalizeAddress(u.searchParams.get("address"));
      if (!ethers.isAddress(address)) { sendJson(res, 400, { error: "invalid address" }); return; }
      sendJson(res, 200, await analyzeWallet(address));
      return;
    }
    if (u.pathname === "/api/analyze-wallets" && req.method === "GET") {
      const full = u.searchParams.get("full") === "1" || u.searchParams.get("full") === "true";
      const results = [];
      for (const w of wallets) {
        try { results.push(full ? await analyzeWallet(w.address) : await analyzeWalletLight(w.address)); }
        catch (err) { results.push({ name: w.name, address: w.address, short: shortAddr(w.address), error: friendlyError(err), score: 0 }); }
      }
      results.sort((a, b) => (b.score || 0) - (a.score || 0));
      sendJson(res, 200, { wallets: results, mode: full ? "full" : "light", updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/orderbook" && req.method === "GET") {
      const tokenId = u.searchParams.get("tokenId") || u.searchParams.get("token_id");
      if (!tokenId) { sendJson(res, 400, { error: "missing tokenId" }); return; }
      const book = await getOrderBook(tokenId);
      sendJson(res, 200, { tokenId, book, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/discover-wallets" && req.method === "GET") {
      const options = {
        category: u.searchParams.get("category") || DISCOVERY_DEFAULT_CATEGORY,
        timePeriod: u.searchParams.get("timePeriod") || DISCOVERY_DEFAULT_PERIOD,
        orderBy: u.searchParams.get("orderBy") || DISCOVERY_DEFAULT_ORDER_BY,
        limit: Number(u.searchParams.get("limit") || DISCOVERY_DEFAULT_LIMIT),
        offset: Number(u.searchParams.get("offset") || 0),
        minPnl: Number(u.searchParams.get("minPnl") || DISCOVERY_MIN_PNL),
        minVol: Number(u.searchParams.get("minVol") || DISCOVERY_MIN_VOL),
        minScore: Number(u.searchParams.get("minScore") || DISCOVERY_MIN_SCORE),
        analyze: u.searchParams.get("analyze") !== "false",
      };
      const candidates = await discoverWalletCandidates(options);
      sendJson(res, 200, { candidates, options, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/discover-wallets/add" && req.method === "POST") {
      const body = await readBody(req);
      const input = Array.isArray(body.wallets) ? body.wallets : Array.isArray(body.candidates) ? body.candidates : [];
      const result = addDiscoveredWallets(input, { maxAdd: Number(body.maxAdd || 50) });
      broadcast({ type: "wallets", wallets: publicWallets(), selectedWallet });
      if (result.added.length || result.updated.length) { restartPolygon("discovered wallets added"); restartExitWatchers(); }
      sendJson(res, 200, { ok: true, ...result, selectedWallet });
      return;
    }
    if (u.pathname === "/api/activity" && req.method === "GET") {
      const address = normalizeAddress(u.searchParams.get("address") || selectedWallet || DEFAULT_WALLET);
      if (!ethers.isAddress(address)) { sendJson(res, 400, { error: "invalid address" }); return; }
      const limit = Number(u.searchParams.get("limit") || ACTIVITY_DEFAULT_LIMIT);
      const offset = Number(u.searchParams.get("offset") || 0);
      const data = await getWalletActivity(address, { limit, offset });
      sendJson(res, 200, data);
      return;
    }
    if (u.pathname === "/api/activities" && req.method === "GET") {
      const limit = Number(u.searchParams.get("limit") || Math.min(50, ACTIVITY_DEFAULT_LIMIT));
      const enabledOnly = u.searchParams.get("enabled") !== "false";
      const list = enabledOnly ? getEnabledWallets() : wallets;
      const results = await mapLimit(list, Math.min(3, DISCOVERY_CONCURRENCY), async (w) => {
        try { return await getWalletActivity(w.address, { limit, offset: 0 }); }
        catch (err) { return { address: w.address, walletName: w.name, items: [], error: friendlyError(err) }; }
      });
      sendJson(res, 200, { wallets: results, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/auto-trade/wallets" && req.method === "GET") {
      sendJson(res, 200, { ok: true, wallets: publicAutoTradeWallets(), defaults: { amountUsd: AUTO_TRADE_DEFAULT_AMOUNT_USD, maxPriceDiff: AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF, mode: AUTO_TRADE_DEFAULT_MODE }, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/auto-trade/wallets" && req.method === "POST") {
      const body = await readBody(req);
      const cfg = upsertAutoTradeWallet(body);
      sendJson(res, 200, { ok: true, config: { ...cfg, walletName: walletLabel(cfg.wallet), shortWallet: shortAddr(cfg.wallet), maxPriceDiffText: `${(Number(cfg.maxPriceDiff || 0) * 100).toFixed(1)}¢` }, wallets: publicAutoTradeWallets() });
      return;
    }
    if (u.pathname.startsWith("/api/auto-trade/wallets/") && req.method === "DELETE") {
      const address = decodeURIComponent(u.pathname.split("/").pop());
      const ok = deleteAutoTradeWallet(address);
      sendJson(res, 200, { ok, wallets: publicAutoTradeWallets() });
      return;
    }
    if (u.pathname === "/api/account/status" && req.method === "GET") {
      const noCache = u.searchParams.get("noCache") === "1" || u.searchParams.get("refresh") === "true";
      const status = await getTradingAccountStatus({ noCache });
      sendJson(res, 200, { ok: true, status, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/trade/status" && req.method === "GET") {
      const ex = requireTradeExecutor();
      sendJson(res, 200, { ok: true, available: true, config: ex.publicTradeConfig(), updatedAt: new Date().toISOString() });
      return;
    }
    // Kill switch control endpoint (for emergency stop from UI)
    if (u.pathname === "/api/trade/kill-switch" && req.method === "GET") {
      const ex = requireTradeExecutor();
      const cfg = ex.publicTradeConfig();
      sendJson(res, 200, {
        ok: true,
        killSwitch: cfg.killSwitch || false,
        mode: cfg.mode || "unknown",
        dryRun: cfg.dryRun || false,
        updatedAt: new Date().toISOString(),
      });
      return;
    }
    if (u.pathname === "/api/trade/kill-switch" && req.method === "POST") {
      const ex = requireTradeExecutor(); // FIX: was missing, caused ReferenceError
      const body = await readBody(req);
      // Parse the new kill switch value (0/1/true/false)
      const newKillSwitch = body.enabled !== undefined ? Boolean(body.enabled) : true;
      // Read the current .env file
      const envPath = path.join(__dirname, ".env");
      let envContent = "";
      try {
        envContent = fs.readFileSync(envPath, "utf8");
      } catch (err) {
        sendJson(res, 500, { ok: false, error: "无法读取 .env 文件" });
        return;
      }
      // Update or add KILL_SWITCH line
      const lines = envContent.split("\n");
      let found = false;
      const updatedLines = lines.map(line => {
        if (line.trim().startsWith("KILL_SWITCH=")) {
          found = true;
          return `KILL_SWITCH=${newKillSwitch ? "true" : "false"}`;
        }
        return line;
      });
      if (!found) {
        updatedLines.push(`KILL_SWITCH=${newKillSwitch ? "true" : "false"}`);
      }
      // Write back
      try {
        fs.writeFileSync(envPath, updatedLines.join("\n"), "utf8");
        // Immediately update in-memory kill switch for immediate effect
        ex.setKillSwitch(newKillSwitch);
        // Broadcast to all connected UI clients so they reflect state immediately
        broadcast({ type: "killSwitch", killSwitch: newKillSwitch });
        sendJson(res, 200, {
          ok: true,
          message: `Kill switch 已${newKillSwitch ? "启用" : "禁用"} (环境变量已更新，已立即生效)`,
          killSwitch: newKillSwitch,
          warning: newKillSwitch ? "所有自动交易已停止" : "自动交易已恢复",
          updatedAt: new Date().toISOString(),
        });
      } catch (err) {
        sendJson(res, 500, { ok: false, error: `写入 .env 失败: ${err.message}` });
      }
      return;
    }
    if (u.pathname === "/api/trade/connect" && req.method === "POST") {
      const ex = requireTradeExecutor();
      const body = await readBody(req);
      const result = await ex.testConnectivity({ derive: Boolean(body.derive), tokenId: body.tokenId || "" });
      sendJson(res, 200, { ok: true, result, config: ex.publicTradeConfig(), updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/trade/derive-creds" && req.method === "POST") {
      const ex = requireTradeExecutor();
      const result = await ex.deriveAndStoreCredentials();
      sendJson(res, 200, { ok: true, result, config: ex.publicTradeConfig(), updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/trade/test-order" && req.method === "POST") {
      const ex = requireTradeExecutor();
      const body = await readBody(req);
      const result = await ex.createSignedOrderOrPost(body);
      sendJson(res, 200, { ok: true, result, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/tracked-projects" && req.method === "GET") {
      const address = normalizeAddress(u.searchParams.get("address") || "");
      const projects = address ? publicTrackedProjects().filter(p => p.wallet === address) : publicTrackedProjects();
      sendJson(res, 200, { projects, trackedExitOnly: TRACKED_EXIT_ONLY, updatedAt: new Date().toISOString() });
      return;
    }
    if (u.pathname === "/api/tracked-projects" && req.method === "POST") {
      const body = await readBody(req);
      const project = upsertTrackedProject(body);
      restartExitWatchers();
      sendJson(res, 200, { ok: true, project, projects: publicTrackedProjects(), trackedExitOnly: TRACKED_EXIT_ONLY });
      return;
    }
    if (u.pathname.startsWith("/api/tracked-projects/") && req.method === "DELETE") {
      const id = decodeURIComponent(u.pathname.split("/").pop());
      const ok = deleteTrackedProject(id);
      restartExitWatchers();
      sendJson(res, 200, { ok, projects: publicTrackedProjects(), trackedExitOnly: TRACKED_EXIT_ONLY });
      return;
    }
    if (u.pathname === "/api/settings" && req.method === "GET") {
      sendJson(res, 200, {
        qqPushHighQualityOnly: QQ_PUSH_HIGH_QUALITY_ONLY,
        qqPushActiveBuyExitOnly: QQ_PUSH_ACTIVE_BUY_EXIT_ONLY,
        signalMinWalletScore: SIGNAL_MIN_WALLET_SCORE,
        signalOnlyTakerOpenAdd: SIGNAL_ONLY_TAKER_OPEN_ADD,
        signalMinNotionalUsd: SIGNAL_MIN_NOTIONAL_USD,
        signalMaxPriceDiff: SIGNAL_MAX_PRICE_DIFF,
        signalMaxPriceDiffText: `${(SIGNAL_MAX_PRICE_DIFF * 100).toFixed(1)}¢`,
        signalMinDepthUsd: SIGNAL_MIN_DEPTH_USD,
        signalFollowSampleUsd: SIGNAL_FOLLOW_SAMPLE_USD,
        chainBalanceCheck: ENABLE_CHAIN_BALANCE_CHECK,
        fetchTimeoutMs: FETCH_TIMEOUT_MS,
        discoveryDefaultCategory: DISCOVERY_DEFAULT_CATEGORY,
        discoveryDefaultPeriod: DISCOVERY_DEFAULT_PERIOD,
        discoveryDefaultOrderBy: DISCOVERY_DEFAULT_ORDER_BY,
        discoveryDefaultLimit: DISCOVERY_DEFAULT_LIMIT,
        discoveryMinPnl: DISCOVERY_MIN_PNL,
        discoveryMinVol: DISCOVERY_MIN_VOL,
        discoveryMinScore: DISCOVERY_MIN_SCORE,
        uiSecretEnabled: !!UI_SECRET,
        marketCacheTtlMs: MARKET_CACHE_TTL_MS,
        orderbookCacheTtlMs: ORDERBOOK_CACHE_TTL_MS,
        activityDefaultLimit: ACTIVITY_DEFAULT_LIMIT,
        activityMaxLimit: ACTIVITY_MAX_LIMIT,
        activityCacheTtlMs: ACTIVITY_CACHE_TTL_MS,
        exitActivityWatchEnabled: EXIT_ACTIVITY_WATCH_ENABLED,
        exitActivityPollMs: EXIT_ACTIVITY_POLL_MS,
        exitActivityLookbackLimit: EXIT_ACTIVITY_LOOKBACK_LIMIT,
        exitPositionWatchEnabled: EXIT_POSITION_WATCH_ENABLED,
        exitPositionPollMs: EXIT_POSITION_POLL_MS,
        exitPositionMinSizeDelta: EXIT_POSITION_MIN_SIZE_DELTA,
        exitPositionMinShareDelta: EXIT_POSITION_MIN_SHARE_DELTA,
        exitPositionMinSizeDeltaPct: EXIT_POSITION_MIN_SIZE_DELTA_PCT,
        exitPositionRequireDisplayShareDrop: EXIT_POSITION_REQUIRE_DISPLAY_SHARE_DROP,
        exitPositionMinValueDeltaUsd: EXIT_POSITION_MIN_VALUE_DELTA_USD,
        exitPositionTrackMinValueUsd: EXIT_POSITION_TRACK_MIN_VALUE_USD,
        exitPositionAlertOnMissing: EXIT_POSITION_ALERT_ON_MISSING,
        exitPositionMissingConfirmPolls: EXIT_POSITION_MISSING_CONFIRM_POLLS,
        exitPositionMaxSignalsPerWalletPoll: EXIT_POSITION_MAX_SIGNALS_PER_WALLET_POLL,
        exitActivityMinUsd: EXIT_ACTIVITY_MIN_USD,
        exitActivityMaxSignalsPerWalletPoll: EXIT_ACTIVITY_MAX_SIGNALS_PER_WALLET_POLL,
        exitBatchEnabled: EXIT_BATCH_ENABLED,
        exitBatchMaxItems: EXIT_BATCH_MAX_ITEMS,
        exitAlertCooldownMs: EXIT_ALERT_COOLDOWN_MS,
        trackedExitOnly: TRACKED_EXIT_ONLY,
        autoTradeWallets: publicAutoTradeWallets(),
        autoTradeWalletsEnabled: autoTradeWallets.filter(x => x.enabled).length,
        autoTradeDefaultAmountUsd: AUTO_TRADE_DEFAULT_AMOUNT_USD,
        autoTradeDefaultMaxPriceDiff: AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF,
        autoTradeDefaultMaxPriceDiffText: `${(AUTO_TRADE_DEFAULT_MAX_PRICE_DIFF * 100).toFixed(1)}¢`,
        accountBalancePushEnabled: ACCOUNT_BALANCE_PUSH_ENABLED,
        accountBalancePushIntervalMs: ACCOUNT_BALANCE_PUSH_INTERVAL_MS,
        accountBalancePollMs: ACCOUNT_BALANCE_POLL_MS,
        accountPositionDustUsd: ACCOUNT_POSITION_DUST_USD,
        accountPositionDustPrice: ACCOUNT_POSITION_DUST_PRICE,
        autoTradeAccountGuardEnabled: AUTO_TRADE_ACCOUNT_GUARD_ENABLED,
        autoTradeMinFreeCashUsd: AUTO_TRADE_MIN_FREE_CASH_USD,
        autoTradeMaxActiveProjects: AUTO_TRADE_MAX_ACTIVE_PROJECTS,
        autoTradeMaxTotalProjects: AUTO_TRADE_MAX_TOTAL_PROJECTS,
        autoTradeMaxSettlementWaitProjects: AUTO_TRADE_MAX_SETTLEMENT_WAIT_PROJECTS,
        autoTradeMaxSettlementWaitUsd: AUTO_TRADE_MAX_SETTLEMENT_WAIT_USD,
        autoTradeSkipIfHoldingSameToken: AUTO_TRADE_SKIP_IF_HOLDING_SAME_TOKEN,
        autoTradeSameTokenMinShares: AUTO_TRADE_SAME_TOKEN_MIN_SHARES,
        autoTakeProfitEnabled: AUTO_TAKE_PROFIT_ENABLED,
        autoTakeProfitPct: AUTO_TAKE_PROFIT_PCT,
        autoTakeProfitMaxPerPoll: AUTO_TAKE_PROFIT_MAX_PER_POLL,
        autoTakeProfitMinValueUsd: AUTO_TAKE_PROFIT_MIN_VALUE_USD,
        tradeAvailable: !!tradeExecutor,
        tradeConfig: tradeExecutor ? tradeExecutor.publicTradeConfig() : null,
        trackedProjectsCount: trackedProjects.filter(p => p.enabled !== false).length,
      });
      return;
    }
    if (u.pathname === "/api/health" && req.method === "GET") {
      sendJson(res, 200, { ok: true, wallets: wallets.length, enabled: getEnabledWallets().length, polygon: polygonWs?.readyState, qqbot: qqConfigured(), qqPushHighQualityOnly: QQ_PUSH_HIGH_QUALITY_ONLY, uiSecretEnabled: !!UI_SECRET, tradeAvailable: !!tradeExecutor });
      return;
    }

    res.writeHead(404); res.end("not found");
  } catch (err) {
    console.error("[HTTP]", err);
    sendJson(res, 500, { error: friendlyError(err) });
  }
});

const bridgeServer = new WebSocket.Server({ server: httpServer });
const browserClients = new Set();
function broadcast(msg) {
  const json = JSON.stringify(msg);
  for (const c of browserClients) if (c.readyState === WebSocket.OPEN) c.send(json);
}
bridgeServer.on("connection", client => {
  browserClients.add(client);
  client.send(JSON.stringify({ type: "init", wallets: publicWallets(), selectedWallet, trackedProjects: publicTrackedProjects(), autoTradeWallets: publicAutoTradeWallets(), accountStatus: accountStatusCache?.value || null, qqbot: qqConfigured(), contracts: CONTRACTS, tradeAvailable: !!tradeExecutor }));
  client.on("close", () => browserClients.delete(client));
});

// ─── Start ─────────────────────────────────────────────────────────────────
loadWallets();
loadTrackedProjects();
loadAutoTradeWallets();
httpServer.listen(UI_PORT, HOST, () => {
  console.log(`\n${"─".repeat(70)}`);
  console.log("  Polymarket Multi-Wallet Monitor");
  console.log(`  浏览器打开: http://${HOST}:${UI_PORT}`);
  console.log(`  监控钱包:   ${getEnabledWallets().length}/${wallets.length}`);
  for (const w of wallets) console.log(`    ${w.enabled ? "✓" : "×"} ${w.name} ${w.address}`);
  console.log(`  Hermes QQ:  ${qqConfigured() ? "已配置" : "未配置"}`);
  console.log(`  退出监控:   ${TRACKED_EXIT_ONLY ? "仅已选择项目" : "全部项目"}，已选择 ${trackedProjects.filter(p => p.enabled !== false).length} 个`);
  console.log(`  自动跟单:   ${autoTradeWallets.filter(x => x.enabled).length}/${autoTradeWallets.length} 个钱包已启用`);
  console.log(`  账户资金:   ${ACCOUNT_BALANCE_PUSH_ENABLED ? `每 ${Math.round(ACCOUNT_BALANCE_PUSH_INTERVAL_MS/60000)} 分钟推送` : "不推送"}`);
  console.log(`  交易模块:   ${tradeExecutor ? "已加载" : "未加载"}`);
  console.log(`${"─".repeat(70)}\n`);
});
connectPolygon();
connectQQGateway();
startExitWatchers();
startAccountStatusWatcher();
