/**
 * [DEPRECATED — 仅供参考，不再维护]
 *
 * 这是早期单钱包命令行监控脚本。
 * 当前主服务已迁移至 ws-bridge.js (npm start)，支持：
 *   - 多钱包监控 + UI 管理
 *   - 实时信号聚合与 followability 评估
 *   - 自动跟单 / 自动平仓
 *   - QQ Bot 推送
 *
 * 本文件与 ws-bridge.js 的逻辑已不同步，不要同时运行两者。
 * 如需调试单钱包历史事件，请用 get-positions.js 或直接查询 data-api.polymarket.com。
 */


const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");
const { ethers } = require("ethers");

const WSS_URL = process.env.POLYGON_WSS;
const WATCH_WALLET = (process.env.WATCH_WALLET || "").trim().toLowerCase();
const PROXY_URL = process.env.PROXY_URL || "http://127.0.0.1:7897";

// Hermes / QQ Bot notification config.
const QQ_APP_ID = process.env.QQ_APP_ID || "";
const QQ_CLIENT_SECRET = process.env.QQ_CLIENT_SECRET || "";
const QQBOT_TARGET_TYPE = (process.env.QQBOT_TARGET_TYPE || "c2c").trim().toLowerCase();
const QQBOT_TARGET_ID = (
  process.env.QQBOT_TARGET_ID ||
  process.env.QQBOT_HOME_CHANNEL ||
  process.env.QQ_HOME_CHANNEL ||
  ""
).trim();
const QQ_SANDBOX = process.env.QQ_SANDBOX === "true";
const QQ_API_BASE = QQ_SANDBOX
  ? "https://sandbox.api.sgroup.qq.com"
  : "https://api.sgroup.qq.com";
const QQ_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken";

if (!WSS_URL) {
  console.error("缺少 POLYGON_WSS，请在 .env 里填写 Polygon WebSocket RPC");
  process.exit(1);
}

if (!WATCH_WALLET || !ethers.isAddress(WATCH_WALLET)) {
  console.error("WATCH_WALLET 不是有效地址");
  process.exit(1);
}

// Polymarket 官方当前 V2 合约 + 旧 CLOB 合约
const CONTRACTS = {
  CTF_EXCHANGE_V2: "0xE111180000d2663C0091e4f400237545B87B996B",
  NEG_RISK_CTF_EXCHANGE_V2: "0xe2222d279d744050d28e00520010520000310F59",

  // 旧版 CLOB 合约，保留监听，避免漏掉仍在旧合约上的事件
  CTF_EXCHANGE_V1: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
  NEG_RISK_CTF_EXCHANGE_V1: "0xC5d563A36AE78145C45a50134d48A1215220f80a",

  CONDITIONAL_TOKENS: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
  PUSD: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
};

const EXCHANGE_ADDRESSES = [
  CONTRACTS.CTF_EXCHANGE_V2,
  CONTRACTS.NEG_RISK_CTF_EXCHANGE_V2,
  CONTRACTS.CTF_EXCHANGE_V1,
  CONTRACTS.NEG_RISK_CTF_EXCHANGE_V1,
];

const iface = new ethers.Interface([
  // V2:
  // OrderFilled(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint8 side,uint256 tokenId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee,bytes32 builder,bytes32 metadata)
  "event OrderFilled(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint8 side,uint256 tokenId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee,bytes32 builder,bytes32 metadata)",

  // V1:
  // OrderFilled(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint256 makerAssetId,uint256 takerAssetId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee)
  "event OrderFilledV1(bytes32 indexed orderHash,address indexed maker,address indexed taker,uint256 makerAssetId,uint256 takerAssetId,uint256 makerAmountFilled,uint256 takerAmountFilled,uint256 fee)",

  // ERC20
  "event Transfer(address indexed from,address indexed to,uint256 value)",

  // ERC1155
  "event TransferSingle(address indexed operator,address indexed from,address indexed to,uint256 id,uint256 value)",
  "event TransferBatch(address indexed operator,address indexed from,address indexed to,uint256[] ids,uint256[] values)",
]);

const TOPICS = {
  ORDER_FILLED_V2: ethers.id(
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
  ),
  ORDER_FILLED_V1: ethers.id(
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
  ),
  ERC20_TRANSFER: ethers.id("Transfer(address,address,uint256)"),
  ERC1155_TRANSFER_SINGLE: ethers.id(
    "TransferSingle(address,address,address,uint256,uint256)"
  ),
  ERC1155_TRANSFER_BATCH: ethers.id(
    "TransferBatch(address,address,address,uint256[],uint256[])"
  ),
};

const walletTopic = ethers.zeroPadValue(WATCH_WALLET, 32).toLowerCase();

let ws = null;
let rpcId = 1;
let reconnectTimer = null;
let pingInterval = null;
const subscriptions = new Map();
const seenLogs = new Set();

function now() {
  return new Date().toISOString();
}

function shortAddr(addr) {
  if (!addr) return "";
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`;
}

function usdcLike(value) {
  try {
    return ethers.formatUnits(value, 6);
  } catch {
    return value.toString();
  }
}

let qqAccessToken = "";
let qqTokenExpireAt = 0;
let qqMsgSeq = 1;

function qqConfigured() {
  return !!(QQ_APP_ID && QQ_CLIENT_SECRET && QQBOT_TARGET_ID);
}

function splitQQText(text, maxLen = 1800) {
  const s = String(text || "");
  if (s.length <= maxLen) return [s];
  const chunks = [];
  let buf = "";
  for (const line of s.split("\n")) {
    const next = buf ? `${buf}\n${line}` : line;
    if (next.length > maxLen) {
      if (buf) chunks.push(buf);
      if (line.length > maxLen) {
        for (let i = 0; i < line.length; i += maxLen) chunks.push(line.slice(i, i + maxLen));
        buf = "";
      } else {
        buf = line;
      }
    } else {
      buf = next;
    }
  }
  if (buf) chunks.push(buf);
  return chunks;
}

async function getQQAccessToken() {
  if (qqAccessToken && Date.now() < qqTokenExpireAt - 60_000) return qqAccessToken;

  const fetch = (await import("node-fetch")).default;
  const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;
  const res = await fetch(QQ_TOKEN_URL, {
    method: "POST",
    agent,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ appId: QQ_APP_ID, clientSecret: QQ_CLIENT_SECRET }),
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) {
    throw new Error(`获取 QQ access_token 失败: HTTP ${res.status} ${JSON.stringify(data)}`);
  }

  qqAccessToken = data.access_token;
  qqTokenExpireAt = Date.now() + Number(data.expires_in || 7200) * 1000;
  return qqAccessToken;
}

async function sendQQTextOnce(text) {
  const token = await getQQAccessToken();
  const fetch = (await import("node-fetch")).default;
  const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;

  let url;
  let body;
  if (QQBOT_TARGET_TYPE === "c2c" || QQBOT_TARGET_TYPE === "user" || QQBOT_TARGET_TYPE === "dm") {
    url = `${QQ_API_BASE}/v2/users/${QQBOT_TARGET_ID}/messages`;
    body = { msg_type: 0, content: text, msg_seq: qqMsgSeq++ };
  } else if (QQBOT_TARGET_TYPE === "group") {
    url = `${QQ_API_BASE}/v2/groups/${QQBOT_TARGET_ID}/messages`;
    body = { msg_type: 0, content: text, msg_seq: qqMsgSeq++ };
  } else if (QQBOT_TARGET_TYPE === "guild" || QQBOT_TARGET_TYPE === "channel") {
    url = `${QQ_API_BASE}/channels/${QQBOT_TARGET_ID}/messages`;
    body = { content: text };
  } else {
    throw new Error(`未知 QQBOT_TARGET_TYPE: ${QQBOT_TARGET_TYPE}，请用 c2c / group / guild`);
  }

  const res = await fetch(url, {
    method: "POST",
    agent,
    headers: {
      "content-type": "application/json",
      "authorization": `QQBot ${token}`,
      "x-union-appid": QQ_APP_ID,
    },
    body: JSON.stringify(body),
  });

  const resultText = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} ${resultText}`);
  return resultText;
}

async function sendHermesQQBot(text) {
  if (!qqConfigured()) {
    console.log("[Hermes QQ Bot] 未配置，跳过推送");
    return;
  }

  try {
    const chunks = splitQQText(text);
    for (const chunk of chunks) await sendQQTextOnce(chunk);
    console.log(`[Hermes QQ Bot] 推送成功，分片 ${chunks.length} 条`);
  } catch (err) {
    console.error("[Hermes QQ Bot 推送失败]", err.message);
  }
}

function signal(text) {
  console.log("\n" + "=".repeat(90));
  console.log(text);
  console.log("=".repeat(90) + "\n");
  sendHermesQQBot(text);
}

function txUrl(hash) {
  return `https://polygonscan.com/tx/${hash}`;
}

function contractName(address) {
  const a = address.toLowerCase();
  for (const [name, addr] of Object.entries(CONTRACTS)) {
    if (addr.toLowerCase() === a) return name;
  }
  return address;
}

// Old string-format OrderFilled parsers removed; use parseOrderFilledV2Event / parseOrderFilledV1Event.
function parseErc20Transfer(log) {
  const from = ethers.getAddress("0x" + log.topics[1].slice(26));
  const to = ethers.getAddress("0x" + log.topics[2].slice(26));
  const value = ethers.AbiCoder.defaultAbiCoder().decode(["uint256"], log.data)[0];

  const direction =
    from.toLowerCase() === WATCH_WALLET ? "转出" :
    to.toLowerCase() === WATCH_WALLET ? "转入" :
    "相关转账";

  return [
    "💵 Polymarket pUSD/ERC20 转账信号",
    "",
    `监听钱包：${WATCH_WALLET}`,
    `方向：${direction}`,
    `合约：${contractName(log.address)}`,
    `from：${from}`,
    `to：${to}`,
    `amount：${usdcLike(value)}`,
    `区块：${Number(log.blockNumber)}`,
    `交易：${txUrl(log.transactionHash)}`,
  ].join("\n");
}

function parseErc1155TransferSingle(log) {
  const decoded = iface.decodeEventLog("TransferSingle", log.data, log.topics);

  const from = decoded.from.toLowerCase();
  const to = decoded.to.toLowerCase();

  const direction =
    from === WATCH_WALLET ? "转出 outcome token" :
    to === WATCH_WALLET ? "转入 outcome token" :
    "相关 ERC1155 变动";

  return [
    "🎟️ Polymarket CTF ERC1155 头寸变动 / TransferSingle",
    "",
    `监听钱包：${WATCH_WALLET}`,
    `方向：${direction}`,
    `operator：${decoded.operator}`,
    `from：${decoded.from}`,
    `to：${decoded.to}`,
    `tokenId：${decoded.id.toString()}`,
    `数量：${decoded.value.toString()}`,
    `区块：${Number(log.blockNumber)}`,
    `交易：${txUrl(log.transactionHash)}`,
  ].join("\n");
}

function parseErc1155TransferBatch(log) {
  const decoded = iface.decodeEventLog("TransferBatch", log.data, log.topics);

  const from = decoded.from.toLowerCase();
  const to = decoded.to.toLowerCase();

  const direction =
    from === WATCH_WALLET ? "批量转出 outcome token" :
    to === WATCH_WALLET ? "批量转入 outcome token" :
    "相关 ERC1155 批量变动";

  const pairs = decoded.ids.map((id, i) => {
    return `  - tokenId=${id.toString()} value=${decoded.values[i].toString()}`;
  });

  return [
    "🎟️ Polymarket CTF ERC1155 头寸变动 / TransferBatch",
    "",
    `监听钱包：${WATCH_WALLET}`,
    `方向：${direction}`,
    `operator：${decoded.operator}`,
    `from：${decoded.from}`,
    `to：${decoded.to}`,
    `明细：`,
    ...pairs,
    `区块：${Number(log.blockNumber)}`,
    `交易：${txUrl(log.transactionHash)}`,
  ].join("\n");
}


// ─── Executable follow signal enrichment ──────────────────────────────────
const marketCacheByToken = new Map();

function shortValue(v, head = 8, tail = 6) {
  const str = String(v || "");
  return str.length > head + tail + 3 ? `${str.slice(0, head)}...${str.slice(-tail)}` : str;
}
function parseMaybeJson(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}
function formatTokenAmount(v) {
  try {
    const n = Number(ethers.formatUnits(v, 6));
    if (!Number.isFinite(n)) return String(v);
    return n >= 1000 ? n.toLocaleString("en-US", { maximumFractionDigits: 2 }) : n.toFixed(2);
  } catch { return String(v); }
}
function buildPolymarketUrl(market, tokenId) {
  const eventSlug = market?.events?.[0]?.slug;
  const slug = market?.slug;
  const base = eventSlug
    ? `https://polymarket.com/event/${eventSlug}`
    : slug
      ? `https://polymarket.com/market/${slug}`
      : null;
  if (!base) return null;
  return tokenId ? `${base}?tid=${encodeURIComponent(tokenId)}` : base;
}
async function apiJson(url) {
  const fetch = (await import("node-fetch")).default;
  const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;
  const res = await fetch(url, { agent, headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return res.json();
}
async function getMarketByToken(tokenId) {
  if (!tokenId) return null;
  if (marketCacheByToken.has(tokenId)) return marketCacheByToken.get(tokenId);
  const empty = { outcome: null, marketQuestion: "未知市场", marketUrl: null };
  try {
    const tokenInfo = await apiJson(`https://clob.polymarket.com/markets-by-token/${encodeURIComponent(tokenId)}`);
    let market = null;
    if (tokenInfo.condition_id) {
      const data = await apiJson(`https://gamma-api.polymarket.com/markets?condition_ids=${encodeURIComponent(tokenInfo.condition_id)}&limit=1`);
      market = Array.isArray(data) ? data[0] : (data?.markets?.[0] || data?.data?.[0] || null);
    }
    const outcomes = parseMaybeJson(market?.shortOutcomes) || parseMaybeJson(market?.outcomes) || ["YES", "NO"];
    const ids = parseMaybeJson(market?.clobTokenIds) || [tokenInfo.primary_token_id, tokenInfo.secondary_token_id].filter(Boolean);
    let idx = ids.findIndex(id => String(id) === String(tokenId));
    if (idx < 0 && String(tokenInfo.primary_token_id) === String(tokenId)) idx = 0;
    if (idx < 0 && String(tokenInfo.secondary_token_id) === String(tokenId)) idx = 1;
    const info = {
      outcome: outcomes[idx] || (idx === 0 ? "YES" : idx === 1 ? "NO" : null),
      marketQuestion: market?.question || market?.title || "未知市场",
      marketUrl: buildPolymarketUrl(market, tokenId),
      conditionId: tokenInfo.condition_id || null,
    };
    marketCacheByToken.set(tokenId, info);
    return info;
  } catch (err) {
    console.error(`[市场信息补全失败] tokenId=${shortValue(tokenId)} ${err.message}`);
    marketCacheByToken.set(tokenId, empty);
    return empty;
  }
}
function amount6ToNumber(v) {
  try { return Number(ethers.formatUnits(v, 6)); } catch { return Number(v); }
}
function estimateTrade(event) {
  // 直接从原始 BigInt 字符串转换，避免经过格式化字符串再 Number() 带来的精度损失
  const maker = amount6ToNumber(event.makerFilled);
  const taker = amount6ToNumber(event.takerFilled);
  let size = null;
  let notional = null;
  if (Number.isFinite(maker) && Number.isFinite(taker) && maker > 0 && taker > 0) {
    if (event.side === "BUY") { notional = maker; size = taker; }
    else if (event.side === "SELL") { notional = taker; size = maker; }
    else { notional = Math.min(maker, taker); size = Math.max(maker, taker); }
  }
  const price = size && size > 0 && notional && notional > 0 ? notional / size : null;
  return {
    priceText: price ? `${(price * 100).toFixed(1)}%` : "-",
    sizeText: size ? `${size.toLocaleString("en-US", { maximumFractionDigits: 2 })} shares` : "-",
    notionalText: notional ? `$${notional.toLocaleString("en-US", { maximumFractionDigits: 2 })}` : "-",
  };
}
function roleKind(role) {
  return role.startsWith("taker") ? "taker" : role.startsWith("maker") ? "maker" : "related";
}
function actionText(side, role) {
  const activePassive = roleKind(role) === "taker" ? "主动" : roleKind(role) === "maker" ? "被动" : "相关";
  return `${activePassive}${side === "SELL" ? "卖出" : "买入"}`;
}
function parseOrderFilledV2Event(log) {
  const d = iface.decodeEventLog("OrderFilled", log.data, log.topics);
  const side = Number(d.side) === 0 ? "BUY" : Number(d.side) === 1 ? "SELL" : `SIDE_${Number(d.side)}`;
  const maker = d.maker.toLowerCase();
  const taker = d.taker.toLowerCase();
  const role = maker === WATCH_WALLET ? "maker/挂单方成交" : taker === WATCH_WALLET ? "taker/吃单方成交" : "相关方";
  return {
    version: "V2", side, role,
    contract: contractName(log.address),
    tokenId: d.tokenId.toString(),
    maker: d.maker, taker: d.taker,
    makerFilled: d.makerAmountFilled.toString(),
    takerFilled: d.takerAmountFilled.toString(),
    fee: d.fee.toString(),
    orderHash: d.orderHash,
    block: Number(log.blockNumber),
    txUrl: txUrl(log.transactionHash),
  };
}
function parseOrderFilledV1Event(log) {
  // V1 data layout after indexed orderHash/maker/taker:
  // [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]
  const d = ethers.AbiCoder.defaultAbiCoder().decode(["uint256", "uint256", "uint256", "uint256", "uint256"], log.data);
  if (d.length < 5) throw new Error("V1 OrderFilled data 解码字段不足");
  const maker = ethers.getAddress("0x" + log.topics[2].slice(26));
  const taker = ethers.getAddress("0x" + log.topics[3].slice(26));
  const makerAssetId = d[0];
  const takerAssetId = d[1];
  const side = makerAssetId === 0n ? "BUY" : takerAssetId === 0n ? "SELL" : "COMPLEX";
  const role = maker.toLowerCase() === WATCH_WALLET ? "maker/挂单方成交" : taker.toLowerCase() === WATCH_WALLET ? "taker/吃单方成交" : "相关方";
  const tokenId = makerAssetId === 0n ? takerAssetId.toString() : makerAssetId.toString();
  return {
    version: "V1", side, role,
    contract: contractName(log.address),
    tokenId,
    maker, taker,
    makerFilled: d[2].toString(),
    takerFilled: d[3].toString(),
    fee: d[4].toString(),
    orderHash: log.topics[1],
    block: Number(log.blockNumber),
    txUrl: txUrl(log.transactionHash),
  };
}
async function formatFollowSignal(event) {
  const market = await getMarketByToken(event.tokenId);
  const est = estimateTrade(event);
  const action = actionText(event.side, event.role);
  const priority = roleKind(event.role) === "taker" ? "高：目标钱包主动吃单" : roleKind(event.role) === "maker" ? "低：目标钱包挂单被动成交" : "中：相关成交";
  const openUrl = market?.marketUrl || event.txUrl;
  return [
    "🚨 Polymarket 跟单信号",
    "",
    `动作：${action}`,
    `市场：${market?.marketQuestion || "未知市场"}`,
    `方向：${market?.outcome || "Outcome Token"}`,
    `估算价格：${est.priceText}`,
    `估算份额：${est.sizeText}`,
    `估算金额：${est.notionalText}`,
    `角色：${event.role}`,
    `优先级：${priority}`,
    `版本：${event.version}`,
    `tokenId：${event.tokenId}`,
    `maker：${event.maker}`,
    `taker：${event.taker}`,
    `makerFilled：${event.makerFilled}`,
    `takerFilled：${event.takerFilled}`,
    `fee：${event.fee}`,
    `orderHash：${event.orderHash}`,
    `区块：${event.block}`,
    `打开市场：${openUrl}`,
    `交易详情：${event.txUrl}`,
  ].join("\n");
}

async function handleLog(log) {
  const uniqueKey = `${log.transactionHash}:${log.logIndex}`;
  if (seenLogs.has(uniqueKey)) return;
  seenLogs.add(uniqueKey);

  if (seenLogs.size > 5000) {
    const pruneCount = Math.max(1, seenLogs.size - 3500);
    let i = 0;
    for (const key of seenLogs) {
      seenLogs.delete(key);
      if (++i >= pruneCount) break;
    }
  }

  const topic0 = log.topics[0].toLowerCase();

  try {
    if (topic0 === TOPICS.ORDER_FILLED_V2.toLowerCase()) {
      signal(await formatFollowSignal(parseOrderFilledV2Event(log)));
      return;
    }

    if (topic0 === TOPICS.ORDER_FILLED_V1.toLowerCase()) {
      signal(await formatFollowSignal(parseOrderFilledV1Event(log)));
      return;
    }

    if (topic0 === TOPICS.ERC20_TRANSFER.toLowerCase()) {
      signal(parseErc20Transfer(log));
      return;
    }

    if (topic0 === TOPICS.ERC1155_TRANSFER_SINGLE.toLowerCase()) {
      signal(parseErc1155TransferSingle(log));
      return;
    }

    if (topic0 === TOPICS.ERC1155_TRANSFER_BATCH.toLowerCase()) {
      signal(parseErc1155TransferBatch(log));
      return;
    }
  } catch (err) {
    console.error("[解析日志失败]", err.message, log);
  }
}

function rpc(method, params) {
  const id = rpcId++;
  ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
  return id;
}

function subscribeLogs(name, filter) {
  const id = rpc("eth_subscribe", ["logs", filter]);
  subscriptions.set(id, { name, filter });
}

function setupSubscriptions() {
  console.log(`[${now()}] 开始订阅链上日志...`);

  // V2 OrderFilled：maker 是 topics[2]，taker 是 topics[3]
  subscribeLogs("V2/V1 OrderFilled - wallet as maker", {
    address: EXCHANGE_ADDRESSES,
    topics: [[TOPICS.ORDER_FILLED_V2, TOPICS.ORDER_FILLED_V1], null, walletTopic],
  });

  subscribeLogs("V2/V1 OrderFilled - wallet as taker", {
    address: EXCHANGE_ADDRESSES,
    topics: [[TOPICS.ORDER_FILLED_V2, TOPICS.ORDER_FILLED_V1], null, null, walletTopic],
  });

  // pUSD/ERC20 Transfer：from / to
  subscribeLogs("pUSD Transfer - from wallet", {
    address: CONTRACTS.PUSD,
    topics: [TOPICS.ERC20_TRANSFER, walletTopic],
  });

  subscribeLogs("pUSD Transfer - to wallet", {
    address: CONTRACTS.PUSD,
    topics: [TOPICS.ERC20_TRANSFER, null, walletTopic],
  });

  // CTF ERC1155 TransferSingle：operator/from/to = topics[1]/[2]/[3]
  subscribeLogs("CTF TransferSingle - from wallet", {
    address: CONTRACTS.CONDITIONAL_TOKENS,
    topics: [TOPICS.ERC1155_TRANSFER_SINGLE, null, walletTopic],
  });

  subscribeLogs("CTF TransferSingle - to wallet", {
    address: CONTRACTS.CONDITIONAL_TOKENS,
    topics: [TOPICS.ERC1155_TRANSFER_SINGLE, null, null, walletTopic],
  });

  // CTF ERC1155 TransferBatch
  subscribeLogs("CTF TransferBatch - from wallet", {
    address: CONTRACTS.CONDITIONAL_TOKENS,
    topics: [TOPICS.ERC1155_TRANSFER_BATCH, null, walletTopic],
  });

  subscribeLogs("CTF TransferBatch - to wallet", {
    address: CONTRACTS.CONDITIONAL_TOKENS,
    topics: [TOPICS.ERC1155_TRANSFER_BATCH, null, null, walletTopic],
  });
}

function connect() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  console.log(`[${now()}] 连接 Polygon WebSocket...`);
  console.log(`[${now()}] 使用代理：${PROXY_URL}`);

  const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;

  ws = new WebSocket(WSS_URL, {
    agent,
    handshakeTimeout: 20_000,
  });

  ws.on("open", () => {
    console.log(`[${now()}] WebSocket 已连接`);
    console.log(`[${now()}] 正在监控钱包：${WATCH_WALLET}`);
    setupSubscriptions();
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.id && msg.result) {
      const sub = subscriptions.get(msg.id);
      if (sub) {
        console.log(`[${now()}] 订阅成功：${sub.name} -> ${msg.result}`);
      }
      return;
    }

    if (msg.method === "eth_subscription" && msg.params && msg.params.result) {
      handleLog(msg.params.result);
    }

    if (msg.error) {
      console.error("[RPC 错误]", msg.error);
    }
  });

  ws.on("error", (err) => {
    console.error(`[${now()}] WebSocket 错误：`, err.message);
  });

  ws.on("close", (code, reason) => {
    console.error(`[${now()}] WebSocket 断开：${code} ${reason}`);
    subscriptions.clear();
    // 清理心跳，避免重连后积累多个并发 interval
    if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
    reconnectTimer = setTimeout(() => {
      console.log(`[${now()}] 尝试重连...`);
      connect();
    }, 3000);
  });

  // 心跳，避免长时间无消息被断开
  if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
  pingInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      clearInterval(pingInterval);
      pingInterval = null;
      return;
    }

    try {
      ws.ping();
    } catch {}
  }, 30_000);
}

connect();
