/**
 * BTC Up or Down 5m — Analysis & Trading Module
 *
 * Features:
 * - Auto-discover active 5m BTC markets on Polymarket
 * - Fetch BTC 1m klines from Binance (through proxy)
 * - Calculate technical indicators (RSI, EMA, MACD, volume)
 * - AI-powered analysis via OpenAI-compatible API
 * - Execute trades via trade-executor
 * - Position monitoring with PnL tracking
 */

require("dotenv").config();

const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");
const { HttpsProxyAgent } = require("https-proxy-agent");

// ─── Helpers ────────────────────────────────────────────────────────────────
function cleanEnvValue(value) {
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
function nowIso() { return new Date().toISOString(); }

// ─── Config ─────────────────────────────────────────────────────────────────
const PROXY_URL = cleanEnvValue(process.env.PROXY_URL);
const agent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;

const BTC5M_ENABLED = envBool("BTC5M_ENABLED", true);
const BTC5M_EVENT_SLUG = cleanEnvValue(process.env.BTC5M_EVENT_SLUG);
const BTC5M_CONDITION_ID = cleanEnvValue(process.env.BTC5M_CONDITION_ID);
const BTC5M_UP_TOKEN_ID = cleanEnvValue(process.env.BTC5M_UP_TOKEN_ID);
const BTC5M_DOWN_TOKEN_ID = cleanEnvValue(process.env.BTC5M_DOWN_TOKEN_ID);
const BTC5M_DEFAULT_TRADE_USD = envNumber("BTC5M_DEFAULT_TRADE_USD", 1);
const BTC5M_DEFAULT_ORDER_TYPE = (cleanEnvValue(process.env.BTC5M_DEFAULT_ORDER_TYPE) || "FAK").toUpperCase();
const BTC5M_SELL_SLIPPAGE = envNumber("BTC5M_SELL_SLIPPAGE", 0.02);

const AI_BASE_URL = cleanEnvValue(process.env.BTC5M_AI_BASE_URL);
const AI_API_KEY = cleanEnvValue(process.env.BTC5M_AI_API_KEY);
const AI_MODEL = cleanEnvValue(process.env.BTC5M_AI_MODEL);
const AI_MAX_TOKENS = envNumber("BTC5M_AI_MAX_TOKENS", 12000);
const AI_TEMPERATURE = envNumber("BTC5M_AI_TEMPERATURE", 0.3);

const CLOB_HOST = cleanEnvValue(process.env.PM_CLOB_HOST) || "https://clob.polymarket.com";

// ─── State ──────────────────────────────────────────────────────────────────
let activeMarket = null;
let marketCacheTime = 0;
const MARKET_CACHE_TTL = 60_000;
let currentPosition = null;
let positionPollTimer = null;
let marketRefreshTimer = null;
let lastAnalysis = null;
let broadcastFn = null; // set by init()

// Per-window analysis cache: key = slug, value = { analysis, phase }
const analysisCache = new Map();
const ANALYSIS_CACHE_MAX = 50;

// ─── Auto-analysis & Event Logging ──────────────────────────────────────────
const AUTO_ANALYSIS_DELAY_MS = 3.5 * 60 * 1000; // 3.5 min into window → predict NEXT candle
let autoAnalysisTimer = null;
let scheduledWindowTs = 0; // track which window the timer is for (prevent duplicate scheduling)
let settlementTimer = null;
let settlementCheckTimer = null;

// ─── Real-time Price WebSocket (CLOB) ────────────────────────────────────────
const CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
let clobWs = null;
let clobWsReconnectTimer = null;
let clobWsReconnectDelay = 1000;
let clobWsConnectedAt = 0;
const CLOB_WS_RECONNECT_MAX = 30000;
let clobWsSubscribedToken = null;
let tradeInProgress = false;
let settleEventLock = Promise.resolve();
let lastBroadcastPrices = null;
let priceHistory = [];
const PRICE_HISTORY_MAX = 50;
let priceBroadcastTimer = null;
const PRICE_BROADCAST_THROTTLE_MS = 100; // broadcast at most every 100ms
let pendingPriceUpdate = null;

// HTTP fallback — 1s polling when WS is down
let httpFallbackTimer = null;
let httpFallbackActive = false;

const EVENT_LOG_DIR = path.join(__dirname, "btc5m-logs");
const EVENT_LOG_FILE = path.join(EVENT_LOG_DIR, "events.jsonl");

function ensureLogDir() {
  if (!fs.existsSync(EVENT_LOG_DIR)) fs.mkdirSync(EVENT_LOG_DIR, { recursive: true });
}

function appendEventLog(entry) {
  ensureLogDir();
  fs.appendFileSync(EVENT_LOG_FILE, JSON.stringify(entry) + "\n");
}

function readEventLogs() {
  ensureLogDir();
  if (!fs.existsSync(EVENT_LOG_FILE)) return [];
  const lines = fs.readFileSync(EVENT_LOG_FILE, "utf8").split("\n").filter(Boolean);
  return lines.map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
}

// Schedule auto-analysis 3.5 minutes into each 5m window (predict NEXT candle)
// Uses activeMarket slug when available for alignment, falls back to wall clock
function scheduleAutoAnalysis(market) {
  // Determine window start time
  const mkt = market || activeMarket;
  let startMs;
  if (mkt?.slug) {
    const match = mkt.slug.match(/-(\d+)$/);
    if (!match) {
      // slug 格式异常，用墙钟并重试
      console.warn(`[BTC5m] slug 格式异常: ${mkt.slug}，30s 后重试`);
      setTimeout(() => scheduleAutoAnalysis(), 30000);
      return;
    }
    startMs = Number(match[1]) * 1000;
  } else {
    const nowSec = Math.floor(Date.now() / 1000);
    startMs = Math.floor(nowSec / 300) * 300 * 1000;
  }

  const windowTs = Math.floor(startMs / 1000);
  const elapsed = Date.now() - startMs;

  // Same window already has a pending timer → skip (prevent duplicate scheduling)
  if (autoAnalysisTimer && scheduledWindowTs === windowTs) {
    return;
  }

  // Clear old timer
  if (autoAnalysisTimer) { clearTimeout(autoAnalysisTimer); autoAnalysisTimer = null; }

  if (elapsed > 4.5 * 60 * 1000) {
    // Window almost expired → schedule for NEXT window instead of giving up
    const nextWindowStart = startMs + 300 * 1000;
    const nextDelay = nextWindowStart + AUTO_ANALYSIS_DELAY_MS - Date.now();
    if (nextDelay > 0) {
      const nextSlug = `btc-updown-5m-${windowTs + 300}`;
      console.log(`[BTC5m] 窗口已过，调度下一次 → ${nextSlug} (${Math.round(nextDelay / 1000)}s后)`);
      scheduledWindowTs = windowTs + 300;
      autoAnalysisTimer = setTimeout(async () => {
        autoAnalysisTimer = null;
        try {
          if (!activeMarket) {
            console.log(`[BTC5m] 无活跃市场，尝试重新发现...`);
            try { await refreshMarket(); } catch {}
          }
          if (!activeMarket) {
            console.warn(`[BTC5m] 仍无活跃市场，跳过本次分析，30s 后重试`);
            setTimeout(() => scheduleAutoAnalysis(), 30000);
            return;
          }
          await runAutoAnalysis();
        }
        catch (err) {
          console.warn(`[BTC5m] 自动分析失败: ${err.message}`);
          setTimeout(() => scheduleAutoAnalysis(), 5000);
        }
      }, nextDelay);
    } else {
      // 下一次也过了，30s 后重试
      console.log(`[BTC5m] 窗口已过，30s 后重试调度`);
      setTimeout(() => scheduleAutoAnalysis(), 30000);
    }
    return;
  }

  // Schedule timer for 3.5 min mark
  const analysisDelay = startMs + AUTO_ANALYSIS_DELAY_MS - Date.now();
  if (analysisDelay <= 0) {
    // 分析点已过，30s 后重试调度
    console.log(`[BTC5m] 分析窗口已过，30s 后重试调度`);
    setTimeout(() => scheduleAutoAnalysis(), 30000);
    return;
  }
  const nextSlug = `btc-updown-5m-${windowTs + 300}`;
  console.log(`[BTC5m] ${Math.round(analysisDelay / 1000)}s 后自动分析 → 预测 ${nextSlug}`);
  scheduledWindowTs = windowTs;
  autoAnalysisTimer = setTimeout(async () => {
    autoAnalysisTimer = null;
    try {
      // If no active market, try to rediscover before analyzing
      if (!activeMarket) {
        console.log(`[BTC5m] 无活跃市场，尝试重新发现...`);
        try { await refreshMarket(); } catch {}
      }
      if (!activeMarket) {
        console.warn(`[BTC5m] 仍无活跃市场，跳过本次分析，30s 后重试`);
        setTimeout(() => scheduleAutoAnalysis(), 30000);
        return;
      }
      await runAutoAnalysis();
    }
    catch (err) {
      console.warn(`[BTC5m] 自动分析失败: ${err.message}`);
      setTimeout(() => scheduleAutoAnalysis(), 5000);
    }
  }, analysisDelay);
}

// Run auto-analysis (called by timer, not user click)
// Uses activeMarket slug for alignment, falls back to wall clock
async function runAutoAnalysis() {
  // Use activeMarket slug if available, otherwise wall clock
  let currentWindow;
  if (activeMarket?.slug) {
    const match = activeMarket.slug.match(/-(\d+)$/);
    currentWindow = match ? Number(match[1]) : Math.floor(Date.now() / 1000);
  } else {
    currentWindow = Math.floor(Date.now() / 1000);
    currentWindow = Math.floor(currentWindow / 300) * 300;
  }
  const nextWindow = currentWindow + 300;
  const slug = `btc-updown-5m-${nextWindow}`;

  console.log(`[BTC5m] runAutoAnalysis: current=${currentWindow} next=${nextWindow} market=${activeMarket?.slug || 'none'}`);

  // Skip if already analyzed this window
  if (analysisCache.has(slug)) {
    console.log(`[BTC5m] 跳过自动分析：${slug} 已有缓存`);
    return analysisCache.get(slug)?.analysis || null;
  }

  console.log(`[BTC5m] 自动分析 → 预测 ${slug}`);
  console.log(`[BTC5m] 开始调用 AI 分析...`);
  const analysis = await runAiAnalysis(false, slug);
  console.log(`[BTC5m] AI 分析完成: direction=${analysis.direction} action=${analysis.suggestedAction} score=${analysis.totalScore}`);

  // Log the event
  appendEventLog({
    type: "analysis",
    slug,
    startTime: new Date(nextWindow * 1000).toISOString(),
    analyzedAt: nowIso(),
    analysis: {
      direction: analysis.direction,
      confidence: analysis.confidence,
      edge: analysis.edge,
      suggestedAction: analysis.suggestedAction,
      marketState: analysis.indicators?.marketState || null,
      reasoning: analysis.reasoning,
      keyFactors: analysis.keyFactors,
      scoreBreakdown: analysis.scoreBreakdown,
      totalScore: analysis.totalScore,
    },
    indicators: analysis.indicators ? {
      currentPrice: analysis.indicators.currentPrice,
      rsi7: analysis.indicators.rsi7,
      rsi14: analysis.indicators.rsi14,
      emaAlignment: analysis.indicators.emaAlignment,
      macdDif: analysis.indicators.macdDif,
      macdSlope: analysis.indicators.macdSlope,
      volumeRatio: analysis.indicators.volumeRatio,
      pricePath: analysis.indicators.pricePath,
      volatilityRatio: analysis.indicators.volatilityRatio,
      volPriceAlign: analysis.indicators.volPriceAlign,
      rangePosition: analysis.indicators.rangePosition,
      consecUp: analysis.indicators.consecUp,
      consecDown: analysis.indicators.consecDown,
      hasVolumeSpike: analysis.indicators.hasVolumeSpike,
      vcpDetected: analysis.indicators.vcpDetected,
      boxBreakout: analysis.indicators.boxBreakout,
      pivotProximity: analysis.indicators.pivotProximity,
      secondEntry: analysis.indicators.secondEntry,
    } : null,
    marketPrices: {
      upPrice: activeMarket?.upPrice || null,
      downPrice: activeMarket?.downPrice || null,
    },
    result: null, // filled by settlement
  });

  if (broadcastFn) {
    broadcastFn({ type: "btc5mAutoAnalysis", slug, analysis });
  }

  // 调度下一次分析（确保链不断）
  scheduleAutoAnalysis();

  return analysis;
}

// Settle an event: query Polymarket for actual outcome
async function settleEvent(market) {
  if (!market?.slug) return;

  // Query Gamma API for market resolution
  let resolved = false;
  let actualDir = null;
  let marketData = null;
  try {
    const data = await fetchJson(`https://gamma-api.polymarket.com/events?slug=${encodeURIComponent(market.slug)}`);
    const ev = Array.isArray(data) ? data[0] : null;
    if (ev?.markets?.length) {
      marketData = ev.markets[0];
      // Check if market is closed/resolved
      if (marketData.closed || marketData.resolved) {
        resolved = true;
        // Get outcome prices to determine winner
        const prices = parseMaybeJson(marketData.outcomePrices) || [];
        const outcomes = parseMaybeJson(marketData.outcomes) || [];
        // outcomePrices: [upPrice, downPrice] — winner = 1.0, loser = 0.0
        if (prices.length >= 2) {
          const upPrice = Number(prices[0]);
          const downPrice = Number(prices[1]);
          if (upPrice >= 0.95) actualDir = "UP";
          else if (downPrice >= 0.95) actualDir = "DOWN";
          // If neither is clear, check which is higher
          if (!actualDir) {
            actualDir = upPrice > downPrice ? "UP" : "DOWN";
          }
        }
      }
    }
  } catch (err) {
    console.warn(`[BTC5m] 查询结算状态失败: ${err.message}`);
  }

  if (!resolved || !actualDir) {
    console.log(`[BTC5m] ${market.slug} 尚未结算`);
    return;
  }

  // Find the analysis log entry for this slug and update it (with lock)
  await (settleEventLock = settleEventLock.then(async () => {
    const logs = readEventLogs();
    let found = false;
    for (let i = logs.length - 1; i >= 0; i--) {
      if (logs[i].type === "analysis" && logs[i].slug === market.slug && !logs[i].result) {
        const aiDirection = logs[i].analysis.direction;
        const won = aiDirection === actualDir;
        logs[i].result = {
          actualDirection: actualDir,
          resolvedBy: "polymarket",
          won,
          settledAt: nowIso(),
        };
        // Rewrite log file
        ensureLogDir();
        fs.writeFileSync(EVENT_LOG_FILE, logs.map(l => JSON.stringify(l)).join("\n") + "\n");
        found = true;

        console.log(`[BTC5m] 结算: ${market.slug} | AI: ${aiDirection} | 实际: ${actualDir} | ${won ? "✅ 胜" : "❌ 负"}`);

        if (broadcastFn) {
          broadcastFn({
            type: "btc5mSettlement",
            slug: market.slug,
            result: logs[i].result,
          });
        }
        break;
      }
    }
    if (!found) {
      console.log(`[BTC5m] 结算: ${market.slug} 未找到对应分析记录`);
    }
  }));
}

// Periodic settlement check (every 60 seconds)
let _settlementRunning = false;
function startSettlementChecker() {
  if (settlementCheckTimer) { clearInterval(settlementCheckTimer); }
  settlementCheckTimer = setInterval(async () => {
    if (_settlementRunning) return;
    _settlementRunning = true;
    try {
      const pending = readEventLogs().filter(l => l.type === "analysis" && !l.result);
      for (const entry of pending) {
        const slugMatch = entry.slug?.match(/-(\d+)$/);
        if (slugMatch) {
          const startSec = Number(slugMatch[1]);
          const endMs = (startSec + 5 * 60) * 1000;
          if (Date.now() > endMs + 30000) {
            console.log(`[BTC5m] 定期结算检查: ${entry.slug}`);
            await settleEvent({ slug: entry.slug });
          }
        }
      }
    } catch (err) {
      console.warn(`[BTC5m] 定期结算检查失败: ${err.message}`);
    } finally {
      _settlementRunning = false;
    }
  }, 60 * 1000);
}

// Get statistics from event logs
function getEventStats() {
  const logs = readEventLogs();
  const settled = logs.filter(l => l.type === "analysis" && l.result);
  const total = settled.length;
  const wins = settled.filter(l => l.result.won).length;
  const losses = total - wins;
  const winRate = total > 0 ? (wins / total * 100).toFixed(1) : "N/A";

  // By suggestedAction
  const byAction = {};
  for (const l of settled) {
    const action = l.analysis.suggestedAction || "unknown";
    if (!byAction[action]) byAction[action] = { total: 0, wins: 0 };
    byAction[action].total++;
    if (l.result.won) byAction[action].wins++;
  }

  // By marketState
  const byState = {};
  for (const l of settled) {
    const state = l.analysis.marketState || l.indicators?.marketState || "unknown";
    if (!byState[state]) byState[state] = { total: 0, wins: 0 };
    byState[state].total++;
    if (l.result.won) byState[state].wins++;
  }

  // By confidence range
  const byConfidence = { low: { total: 0, wins: 0 }, mid: { total: 0, wins: 0 }, high: { total: 0, wins: 0 } };
  for (const l of settled) {
    const conf = l.analysis.confidence;
    const bucket = conf < 0.55 ? "low" : conf < 0.7 ? "mid" : "high";
    byConfidence[bucket].total++;
    if (l.result.won) byConfidence[bucket].wins++;
  }

  // Recent 10
  const recent = settled.slice(-10).map(l => ({
    slug: l.slug,
    direction: l.analysis.direction,
    confidence: l.analysis.confidence,
    edge: l.analysis.edge,
    suggestedAction: l.analysis.suggestedAction,
    reasoning: l.analysis.reasoning,
    actual: l.result.actualDirection,
    won: l.result.won,
    priceChange: l.result.priceChange,
  }));

  // Pending (not yet settled)
  const pending = logs.filter(l => l.type === "analysis" && !l.result).map(l => ({
    slug: l.slug,
    direction: l.analysis.direction,
    confidence: l.analysis.confidence,
    analyzedAt: l.analyzedAt,
  }));

  return {
    total,
    wins,
    losses,
    winRate,
    byAction,
    byState,
    byConfidence,
    recent,
    pending,
    updatedAt: nowIso(),
  };
}

// ─── Window Phase ───────────────────────────────────────────────────────────
// Returns: { phase, elapsed, remaining, elapsedPct }
// phase: "early" (0-1min) | "optimal" (1-4min) | "late" (4-5min) | "expired"
function getWindowPhase(market) {
  if (!market?.slug) return { phase: "unknown", elapsed: 0, remaining: 0, elapsedPct: 0 };
  const match = market.slug.match(/-(\d+)$/);
  if (!match) return { phase: "unknown", elapsed: 0, remaining: 0, elapsedPct: 0 };
  const startMs = Number(match[1]) * 1000;
  const endMs = startMs + 5 * 60 * 1000;
  const now = Date.now();
  const elapsed = now - startMs;
  const remaining = endMs - now;
  const total = 5 * 60 * 1000;
  const elapsedPct = Math.max(0, Math.min(100, (elapsed / total) * 100));

  if (remaining <= 0) return { phase: "expired", elapsed: 0, remaining: 0, elapsedPct: 100 };
  if (elapsed < 0) return { phase: "upcoming", elapsed: 0, remaining: Math.round(remaining / 1000), elapsedPct: 0 };
  if (elapsed < 1 * 60 * 1000) return { phase: "early", elapsed: Math.round(elapsed / 1000), remaining: Math.round(remaining / 1000), elapsedPct };
  if (remaining < 1 * 60 * 1000) return { phase: "late", elapsed: Math.round(elapsed / 1000), remaining: Math.round(remaining / 1000), elapsedPct };
  return { phase: "optimal", elapsed: Math.round(elapsed / 1000), remaining: Math.round(remaining / 1000), elapsedPct };
}

// ─── Fetch utility ──────────────────────────────────────────────────────────
async function fetchJson(url, options = {}) {
  const fetch = (await import("node-fetch")).default;
  const timeoutMs = Number(options.timeoutMs || 20000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`请求超时 ${timeoutMs}ms`)), timeoutMs);
  try {
    const res = await fetch(url, {
      ...options,
      agent,
      signal: controller.signal,
      headers: { accept: "application/json", ...(options.headers || {}) },
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`HTTP ${res.status} ${url}\n${text.slice(0, 500)}`);
    const json = JSON.parse(text);
    return json;
  } catch (err) {
    if (err?.name === "AbortError") throw new Error(`请求超时 ${timeoutMs}ms`);
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

// ─── Market Discovery ───────────────────────────────────────────────────────
async function refreshMarket() {
  // Method 1: Manual config from .env
  if (BTC5M_CONDITION_ID && BTC5M_UP_TOKEN_ID && BTC5M_DOWN_TOKEN_ID) {
    activeMarket = {
      conditionId: BTC5M_CONDITION_ID,
      upTokenId: BTC5M_UP_TOKEN_ID,
      downTokenId: BTC5M_DOWN_TOKEN_ID,
      question: "BTC Up or Down 5m",
      outcomes: ["Up", "Down"],
      source: "manual",
      discoveredAt: nowIso(),
    };
    marketCacheTime = Date.now();
    scheduleMarketRefresh();
    stopPricePoller(); startPricePoller();
    return activeMarket;
  }

  // Method 2: Gamma API — direct slug lookup (most reliable)
  // Slug pattern: btc-updown-5m-{unix_seconds}
  // Try current ± 3 windows (15 min range)
  try {
    const nowSec = Math.floor(Date.now() / 1000);
    const currentWindow = Math.floor(nowSec / 300) * 300; // floor to nearest 5 min
    const offsets = [0, 300, -300, 600, -600, 900, -900];
    for (const offset of offsets) {
      const windowTs = currentWindow + offset;
      const slug = `btc-updown-5m-${windowTs}`;
      try {
        const data = await fetchJson(
          `https://gamma-api.polymarket.com/events?slug=${encodeURIComponent(slug)}`,
          { timeoutMs: 8000 }
        );
        const ev = Array.isArray(data) ? data[0] : null;
        if (!ev || !ev.markets || !ev.markets.length) continue;

        // Find first non-closed, active market
        const m = ev.markets.find(mk => !mk.closed && mk.active !== false);
        if (!m) { console.log(`[BTC5m] slug ${slug} 找到但市场已关闭`); continue; }

        const tokens = parseMaybeJson(m.clobTokenIds) || parseMaybeJson(m.clob_token_ids) || [];
        if (tokens.length < 2) continue;

        // Time validation: skip if market is expired or too far in the future
        const marketStartMs = windowTs * 1000;
        const marketEndMs = marketStartMs + 5 * 60 * 1000;
        const now = Date.now();
        if (now > marketEndMs + 5 * 60 * 1000) {
          console.log(`[BTC5m] slug ${slug} 已过期 ${Math.round((now - marketEndMs) / 60000)}min，跳过`);
          continue;
        }
        if (marketStartMs > now + 30 * 60 * 1000) {
          console.log(`[BTC5m] slug ${slug} 距开始 ${Math.round((marketStartMs - now) / 60000)}min，跳过`);
          continue;
        }

        const outcomes = parseMaybeJson(m.outcomes) || parseMaybeJson(m.shortOutcomes) || ["Up", "Down"];
        const prices = parseMaybeJson(m.outcomePrices) || [];
        const upIdx = outcomes.findIndex(o => /up|yes/i.test(o));

        // Calculate phase for logging
        const elapsed = Math.round((now - marketStartMs) / 1000);
        const remaining = Math.round((marketEndMs - now) / 1000);
        console.log(`[BTC5m] 发现市场 ${slug} | 已过 ${elapsed}s | 剩余 ${remaining}s`);

        activeMarket = {
          conditionId: m.condition_id || m.conditionId,
          upTokenId: tokens[upIdx >= 0 ? upIdx : 0] || tokens[0],
          downTokenId: tokens[upIdx >= 0 ? 1 - upIdx : 1] || tokens[1],
          question: m.question || ev.title,
          outcomes,
          upPrice: prices[upIdx >= 0 ? upIdx : 0] || null,
          downPrice: prices[upIdx >= 0 ? 1 - upIdx : 1] || null,
          slug: ev.slug,
          source: "gamma-slug",
          discoveredAt: nowIso(),
        };
        marketCacheTime = Date.now();
        scheduleMarketRefresh();
        stopPricePoller(); startPricePoller();
        return activeMarket;
      } catch (err) {
        console.log(`[BTC5m] slug ${slug} 查询失败: ${err.message}`);
      }
    }
  } catch (err) {
    console.warn("[BTC5m] Gamma slug 查询失败:", err.message);
  }

  // Method 3: Gamma API — fallback: search active events list
  try {
    const events = await fetchJson(
      `https://gamma-api.polymarket.com/events?limit=200&active=true&closed=false&order=startDate&ascending=false`,
      { timeoutMs: 15000 }
    );
    const evItems = Array.isArray(events) ? events : [];
    const now = Date.now();
    const btc5mEvents = evItems.filter(e => {
      const slug = (e.slug || "").toLowerCase();
      return slug.startsWith("btc-updown-5m-") && e.markets && e.markets.length > 0;
    });
    console.log(`[BTC5m] Method 3: 找到 ${btc5mEvents.length} 个 BTC 5m 事件`);
    let bestMarket = null;
    let bestScore = Infinity;
    for (const ev of btc5mEvents) {
      const slugMatch = (ev.slug || "").match(/-(\d+)$/);
      if (!slugMatch) continue;
      const startMs = Number(slugMatch[1]) * 1000;
      const endMs = startMs + 5 * 60 * 1000;
      // Skip expired (>5min past) or too far future (>30min)
      if (now > endMs + 5 * 60 * 1000) continue;
      if (startMs > now + 30 * 60 * 1000) continue;
      for (const m of ev.markets) {
        if (m.closed || m.active === false) continue;
        const tokens = parseMaybeJson(m.clobTokenIds) || parseMaybeJson(m.clob_token_ids) || [];
        if (tokens.length < 2) continue;
        let score;
        if (startMs <= now && now <= endMs) {
          score = 0;
        } else if (startMs > now) {
          score = startMs - now;
        } else {
          score = (now - endMs) + 1e9;
        }
        if (score < bestScore) {
          bestScore = score;
          const outcomes = parseMaybeJson(m.outcomes) || parseMaybeJson(m.shortOutcomes) || ["Up", "Down"];
          const prices = parseMaybeJson(m.outcomePrices) || [];
          const upIdx = outcomes.findIndex(o => /up|yes/i.test(o));
          bestMarket = {
            conditionId: m.condition_id || m.conditionId,
            upTokenId: tokens[upIdx >= 0 ? upIdx : 0] || tokens[0],
            downTokenId: tokens[upIdx >= 0 ? 1 - upIdx : 1] || tokens[1],
            question: m.question || ev.title,
            outcomes,
            upPrice: prices[upIdx >= 0 ? upIdx : 0] || null,
            downPrice: prices[upIdx >= 0 ? 1 - upIdx : 1] || null,
            slug: ev.slug,
            source: "gamma-event",
            discoveredAt: nowIso(),
          };
        }
      }
    }
    if (bestMarket) {
      console.log(`[BTC5m] Method 3 发现市场 ${bestMarket.slug}`);
      activeMarket = bestMarket;
      marketCacheTime = Date.now();
      scheduleMarketRefresh();
      stopPricePoller(); startPricePoller();
      return activeMarket;
    }
  } catch (err) {
    console.warn("[BTC5m] Gamma 事件搜索失败:", err.message);
  }

  // Method 3: CLOB sampling-markets search
  try {
    let cursor = "MA=="; // base64 of "0" — initial cursor per CLOB client convention
    for (let page = 0; page < 20 && cursor; page++) {
      const data = await fetchJson(`${CLOB_HOST}/sampling-markets?next_cursor=${encodeURIComponent(cursor)}`);
      const items = data?.data || [];
      if (!items.length) break;
      cursor = data?.next_cursor;
      if (!cursor || cursor === null || cursor === "LTE=") break;
      for (const m of items) {
        const q = String(m.question || "").toLowerCase();
        // Match BTC 5-minute style markets
        if ((q.includes("btc") || q.includes("bitcoin")) &&
            (q.includes("up or down") || q.includes("higher or lower") || q.includes("5 min") || q.includes("5-minute"))) {
          const outcomes = m.tokens || [];
          if (outcomes.length >= 2 && !m.closed) {
            const upTok = outcomes.find(t => /up|yes/i.test(String(t.outcome || "")));
            const downTok = outcomes.find(t => /down|no/i.test(String(t.outcome || "")));
            activeMarket = {
              conditionId: m.condition_id,
              upTokenId: upTok?.token_id || outcomes[0]?.token_id,
              downTokenId: downTok?.token_id || outcomes[1]?.token_id,
              question: m.question,
              outcomes: outcomes.map(t => t.outcome),
              source: "clob",
              discoveredAt: nowIso(),
            };
            marketCacheTime = Date.now();
            scheduleMarketRefresh();
            stopPricePoller(); startPricePoller();
            return activeMarket;
          }
        }
      }
    }
  } catch (err) {
    console.warn("[BTC5m] CLOB sampling 搜索失败:", err.message);
  }


  if (activeMarket) scheduleMarketRefresh();
  return null;
}

function getActiveMarket() {
  if (activeMarket && Date.now() - marketCacheTime < MARKET_CACHE_TTL) return activeMarket;
  return activeMarket; // may be stale but still return it
}

function parseMaybeJson(v) {
  if (Array.isArray(v)) return v;
  if (typeof v === "string") { try { return JSON.parse(v); } catch { return null; } }
  return null;
}

// ─── Auto-refresh market when 5m window expires ─────────────────────────────
function scheduleMarketRefresh() {
  if (marketRefreshTimer) { clearTimeout(marketRefreshTimer); marketRefreshTimer = null; }
  if (!activeMarket?.slug) return;
  // Slug format: btc-updown-5m-{unix_seconds}
  const match = activeMarket.slug.match(/-(\d+)$/);
  if (!match) return;
  const startSec = Number(match[1]);
  const endMs = (startSec + 5 * 60) * 1000; // +5 minutes
  const delayMs = endMs - Date.now() + 5000; // +5s buffer after expiry
  if (delayMs <= 0) return; // already expired
  console.log(`[BTC5m] ${Math.round(delayMs / 1000)}s 后自动刷新下一个 5m 窗口`);
  marketRefreshTimer = setTimeout(async () => {
    try {
      await refreshMarket();
      if (broadcastFn && activeMarket) {
        broadcastFn({ type: "btc5mMarket", market: activeMarket });
      }
    } catch (err) {
      console.warn(`[BTC5m] 市场刷新失败: ${err.message}`);
    }
    // 即使刷新失败也尝试调度分析（用现有 activeMarket 或墙钟）
    scheduleAutoAnalysis();
    scheduleMarketRefresh(); // schedule next refresh
  }, delayMs);
}

// ─── BTC Price & Klines ─────────────────────────────────────────────────────
async function fetchBtcKlines(interval = "1m", limit = 60, retries = 3) {
  const url = `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=${interval}&limit=${limit}`;
  for (let i = 0; i < retries; i++) {
    try {
      const data = await fetchJson(url, { timeoutMs: 15000 });
      return data;
    } catch (err) {
      if (i < retries - 1) {
        console.warn(`[BTC5m] Binance API 失败 (${i + 1}/${retries}): ${err.message}，2s 后重试`);
        await new Promise(r => setTimeout(r, 2000));
      } else {
        throw err;
      }
    }
  }
}

// ─── Technical Indicators (pure JS) ─────────────────────────────────────────
function ema(prices, period) {
  if (!prices.length) return 0;
  const k = 2 / (period + 1);
  let e = prices[0];
  for (let i = 1; i < prices.length; i++) e = prices[i] * k + e * (1 - k);
  return e;
}

function rsi(closes, period = 14) {
  if (closes.length <= period) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  if (losses === 0) return 100;
  return 100 - 100 / (1 + gains / losses);
}

function macd(closes) {
  const ema12 = ema(closes, 12);
  const ema26 = ema(closes, 26);
  const dif = ema12 - ema26;
  return { dif, ema12, ema26 };
}

function average(arr) {
  if (!arr.length) return 0;
  return arr.reduce((s, v) => s + v, 0) / arr.length;
}

function calculateIndicators(klines) {
  // Filter out malformed kline entries
  const safe = Array.isArray(klines) ? klines.filter(k => Array.isArray(k) && k.length >= 6) : [];
  if (safe.length === 0) {
    return { currentPrice: 0, priceChange5m: 0, rsi7: 50, rsi14: 50, emaAlignment: 'mixed', macdDif: 0, macdSlope: 0, macdTrend: 'neutral', volumeRatio: 1, hasVolumeSpike: false, volPriceAlign: '中性', upCandles: 0, downCandles: 0, consecUp: 0, consecDown: 0, high5: 0, low5: 0, rangePosition: 50, pricePath: 'flat', volatility5m: 0, volatilityRatio: 1, lastCandle: null, klinesCount: 0, latestTime: '', rsi7Percentile: 50 };
  }
  const closes = safe.map(k => parseFloat(k[4]));
  const volumes = safe.map(k => parseFloat(k[5]));
  const highs = safe.map(k => parseFloat(k[2]));
  const lows = safe.map(k => parseFloat(k[3]));
  const opens = safe.map(k => parseFloat(k[1]));

  const last = closes[closes.length - 1];
  const prev = closes[closes.length - 2] || last;
  const rsi7 = rsi(closes, 7);
  const rsi14 = rsi(closes, 14);
  const ema5 = ema(closes.slice(-20), 5);
  const ema13 = ema(closes.slice(-30), 13);
  const ema21 = ema(closes.slice(-40), 21);
  const macdVal = macd(closes);
  const rVol = average(volumes.slice(-5));
  const pVol = average(volumes.slice(-15, -5));
  const volRatio = pVol > 0 ? rVol / pVol : 1;

  const last5 = safe.slice(-5);
  let upCandles = 0, downCandles = 0;
  last5.forEach(k => { if (parseFloat(k[4]) >= parseFloat(k[1])) upCandles++; else downCandles++; });

  const high5 = Math.max(...highs.slice(-5));
  const low5 = Math.min(...lows.slice(-5));

  // Last candle analysis
  const lastK = safe[safe.length - 1];
  const lastBody = Math.abs(parseFloat(lastK[4]) - parseFloat(lastK[1]));
  const lastUpperWick = parseFloat(lastK[2]) - Math.max(parseFloat(lastK[4]), parseFloat(lastK[1]));
  const lastLowerWick = Math.min(parseFloat(lastK[4]), parseFloat(lastK[1])) - parseFloat(lastK[3]);

  // === NEW: Enhanced indicators for professional prompt ===

  // MACD DIF slope (compare current DIF with 3 candles ago)
  const macd3ago = macd(closes.slice(0, -3));
  const macdSlope = macdVal.dif - macd3ago.dif;

  // RSI(7) percentile in recent 60 candles (where does current RSI sit in recent range)
  const rsi7Series = [];
  for (let i = 14; i < closes.length; i++) {
    rsi7Series.push(rsi(closes.slice(0, i + 1), 7));
  }
  const rsi7Min = Math.min(...rsi7Series);
  const rsi7Max = Math.max(...rsi7Series);
  const rsi7Percentile = rsi7Max > rsi7Min ? ((rsi7 - rsi7Min) / (rsi7Max - rsi7Min) * 100) : 50;

  // Volume spike detection: any candle in last 5 min with volume > 2x avg of 20
  const vol20avg = average(volumes.slice(-25, -5));
  const last5Vols = volumes.slice(-5);
  const volumeSpikes = last5Vols.map((v, i) => ({
    index: safe.length - 5 + i,
    volume: v,
    ratio: vol20avg > 0 ? v / vol20avg : 0,
    isSpike: v > vol20avg * 2,
    direction: closes[safe.length - 5 + i] >= opens[safe.length - 5 + i] ? "up" : "down",
  }));
  const hasVolumeSpike = volumeSpikes.some(s => s.isSpike);

  // Price path classification (last 5 candles)
  const closes5 = closes.slice(-5);
  const totalMove = closes5[closes5.length - 1] - closes5[0];
  const totalRange = Math.max(...highs.slice(-5)) - Math.min(...lows.slice(-5));
  const netMove = Math.abs(totalMove);
  const volatility5m = totalRange;
  // Classify: straight (netMove > 60% of range), grind (30-60%), chop (<30%)
  let pricePath;
  if (totalRange === 0) pricePath = "flat";
  else if (netMove / totalRange > 0.6) pricePath = totalMove > 0 ? "straight_up" : "straight_down";
  else if (netMove / totalRange > 0.3) pricePath = totalMove > 0 ? "grind_up" : "grind_down";
  else pricePath = "chop";

  // Volatility comparison: current 5m range vs average of previous 4 windows
  const avgRange4 = [];
  for (let w = 1; w <= 4; w++) {
    const s = Math.max(0, closes.length - 5 * (w + 1));
    const e = closes.length - 15 * w;
    if (s < e) {
      avgRange4.push(Math.max(...highs.slice(s, e)) - Math.min(...lows.slice(s, e)));
    }
  }
  const avgVolatility = average(avgRange4);
  const volatilityRatio = avgVolatility > 0 ? volatility5m / avgVolatility : 1;

  // Consecutive candle count
  let consecUp = 0, consecDown = 0;
  for (let i = closes.length - 1; i >= 0; i--) {
    if (closes[i] >= opens[i]) { if (consecDown > 0) break; consecUp++; }
    else { if (consecUp > 0) break; consecDown++; }
  }

  // Last 3 candles: volume-price alignment
  const last3 = safe.slice(-3);
  let volPriceAlign = "mixed";
  const last3Moves = last3.map(k => parseFloat(k[4]) - parseFloat(k[1]));
  const last3Vols = last3.map(k => parseFloat(k[5]));
  const allUp = last3Moves.every(m => m > 0);
  const allDown = last3Moves.every(m => m < 0);
  const volIncreasing = last3Vols[2] > last3Vols[1] && last3Vols[1] > last3Vols[0];
  const volDecreasing = last3Vols[2] < last3Vols[1] && last3Vols[1] < last3Vols[0];
  if (allUp && volIncreasing) volPriceAlign = "价涨量增";
  else if (allUp && volDecreasing) volPriceAlign = "价涨量缩";
  else if (allDown && volIncreasing) volPriceAlign = "价跌量增";
  else if (allDown && volDecreasing) volPriceAlign = "价跌量缩";

  // --- Compute marketState from indicators (code-driven, not AI self-classified) ---
  const emaAlignment = ema5 > ema13 && ema13 > ema21 ? "bullish" : ema5 < ema13 && ema13 < ema21 ? "bearish" : "mixed";
  const rangePositionNum = (last - low5) / (high5 - low5 || 1) * 100;
  let marketState;
  if ((pricePath === "chop" || pricePath === "flat") && emaAlignment === "mixed") {
    marketState = "noise";
  } else if (volatilityRatio < 0.5 && (rangePositionNum > 85 || rangePositionNum < 15)) {
    marketState = "breakout_ready";
  } else if ((pricePath.includes("up") || pricePath.includes("down")) && emaAlignment !== "mixed") {
    const pathDir = pricePath.includes("up") ? "bullish" : "bearish";
    marketState = pathDir === emaAlignment ? "trending" : "mean_reverting";
  } else if (pricePath.includes("up") || pricePath.includes("down")) {
    marketState = "mean_reverting";
  } else {
    marketState = "noise";
  }

  // === NEW: Top Trader Indicators ===

  // VCP (Volatility Contraction Pattern) - Minervini
  // 3 consecutive ranges, each should be < 70% of previous
  const vcpRanges = [];
  for (let w = 0; w < 3; w++) {
    const start = Math.max(0, closes.length - 10 * (w + 1));
    const end = closes.length - 10 * w;
    if (start < end) {
      vcpRanges.push(Math.max(...highs.slice(start, end)) - Math.min(...lows.slice(start, end)));
    }
  }
  const vcpDetected = vcpRanges.length === 3 &&
    vcpRanges[1] < vcpRanges[0] * 0.7 &&
    vcpRanges[2] < vcpRanges[1] * 0.7;
  const vcpRatio = vcpRanges.length === 3 ? +(vcpRanges[2] / vcpRanges[0]).toFixed(3) : 1;

  // Box Breakout (Darvas) - last 30 candles define the box
  const boxPeriod = Math.min(30, closes.length);
  const boxHigh = Math.max(...highs.slice(-boxPeriod));
  const boxLow = Math.min(...lows.slice(-boxPeriod));
  const boxRange = boxHigh - boxLow;
  const lastClose = closes[closes.length - 1];
  const prevClose = closes[closes.length - 2] || lastClose;
  let boxBreakout = "none";
  if (lastClose > boxHigh && prevClose <= boxHigh) boxBreakout = "up";
  else if (lastClose < boxLow && prevClose >= boxLow) boxBreakout = "down";
  else if (lastClose > boxHigh) boxBreakout = "above";
  else if (lastClose < boxLow) boxBreakout = "below";

  // Pivot Points - find significant swing highs/lows (reversals of 5+ candles)
  const pivots = [];
  for (let i = 3; i < closes.length - 3; i++) {
    const isSwingHigh = highs[i] >= Math.max(...highs.slice(Math.max(0, i - 5), i)) &&
                        highs[i] >= Math.max(...highs.slice(i + 1, Math.min(closes.length, i + 6)));
    const isSwingLow = lows[i] <= Math.min(...lows.slice(Math.max(0, i - 5), i)) &&
                       lows[i] <= Math.min(...lows.slice(i + 1, Math.min(closes.length, i + 6)));
    if (isSwingHigh) pivots.push({ type: "high", price: highs[i] });
    if (isSwingLow) pivots.push({ type: "low", price: lows[i] });
  }
  const recentPivots = pivots.slice(-6); // keep last 6
  const nearestPivot = recentPivots.reduce((nearest, p) => {
    const dist = Math.abs(last - p.price);
    return dist < nearest.dist ? { ...p, dist } : nearest;
  }, { type: null, price: 0, dist: Infinity });
  const pivotProximity = nearestPivot.dist < boxRange * 0.1 ? "at" :
                         nearestPivot.dist < boxRange * 0.25 ? "near" : "far";

  // Second Entry / Failed Break detection
  // Did the last 3 candles have a breakout that failed (returned inside box)?
  let secondEntry = "none";
  if (closes.length >= 5) {
    for (let i = closes.length - 4; i >= Math.max(0, closes.length - 6); i--) {
      const prevOutside = (highs[i] > boxHigh || lows[i] < boxLow);
      const returned = closes[i] <= boxHigh && closes[i] >= boxLow;
      if (prevOutside && returned) {
        // Failed break detected, current breakout in same direction is second entry
        if (lastClose > boxHigh) secondEntry = "long";
        else if (lastClose < boxLow) secondEntry = "short";
        break;
      }
    }
  }

  // ATR(14) for stop distance calculation
  const trs = [];
  for (let i = 1; i < closes.length; i++) {
    trs.push(Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    ));
  }
  const atr14 = trs.length >= 14 ? average(trs.slice(-14)) : (trs.length > 0 ? average(trs) : 1);

  return {
    currentPrice: last,
    priceChange1m: last - prev,
    priceChange1mPct: ((last - prev) / prev * 100).toFixed(4),
    priceChange5m: last - (closes[closes.length - 6] || last),
    priceChange5mFromOpen: last - closes[0],
    rsi7: +rsi7.toFixed(1),
    rsi14: +rsi14.toFixed(1),
    rsi7Percentile: +rsi7Percentile.toFixed(1),
    ema5: +ema5.toFixed(1),
    ema13: +ema13.toFixed(1),
    ema21: +ema21.toFixed(1),
    emaAlignment: ema5 > ema13 && ema13 > ema21 ? "bullish" : ema5 < ema13 && ema13 < ema21 ? "bearish" : "mixed",
    macdDif: +macdVal.dif.toFixed(2),
    macdSlope: +macdSlope.toFixed(4),
    macdTrend: macdVal.dif > 0 ? "bullish" : "bearish",
    volumeRatio: +volRatio.toFixed(2),
    hasVolumeSpike,
    volumeSpikes: volumeSpikes.filter(s => s.isSpike),
    volPriceAlign,
    upCandles,
    downCandles,
    consecUp,
    consecDown,
    high5,
    low5,
    rangePosition: rangePositionNum.toFixed(1),
    pricePath,
    volatility5m: +volatility5m.toFixed(1),
    volatilityRatio: +volatilityRatio.toFixed(2),
    marketState,
    // Top Trader Indicators
    vcpDetected,
    vcpRatio,
    boxHigh: +boxHigh.toFixed(1),
    boxLow: +boxLow.toFixed(1),
    boxBreakout,
    nearestPivot: nearestPivot.type ? { type: nearestPivot.type, price: +nearestPivot.price.toFixed(1) } : null,
    pivotProximity,
    secondEntry,
    atr14: +atr14.toFixed(1),
    lastCandle: {
      isUp: parseFloat(lastK[4]) >= parseFloat(lastK[1]),
      body: +lastBody.toFixed(1),
      upperWick: +lastUpperWick.toFixed(1),
      lowerWick: +lastLowerWick.toFixed(1),
      wickRatio: lastLowerWick > 0 ? +(lastUpperWick / lastLowerWick).toFixed(2) : 99,
      volume: +parseFloat(lastK[5]).toFixed(1),
    },
    klinesCount: safe.length,
    latestTime: new Date(safe[safe.length - 1][0]).toISOString(),
  };
}

// ─── AI Analysis ────────────────────────────────────────────────────────────
const BTC_ANALYSIS_SYSTEM_PROMPT = `OUTPUT ONLY VALID JSON. NO REASONING. NO EXPLANATION. NO OTHER TEXT.

你是BTC 5分钟交易员。预测下一根K线方向。

## 重要
你看到的评分是代码预计算的。你的任务是确认或微调（特别是结构和量能），不是重新分析所有数据。直接输出JSON。

## 规则
- 总分>=3且信心足→BUY_UP或BUY_DOWN
- 总分<3或信心不足→SKIP
- consecUp>=3做多=禁止, consecDown>=3做空=禁止
- 连续2根同方向后追=SKIP

## 输出（用短字段名）
{"d":"UP或DOWN","c":0.6,"e":0.5,"r":2.0,"i":"失效价","a":"BUY_UP/BUY_DOWN/SKIP","n":"≤20字","k":["信号"],"s":{"t":1,"s":1,"v":0,"p":0,"r":1}}

d=方向 c=信心 e=边缘 r=RR i=失效 a=动作 n=理由 k=因子 s=评分(t趋势s结构v量能p形态r风险)`;

// ─── Orderbook Fetch (for microstructure data) ──────────────────────────────
async function fetchOrderbook(tokenId) {
  try {
    const book = await getOrderBook(tokenId);
    const bids = (book?.bids || []).slice(0, 5).map(l => {
      const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
      const s = Array.isArray(l) ? Number(l[1]) : Number(l?.size ?? l?.s);
      return { price: p, size: s };
    }).filter(l => l.price > 0);
    const asks = (book?.asks || []).slice(0, 5).map(l => {
      const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
      const s = Array.isArray(l) ? Number(l[1]) : Number(l?.size ?? l?.s);
      return { price: p, size: s };
    }).filter(l => l.price > 0);

    const totalBidDepth = bids.reduce((s, b) => s + b.size, 0);
    const totalAskDepth = asks.reduce((s, a) => s + a.size, 0);
    const bestBid = bids[0]?.price || 0;
    const bestAsk = asks[0]?.price || 0;
    const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;

    return { bids, asks, bestBid, bestAsk, spread, totalBidDepth, totalAskDepth };
  } catch {
    return null;
  }
}

// ─── Time-to-expiry from slug ───────────────────────────────────────────────
function getTimeToExpiry(market) {
  if (!market?.slug) return null;
  const match = market.slug.match(/-(\d+)$/);
  if (!match) return null;
  const startSec = Number(match[1]);
  const endMs = (startSec + 5 * 60) * 1000;
  const remainMs = endMs - Date.now();
  if (remainMs <= 0) return 0;
  return Math.round(remainMs / 1000); // seconds
}

// ─── Pre-compute Scores (code-driven, reduces AI cognitive load) ─────────────
function countStructuralSignals(indicators, klines) {
  const safe = Array.isArray(klines) ? klines.filter(k => Array.isArray(k) && k.length >= 5) : [];
  if (safe.length < 10) return { bullish: 0, bearish: 0, aligned: false, dominant: 'mixed', count: 0, signals: [] };

  const kVal = (k, i) => (k && k[i] != null) ? parseFloat(k[i]) : NaN;
  const signals = [];
  let bullish = 0, bearish = 0;

  // Liquidity sweep
  const recent5 = safe.slice(-6, -1);
  const curHigh = kVal(safe[safe.length - 1], 2);
  const curLow = kVal(safe[safe.length - 1], 3);
  const curClose = kVal(safe[safe.length - 1], 4);
  const prevHigh5 = Math.max(...recent5.map(k => kVal(k, 2)));
  const prevLow5 = Math.min(...recent5.map(k => kVal(k, 3)));
  if (curHigh > prevHigh5 && curClose < prevHigh5) {
    signals.push('流动性扫描上方');
    bearish++;
  }
  if (curLow < prevLow5 && curClose > prevLow5) {
    signals.push('流动性扫描下方');
    bullish++;
  }

  // FVG
  for (let i = safe.length - 4; i < safe.length - 1; i++) {
    const k1h = kVal(safe[i], 2), k3l = kVal(safe[i + 2], 3);
    const k1l = kVal(safe[i], 3), k3h = kVal(safe[i + 2], 2);
    if (k3l > k1h && (k3l - k1h) > 5) { signals.push('看涨FVG'); bullish++; }
    if (k1l > k3h && (k1l - k3h) > 5) { signals.push('看跌FVG'); bearish++; }
  }

  // Order Block
  const bodies = safe.slice(-10).map(k => Math.abs(kVal(k, 4) - kVal(k, 1)));
  const avgBody = bodies.reduce((s, b) => s + b, 0) / bodies.length;
  for (let i = safe.length - 10; i < safe.length - 1; i++) {
    const body = Math.abs(kVal(safe[i], 4) - kVal(safe[i], 1));
    if (body > avgBody * 1.5 && avgBody > 0) {
      const isBull = kVal(safe[i], 4) > kVal(safe[i], 1);
      signals.push(isBull ? '看涨OB' : '看跌OB');
      if (isBull) bullish++; else bearish++;
    }
  }

  const dominant = bullish > bearish ? 'bullish' : bearish > bullish ? 'bearish' : 'mixed';
  return { bullish, bearish, aligned: bullish === 0 || bearish === 0, dominant, count: signals.length, signals: signals.slice(0, 3) };
}

function precomputeScores(indicators, orderbook, klines) {
  // 1. 趋势对齐 (0-2)
  let trend = 0;
  if (indicators.emaAlignment !== 'mixed') {
    trend = 1;
    if ((indicators.emaAlignment === 'bullish' && parseFloat(indicators.rangePosition) > 50) ||
        (indicators.emaAlignment === 'bearish' && parseFloat(indicators.rangePosition) < 50)) {
      trend = 2;
    }
  }

  // 2. 结构信号 (0-2)
  const struct = countStructuralSignals(indicators, klines);
  let structure = 0;
  if (struct.count >= 2 && struct.aligned) structure = 2;
  else if (struct.count >= 1) structure = 1;

  // 3. 量能确认 (0-2)
  let volume = 0;
  if (indicators.hasVolumeSpike && indicators.volPriceAlign.includes('增')) volume = 2;
  else if (indicators.hasVolumeSpike || indicators.volatilityRatio > 1.5) volume = 1;

  // 4. 突破形态 (0-2)
  let pattern = 0;
  if (indicators.secondEntry !== 'none') pattern = 2;
  else if (indicators.vcpDetected || indicators.boxBreakout === 'up' || indicators.boxBreakout === 'down') pattern = 2;
  else if (indicators.boxBreakout === 'above' || indicators.boxBreakout === 'below' || indicators.pivotProximity === 'at') pattern = 1;

  // 5. 风险回报 (0-2)
  let rr = 0;
  if (indicators.atr14 > 0) {
    rr = 2; // 有ATR就有止损距离，默认RR合理
  }

  const total = trend + structure + volume + pattern + rr;
  return { trend, structure, volume, pattern, rr, total, struct };
}

function buildAnalysisPrompt(indicators, marketContext, orderbook, klines) {
  // Helper: safely access kline array element
  const kVal = (kline, fieldIdx) => {
    if (!kline || kline[fieldIdx] === undefined || kline[fieldIdx] === null) return NaN;
    return parseFloat(kline[fieldIdx]);
  };

  // Pre-compute scores to reduce AI cognitive load
  const scores = precomputeScores(indicators, orderbook, klines);

  // Market state label
  const msLabel = { trending: '趋势', mean_reverting: '回归', breakout_ready: '突破', noise: '噪音' };
  const ms = msLabel[indicators.marketState] || indicators.marketState;

  // HTF bias (compact)
  const safeK = Array.isArray(klines) ? klines.filter(k => Array.isArray(k) && k.length >= 5) : [];
  let htf = '';
  if (safeK.length >= 15) {
    const cp = kVal(safeK[safeK.length - 1], 4);
    if (safeK.length >= 60) {
      // Use oldest available kline as ~1h reference
      const p60 = kVal(safeK[0], 4);
      const c60 = cp - p60;
      htf = `HTF1h:${c60 > 0 ? '+' : ''}${c60.toFixed(0)}`;
    } else {
      const p15 = kVal(safeK[safeK.length - 16], 4);
      const c15 = cp - p15;
      htf = `HTF15m:${c15 > 0 ? '+' : ''}${c15.toFixed(0)}`;
    }
  }

  // Build compact prompt (~500 chars)
  let prompt = `BTC 5m预测。状态:${ms}。价格${indicators.currentPrice.toFixed(0)} EMA:${indicators.emaAlignment}`;
  prompt += ` MACD:${indicators.macdDif.toFixed(0)}斜率${indicators.macdSlope > 0 ? '+' : ''}${indicators.macdSlope.toFixed(1)}`;
  prompt += ` RSI7:${indicators.rsi7}`;
  if (indicators.consecUp >= 2) prompt += ` 连涨${indicators.consecUp}`;
  else if (indicators.consecDown >= 2) prompt += ` 连跌${indicators.consecDown}`;
  prompt += `\n`;

  prompt += `评分T:${scores.trend}/2 S:${scores.structure}/2 V:${scores.volume}/2 P:${scores.pattern}/2 RR:${scores.rr}/2 总:${scores.total}/10\n`;

  if (scores.struct.signals.length > 0) {
    prompt += `结构:${scores.struct.signals.join(',')} 方向:${scores.struct.dominant}\n`;
  }

  prompt += `箱体:${indicators.boxLow.toFixed(0)}-${indicators.boxHigh.toFixed(0)} ${indicators.boxBreakout}`;
  prompt += ` VCP:${indicators.vcpDetected ? 'Y' : 'N'} SE:${indicators.secondEntry}`;
  prompt += ` Pivot:${indicators.pivotProximity}\n`;

  if (orderbook) {
    const r = orderbook.totalAskDepth > 0 ? (orderbook.totalBidDepth / orderbook.totalAskDepth) : 1;
    prompt += `盘口:${r > 1.3 ? '买厚' : r < 0.7 ? '卖厚' : '均衡'}`;
  }
  if (htf) prompt += ` ${htf}`;
  if (marketContext?.upPrice) {
    prompt += ` PM:U${Math.round(marketContext.upPrice * 100)}%`;
  }
  prompt += `\n`;

  prompt += `总分>=3→BUY。<3→SKIP。可微调±1分。输出JSON。`;

  return prompt;
}

function parseAiResponse(data) {
  try {
    const msg = data?.choices?.[0]?.message || {};

    // Try to find JSON in content first, then reasoning_content
    // MIMO: thinking in reasoning_content, answer in content
    // Some models: everything in content
    const fields = [msg.content, msg.reasoning_content].filter(Boolean);

    let jsonStr = null;
    for (const text of fields) {
      if (!text) continue;
      // Try markdown code block first
      const codeBlockMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (codeBlockMatch) { jsonStr = codeBlockMatch[1]; break; }
      // Find all opening braces and try to extract balanced JSON objects
      const openBraces = [];
      for (let i = 0; i < text.length; i++) {
        if (text[i] === "{") openBraces.push(i);
      }
      // Try from last to first
      for (let bi = openBraces.length - 1; bi >= 0; bi--) {
        const start = openBraces[bi];
        let depth = 0;
        for (let i = start; i < text.length; i++) {
          if (text[i] === "{") depth++;
          if (text[i] === "}") { depth--; if (depth === 0) {
            const candidate = text.slice(start, i + 1);
            try {
              const parsed = JSON.parse(candidate);
              if (parsed && (parsed.direction || parsed.d)) { jsonStr = candidate; break; }
            } catch {}
          }}
        }
        if (jsonStr) break;
      }
      if (jsonStr) break;
    }

    if (!jsonStr) {
      const preview = (msg.content || msg.reasoning_content || "").slice(0, 200);
      throw new Error(`未找到 JSON。预览: ${preview}`);
    }

    const result = JSON.parse(jsonStr);
    // Support short field names (d→direction, c→confidence, etc.)
    const dir = result.direction || result.d;
    if (!["UP", "DOWN"].includes(dir)) {
      throw new Error(`Invalid direction: ${dir}`);
    }
    const sb = result.scoreBreakdown || result.s || null;
    const totalScore = sb ? Object.values(sb).reduce((s, v) => s + (Number(v) || 0), 0) : null;
    return {
      direction: dir,
      confidence: Math.max(0, Math.min(1, Number(result.confidence || result.c) || 0.5)),
      reasoning: String(result.reasoning || result.n || ""),
      keyFactors: Array.isArray(result.keyFactors || result.k) ? (result.keyFactors || result.k) : [],
      edge: Number(result.edge || result.e) || null,
      rrRatio: Number(result.rrRatio || result.r) || null,
      invalidation: String(result.invalidation || result.i || ""),
      suggestedAction: result.suggestedAction || result.a || null,
      scoreBreakdown: sb,
      totalScore,
      model: data?.model || AI_MODEL,
      timestamp: nowIso(),
    };
  } catch (err) {
    throw new Error(`AI 响应解析失败: ${err.message}. Raw: ${JSON.stringify(data).slice(0, 300)}`);
  }
}

async function runAiAnalysis(forceRefresh = false, slugOverride = null) {
  if (!AI_API_KEY) throw new Error("BTC5M_AI_API_KEY 未设置");
  if (!AI_BASE_URL) throw new Error("BTC5M_AI_BASE_URL 未设置");
  console.log(`[BTC5m] runAiAnalysis: API=${AI_BASE_URL} model=${AI_MODEL} maxTokens=${AI_MAX_TOKENS} proxy=${PROXY_URL || 'none'}`);

  // Quick connectivity test (5s timeout)
  try {
    const testFetch = (await import("node-fetch")).default;
    const testUrl = AI_BASE_URL.replace(/\/$/, "");
    const testCtrl = new AbortController();
    const testTimer = setTimeout(() => testCtrl.abort(), 5000);
    const testRes = await testFetch(testUrl, { agent, method: "HEAD", signal: testCtrl.signal });
    clearTimeout(testTimer);
    console.log(`[BTC5m] API 连通性测试: HTTP ${testRes.status}`);
  } catch (testErr) {
    console.warn(`[BTC5m] API 连通性测试失败: ${testErr.message}，继续尝试...`);
  }

  // Use slugOverride if provided, otherwise fall back to activeMarket
  const slug = slugOverride || activeMarket?.slug;
  const cacheKey = slug || "unknown";
  const phaseInfo = slug ? getWindowPhase({ slug }) : getWindowPhase(activeMarket);

  // If in optimal phase and cached, return trusted result
  if (!forceRefresh && phaseInfo.phase === "optimal" && analysisCache.has(cacheKey)) {
    const cached = analysisCache.get(cacheKey);
    if (cached.phase === "optimal") {
      console.log(`[BTC5m] 返回最佳窗口分析 (cache hit, optimal)`);
      cached.analysis._cached = true;
      cached.analysis._phase = phaseInfo;
      lastAnalysis = cached.analysis;
      return cached.analysis;
    }
  }

  // If not in optimal phase, warn but still allow
  let phaseWarning = null;
  if (phaseInfo.phase === "upcoming") {
    phaseWarning = `预测窗口还未开始，结果仅供参考`;
  } else if (phaseInfo.phase === "early") {
    phaseWarning = `窗口刚开始（${Math.round(phaseInfo.elapsed / 60)}分钟），当前K线数据不足，预测下一根K线的结果仅供参考`;
  } else if (phaseInfo.phase === "late") {
    phaseWarning = `窗口即将结束（剩${Math.round(phaseInfo.remaining / 60)}分钟），分析时间紧张`;
  } else if (phaseInfo.phase === "expired") {
    throw new Error("当前窗口已过期，请等待下一个 5 分钟窗口");
  }

  // Fetch BTC data
  const klines = await fetchBtcKlines("1m", 60);
  const indicators = calculateIndicators(klines);

  // Fetch orderbook for microstructure data
  let orderbook = null;
  if (activeMarket?.upTokenId) {
    orderbook = await fetchOrderbook(activeMarket.upTokenId);
  }

  // Build prompt
  const prompt = buildAnalysisPrompt(indicators, activeMarket, orderbook, klines);

  // Call AI with retry on truncation and network errors
  const fetch = (await import("node-fetch")).default;
  const url = `${AI_BASE_URL.replace(/\/$/, "")}/chat/completions`;
  let data = null;
  let currentMaxTokens = AI_MAX_TOKENS;

  const ANALYSIS_DEADLINE_MS = 75_000; // 75s hard deadline (90s window - 15s buffer)
  const analysisStart = Date.now();
  const MAX_ATTEMPTS = 2; // 2 attempts to stay within time budget

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    const elapsed = Date.now() - analysisStart;
    if (elapsed > ANALYSIS_DEADLINE_MS) {
      console.log(`[BTC5m] 分析超时 (${elapsed}ms)，放弃本次分析`);
      throw new Error(`分析超时: ${elapsed}ms > ${ANALYSIS_DEADLINE_MS}ms`);
    }

    const remainingMs = ANALYSIS_DEADLINE_MS - elapsed;
    const attemptTimeout = Math.min(40_000, remainingMs - 5000); // 40s max per attempt, leave 5s buffer
    if (attemptTimeout < 10_000) {
      console.log(`[BTC5m] 剩余时间不足 (${remainingMs}ms)，放弃`);
      throw new Error(`剩余时间不足: ${remainingMs}ms`);
    }

    console.log(`[BTC5m] AI API 调用尝试 ${attempt + 1}/${MAX_ATTEMPTS} (maxTokens=${currentMaxTokens}, timeout=${attemptTimeout}ms, elapsed=${elapsed}ms)`);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(new Error(`API超时 ${attemptTimeout}ms`)), attemptTimeout);
    let resData = null;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${AI_API_KEY}`,
        },
        body: JSON.stringify({
          model: AI_MODEL,
          messages: [
            { role: "system", content: BTC_ANALYSIS_SYSTEM_PROMPT },
            { role: "user", content: prompt },
          ],
          max_tokens: currentMaxTokens,
          temperature: AI_TEMPERATURE,
          reasoning_effort: "low",
        }),
        agent,
        signal: controller.signal,
      });

      resData = await res.json();
      if (!res.ok) {
        if (res.status === 429 || res.status >= 500) {
          const delay = 1000; // 1s fixed delay for rate limit/server error
          console.log(`[BTC5m] AI API ${res.status}，${delay}ms 后重试 (${attempt + 1}/${MAX_ATTEMPTS})`);
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        throw new Error(`AI API HTTP ${res.status}: ${JSON.stringify(resData).slice(0, 300)}`);
      }

      const finishReason = resData?.choices?.[0]?.finish_reason;
      if (finishReason === "length") {
        currentMaxTokens = Math.min(currentMaxTokens + 1500, 8000);
        console.log(`[BTC5m] AI 响应被截断，增加 token 到 ${currentMaxTokens} 重试 (${attempt + 1}/5)`);
        continue;
      }
    } catch (err) {
      if (err.name === "AbortError") {
        console.log(`[BTC5m] AI API 超时/中止 (${attemptTimeout}ms): ${err.message || 'no reason'}，重试 (${attempt + 1}/${MAX_ATTEMPTS})`);
        continue;
      }
      console.error(`[BTC5m] AI API 请求异常 (${attempt + 1}/${MAX_ATTEMPTS}): ${err.message}`);
      await new Promise(r => setTimeout(r, 500));
      continue;
    } finally {
      clearTimeout(timeout);
    }

    // API call succeeded — try parsing JSON response
    try {
      const analysis = parseAiResponse(resData);
      analysis.indicators = indicators;
      analysis.market = activeMarket;
      analysis._phase = phaseInfo;
      if (phaseWarning) analysis._phaseWarning = phaseWarning;
      lastAnalysis = analysis;

      // Store in per-window cache (prefer optimal phase)
      const existing = analysisCache.get(cacheKey);
      if (!existing || (existing.phase !== "optimal" && phaseInfo.phase === "optimal")) {
        analysisCache.set(cacheKey, { analysis, phase: phaseInfo.phase });
      }
      // Evict old entries
      if (analysisCache.size > ANALYSIS_CACHE_MAX) {
        const oldest = analysisCache.keys().next().value;
        analysisCache.delete(oldest);
      }

      return analysis;
    } catch (parseErr) {
      // JSON parse failed — likely truncated response, retry with more tokens
      currentMaxTokens = Math.min(currentMaxTokens + 1000, 6000);
      console.log(`[BTC5m] AI 响应解析失败，增加 token 到 ${currentMaxTokens} 重试 (${attempt + 1}/${MAX_ATTEMPTS}): ${parseErr.message.slice(0, 100)}`);
      continue;
    }
  }

  throw new Error(`AI 请求失败（${MAX_ATTEMPTS}次重试后，耗时${Date.now() - analysisStart}ms）`);
}

// ─── Trading ────────────────────────────────────────────────────────────────
async function getOrderBook(tokenId) {
  const attempts = [
    `${CLOB_HOST}/book?token_id=${encodeURIComponent(tokenId)}`,
    `${CLOB_HOST}/orderbook?token_id=${encodeURIComponent(tokenId)}`,
  ];
  for (const url of attempts) {
    try { return await fetchJson(url); } catch {}
  }
  throw new Error("orderbook 获取失败");
}

// Lightweight price fetch — only fetch UP orderbook, derive DOWN from it
async function getPrices() {
  const market = getActiveMarket();
  if (!market) return null;
  const result = { slug: market.slug, up: null, down: null, btcPrice: null, updatedAt: nowIso() };

  try {
    const upBook = await getOrderBook(market.upTokenId);
    const bids = upBook?.bids || [];
    const asks = upBook?.asks || [];

    // Parse and sort: bids DESC, asks ASC — CLOB API 不保证排序
    const parsedBids = bids.map(l => {
      const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
      return Number.isFinite(p) && p > 0 ? p : 0;
    }).filter(p => p > 0).sort((a, b) => b - a);
    const parsedAsks = asks.map(l => {
      const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
      return Number.isFinite(p) && p > 0 ? p : 0;
    }).filter(p => p > 0).sort((a, b) => a - b);

    const bestBid = parsedBids[0] || 0;
    const bestAsk = parsedAsks[0] || 0;
    result.up = {
      bid: bestBid || null,
      ask: bestAsk || null,
      mid: bestBid && bestAsk ? +((bestBid + bestAsk) / 2).toFixed(4) : null
    };
    if (result.up.mid) {
      result.down = {
        bid: +(1 - bestAsk).toFixed(4),
        ask: +(1 - bestBid).toFixed(4),
        mid: +(1 - result.up.mid).toFixed(4)
      };
    }
  } catch {}

  return result;
}

async function executeTrade({ direction, amountUsd, mode }) {
  if (tradeInProgress) throw new Error("已有交易执行中，请稍后再试");
  tradeInProgress = true;
  try {
  const market = getActiveMarket();
  if (!market) throw new Error("没有找到活跃的 BTC 5m 市场，请先刷新市场");

  const tokenId = direction === "UP" ? market.upTokenId : market.downTokenId;
  if (!tokenId) throw new Error(`${direction} token ID 未知`);

  // Get current price from orderbook
  const book = await getOrderBook(tokenId);
  const asks = book?.asks || [];
  let bestAsk = null;
  for (const level of asks) {
    const p = Array.isArray(level) ? Number(level[0]) : Number(level?.price ?? level?.p);
    if (Number.isFinite(p) && p > 0) { bestAsk = p; break; }
  }
  if (!bestAsk || bestAsk <= 0) throw new Error(`${direction} 无可用 ask 价格`);

  const ex = require("./trade-executor");
  const result = await ex.createSignedOrderOrPost({
    mode: mode || "paper",
    tokenId,
    side: "BUY",
    price: bestAsk,
    amountUsd: Number(amountUsd) || BTC5M_DEFAULT_TRADE_USD,
    orderType: BTC5M_DEFAULT_ORDER_TYPE,
    confirmLive: mode === "live" ? "I_UNDERSTAND" : undefined,
    source: "btc5m_manual",
  });

  // Track position
  if (result.ok && result.mode !== "paper") {
    currentPosition = {
      tokenId,
      direction,
      entryPrice: bestAsk,
      shares: result.plan?.shares || 0,
      amountUsd: Number(amountUsd) || BTC5M_DEFAULT_TRADE_USD,
      entryTime: nowIso(),
      marketConditionId: market.conditionId,
      marketQuestion: market.question,
    };
    startPositionPoll();
  }

  return result;
  } finally { tradeInProgress = false; }
}

async function sellPosition({ mode } = {}) {
  if (tradeInProgress) throw new Error("已有交易执行中，请稍后再试");
  tradeInProgress = true;
  try {
  if (!currentPosition) throw new Error("无 BTC 5m 持仓可卖");
  const { tokenId, shares } = currentPosition;

  const book = await getOrderBook(tokenId);
  const bids = book?.bids || [];
  let bestBid = null;
  for (const level of bids) {
    const p = Array.isArray(level) ? Number(level[0]) : Number(level?.price ?? level?.p);
    if (Number.isFinite(p) && p > 0) { bestBid = p; break; }
  }
  if (!bestBid || bestBid < 0.01) throw new Error("bid 不足，无法卖出");

  const limitPrice = Math.max(0.01, Number((bestBid - BTC5M_SELL_SLIPPAGE).toFixed(4)));

  const ex = require("./trade-executor");
  const result = await ex.createSignedOrderOrPost({
    mode: mode || "live",
    tokenId,
    side: "SELL",
    price: limitPrice,
    shares,
    amountUsd: shares * limitPrice,
    orderType: BTC5M_DEFAULT_ORDER_TYPE,
    confirmLive: mode === "live" ? "I_UNDERSTAND" : undefined,
    source: "btc5m_sell",
  });

  if (result.ok) {
    currentPosition = null;
    stopPositionPoll();
  }
  return result;
  } finally { tradeInProgress = false; }
}

// ─── Position Monitoring ────────────────────────────────────────────────────
async function getPositionStatus() {
  if (!currentPosition) return null;

  try {
    const book = await getOrderBook(currentPosition.tokenId);
    let currentPrice = null;
    const bids = book?.bids || [];
    for (const level of bids) {
      const p = Array.isArray(level) ? Number(level[0]) : Number(level?.price ?? level?.p);
      if (Number.isFinite(p) && p > 0) { currentPrice = p; break; }
    }

    const entryValue = currentPosition.shares * currentPosition.entryPrice;
    const currentValue = currentPrice ? currentPosition.shares * currentPrice : null;
    const pnl = currentValue !== null ? currentValue - entryValue : null;
    const pnlPct = entryValue > 0 && pnl !== null ? (pnl / entryValue * 100) : null;

    // Detect resolution
    let resolved = false;
    let resolution = null;
    if (currentPrice !== null) {
      if (currentPrice >= 0.95) { resolved = true; resolution = "WIN"; }
      if (currentPrice <= 0.05) { resolved = true; resolution = "LOSE"; }
    }

    return {
      ...currentPosition,
      currentPrice,
      entryValue: +entryValue.toFixed(2),
      currentValue: currentValue !== null ? +currentValue.toFixed(2) : null,
      pnl: pnl !== null ? +pnl.toFixed(2) : null,
      pnlPct: pnlPct !== null ? +pnlPct.toFixed(1) : null,
      resolved,
      resolution,
      updatedAt: nowIso(),
    };
  } catch (err) {
    return { ...currentPosition, error: err.message, updatedAt: nowIso() };
  }
}

function startPositionPoll() {
  stopPositionPoll();
  positionPollTimer = setInterval(async () => {
    if (!currentPosition) { stopPositionPoll(); return; }
    try {
      const status = await getPositionStatus();
      if (status && broadcastFn) {
        broadcastFn({ type: "btc5mPosition", position: status });
      }
      if (status?.resolved) {
        currentPosition = null;
        stopPositionPoll();
      }
    } catch (e) { console.warn(`[BTC5m] 持仓轮询失败: ${e.message}`); }
  }, 10_000);
}

function stopPositionPoll() {
  if (positionPollTimer) { clearInterval(positionPollTimer); positionPollTimer = null; }
}

// ─── Real-time Price WebSocket (CLOB) ────────────────────────────────────────
function connectClobWs() {
  if (clobWs && (clobWs.readyState === WebSocket.OPEN || clobWs.readyState === WebSocket.CONNECTING)) return;

  console.log(`[BTC5m] CLOB WS 连接中...`);
  try {
    const wsOpts = {
      headers: { "Origin": "https://polymarket.com" }
    };
    if (agent) wsOpts.agent = agent;
    clobWs = new WebSocket(CLOB_WS_URL, wsOpts);
  } catch (err) {
    console.warn("[BTC5m] CLOB WS 创建失败:", err.message);
    scheduleClobWsReconnect();
    return;
  }

  clobWs.on("open", () => {
    console.log("[BTC5m] CLOB WS 已连接");
    clobWsConnectedAt = Date.now();
    // Subscribe to active market
    if (activeMarket?.upTokenId) {
      clobWsSubscribe(activeMarket.upTokenId);
    }
  });

  clobWs.on("message", (raw) => {
    try {
      const msg = JSON.parse(String(raw));
      handleClobWsMessage(msg);
    } catch (e) { console.warn(`[BTC5m] CLOB WS 消息解析失败: ${e.message}`); }
  });

  clobWs.on("close", (code, reason) => {
    console.log(`[BTC5m] CLOB WS 断开 (${code})`);
    clobWsSubscribedToken = null;
    // Reset backoff only if connection was stable for 30s+
    if (clobWsConnectedAt && Date.now() - clobWsConnectedAt > 30_000) {
      clobWsReconnectDelay = 1000;
    }
    clobWsConnectedAt = 0;
    startHttpFallback(); // immediate fallback
    scheduleClobWsReconnect();
  });

  clobWs.on("error", (err) => {
    console.warn("[BTC5m] CLOB WS 错误:", err.message);
  });
}

function scheduleClobWsReconnect() {
  if (clobWsReconnectTimer) return;
  console.log(`[BTC5m] CLOB WS ${clobWsReconnectDelay / 1000}s 后重连...`);
  clobWsReconnectTimer = setTimeout(() => {
    clobWsReconnectTimer = null;
    connectClobWs();
  }, clobWsReconnectDelay);
  clobWsReconnectDelay = Math.min(clobWsReconnectDelay * 2, CLOB_WS_RECONNECT_MAX);
}

function clobWsSubscribe(tokenId) {
  if (!clobWs || clobWs.readyState !== WebSocket.OPEN) return;
  if (clobWsSubscribedToken === tokenId) return;

  if (clobWsSubscribedToken) {
    clobWsSend({ auth: {}, type: "unsubscribe", channel: "book", markets: [clobWsSubscribedToken] });
    clobWsSend({ auth: {}, type: "unsubscribe", channel: "price_change", markets: [clobWsSubscribedToken] });
  }

  clobWsSend({ auth: {}, type: "subscribe", channel: "book", markets: [tokenId] });
  clobWsSend({ auth: {}, type: "subscribe", channel: "price_change", markets: [tokenId] });
  clobWsSubscribedToken = tokenId;
  console.log(`[BTC5m] CLOB WS 订阅: ${tokenId.slice(0, 16)}...`);
}

function clobWsUnsubscribe(tokenId) {
  if (!clobWs || clobWs.readyState !== WebSocket.OPEN) return;
  clobWsSend({ auth: {}, type: "unsubscribe", channel: "book", markets: [tokenId] });
  clobWsSend({ auth: {}, type: "unsubscribe", channel: "price_change", markets: [tokenId] });
  if (clobWsSubscribedToken === tokenId) clobWsSubscribedToken = null;
}

function clobWsSend(obj) {
  try { clobWs.send(JSON.stringify(obj)); } catch {}
}

let lastLogTime = 0;

function broadcastClobPrice(bestBid, bestAsk) {
  const upMid = +((bestBid + bestAsk) / 2).toFixed(4);
  const downMid = +(1 - upMid).toFixed(4);
  const spread = +(bestAsk - bestBid).toFixed(4);

  const prevMid = lastBroadcastPrices?.up?.mid || null;
  let direction = "flat";
  if (prevMid !== null) {
    const diff = upMid - prevMid;
    if (diff >= 0.003) direction = "up";
    else if (diff <= -0.003) direction = "down";
  }

  priceHistory.push(upMid);
  if (priceHistory.length > PRICE_HISTORY_MAX) priceHistory.shift();

  pendingPriceUpdate = {
    type: "btc5mPrices",
    slug: activeMarket?.slug || "",
    up: { bid: bestBid, ask: bestAsk, mid: upMid },
    down: { bid: +(1 - bestAsk).toFixed(4), ask: +(1 - bestBid).toFixed(4), mid: downMid },
    spread,
    direction,
    prevMid,
    priceHistory: [...priceHistory],
    updatedAt: nowIso(),
  };

  if (!priceBroadcastTimer) {
    priceBroadcastTimer = setTimeout(() => {
      priceBroadcastTimer = null;
      if (pendingPriceUpdate && broadcastFn) {
        broadcastFn(pendingPriceUpdate);
        lastBroadcastPrices = { up: pendingPriceUpdate.up };
        const now = Date.now();
        if (now - lastLogTime > 1000) {
          console.log(`[BTC5m] WS价格: UP ${(pendingPriceUpdate.up.mid * 100).toFixed(1)}¢ / DOWN ${((1 - pendingPriceUpdate.up.mid) * 100).toFixed(1)}¢ ${pendingPriceUpdate.direction !== 'flat' ? '(' + pendingPriceUpdate.direction + ')' : ''}`);
          lastLogTime = now;
        }
        pendingPriceUpdate = null;
      }
    }, PRICE_BROADCAST_THROTTLE_MS);
  }
}

function handleClobWsMessage(msg) {
  // WS is alive — stop HTTP fallback
  if (httpFallbackActive) stopHttpFallback();

  // price_change channel — lightweight tick
  if (msg.event === "price_change" || msg.channel === "price_change") {
    const bestBid = Number(msg.best_bid || msg.data?.best_bid);
    const bestAsk = Number(msg.best_ask || msg.data?.best_ask);
    if (bestBid > 0 && bestAsk > 0) broadcastClobPrice(bestBid, bestAsk);
    return;
  }

  // book channel — full orderbook snapshot/delta
  if (msg.channel !== "book" || !msg.data) return;
  const bookData = msg.data;
  const bidsRaw = bookData.bids || [];
  const asksRaw = bookData.asks || [];

  const bids = bidsRaw.map(l => {
    const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
    return p > 0 ? p : 0;
  }).filter(p => p > 0).sort((a, b) => b - a);

  const asks = asksRaw.map(l => {
    const p = Array.isArray(l) ? Number(l[0]) : Number(l?.price ?? l?.p);
    return p > 0 ? p : 0;
  }).filter(p => p > 0).sort((a, b) => a - b);

  const bestBid = bids[0] || 0;
  const bestAsk = asks[0] || 0;
  if (bestBid > 0 && bestAsk > 0) broadcastClobPrice(bestBid, bestAsk);
}

// ─── HTTP Fallback (exponential backoff when WS is down) ────────────────────
let httpFallbackDelay = 1000;
const HTTP_FALLBACK_MAX_DELAY = 30_000;
function startHttpFallback() {
  if (httpFallbackTimer) return;
  httpFallbackActive = true;
  httpFallbackDelay = 1000;
  console.log("[BTC5m] HTTP 轮询保底启动 (1s → 指数退避)");
  function poll() {
    if (!httpFallbackActive) return;
    httpFallbackTimer = setTimeout(async () => {
      if (!httpFallbackActive) return;
      try {
        const prices = await getPrices();
        if (prices?.up?.mid) {
          broadcastClobPrice(prices.up.bid || prices.up.mid, prices.up.ask || prices.up.mid);
          httpFallbackDelay = 1000; // reset on success
        } else {
          httpFallbackDelay = Math.min(httpFallbackDelay * 2, HTTP_FALLBACK_MAX_DELAY);
        }
      } catch {
        httpFallbackDelay = Math.min(httpFallbackDelay * 2, HTTP_FALLBACK_MAX_DELAY);
      }
      poll();
    }, httpFallbackDelay);
  }
  poll();
}

function stopHttpFallback() {
  if (httpFallbackTimer) { clearTimeout(httpFallbackTimer); httpFallbackTimer = null; }
  httpFallbackActive = false;
}

function startPricePoller() {
  lastBroadcastPrices = null;
  priceHistory = [];
  pendingPriceUpdate = null;
  // Force-close any lingering connection from previous market
  if (clobWs) {
    try {
      clobWs.removeAllListeners();
      clobWs.on("error", () => {}); // swallow pending errors
      if (clobWs.readyState === WebSocket.OPEN) {
        clobWs.terminate();
      } else {
        clobWs.close(); // safe for CONNECTING state
      }
    } catch {}
    clobWs = null;
    clobWsSubscribedToken = null;
  }
  // Start HTTP fallback immediately — WS will stop it once connected
  startHttpFallback();
  connectClobWs();
}

function stopPricePoller() {
  if (clobWsSubscribedToken && clobWs) {
    clobWsUnsubscribe(clobWsSubscribedToken);
  }
  if (clobWs) {
    try {
      clobWs.removeAllListeners();
      clobWs.on("error", () => {});
      clobWs.close();
    } catch {}
    clobWs = null;
  }
  if (clobWsReconnectTimer) { clearTimeout(clobWsReconnectTimer); clobWsReconnectTimer = null; }
  if (priceBroadcastTimer) { clearTimeout(priceBroadcastTimer); priceBroadcastTimer = null; }
  stopHttpFallback();
  clobWsSubscribedToken = null;
  lastBroadcastPrices = null;
  priceHistory = [];
  pendingPriceUpdate = null;
}

// ─── Full Status ────────────────────────────────────────────────────────────
async function getMarketStatus() {
  const position = await getPositionStatus();
  const phase = getWindowPhase(activeMarket);

  // Fetch live CLOB prices so UI doesn't show stale Gamma 50/50
  if (activeMarket?.upTokenId) {
    try {
      const prices = await getPrices();
      if (prices?.up?.mid) {
        activeMarket.upPrice = prices.up.mid;
        activeMarket.downPrice = prices.down?.mid || +(1 - prices.up.mid).toFixed(4);
      }
    } catch {}
  }

  return {
    enabled: BTC5M_ENABLED,
    market: activeMarket,
    phase,
    position,
    lastAnalysis,
    config: {
      aiConfigured: !!(AI_API_KEY && AI_BASE_URL && AI_MODEL),
      defaultTradeUsd: BTC5M_DEFAULT_TRADE_USD,
      defaultOrderType: BTC5M_DEFAULT_ORDER_TYPE,
    },
    updatedAt: nowIso(),
  };
}

// ─── Init ───────────────────────────────────────────────────────────────────
async function init(broadcast) {
  broadcastFn = broadcast || null;
  if (!BTC5M_ENABLED) {
    console.log("[BTC5m] 功能已禁用 (BTC5M_ENABLED=false)");
    return;
  }
  ensureLogDir();
  startSettlementChecker();
  // Discover market FIRST (analysis needs market slug for alignment)
  try {
    await refreshMarket();
    console.log("[BTC5m] 市场发现:", activeMarket ? `${activeMarket.question} (${activeMarket.source})` : "未找到");
    startPricePoller();
  } catch (err) {
    console.warn("[BTC5m] 市场发现失败:", err.message);
  }
  // Schedule analysis AFTER market discovery (ensures correct window alignment)
  scheduleAutoAnalysis();
  // Check settlement for any pending events from previous sessions
  try {
    const pending = readEventLogs().filter(l => l.type === "analysis" && !l.result);
    for (const entry of pending) {
      const slugMatch = entry.slug?.match(/-(\d+)$/);
      if (slugMatch) {
        const startSec = Number(slugMatch[1]);
        const endMs = (startSec + 5 * 60) * 1000;
        if (Date.now() > endMs + 30000) {
          // This window has expired, try to settle it
          await settleEvent({ slug: entry.slug });
        }
      }
    }
  } catch {}
}

module.exports = {
  init,
  getActiveMarket,
  refreshMarket,
  fetchBtcKlines,
  calculateIndicators,
  runAutoAnalysis,
  getEventStats,
  readEventLogs,
  getPrices,
  runAiAnalysis,
  executeTrade,
  sellPosition,
  getPositionStatus,
  getMarketStatus,
  startPricePoller,
  stopPricePoller,
};
