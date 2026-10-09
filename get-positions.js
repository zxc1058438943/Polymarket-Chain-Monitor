#!/usr/bin/env node
"use strict";

/**
 * Query Polymarket positions for a wallet.
 *
 * Usage:
 *   npm run positions -- 0xYourWalletAddress
 *   WATCH_WALLET=0xYourWalletAddress npm run positions
 *   PM_FUNDER_ADDRESS=0xYourWalletAddress npm run positions
 *
 * Requires Node.js >= 18 (built-in fetch). Does not place or sign orders.
 */
require("dotenv").config();

const { ethers } = require("ethers");

const DATA_API_BASE = (process.env.PM_DATA_API_BASE_URL || "https://data-api.polymarket.com").replace(/\/+$/, "");
const REQUEST_TIMEOUT_MS = Math.max(1000, Number(process.env.POSITIONS_TIMEOUT_MS) || 15000);
const PAGE_SIZE = 500;
const MAX_PAGES = Math.max(1, Math.min(100, Number(process.env.POSITIONS_MAX_PAGES) || 10));

function resolveWallet(argv = process.argv, env = process.env) {
  const arg = argv.slice(2).find((item) => !item.startsWith("--"));
  const value = arg || env.PM_FUNDER_ADDRESS || env.WATCH_WALLET || "";
  if (!value) {
    throw new Error(
      "缺少钱包地址。请使用 npm run positions -- 0x地址，或设置 PM_FUNDER_ADDRESS / WATCH_WALLET。"
    );
  }
  if (!ethers.isAddress(value)) {
    throw new Error("钱包地址格式无效，请提供完整的 EVM 地址（0x 开头，42 个字符）。");
  }
  return ethers.getAddress(value);
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function booleanOrFalse(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) && value !== 0;
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

function formatPosition(position = {}) {
  const size = numberOrNull(position.size);
  const avgPrice = numberOrNull(position.avgPrice);
  const curPrice = numberOrNull(position.curPrice);
  const currentValue = numberOrNull(position.currentValue);
  const cashPnl = numberOrNull(position.cashPnl);
  const percentPnl = numberOrNull(position.percentPnl);
  return {
    market: position.title || position.market || "未知市场",
    outcome: position.outcome || "-",
    size,
    avgPrice,
    curPrice,
    currentValue,
    cashPnl,
    percentPnl,
    redeemable: booleanOrFalse(position.redeemable),
    endDate: position.endDate || null,
    asset: position.asset || null,
  };
}

async function fetchPositions(wallet, fetchImpl = globalThis.fetch) {
  if (typeof fetchImpl !== "function") {
    throw new Error("当前 Node.js 运行环境没有内置 fetch；请升级到 Node.js 18 或更高版本。");
  }
  const positions = [];
  let reachedEnd = false;
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const url = new URL(`${DATA_API_BASE}/positions`);
    url.searchParams.set("user", wallet);
    url.searchParams.set("limit", String(PAGE_SIZE));
    url.searchParams.set("offset", String(page * PAGE_SIZE));
    url.searchParams.set("sizeThreshold", "0");

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    let response;
    try {
      response = await fetchImpl(url, {
        method: "GET",
        headers: { accept: "application/json", "user-agent": "polymarket-chain-monitor/1.0" },
        signal: controller.signal,
      });
    } catch (error) {
      if (error && error.name === "AbortError") {
        throw new Error(`查询持仓超时（${REQUEST_TIMEOUT_MS}ms）。请检查网络或代理。`);
      }
      throw new Error(`查询 Polymarket 持仓失败：${error.message || error}`);
    } finally {
      clearTimeout(timer);
    }

    if (!response.ok) {
      const detail = await response.text().catch(() => "");
      throw new Error(`Polymarket Data API 返回 HTTP ${response.status}${detail ? `: ${detail.slice(0, 300)}` : ""}`);
    }

    let data;
    try {
      data = await response.json();
    } catch {
      throw new Error("Polymarket Data API 返回的不是有效 JSON。");
    }
    if (!Array.isArray(data)) {
      throw new Error("Polymarket Data API 返回格式异常：预期为持仓数组。");
    }

    positions.push(...data);
    if (data.length < PAGE_SIZE) {
      reachedEnd = true;
      break;
    }
  }
  const formatted = positions.map(formatPosition);
  Object.defineProperty(formatted, "truncated", { value: !reachedEnd, enumerable: false });
  return formatted;
}

function printPositions(wallet, positions) {
  console.log(`钱包：${wallet}`);
  console.log(`持仓数量：${positions.length}`);
  if (positions.truncated) {
    console.warn(`注意：查询已达到最多 ${MAX_PAGES} 页（每页 ${PAGE_SIZE} 条），可能还有未显示持仓。可设置 POSITIONS_MAX_PAGES 提高上限。`);
  }
  if (process.argv.includes("--json")) {
    console.log(JSON.stringify(positions, null, 2));
    return;
  }
  if (positions.length === 0) {
    console.log("当前没有查询到持仓。");
    return;
  }

  const money = (n) => n === null ? "-" : n.toFixed(4);
  const rows = positions.map((p) => ({
    市场: p.market.length > 42 ? `${p.market.slice(0, 39)}...` : p.market,
    结果: p.outcome,
    份额: money(p.size),
    均价: money(p.avgPrice),
    现价: money(p.curPrice),
    当前价值: money(p.currentValue),
    浮动盈亏: money(p.cashPnl),
    盈亏率: p.percentPnl === null ? "-" : `${p.percentPnl.toFixed(2)}%`,
    可赎回: p.redeemable ? "是" : "否",
  }));
  console.table(rows);
}

async function main() {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    console.log("用法：npm run positions -- <钱包地址> [--json]");
    console.log("也可设置 PM_FUNDER_ADDRESS 或 WATCH_WALLET 环境变量。");
    return;
  }
  const wallet = resolveWallet();
  const positions = await fetchPositions(wallet);
  printPositions(wallet, positions);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(`错误：${error.message || error}`);
    process.exitCode = 1;
  });
}

module.exports = { resolveWallet, formatPosition, booleanOrFalse, fetchPositions };
