"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { ethers } = require("ethers");
const { resolveWallet, formatPosition, booleanOrFalse, fetchPositions } = require("../get-positions.js");

const wallet = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea";

test("resolveWallet prefers a positional argument and checksums the address", () => {
  assert.equal(resolveWallet(["node", "get-positions.js", wallet], { WATCH_WALLET: "bad" }), ethers.getAddress(wallet));
});

test("resolveWallet falls back to environment and rejects a missing or invalid address", () => {
  assert.equal(resolveWallet(["node", "get-positions.js"], { WATCH_WALLET: wallet }), ethers.getAddress(wallet));
  assert.throws(() => resolveWallet(["node", "get-positions.js"], {}), /缺少钱包地址/);
  assert.throws(() => resolveWallet(["node", "get-positions.js", "not-an-address"], {}), /格式无效/);
});

test("formatPosition normalizes numbers and optional fields", () => {
  const result = formatPosition({
    title: "Example market",
    outcome: "Yes",
    size: "2.5",
    avgPrice: "0.4",
    curPrice: 0.6,
    currentValue: "1.5",
    cashPnl: "0.5",
    percentPnl: "50",
    redeemable: true,
  });
  assert.equal(result.market, "Example market");
  assert.equal(result.size, 2.5);
  assert.equal(result.currentValue, 1.5);
  assert.equal(result.redeemable, true);
  assert.equal(result.asset, null);
  assert.equal(formatPosition({ redeemable: "false" }).redeemable, false);
  assert.equal(formatPosition({ redeemable: "true" }).redeemable, true);
  assert.equal(booleanOrFalse("off"), false);
  assert.equal(booleanOrFalse(1), true);
});

test("fetchPositions forms a bounded Data API request and formats results", async () => {
  let requestedUrl;
  const fakeFetch = async (url) => {
    requestedUrl = url;
    return {
      ok: true,
      json: async () => [{
        title: "Test market",
        outcome: "No",
        size: "4",
        currentValue: "1.2",
      }],
    };
  };

  const rows = await fetchPositions(wallet, fakeFetch);
  assert.equal(requestedUrl.pathname, "/positions");
  assert.equal(requestedUrl.searchParams.get("user"), wallet);
  assert.equal(requestedUrl.searchParams.get("limit"), "500");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].market, "Test market");
  assert.equal(rows[0].size, 4);
  assert.equal(rows.truncated, false);
});

test("fetchPositions reports non-success HTTP responses clearly", async () => {
  await assert.rejects(
    fetchPositions(wallet, async () => ({
      ok: false,
      status: 503,
      text: async () => "service unavailable",
    })),
    /HTTP 503/
  );
});

test("fetchPositions flags a result set that reaches the configured page cap", async () => {
  let calls = 0;
  const page = Array.from({ length: 500 }, (_, i) => ({ asset: `token-${calls}-${i}`, size: "1" }));
  const rows = await fetchPositions(wallet, async (url) => {
    calls += 1;
    return { ok: true, json: async () => page };
  });
  assert.equal(calls, 10);
  assert.equal(rows.length, 5000);
  assert.equal(rows.truncated, true);
});

test("fetchPositions rejects malformed API response shapes", async () => {
  await assert.rejects(
    fetchPositions(wallet, async () => ({ ok: true, json: async () => ({ positions: [] }) })),
    /返回格式异常/
  );
});

test("boolean-like API fields are parsed instead of using JavaScript truthiness", () => {
  assert.equal(formatPosition({ redeemable: "false" }).redeemable, false);
  assert.equal(formatPosition({ redeemable: "true" }).redeemable, true);
});
