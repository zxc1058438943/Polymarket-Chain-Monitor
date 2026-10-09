"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const requiredJavaScript = [
  "ws-bridge.js",
  "monitor-polymarket-ws.js",
  "get-positions.js",
  "btc-5m-analyzer.js",
  "scripts/check-repo.js",
  "test/repo.test.js",
];

function readJson(relativePath) {
  const absolutePath = path.join(root, relativePath);
  assert.ok(fs.existsSync(absolutePath), `缺少文件：${relativePath}`);
  try {
    return JSON.parse(fs.readFileSync(absolutePath, "utf8"));
  } catch (error) {
    throw new Error(`${relativePath} 不是有效 JSON：${error.message}`);
  }
}

function checkJavaScriptSyntax() {
  for (const relativePath of requiredJavaScript) {
    const absolutePath = path.join(root, relativePath);
    assert.ok(fs.existsSync(absolutePath), `缺少 JavaScript 文件：${relativePath}`);
    try {
      execFileSync(process.execPath, ["--check", absolutePath], { stdio: "pipe" });
    } catch (error) {
      const output = [error.stdout, error.stderr].filter(Boolean).map(String).join("\n");
      throw new Error(`JavaScript 语法检查失败：${relativePath}\n${output}`);
    }
  }
}

function checkInlineHtmlScripts() {
  const htmlPath = path.join(root, "index.html");
  assert.ok(fs.existsSync(htmlPath), "缺少 index.html");
  const html = fs.readFileSync(htmlPath, "utf8");
  const scriptPattern = /<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi;
  let match;
  let checked = 0;
  while ((match = scriptPattern.exec(html)) !== null) {
    if (/\bsrc\s*=/i.test(match[1])) continue;
    const source = match[2].trim();
    if (!source) continue;
    try {
      new vm.Script(source, { filename: `index.html:inline-script-${++checked}` });
    } catch (error) {
      throw new Error(`index.html 内嵌脚本语法错误（第 ${checked + 1} 个脚本）：${error.message}`);
    }
  }
  assert.ok(checked > 0, "index.html 没有找到可检查的内嵌脚本");
}

function checkPackageMetadata() {
  const pkg = readJson("package.json");
  const lock = readJson("package-lock.json");
  assert.equal(pkg.type, "commonjs", "package.json 的 type 应为 commonjs");
  assert.ok(fs.existsSync(path.join(root, pkg.main)), `package.json main 指向不存在文件：${pkg.main}`);
  assert.ok(pkg.scripts && pkg.scripts.start, "缺少启动命令");
  assert.ok(pkg.scripts.positions, "缺少持仓查询命令");
  assert.ok(pkg.scripts.check, "缺少统一检查命令");
  assert.ok(pkg.scripts.test, "缺少测试命令");

  const lockRoot = lock.packages && lock.packages[""];
  assert.ok(lockRoot, "package-lock.json 缺少根包元数据");
  assert.deepEqual(lockRoot.dependencies || {}, pkg.dependencies || {}, "package.json 与 package-lock.json 依赖声明不一致");
}

function checkReadmeLinks() {
  const readmePath = path.join(root, "README.md");
  assert.ok(fs.existsSync(readmePath), "缺少 README.md");
  const readme = fs.readFileSync(readmePath, "utf8");
  const links = [...readme.matchAll(/\[[^\]]*\]\(([^)]+)\)/g)];
  for (const match of links) {
    const target = match[1].trim();
    if (!target || /^(?:https?:|mailto:|#)/i.test(target)) continue;
    const localTarget = decodeURIComponent(target.split("#")[0].split("?")[0]);
    if (!localTarget) continue;
    assert.ok(
      fs.existsSync(path.resolve(root, localTarget)),
      `README 本地链接指向不存在文件：${target}`
    );
  }
}

function checkSafeEnvTemplate() {
  const envPath = path.join(root, ".env.example");
  assert.ok(fs.existsSync(envPath), "缺少 .env.example");
  const values = {};
  for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const separator = trimmed.indexOf("=");
    if (separator < 0) continue;
    values[trimmed.slice(0, separator).trim()] = trimmed.slice(separator + 1).trim();
  }
  assert.equal(values.AUTO_TRADE_ENABLED, "false", "示例环境必须默认关闭自动交易");
  assert.equal(values.AUTO_TRADE_MODE, "paper", "示例环境必须默认使用模拟模式");
  assert.equal(values.AUTO_TRADE_DRY_RUN, "true", "示例环境必须默认启用 dry-run");
  assert.equal(values.AUTO_TRADE_ALLOW_LIVE_AUTO, "false", "示例环境不得默认允许实盘自动交易");
  assert.equal(values.KILL_SWITCH, "true", "示例环境应默认打开紧急停止开关");
}

function main() {
  checkPackageMetadata();
  checkJavaScriptSyntax();
  checkInlineHtmlScripts();
  checkReadmeLinks();
  checkSafeEnvTemplate();
  readJson("auto-trade-wallets.json");
  console.log("准入自检通过：依赖清单、JavaScript 语法、HTML 内嵌脚本、README 本地链接、环境模板安全默认值与钱包 JSON 均正常。");
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(`准入自检失败：${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = { main, checkPackageMetadata, checkJavaScriptSyntax, checkInlineHtmlScripts, checkReadmeLinks, checkSafeEnvTemplate };
