// Run against a local HTTP server: NODE_PATH=output/dashboard-browser/node_modules node scripts/test_dashboard.cjs
const assert = require("node:assert/strict");
const { chromium } = require("playwright");

async function assertOutcomeCaptions(page, width) {
  const captions = await page
    .locator(".outcome-stat span")
    .evaluateAll((nodes) =>
      nodes.map((node) => {
        const range = document.createRange();
        range.selectNodeContents(node);
        const text = range.getBoundingClientRect();
        const container = node.parentElement.getBoundingClientRect();
        return {
          label: node.textContent,
          left: text.left,
          right: text.right,
          containerLeft: container.left,
          containerRight: container.right,
        };
      }),
    );
  assert.equal(captions.length, 3);
  for (const [index, caption] of captions.entries()) {
    assert.ok(
      caption.left >= caption.containerLeft &&
        caption.right <= caption.containerRight,
      `Outcome caption overflows its column at ${width}px: ${caption.label}`,
    );
    if (index > 0) {
      assert.ok(
        caption.left >= captions[index - 1].right,
        `Outcome captions overlap at ${width}px: ${captions[index - 1].label} / ${caption.label}`,
      );
    }
  }
}

async function main() {
  const browser = await chromium.launch({
    headless: true,
    channel: process.env.DASHBOARD_BROWSER_CHANNEL || undefined,
  });
  const errors = [];
  try {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1100 },
      deviceScaleFactor: 1,
    });
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    const url = process.env.DASHBOARD_URL || "http://127.0.0.1:8765/";
    const response = await page.goto(url);
    assert.equal(response.status(), 200);
    await page.locator("#dashboard").waitFor({ state: "visible" });
    const snapshot = await page.evaluate(async () =>
      (await fetch("assets/snapshot.json")).json(),
    );
    const fmt = (n) => n.toLocaleString("en-US");
    assert.equal(
      await page.locator("#sample-count").innerText(),
      fmt(snapshot.counts.bronze),
    );
    assert.equal(
      await page.locator("#valid-count").innerText(),
      fmt(snapshot.counts.silver),
    );
    assert.equal(
      await page.locator("#quality-quarantine").innerText(),
      fmt(snapshot.counts.quarantine),
    );
    assert.equal(
      await page.locator("#opening-count").innerText(),
      fmt(snapshot.groups.opening.length),
    );
    assert.equal(await page.locator("#opening-list .opening-row").count(), 10);
    assert.equal(
      await page.locator("#speed-list .opening-row").count(),
      snapshot.groups.speed.length,
    );
    assert.equal(
      await page.locator("#rating-chart button").count(),
      snapshot.groups.rating_bucket.length,
    );
    assert.equal(
      await page.locator("#archive-link").getAttribute("href"),
      snapshot.source.url,
    );
    const most = [...snapshot.groups.opening].sort(
      (a, b) => b.games - a.games || a.label.localeCompare(b.label),
    )[0];
    assert.equal(
      await page.locator("#opening-list .row-name").first().innerText(),
      most.label,
    );
    for (const [family, outcome] of [
      ["Italian Game", "white_wins"],
      ["Ruy Lopez", "black_wins"],
      ["Italian Game", "draws"],
    ]) {
      const matches = snapshot.groups.opening.filter((row) =>
        row.label.toLowerCase().includes(family.toLowerCase()),
      );
      assert.ok(
        matches.length > 0,
        `Missing real opening labels for ${family}`,
      );
      const shortcut = page
        .locator("#opening-quick-picks")
        .getByRole("button", { name: family, exact: true });
      await shortcut.click();
      assert.equal(await shortcut.getAttribute("aria-pressed"), "true");
      assert.equal(await page.locator("#opening-search").inputValue(), family);
      await page.locator("#opening-result").selectOption(outcome);
      const matchingRows = matches
        .filter((row) => row[outcome] > 0)
        .sort(
          (a, b) =>
            b[outcome] - a[outcome] ||
            b.games - a.games ||
            a.label.localeCompare(b.label),
        );
      assert.ok(matchingRows.length > 0);
      const visible = matchingRows.slice(0, 10);
      assert.deepEqual(
        await page.locator("#opening-list .row-name").allTextContents(),
        visible.map((row) => row.label),
      );
      assert.deepEqual(
        await page.locator("#opening-list .row-value strong").allTextContents(),
        visible.map((row) => fmt(row[outcome])),
      );
      assert.ok(
        (await page.locator("#opening-summary").innerText()).includes(
          fmt(matchingRows.reduce((sum, row) => sum + row[outcome], 0)),
        ),
      );
    }
    await page.locator("#opening-search").fill("no-such-opening-zzzz");
    await page.locator("#reset-filters").click();
    assert.equal(await page.locator("#opening-result").inputValue(), "all");
    assert.equal(
      await page.locator("#opening-quick-picks [aria-pressed=true]").count(),
      0,
    );
    await page.locator("#opening-search").fill("Sicilian");
    assert.ok(
      (await page.locator("#opening-list .row-name").allTextContents()).every(
        (name) => /sicilian/i.test(name),
      ),
    );
    await page.locator("#min-games").selectOption("20");
    await page.locator("#opening-sort").selectOption("white_wins");
    const expected = snapshot.groups.opening
      .filter((row) => /sicilian/i.test(row.label) && row.games >= 20)
      .sort(
        (a, b) =>
          b.white_wins / b.games - a.white_wins / a.games ||
          b.games - a.games ||
          a.label.localeCompare(b.label),
      );
    assert.deepEqual(
      await page.locator("#opening-list .row-name").allTextContents(),
      expected.slice(0, 10).map((row) => row.label),
    );
    await page.locator("#opening-search").fill("no-such-opening-zzzz");
    assert.equal(await page.locator("#opening-empty").isVisible(), true);
    await page.locator("#reset-filters").click();
    assert.equal(await page.locator("#opening-search").inputValue(), "");
    assert.equal(await page.locator("#opening-list .opening-row").count(), 10);
    await page.locator("#opening-limit").selectOption("20");
    assert.equal(await page.locator("#opening-list .opening-row").count(), 20);
    await page.locator("#opening-limit").selectOption("10");
    await page.locator("#rating-metric").selectOption("white_wins");
    assert.match(await page.locator(".rating-value").first().innerText(), /%$/);
    await page.locator("#rating-chart button").first().click();
    const first = [...snapshot.groups.rating_bucket].sort(
      (a, b) => parseInt(a.label) - parseInt(b.label),
    )[0];
    assert.match(
      await page.locator("#rating-detail").innerText(),
      new RegExp(first.label),
    );
    assert.equal(
      await page
        .locator("#rating-chart button")
        .first()
        .getAttribute("aria-pressed"),
      "true",
    );
    await page.locator("#rating-metric").selectOption("games");
    const largestRating = [...snapshot.groups.rating_bucket].sort(
      (a, b) => b.games - a.games,
    )[0];
    await page
      .getByRole("button", { name: new RegExp(`^${largestRating.label}:`) })
      .click();
    await page.evaluate(() => window.scrollTo(0, 0));
    assert.ok(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    );
    if (process.env.DASHBOARD_SCREENSHOT)
      await page.screenshot({
        path: process.env.DASHBOARD_SCREENSHOT,
        fullPage: false,
      });
    for (const width of [320, 360, 390, 560, 768, 1440]) {
      await page.setViewportSize({ width, height: 844 });
      assert.ok(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
        `Unexpected page overflow at ${width}px`,
      );
      await assertOutcomeCaptions(page, width);
      await page.locator("#opening-search").fill("Caro-Kann");
      assert.ok((await page.locator("#opening-list .opening-row").count()) > 0);
      await page.locator("#opening-search").fill("");
    }
    assert.deepEqual(errors, []);
    console.log(
      "PASS desktop/mobile: snapshot, counts, opening shortcuts, result filters, search, sort, limit, reset, rating metric, bar selection, responsive widths, outcome caption layout, no console errors",
    );
  } finally {
    await browser.close();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
