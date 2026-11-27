// Run against a local HTTP server with Playwright available in NODE_PATH.
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const fmt = (n) => n.toLocaleString("en-US");
const pct = (value) => `${(value * 100).toFixed(1)}%`;
const countKeys = ["games", "white_wins", "black_wins", "draws"];

function expectedRows(
  snapshot,
  {
    query = "",
    group = "family",
    rating = "all",
    speed = "all",
    result = "all",
    minimum = 1,
    sort = "games",
  } = {},
) {
  const groups = new Map();
  for (const entry of snapshot.marts.opening_context.rows) {
    const [
      opening,
      eco,
      ratingBucket,
      control,
      games,
      whiteWins,
      blackWins,
      draws,
    ] = entry;
    if (rating !== "all" && ratingBucket !== rating) continue;
    if (speed !== "all" && control !== speed) continue;
    const family = opening.split(":", 1)[0].trim() || "Unknown";
    if (
      ![opening, family, eco].some((name) =>
        name.toLowerCase().includes(query.toLowerCase()),
      )
    )
      continue;
    const label = group === "family" ? family : group === "eco" ? eco : opening;
    if (!groups.has(label))
      groups.set(label, {
        label,
        games: 0,
        white_wins: 0,
        black_wins: 0,
        draws: 0,
      });
    const row = groups.get(label);
    row.games += games;
    row.white_wins += whiteWins;
    row.black_wins += blackWins;
    row.draws += draws;
  }
  const count = (row) => (result === "all" ? row.games : row[result]);
  const rows = [...groups.values()].filter(
    (row) => row.games >= minimum && count(row) > 0,
  );
  const measure = (row) =>
    sort === "games" ? count(row) : row[sort] / row.games;
  return rows.sort(
    (a, b) =>
      measure(b) - measure(a) ||
      b.games - a.games ||
      a.label.localeCompare(b.label),
  );
}

async function assertRows(page, expected, result = "all", limit = 10) {
  const visible = expected.slice(0, limit);
  const total = expected.reduce(
    (sum, row) => sum + (result === "all" ? row.games : row[result]),
    0,
  );
  assert.deepEqual(
    await page.locator("#opening-body .row-name").allTextContents(),
    visible.map((row) => row.label),
  );
  assert.deepEqual(
    await page.locator("#opening-body .row-value strong").allTextContents(),
    visible.map((row) => fmt(result === "all" ? row.games : row[result])),
  );
  for (const [color, key] of [
    ["white", "white_wins"],
    ["black", "black_wins"],
    ["draw", "draws"],
  ]) {
    assert.deepEqual(
      await page
        .locator(`#opening-body .${color}-cell strong`)
        .allTextContents(),
      visible.map((row) => fmt(row[key])),
    );
    assert.deepEqual(
      await page
        .locator(`#opening-body .${color}-cell small`)
        .allTextContents(),
      visible.map((row) => pct(row[key] / row.games)),
    );
  }
  assert.ok(
    (await page.locator("#opening-summary").innerText()).includes(fmt(total)),
  );
}

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
      `Outcome caption overflows at ${width}px: ${caption.label}`,
    );
    if (index > 0)
      assert.ok(
        caption.left >= captions[index - 1].right,
        `Outcome captions overlap at ${width}px`,
      );
  }
}

async function reset(page) {
  await page.locator("#opening-search").fill("no-such-opening-zzzz");
  assert.equal(await page.locator("#opening-empty").isVisible(), true);
  await page.locator("#reset-filters").click();
  for (const [id, value] of [
    ["opening-search", ""],
    ["opening-rating", "all"],
    ["opening-speed", "all"],
    ["opening-result", "all"],
    ["opening-group", "family"],
    ["opening-sort", "games"],
    ["min-games", "1"],
  ]) {
    assert.equal(await page.locator(`#${id}`).inputValue(), value);
  }
  assert.equal(
    await page.locator("#perspective-white").getAttribute("aria-pressed"),
    "true",
  );
}

async function main() {
  const browser = await chromium.launch({
    headless: true,
    channel: process.env.DASHBOARD_BROWSER_CHANNEL || undefined,
  });
  const errors = [];
  try {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 1450 },
      deviceScaleFactor: 1,
    });
    page.on("pageerror", (e) => errors.push(e.message));
    page.on("console", (e) => {
      if (e.type() === "error") errors.push(e.text());
    });
    const response = await page.goto(
      process.env.DASHBOARD_URL || "http://127.0.0.1:8765/",
    );
    assert.equal(response.status(), 200);
    await page.locator("#dashboard").waitFor({ state: "visible" });
    const snapshot = await page.evaluate(async () =>
      (await fetch("assets/snapshot.json")).json(),
    );
    assert.equal(snapshot.schema_version, 2);
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
    assert.equal(
      await page.locator("#archive-link").getAttribute("href"),
      snapshot.source.url,
    );
    assert.equal(
      await page.locator("#speed-list .opening-row").count(),
      snapshot.groups.speed.length,
    );
    assert.equal(
      await page.locator("#rating-chart button").count(),
      snapshot.groups.rating_bucket.length,
    );
    assert.equal(
      await page.locator("#gap-list .opening-row").count(),
      snapshot.marts.rating_gap_metrics.length,
    );
    assert.equal(
      await page.locator("#termination-list .opening-row").count(),
      snapshot.marts.termination_metrics.length,
    );
    assert.equal(
      await page.locator("#length-list .opening-row").count(),
      snapshot.marts.length_metrics.length,
    );
    assert.match(
      await page.locator("#length-summary").innerText(),
      new RegExp(fmt(snapshot.coverage.length_known)),
    );
    await assertRows(page, expectedRows(snapshot));
    for (const anchor of ["#openings", "#sample", "#pipeline"]) {
      assert.equal(await page.locator(anchor).count(), 1);
      assert.equal(await page.locator(`nav a[href="${anchor}"]`).count(), 1);
    }

    for (const [family, result] of [
      ["Italian Game", "white_wins"],
      ["Ruy Lopez", "black_wins"],
      ["Italian Game", "draws"],
    ]) {
      const button = page
        .locator("#opening-quick-picks")
        .getByRole("button", { name: family, exact: true });
      await button.click();
      assert.equal(await button.getAttribute("aria-pressed"), "true");
      assert.equal(await page.locator("#opening-search").inputValue(), family);
      await page.locator("#opening-result").selectOption(result);
      await assertRows(
        page,
        expectedRows(snapshot, { query: family, group: "opening", result }),
        result,
      );
    }
    await reset(page);
    await page
      .locator("#defense-quick-picks")
      .getByRole("button", { name: "Sicilian Defense", exact: true })
      .click();
    await page.locator("#perspective-black").click();
    assert.equal(
      await page.locator("#perspective-black").getAttribute("aria-pressed"),
      "true",
    );
    assert.equal(
      await page.locator("#opening-body .black-cell.perspective-cell").count(),
      10,
    );
    await assertRows(
      page,
      expectedRows(snapshot, {
        query: "Sicilian Defense",
        group: "opening",
        sort: "black_wins",
      }),
    );
    await reset(page);
    // A real ECO and two dimensions exercise the context mart instead of marginal aggregates.
    await page.locator("#opening-search").fill("C50");
    await page.locator("#opening-group").selectOption("eco");
    await page.locator("#opening-rating").selectOption("1600-1799");
    await page.locator("#opening-speed").selectOption("blitz");
    await page.locator("#min-games").selectOption("5");
    const slice = expectedRows(snapshot, {
      query: "C50",
      group: "eco",
      rating: "1600-1799",
      speed: "blitz",
      minimum: 5,
    });
    assert.ok(slice.length > 0);
    await assertRows(page, slice);
    await page.locator("#opening-result").selectOption("white_wins");
    await assertRows(
      page,
      expectedRows(snapshot, {
        query: "C50",
        group: "eco",
        rating: "1600-1799",
        speed: "blitz",
        minimum: 5,
        result: "white_wins",
      }),
      "white_wins",
    );
    assert.equal(
      await page.locator("#speed-list .opening-row").count(),
      snapshot.groups.speed.length,
    );
    await reset(page);
    await page.locator("#opening-limit").selectOption("20");
    await assertRows(page, expectedRows(snapshot), "all", 20);
    await page.locator("#opening-limit").selectOption("10");
    await page.locator("#rating-metric").selectOption("white_wins");
    assert.match(await page.locator(".rating-value").first().innerText(), /%$/);
    await page.locator("#rating-chart button").first().click();
    assert.equal(
      await page
        .locator("#rating-chart button")
        .first()
        .getAttribute("aria-pressed"),
      "true",
    );
    await page.locator("#rating-metric").selectOption("games");
    await page.evaluate(() => window.scrollTo(0, 0));
    if (process.env.DASHBOARD_SCREENSHOT)
      await page.screenshot({
        path: process.env.DASHBOARD_SCREENSHOT,
        fullPage: false,
      });
    for (const width of [320, 360, 390, 560, 768, 1440]) {
      await page.setViewportSize({ width, height: 844 });
      await page.evaluate(() => window.scrollTo(0, 0));
      if ([320, 390, 1440].includes(width)) {
        assert.equal(
          await page.locator("h1").innerText(),
          "Explore chess openings",
        );
        const controls = [
          page.locator("#opening-search"),
          page.getByRole("button", { name: "Italian Game", exact: true }),
          page.getByRole("button", { name: "Ruy Lopez", exact: true }),
          page.getByRole("button", { name: "Sicilian Defense", exact: true }),
        ];
        for (const control of controls) {
          const box = await control.boundingBox();
          assert.ok(
            box && box.x >= 0 && box.x + box.width <= width &&
              box.y >= 0 && box.y + box.height <= 844,
            `Primary opening control is below the fold or clipped at ${width}px`,
          );
        }
        const firstRow = await page
          .locator("#opening-body .opening-row")
          .first()
          .boundingBox();
        assert.ok(
          firstRow && firstRow.y >= 0 && firstRow.y + firstRow.height <= 844,
          `Opening results are below the fold at ${width}px`,
        );
        for (const name of ["Italian Game", "Ruy Lopez", "Sicilian Defense"]) {
          await page.getByRole("button", { name, exact: true }).click();
          assert.equal(await page.locator("#opening-search").inputValue(), name);
          await assertRows(page, expectedRows(snapshot, {
            query: name,
            group: "opening",
          }));
          await reset(page);
        }
        if (width === 390) {
          // A later chip remains reachable in the horizontally scrolling shortcuts.
          await page.getByRole("button", { name: "Pirc Defense", exact: true }).click();
          await page.locator("#opening-rating").selectOption("1600-1799");
          await page.locator("#opening-speed").selectOption("blitz");
          await page.locator("#min-games").selectOption("5");
          await assertRows(page, expectedRows(snapshot, {
            query: "Pirc Defense",
            group: "opening",
            rating: "1600-1799",
            speed: "blitz",
            minimum: 5,
          }));
          await reset(page);
        }
      }
      assert.ok(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
        `Page overflow at ${width}px`,
      );
      await assertOutcomeCaptions(page, width);
      assert.ok(
        await page
          .locator("#opening-list")
          .evaluate((el) => el.scrollWidth <= el.clientWidth),
        `Opening table overflow at ${width}px`,
      );
      await page.locator("#opening-search").fill("Caro-Kann");
      assert.ok((await page.locator("#opening-body .opening-row").count()) > 0);
      await page.locator("#opening-search").fill("");
    }
    assert.deepEqual(errors, []);
    console.log(
      "PASS: snapshot and charts, opening families/ECO, color perspective, result/rating/speed/minimum slices, exact outcomes and rates, reset, responsive layout, no browser errors",
    );
  } finally {
    await browser.close();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
