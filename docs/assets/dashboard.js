"use strict";

const $ = (id) => document.getElementById(id);
const number = new Intl.NumberFormat("en-US");
const fmt = (value) => number.format(value);
const percent = (value) => `${(value * 100).toFixed(1)}%`;
const outcomes = [
  { key: "white_wins", label: "White wins", color: "white" },
  { key: "black_wins", label: "Black wins", color: "black" },
  { key: "draws", label: "Draws", color: "draw" },
];
const dateFormat = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "long",
  year: "numeric",
  timeZone: "UTC",
});
const timeFormat = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
  timeZone: "UTC",
});

function node(tag, className, content) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (content !== undefined) element.textContent = content;
  return element;
}

function segment(track, item, count, denominator) {
  if (!count) return;
  const part = node("i", item.color);
  part.style.flexBasis = percent(count / denominator);
  part.style.flexGrow = String(count);
  part.title = `${item.label}: ${fmt(count)} (${percent(count / denominator)})`;
  track.append(part);
}

function gameDescription(row) {
  return `${row.label}: ${fmt(row.games)} games, ${fmt(row.white_wins)} white wins, ${fmt(row.black_wins)} black wins, ${fmt(row.draws)} draws`;
}

function overview(data) {
  const { source, counts, groups } = data;
  const first = new Date(source.first_game_utc);
  const last = new Date(source.last_game_utc);
  const month = new Intl.DateTimeFormat("en-GB", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
  $("archive-month").textContent = month.format(
    new Date(`${source.month}-01T00:00:00Z`),
  );
  const sameDay = dateFormat.format(first) === dateFormat.format(last);
  $("sample-window").textContent = sameDay
    ? `${dateFormat.format(first)} · ${timeFormat.format(first)}–${timeFormat.format(last)} UTC`
    : `${dateFormat.format(first)} ${timeFormat.format(first)} – ${dateFormat.format(last)} ${timeFormat.format(last)} UTC`;
  $("retrieved").textContent =
    `Retrieved ${dateFormat.format(new Date(source.retrieved_at_utc))} · UTC`;
  $("sample-count").textContent = fmt(counts.bronze);
  $("valid-count").textContent = fmt(counts.silver);
  $("valid-caption").textContent =
    `${fmt(counts.quarantine)} quarantined · ${((100 * counts.silver) / counts.bronze).toFixed(2)}% accepted`;
  $("opening-count").textContent = fmt(groups.opening.length);
  $("white-rate").textContent = percent(
    data.outcomes.white_wins / counts.silver,
  );
  $("quality-bronze").textContent = fmt(counts.bronze);
  $("quality-silver").textContent = fmt(counts.silver);
  $("quality-quarantine").textContent = fmt(counts.quarantine);
  $("opening-groups").textContent = `${fmt(groups.opening.length)} groups`;
  $("source-hash").textContent = source.sha256;
  $("archive-link").href = source.url;
  $("outcome-total").textContent = `${fmt(counts.silver)} validated games`;
  const track = $("outcome-track");
  track.setAttribute(
    "aria-label",
    outcomes.map((o) => `${o.label}: ${fmt(data.outcomes[o.key])}`).join(", "),
  );
  const stats = $("outcome-stats");
  for (const outcome of outcomes) {
    const count = data.outcomes[outcome.key];
    const part = node("span", outcome.color);
    part.style.flex = String(count);
    part.title = `${outcome.label}: ${fmt(count)} (${percent(count / counts.silver)})`;
    if (count) track.append(part);
    const stat = node("div", "outcome-stat");
    stat.append(
      node("i", `dot ${outcome.color}`),
      node("strong", null, percent(count / counts.silver)),
      node("span", null, `${outcome.label} · ${fmt(count)}`),
    );
    stats.append(stat);
  }
}

function horizontalRow(row, maxGames, metric = "games") {
  const container = node("div", "opening-row");
  const chart = node("div");
  const title = node("p", "row-name", row.label);
  const lane = node("div", "bar-lane");
  const bar = node("div", "bar-value");
  bar.style.width = percent(row.games / maxGames);
  bar.setAttribute("role", "img");
  bar.setAttribute("aria-label", gameDescription(row));
  bar.title = gameDescription(row);
  outcomes.forEach((o) => segment(bar, o, row[o.key], row.games));
  lane.append(bar);
  chart.append(title, lane);
  const value = node("div", "row-value");
  if (metric === "games") {
    value.append(
      node("strong", null, fmt(row.games)),
      node("small", null, `${percent(row.white_wins / row.games)} W`),
    );
  } else {
    value.append(
      node("strong", null, percent(row[metric] / row.games)),
      node("small", null, `${fmt(row.games)} games`),
    );
  }
  container.append(chart, value);
  return container;
}

function initOpeningExplorer(rows) {
  const search = $("opening-search");
  const sort = $("opening-sort");
  const minimum = $("min-games");
  const limit = $("opening-limit");
  const list = $("opening-list");
  function render() {
    const query = search.value.trim().toLowerCase();
    const metric = sort.value;
    const matches = rows.filter(
      (row) =>
        row.label.toLowerCase().includes(query) &&
        row.games >= Number(minimum.value),
    );
    const measure = (row) =>
      metric === "games" ? row.games : row[metric] / row.games;
    matches.sort(
      (a, b) =>
        measure(b) - measure(a) ||
        b.games - a.games ||
        a.label.localeCompare(b.label),
    );
    const displayed = matches.slice(0, Number(limit.value));
    $("opening-summary").textContent =
      `Showing ${fmt(displayed.length)} of ${fmt(matches.length)} matching openings`;
    $("opening-empty").hidden = matches.length !== 0;
    list.replaceChildren();
    if (!matches.length) return;
    const maxGames = Math.max(...matches.map((row) => row.games));
    list.append(
      ...displayed.map((row) => horizontalRow(row, maxGames, metric)),
    );
  }
  search.addEventListener("input", render);
  [sort, minimum, limit].forEach((control) =>
    control.addEventListener("change", render),
  );
  $("reset-filters").addEventListener("click", () => {
    search.value = "";
    sort.value = "games";
    minimum.value = "1";
    limit.value = "10";
    render();
    search.focus();
  });
  render();
}

function initSpeeds(rows) {
  const order = ["bullet", "blitz", "rapid", "classical"];
  const sorted = [...rows].sort(
    (a, b) => order.indexOf(a.label) - order.indexOf(b.label),
  );
  const max = Math.max(...rows.map((row) => row.games));
  $("speed-list").append(
    ...sorted.map((row) =>
      horizontalRow(
        { ...row, label: row.label[0].toUpperCase() + row.label.slice(1) },
        max,
      ),
    ),
  );
}

function initRating(rows) {
  const sorted = [...rows].sort(
    (a, b) => parseInt(a.label, 10) - parseInt(b.label, 10),
  );
  let selected = sorted.reduce((a, b) => (a.games > b.games ? a : b));
  const chart = $("rating-chart");
  const control = $("rating-metric");
  function detail() {
    const element = $("rating-detail");
    element.replaceChildren(
      node("strong", null, `${selected.label} Elo`),
      node("span", "detail-number", `${fmt(selected.games)} games`),
    );
    outcomes.forEach((o) =>
      element.append(
        node(
          "span",
          null,
          `${o.label}: ${fmt(selected[o.key])} (${percent(selected[o.key] / selected.games)})`,
        ),
      ),
    );
  }
  function render() {
    const metric = control.value;
    const max =
      metric === "games" ? Math.max(...sorted.map((r) => r.games)) : 1;
    chart.style.setProperty("--buckets", String(sorted.length));
    chart.replaceChildren();
    for (const row of sorted) {
      const measure = metric === "games" ? row.games : row[metric] / row.games;
      const button = node("button", "rating-bar");
      button.type = "button";
      button.setAttribute("aria-pressed", String(row.label === selected.label));
      button.setAttribute("aria-label", gameDescription(row));
      const column = node("span", "rating-column");
      column.style.setProperty("--height", String((72 * measure) / max));
      if (metric === "games") {
        outcomes.forEach((o) => segment(column, o, row[o.key], row.games));
      } else {
        const outcome = outcomes.find((o) => o.key === metric);
        segment(column, outcome, 1, 1);
      }
      button.append(
        node(
          "span",
          "rating-value",
          metric === "games" ? fmt(row.games) : percent(measure),
        ),
        column,
        node("span", "rating-label", row.label.split("-")[0]),
      );
      button.addEventListener("click", () => {
        selected = row;
        chart
          .querySelectorAll("button")
          .forEach((b) => b.setAttribute("aria-pressed", String(b === button)));
        detail();
      });
      chart.append(button);
    }
    detail();
  }
  control.addEventListener("change", render);
  render();
}

async function load() {
  try {
    const response = await fetch("assets/snapshot.json");
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (data.schema_version !== 1 || data.counts.silver < 1)
      throw new Error("Unsupported snapshot");
    overview(data);
    initOpeningExplorer(data.groups.opening);
    initSpeeds(data.groups.speed);
    initRating(data.groups.rating_bucket);
    $("load-status").hidden = true;
    $("dashboard").hidden = false;
  } catch (error) {
    console.error("Dashboard snapshot error", error);
    $("load-status").textContent =
      "The aggregate snapshot could not be loaded. Refresh the page or serve the docs folder over HTTP (see the README).";
  }
}
load();
