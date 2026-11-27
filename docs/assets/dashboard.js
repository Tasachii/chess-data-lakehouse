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

function matchingGames(row, result) {
  return result === "all" ? row.games : row[result];
}

function horizontalRow(row, maxGames, metric = "games", result = "all") {
  const count = matchingGames(row, result);
  const selected = outcomes.find((outcome) => outcome.key === result);
  const container = node("div", "opening-row");
  const chart = node("div");
  const title = node("p", "row-name", row.label);
  const lane = node("div", "bar-lane");
  const bar = node("div", "bar-value");
  bar.style.width = percent(count / maxGames);
  const description = selected
    ? `${fmt(count)} ${selected.label.toLowerCase()} in ${gameDescription(row)}`
    : gameDescription(row);
  bar.setAttribute("role", "img");
  bar.setAttribute("aria-label", description);
  bar.title = description;
  const segments = selected ? [selected] : outcomes;
  segments.forEach((o) => segment(bar, o, row[o.key], count));
  lane.append(bar);
  chart.append(title, lane);
  const value = node("div", "row-value");
  if (metric === "games") {
    value.append(
      node("strong", null, fmt(count)),
      node(
        "small",
        null,
        selected
          ? `${percent(count / row.games)} of total`
          : `${percent(row.white_wins / row.games)} W`,
      ),
    );
  } else {
    value.append(
      node("strong", null, percent(row[metric] / row.games)),
      node(
        "small",
        null,
        selected ? `${fmt(count)} matching` : `${fmt(row.games)} games`,
      ),
    );
  }
  container.append(chart, value);
  return container;
}

function openingFamily(label) {
  return label.split(":", 1)[0].trim() || "Unknown";
}

function openingTableRow(row, result, perspective) {
  const tr = node("tr", "opening-row");
  tr.setAttribute("role", "row");
  const title = node("th", "opening-name");
  title.scope = "row";
  title.setAttribute("role", "rowheader");
  title.append(node("span", "row-name", row.label));
  const codes = [...row.ecos].sort();
  title.append(
    node(
      "small",
      "eco-label",
      codes.length > 4
        ? `${codes.slice(0, 4).join(", ")} +${codes.length - 4}`
        : codes.join(", "),
    ),
  );
  title.title = codes.join(", ");
  const lane = node("div", "bar-lane");
  const bar = node("div", "bar-value");
  bar.style.width = "100%";
  bar.setAttribute("role", "img");
  bar.setAttribute("aria-label", gameDescription(row));
  outcomes.forEach((o) => segment(bar, o, row[o.key], row.games));
  lane.append(bar);
  title.append(lane);
  tr.append(title);
  const games = node("td", "row-value");
  games.setAttribute("role", "cell");
  games.dataset.label = result === "all" ? "Games" : "Matching";
  games.append(node("strong", null, fmt(matchingGames(row, result))));
  if (result !== "all")
    games.append(node("small", null, `${fmt(row.games)} total`));
  tr.append(games);
  for (const outcome of outcomes) {
    const cell = node("td", `outcome-cell ${outcome.color}-cell`);
    cell.setAttribute("role", "cell");
    cell.dataset.label = outcome.label;
    if (outcome.key === `${perspective}_wins`)
      cell.classList.add("perspective-cell");
    cell.append(
      node("strong", null, `${fmt(row[outcome.key])}`),
      node("small", null, percent(row[outcome.key] / row.games)),
    );
    tr.append(cell);
  }
  return tr;
}

function initOpeningExplorer(data) {
  const context = data.marts.opening_context;
  const rows = context.rows.map((row) =>
    Object.fromEntries(context.columns.map((column, i) => [column, row[i]])),
  );
  const search = $("opening-search"),
    result = $("opening-result"),
    sort = $("opening-sort");
  const group = $("opening-group"),
    rating = $("opening-rating"),
    speed = $("opening-speed");
  const minimum = $("min-games"),
    limit = $("opening-limit"),
    body = $("opening-body");
  let perspective = "white";
  const white = $("perspective-white"),
    black = $("perspective-black");
  const buttons = [];
  for (const entry of [...data.groups.rating_bucket].sort(
    (a, b) => parseInt(a.label, 10) - parseInt(b.label, 10),
  )) {
    const option = node("option", null, entry.label);
    option.value = entry.label;
    rating.append(option);
  }
  for (const label of ["bullet", "blitz", "rapid", "classical"]) {
    if (!data.groups.speed.some((row) => row.label === label)) continue;
    const option = node(
      "option",
      null,
      label[0].toUpperCase() + label.slice(1),
    );
    option.value = label;
    speed.append(option);
  }
  const shortcutGroups = [
    [
      $("opening-quick-picks"),
      [
        "Italian Game",
        "Ruy Lopez",
        "Queen's Gambit",
        "London System",
        "English Opening",
        "Scotch Game",
      ],
    ],
    [
      $("defense-quick-picks"),
      [
        "Sicilian Defense",
        "Caro-Kann Defense",
        "French Defense",
        "King's Indian Defense",
        "Scandinavian Defense",
        "Pirc Defense",
      ],
    ],
  ];
  for (const [container, families] of shortcutGroups) {
    for (const family of families) {
      const matching = data.groups.opening.filter((row) =>
        row.label.toLowerCase().includes(family.toLowerCase()),
      );
      if (!matching.length) continue;
      const button = node("button", "quick-pick", family);
      button.type = "button";
      button.title = `${fmt(matching.reduce((sum, row) => sum + row.games, 0))} games across ${fmt(matching.length)} labels in the full sample`;
      button.addEventListener("click", () => {
        search.value = family;
        group.value = "opening";
        perspective =
          container.id === "defense-quick-picks" ? "black" : "white";
        render();
      });
      container.append(button);
      buttons.push(button);
    }
  }
  function render() {
    const query = search.value.trim().toLowerCase();
    const grouped = new Map();
    for (const row of rows) {
      if (rating.value !== "all" && row.rating_bucket !== rating.value)
        continue;
      if (speed.value !== "all" && row.speed !== speed.value) continue;
      const family = openingFamily(row.opening);
      if (
        query &&
        ![row.opening, family, row.eco].some((value) =>
          value.toLowerCase().includes(query),
        )
      )
        continue;
      const label =
        group.value === "family"
          ? family
          : group.value === "eco"
            ? row.eco
            : row.opening;
      if (!grouped.has(label))
        grouped.set(label, {
          label,
          games: 0,
          white_wins: 0,
          black_wins: 0,
          draws: 0,
          ecos: new Set(),
        });
      const item = grouped.get(label);
      item.ecos.add(row.eco);
      for (const key of ["games", "white_wins", "black_wins", "draws"])
        item[key] += row[key];
    }
    const selectedResult = result.value;
    const countFor = (row) => matchingGames(row, selectedResult);
    const matches = [...grouped.values()].filter(
      (row) => row.games >= Number(minimum.value) && countFor(row) > 0,
    );
    const metric =
      sort.value === "perspective" ? `${perspective}_wins` : sort.value;
    const measure = (row) =>
      metric === "games" ? countFor(row) : row[metric] / row.games;
    matches.sort(
      (a, b) =>
        measure(b) - measure(a) ||
        b.games - a.games ||
        a.label.localeCompare(b.label),
    );
    const displayed = matches.slice(0, Number(limit.value));
    const matchingTotal = matches.reduce((sum, row) => sum + countFor(row), 0);
    const resultLabel =
      outcomes
        .find((outcome) => outcome.key === selectedResult)
        ?.label.toLowerCase() || "games";
    $("opening-summary").textContent =
      `Showing ${fmt(displayed.length)} of ${fmt(matches.length)} groups · ${fmt(matchingTotal)} ${resultLabel}`;
    $("opening-games-heading").textContent =
      selectedResult === "all" ? "Games" : "Matching games";
    sort.options[0].textContent =
      selectedResult === "all" ? "Most played" : "Most matching results";
    white.setAttribute("aria-pressed", String(perspective === "white"));
    black.setAttribute("aria-pressed", String(perspective === "black"));
    $("perspective-note").textContent =
      `${perspective === "white" ? "White" : "Black"} win rates use all games in the selected group and slice.`;
    $("opening-list").dataset.perspective = perspective;
    const groupLabel = {
      family: "families",
      opening: "opening labels",
      eco: "ECO codes",
    }[group.value];
    $("opening-groups").textContent = `${fmt(matches.length)} ${groupLabel}`;
    buttons.forEach((button) =>
      button.setAttribute(
        "aria-pressed",
        String(button.textContent.toLowerCase() === query),
      ),
    );
    $("opening-empty").hidden = matches.length !== 0;
    $("opening-list").hidden = matches.length === 0;
    body.replaceChildren(
      ...displayed.map((row) =>
        openingTableRow(row, selectedResult, perspective),
      ),
    );
  }
  search.addEventListener("input", render);
  [result, sort, minimum, limit, group, rating, speed].forEach((control) =>
    control.addEventListener("change", render),
  );
  [white, black].forEach((control, i) =>
    control.addEventListener("click", () => {
      perspective = i === 0 ? "white" : "black";
      sort.value = "perspective";
      render();
    }),
  );
  $("reset-filters").addEventListener("click", () => {
    search.value = "";
    result.value = "all";
    sort.value = "games";
    minimum.value = "1";
    limit.value = "10";
    group.value = "family";
    rating.value = "all";
    speed.value = "all";
    perspective = "white";
    render();
    search.focus();
  });
  render();
}

function initContext(data) {
  const gapOrder = [
    "≤ -400",
    "-399 to -200",
    "-199 to -100",
    "-99 to +99",
    "+100 to +199",
    "+200 to +399",
    "≥ +400",
  ];
  const gapRows = [...data.marts.rating_gap_metrics].sort(
    (a, b) => gapOrder.indexOf(a.label) - gapOrder.indexOf(b.label),
  );
  for (const row of gapRows) {
    const element = horizontalRow(row, row.games);
    element.querySelector(".row-value small").textContent =
      `${percent(row.white_wins / row.games)} W`;
    $("gap-list").append(element);
  }
  const endings = [...data.marts.termination_metrics].sort(
    (a, b) => b.games - a.games || a.label.localeCompare(b.label),
  );
  const maxEnding = Math.max(...endings.map((row) => row.games));
  $("termination-list").append(
    ...endings.map((row) => horizontalRow(row, maxEnding)),
  );
  const lengthOrder = [
    "1–20",
    "21–40",
    "41–60",
    "61–80",
    "81–100",
    "101–120",
    "121+",
    "Unknown",
  ];
  const lengthRows = [...data.marts.length_metrics].sort(
    (a, b) => lengthOrder.indexOf(a.label) - lengthOrder.indexOf(b.label),
  );
  const maxLength = Math.max(...lengthRows.map((row) => row.games));
  $("length-list").append(
    ...lengthRows.map((row) => horizontalRow(row, maxLength)),
  );
  const known = data.coverage.length_known;
  $("length-summary").textContent = known
    ? `${(data.coverage.total_plies / known).toFixed(1)} average plies · ${fmt(known)} known games`
    : "No reliable mainline lengths";
  for (const [label, key] of [
    ["ECO codes", "eco_known"],
    ["Termination", "termination_known"],
    ["Mainline length", "length_known"],
  ]) {
    const row = node("div", "coverage-row");
    row.append(
      node("span", null, label),
      node(
        "strong",
        null,
        `${fmt(data.coverage[key])} / ${fmt(data.counts.silver)}`,
      ),
    );
    $("coverage-list").append(row);
  }
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
    if (data.schema_version !== 2 || data.counts.silver < 1)
      throw new Error("Unsupported snapshot");
    overview(data);
    initOpeningExplorer(data);
    initContext(data);
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
