import "./style.css";

type Alert = { at_utc: string; component: string; severity: string; code: string };
type Commit = { at_utc: string; received: number; inserted: number };
type Metrics = {
  generated_at_utc: string;
  market: { state: string; next_expected_open_utc: string };
  health: { status: string; reasons: string[] };
  extractor: {
    status: string; last_event_utc: string | null; last_bar_utc: string | null;
    messages_received: number | null; messages_sent: number | null;
    delivery_failures: number | null; errors: number | null;
    final_metrics_at_utc: string | null; shutdown_reason: string | null;
  };
  loader: { last_commit_utc: string | null; last_received: number | null; last_inserted: number | null; recent_commits: Commit[] };
  alerts: Alert[];
  links: { extractor_logs: string; loader_logs: string };
};

const timeFormat = new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC", dateStyle: "medium", timeStyle: "short", hour12: false,
});
const numberFormat = new Intl.NumberFormat("en-US");

function byId<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing dashboard element: ${id}`);
  return element as T;
}

function utc(value: string | null): string {
  return value ? `${timeFormat.format(new Date(value))} UTC` : "—";
}

function metric(value: number | null): string {
  return value === null ? "—" : numberFormat.format(value);
}

function title(value: string): string {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function renderChart(commits: Commit[]): void {
  const chart = byId<HTMLDivElement>("commit-chart");
  const empty = byId<HTMLParagraphElement>("chart-empty");
  if (commits.length === 0) {
    chart.replaceChildren();
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  const largest = Math.max(...commits.map((commit) => commit.inserted), 1);
  chart.replaceChildren(...commits.slice(-24).map((commit) => {
    const column = document.createElement("div");
    const bar = document.createElement("span");
    bar.style.height = `${Math.max(8, (commit.inserted / largest) * 100)}%`;
    bar.title = `${utc(commit.at_utc)} · ${numberFormat.format(commit.inserted)} inserted`;
    column.append(bar);
    return column;
  }));
}

function renderAlerts(alerts: Alert[]): void {
  const list = byId<HTMLUListElement>("alerts");
  list.replaceChildren();
  if (alerts.length === 0) {
    list.innerHTML = "<li>No recent alerts.</li>";
    return;
  }
  alerts.forEach((alert) => {
    const item = document.createElement("li");
    item.textContent = `${utc(alert.at_utc)} · ${alert.severity} · ${title(alert.code)}`;
    list.append(item);
  });
}

function render(metricsData: Metrics): void {
  const { market, health, extractor, loader } = metricsData;
  const badge = byId<HTMLSpanElement>("health-badge");
  badge.textContent = title(health.status);
  badge.className = `badge ${health.status}`;
  byId("generated-at").textContent = `Refreshed ${utc(metricsData.generated_at_utc)}`;
  byId("market-state").textContent = title(market.state);
  byId("next-open").textContent = utc(market.next_expected_open_utc);
  byId("last-bar").textContent = utc(extractor.last_bar_utc);
  byId("extractor-state").textContent = title(extractor.status);
  byId("last-commit").textContent = utc(loader.last_commit_utc);
  byId("commit-detail").textContent = loader.last_inserted === null
    ? "No recent commit" : `${metric(loader.last_received)} received · ${metric(loader.last_inserted)} inserted`;
  byId("received").textContent = metric(extractor.messages_received);
  byId("sent").textContent = metric(extractor.messages_sent);
  byId("delivery-failures").textContent = metric(extractor.delivery_failures);
  byId("extractor-errors").textContent = metric(extractor.errors);
  byId<HTMLAnchorElement>("extractor-link").href = metricsData.links.extractor_logs;
  byId<HTMLAnchorElement>("loader-link").href = metricsData.links.loader_logs;
  byId("shutdown-detail").textContent = extractor.shutdown_reason
    ? `Clean shutdown after ${title(extractor.shutdown_reason)}.` : "Session is active or final metrics are pending.";
  byId("final-metrics").textContent = extractor.final_metrics_at_utc
    ? `Final metrics recorded ${utc(extractor.final_metrics_at_utc)}.` : "Final extractor metrics are not yet available.";
  byId("summary").textContent = health.reasons.length === 0
    ? `${title(market.state)} · no current dashboard concerns.`
    : `${title(market.state)} · ${health.reasons.map(title).join(", ")}.`;
  const notice = byId<HTMLDivElement>("notice");
  notice.hidden = market.state !== "weekend" && market.state !== "closed";
  notice.textContent = "No bars are expected outside the market session. The last completed session remains visible.";
  renderChart(loader.recent_commits);
  renderAlerts(metricsData.alerts);
}

async function load(): Promise<void> {
  try {
    const response = await fetch("./metrics.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`Metrics request failed: ${response.status}`);
    render(await response.json() as Metrics);
  } catch (error) {
    byId("summary").textContent = "Public metrics are not available yet.";
    byId("health-badge").textContent = "Unavailable";
    byId("health-badge").className = "badge unknown";
    byId("notice").hidden = false;
    byId("notice").textContent = error instanceof Error ? error.message : "Unable to load metrics.";
  }
}

void load();
