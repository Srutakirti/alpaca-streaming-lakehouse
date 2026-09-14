import "./style.css";

type Alert = { at_utc: string; component: string; severity: string; code: string };
type Commit = { at_utc: string; bars: number | null; source_records: number | null; duration_ms: number | null };
type Loader = {
  status: string; state: string | null; latest_heartbeat_utc: string | null;
  process_started_at_utc: string | null; last_input_utc: string | null;
  last_commit_utc: string | null; last_commit_duration_ms: number | null;
  aggregate_lag_records: number | null; buffered_bars: number | null;
  buffered_source_records: number | null; failure_count: number;
  failure_count_is_capped: boolean;
  last_failure_utc: string | null; last_failure_code: string | null;
  recent_commits: Commit[];
};
type Table = {
  status: "available" | "unavailable"; reason: string | null;
  last_metadata_update_utc: string | null; current_snapshot_commit_utc: string | null;
  latest_operation: string | null; latest_added_records: number | null;
  latest_added_data_files: number | null; latest_added_files_size_bytes: number | null;
  total_records: number | null; total_data_files: number | null; total_files_size_bytes: number | null;
  average_rows_per_data_file: number | null; average_data_file_size_bytes: number | null;
  total_delete_files: number | null; total_position_deletes: number | null;
  total_equality_deletes: number | null; snapshot_history_count: number | null;
  metadata_history_count: number | null;
};
type Cost = {
  status: "available" | "unavailable"; reason: string | null; currency: string | null;
  aggregated_at_utc: string | null; source_exported_at_utc: string | null;
  today_net_cost: number | null; seven_day_net_cost: number | null; month_to_date_net_cost: number | null;
  daily_costs: { day_utc: string; net_cost: number }[];
  top_services: { service_name: string; net_cost: number }[];
};
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
  loader: Loader;
  table: Table;
  costs: Cost;
  alerts: Alert[];
};

const utcTimeFormat = new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC", dateStyle: "medium", timeStyle: "short", hour12: false,
});
const istTimeFormat = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Asia/Kolkata", dateStyle: "medium", timeStyle: "short", hour12: false,
});
const numberFormat = new Intl.NumberFormat("en-US");

function byId<T extends HTMLElement>(id: string): T {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing dashboard element: ${id}`);
  return element as T;
}

function utc(value: string | null): string {
  return value ? `${utcTimeFormat.format(new Date(value))} UTC` : "—";
}

function utcAndIst(value: string): string {
  const instant = new Date(value);
  return `${utcTimeFormat.format(instant)} UTC · ${istTimeFormat.format(instant)} IST`;
}

function metric(value: number | null): string {
  return value === null ? "—" : numberFormat.format(value);
}

function bytes(value: number | null): string {
  if (value === null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[index]}`;
}

function money(value: number | null, currency: string | null): string {
  if (value === null || currency === null) return "—";
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency", currency, minimumFractionDigits: 2, maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${value.toFixed(2)} ${currency}`;
  }
}

function duration(value: number | null): string {
  if (value === null) return "—";
  return value < 1000 ? `${numberFormat.format(value)} ms` : `${(value / 1000).toFixed(1)} s`;
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
  const largest = Math.max(...commits.map((commit) => commit.duration_ms ?? 0), 1);
  chart.replaceChildren(...commits.slice(-24).map((commit) => {
    const column = document.createElement("div");
    const bar = document.createElement("span");
    bar.style.height = `${Math.max(8, ((commit.duration_ms ?? 0) / largest) * 100)}%`;
    bar.title = `${utc(commit.at_utc)} · ${duration(commit.duration_ms)} · ${metric(commit.bars)} bars`;
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

function renderCostChart(costs: Cost): void {
  const chart = byId<HTMLDivElement>("cost-chart");
  const empty = byId<HTMLParagraphElement>("cost-chart-empty");
  if (costs.daily_costs.length === 0) {
    chart.replaceChildren();
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  const largest = Math.max(...costs.daily_costs.map((cost) => Math.abs(cost.net_cost)), 0.01);
  chart.replaceChildren(...costs.daily_costs.map((cost) => {
    const column = document.createElement("div");
    const bar = document.createElement("span");
    bar.style.height = `${Math.max(8, (Math.abs(cost.net_cost) / largest) * 100)}%`;
    bar.title = `${cost.day_utc} · ${money(cost.net_cost, costs.currency)}`;
    column.append(bar);
    return column;
  }));
}

function renderTopServices(costs: Cost): void {
  const list = byId<HTMLUListElement>("top-services");
  list.replaceChildren();
  if (costs.top_services.length === 0) {
    list.innerHTML = "<li>No cost data available.</li>";
    return;
  }
  costs.top_services.forEach((service, index) => {
    const item = document.createElement("li");
    item.textContent = `${index + 1}. ${service.service_name}`;
    const amount = document.createElement("strong");
    amount.textContent = money(service.net_cost, costs.currency);
    item.append(amount);
    list.append(item);
  });
}

function render(metricsData: Metrics): void {
  const { market, health, extractor, loader, table, costs } = metricsData;
  const badge = byId<HTMLSpanElement>("health-badge");
  badge.textContent = title(health.status);
  badge.className = `badge ${health.status}`;
  byId("generated-at").textContent = `Refreshed ${utcAndIst(metricsData.generated_at_utc)}`;
  byId("market-state").textContent = title(market.state);
  byId("next-open").textContent = utc(market.next_expected_open_utc);
  byId("last-bar").textContent = utc(extractor.last_bar_utc);
  byId("extractor-state").textContent = title(extractor.status);
  byId("last-commit").textContent = utc(loader.last_commit_utc);
  byId("commit-detail").textContent = loader.last_commit_utc === null
    ? "No recent durable commit"
    : `${title(loader.state ?? loader.status)} · ${duration(loader.last_commit_duration_ms)} durable commit`;
  byId("loader-state").textContent = loader.state ? title(loader.state) : "—";
  byId("loader-lag").textContent = metric(loader.aggregate_lag_records);
  byId("loader-buffered-bars").textContent = metric(loader.buffered_bars);
  byId("loader-commit-duration").textContent = duration(loader.last_commit_duration_ms);
  byId("loader-health-detail").textContent = loader.latest_heartbeat_utc
    ? `Heartbeat ${utc(loader.latest_heartbeat_utc)} · last input ${utc(loader.last_input_utc)} · process started ${utc(loader.process_started_at_utc)}.`
    : "No structured loader heartbeat in the dashboard window.";
  byId("loader-failure-detail").textContent = loader.failure_count === 0
    ? "No loader failures in the dashboard window."
    : `${metric(loader.failure_count)}${loader.failure_count_is_capped ? "+" : ""} loader failure${loader.failure_count === 1 ? "" : "s"}; latest ${utc(loader.last_failure_utc)} (${title(loader.last_failure_code ?? "unknown")}).`;
  byId("received").textContent = metric(extractor.messages_received);
  byId("sent").textContent = metric(extractor.messages_sent);
  byId("delivery-failures").textContent = metric(extractor.delivery_failures);
  byId("extractor-errors").textContent = metric(extractor.errors);
  byId("table-records").textContent = metric(table.total_records);
  byId("table-files").textContent = metric(table.total_data_files);
  byId("table-size").textContent = bytes(table.total_files_size_bytes);
  byId("table-operation").textContent = table.latest_operation ? title(table.latest_operation) : "—";
  byId("table-average-file").textContent = bytes(table.average_data_file_size_bytes);
  byId("table-average-rows").textContent = metric(table.average_rows_per_data_file);
  byId("table-snapshots").textContent = metric(table.snapshot_history_count);
  byId("table-delete-files").textContent = metric(table.total_delete_files);
  byId("table-commit-detail").textContent = table.status === "available"
    ? `Current Iceberg commit ${utc(table.current_snapshot_commit_utc)} · ${metric(table.latest_added_records)} rows · ${metric(table.latest_added_data_files)} file${table.latest_added_data_files === 1 ? "" : "s"} · ${bytes(table.latest_added_files_size_bytes)}.`
    : "Table metadata is not available in this dashboard run.";
  byId("table-maintenance-detail").textContent = table.status === "available"
    ? `Metadata updated ${utc(table.last_metadata_update_utc)} · ${metric(table.metadata_history_count)} retained metadata versions · ${metric(table.total_delete_files)} delete files.`
    : "Table health is informational and does not affect pipeline health.";
  byId("cost-today").textContent = money(costs.today_net_cost, costs.currency);
  byId("cost-seven-day").textContent = money(costs.seven_day_net_cost, costs.currency);
  byId("cost-mtd").textContent = money(costs.month_to_date_net_cost, costs.currency);
  byId("cost-detail").textContent = costs.status === "available"
    ? `Estimated usage cost · UTC · billing export refreshed ${utc(costs.source_exported_at_utc)} · aggregate refreshed ${utc(costs.aggregated_at_utc)}.`
    : "Cost data is not available in this dashboard run. Pipeline health is unaffected.";
  byId("shutdown-detail").textContent = extractor.shutdown_reason
    ? `Clean shutdown after ${title(extractor.shutdown_reason)}.`
    : extractor.status === "observed" ? "Live session observed; final metrics are pending."
    : "No current session observed.";
  byId("final-metrics").textContent = extractor.final_metrics_at_utc
    ? `Final metrics recorded ${utc(extractor.final_metrics_at_utc)}.`
    : extractor.status === "observed" ? "Live session; final metrics will appear after the idle shutdown window."
    : "Final extractor metrics are not yet available.";
  byId("summary").textContent = health.reasons.length === 0
    ? `${title(market.state)} · no current dashboard concerns.`
    : `${title(market.state)} · ${health.reasons.map(title).join(", ")}.`;
  const notice = byId<HTMLDivElement>("notice");
  notice.hidden = market.state !== "weekend" && market.state !== "closed";
  notice.textContent = "No bars are expected outside the market session. The last completed session remains visible.";
  renderChart(loader.recent_commits);
  renderCostChart(costs);
  renderTopServices(costs);
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
