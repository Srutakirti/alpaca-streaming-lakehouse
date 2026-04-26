import { useQuery } from '@tanstack/react-query'
import { useEffect, useRef } from 'react'
import { createChart, ColorType, LineStyle, LineSeries } from 'lightweight-charts'

interface PipelineStatus {
  extractor: Record<string, unknown> | null
  loader: Record<string, unknown> | null
  iceberg: {
    snapshot_count?: number
    latest_snapshot_ts?: number
    error?: string
  }
}

interface MetricPoint {
  _ts: string
  consumer_lag?: number
  messages_consumed?: number
}

function msAgo(ms: number): string {
  const diff = Math.round((Date.now() - ms) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

function StatusCard({ label, value, sub, colorClass }: {
  label: string; value: string; sub?: string; colorClass?: string
}) {
  return (
    <div className="card">
      <div className="card-label">{label}</div>
      <div className={`card-value ${colorClass ?? ''}`}>{value}</div>
      {sub && <div className="card-sub">{sub}</div>}
    </div>
  )
}

function LagSparkline({ data }: { data: MetricPoint[] }) {
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!ref.current || data.length === 0) return
    const chart = createChart(ref.current, {
      layout: { background: { type: ColorType.Solid, color: '#1a1d27' }, textColor: '#64748b' },
      grid: { vertLines: { color: '#2d3148' }, horzLines: { color: '#2d3148' } },
      timeScale: { timeVisible: true, secondsVisible: false },
      handleScroll: false,
      handleScale: false,
    })
    const series = chart.addSeries(LineSeries, { color: '#7c83ff', lineWidth: 1, lineStyle: LineStyle.Solid })
    const points = data
      .filter(d => d._ts && d.consumer_lag !== undefined && d.consumer_lag >= 0)
      .map(d => ({ time: Math.floor(new Date(d._ts).getTime() / 1000) as any, value: d.consumer_lag! }))
    if (points.length > 0) {
      series.setData(points)
      chart.timeScale().fitContent()
    }
    return () => chart.remove()
  }, [data])

  if (data.length === 0) return <div className="loading">No lag data yet</div>
  return <div ref={ref} className="sparkline-wrap" />
}

export default function Monitoring() {
  const { data: status, error } = useQuery<PipelineStatus>({
    queryKey: ['pipeline-status'],
    queryFn: () => fetch('/api/pipeline/status').then(r => r.json()),
    refetchInterval: 5000,
  })

  const { data: lagSeries = [] } = useQuery<MetricPoint[]>({
    queryKey: ['pipeline-metrics-loader'],
    queryFn: () => fetch('/api/pipeline/metrics?component=loader&minutes=60').then(r => r.json()),
    refetchInterval: 30000,
  })

  if (error) return <div className="error-msg">Failed to load pipeline status.</div>

  const ext = status?.extractor as Record<string, unknown> | null
  const ldr = status?.loader as Record<string, unknown> | null
  const ice = status?.iceberg

  const extConnected = ext !== null
  const loaderLag = ldr ? Number(ldr.consumer_lag ?? -1) : -1
  const lastFlushRecords = ldr ? Number(ldr.last_flush_records ?? 0) : 0
  const lastFlushMs = ldr ? Number(ldr.last_flush_duration_ms ?? 0) : 0
  const snapshotTs = ice?.latest_snapshot_ts

  return (
    <div>
      <div className="page-title">Pipeline Monitoring</div>

      <div className="cards">
        <StatusCard
          label="Extractor"
          value={status === undefined ? '...' : extConnected ? 'Connected' : 'No data'}
          colorClass={status === undefined ? 'muted' : extConnected ? 'green' : 'yellow'}
          sub={ext ? `${ext.messages_sent ?? 0} msgs sent` : undefined}
        />
        <StatusCard
          label="Consumer Lag"
          value={status === undefined ? '...' : loaderLag < 0 ? 'N/A' : String(loaderLag)}
          colorClass={loaderLag < 0 ? 'muted' : loaderLag === 0 ? 'green' : loaderLag < 100 ? 'yellow' : 'red'}
          sub={ldr ? `${ldr.batches_flushed ?? 0} batches flushed` : undefined}
        />
        <StatusCard
          label="Last Batch"
          value={status === undefined ? '...' : lastFlushRecords > 0 ? `${lastFlushRecords} rows` : '—'}
          colorClass={lastFlushRecords > 0 ? 'green' : 'muted'}
          sub={lastFlushMs > 0 ? `${lastFlushMs.toFixed(0)} ms` : undefined}
        />
        <StatusCard
          label="Iceberg Freshness"
          value={status === undefined ? '...' : snapshotTs ? msAgo(snapshotTs) : '—'}
          colorClass={snapshotTs && Date.now() - snapshotTs < 120_000 ? 'green' : 'yellow'}
          sub={ice ? `${ice.snapshot_count ?? 0} snapshots` : undefined}
        />
        <StatusCard
          label="Append Errors"
          value={status === undefined ? '...' : String(ldr?.iceberg_append_errors ?? 0)}
          colorClass={ldr?.iceberg_append_errors ? 'red' : 'green'}
        />
        <StatusCard
          label="Last Commit"
          value={ldr?.last_commit_ts ? String(ldr.last_commit_ts).slice(11, 19) : '—'}
          colorClass="muted"
          sub={ldr?.last_commit_ts ? String(ldr.last_commit_ts).slice(0, 10) : undefined}
        />
      </div>

      <div className="section">
        <div className="section-title">Consumer Lag (last 60 min)</div>
        <LagSparkline data={lagSeries} />
      </div>
    </div>
  )
}
