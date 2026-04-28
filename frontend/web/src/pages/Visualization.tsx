import { useState, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { createChart, ColorType, CandlestickSeries, HistogramSeries } from 'lightweight-charts'
import { formatUtcChartLabel, formatUtcChartTick } from '../lib/time'

interface Bar {
  t: string
  o: number
  h: number
  l: number
  c: number
  v: number
}

function toUnix(iso: string): number {
  return Math.floor(new Date(iso).getTime() / 1000)
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/
const TIME_RE = /^\d{2}:\d{2}(:\d{2})?$/

const isValidDate = (s: string) => s === '' || DATE_RE.test(s)
const isValidTime = (s: string) => s === '' || TIME_RE.test(s)

// Combine separate date + time UTC strings into a Z-suffixed ISO. Returns '' if either is malformed.
function combineUtcISO(date: string, time: string, defaultTime: string): string {
  if (!DATE_RE.test(date)) return ''
  if (time !== '' && !TIME_RE.test(time)) return ''
  const t = time || defaultTime
  const padded = t.length === 5 ? `${t}:00` : t  // HH:MM → HH:MM:SS
  return `${date}T${padded}Z`
}

function utcDateString(offsetSeconds = 0): string {
  const d = new Date(Date.now() + offsetSeconds * 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
}

export default function Visualization() {
  const [symbol, setSymbol] = useState('')
  // Draft inputs — what the user is typing. Not used by the query directly.
  const [fromDate, setFromDate] = useState(utcDateString(-7 * 86400))
  const [fromTime, setFromTime] = useState('00:00:00')
  const [toDate, setToDate] = useState('')        // empty = no upper bound
  const [toTime, setToTime] = useState('')
  // Applied snapshot — set only when the user clicks Apply (or hits Enter).
  // The chart query uses these.
  const [appliedFromISO, setAppliedFromISO] = useState(
    combineUtcISO(utcDateString(-7 * 86400), '00:00:00', '00:00:00')
  )
  const [appliedToISO, setAppliedToISO] = useState('')
  const [uniqueTimestamps, setUniqueTimestamps] = useState(0)
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: symbols = [] } = useQuery<string[]>({
    queryKey: ['symbols'],
    queryFn: () => fetch('/api/symbols').then(r => r.json()),
    staleTime: 60_000,
  })

  useEffect(() => {
    if (!symbol && symbols.length > 0) setSymbol(symbols[0])
  }, [symbols, symbol])

  // Draft validity — used for the Apply button enable state and red borders.
  const draftFromISO = combineUtcISO(fromDate, fromTime, '00:00:00')
  const draftToISO = combineUtcISO(toDate, toTime, '23:59:59')
  const draftValid =
    isValidDate(fromDate) && isValidTime(fromTime) &&
    isValidDate(toDate) && isValidTime(toTime) &&
    // From must be present and parseable
    DATE_RE.test(fromDate) &&
    // To: either fully empty OR fully valid date (time optional)
    (toDate === '' || DATE_RE.test(toDate))
  const draftDirty = draftFromISO !== appliedFromISO || draftToISO !== appliedToISO

  const applyFilter = () => {
    if (!draftValid) return
    setAppliedFromISO(draftFromISO)
    setAppliedToISO(draftToISO)
  }
  const clearTo = () => {
    setToDate(''); setToTime('')
    setAppliedToISO('')
  }
  const onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') applyFilter()
  }

  const { data: bars = [], isFetching } = useQuery<Bar[]>({
    queryKey: ['bars', symbol, appliedFromISO, appliedToISO],
    queryFn: () => {
      const params = new URLSearchParams({ symbol, limit: '5000' })
      if (appliedFromISO) params.set('from', appliedFromISO)
      if (appliedToISO) params.set('to', appliedToISO)
      return fetch(`/api/bars?${params.toString()}`).then(r => r.json())
    },
    enabled: !!symbol,
    staleTime: 30_000,
  })

  useEffect(() => {
    if (!chartRef.current || bars.length === 0) return

    const chart = createChart(chartRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#1a1d27' }, textColor: '#94a3b8' },
      grid: { vertLines: { color: '#2d3148' }, horzLines: { color: '#2d3148' } },
      timeScale: {
        timeVisible: true,
        secondsVisible: true,
        tickMarkFormatter: (t: number) => formatUtcChartTick(t, true),
      },
      localization: { timeFormatter: (t: number) => formatUtcChartLabel(t, true) },
      width: chartRef.current.clientWidth,
      height: 300,
    })

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#4ade80',
      downColor: '#f87171',
      borderUpColor: '#4ade80',
      borderDownColor: '#f87171',
      wickUpColor: '#4ade80',
      wickDownColor: '#f87171',
    })

    const volSeries = chart.addSeries(HistogramSeries, {
      color: '#7c83ff44',
      priceFormat: { type: 'volume' },
      priceScaleId: 'vol',
    })
    chart.priceScale('vol').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })

    // Deduplicate by unix second (lightweight-charts requires strictly ascending times).
    // When multiple bars share a timestamp (e.g. synthetic data), merge into one candle.
    const sorted = [...bars].sort((a, b) => a.t.localeCompare(b.t))
    const byTime = new Map<number, Bar>()
    for (const b of sorted) {
      const ts = toUnix(b.t)
      const existing = byTime.get(ts)
      if (!existing) {
        byTime.set(ts, { ...b })
      } else {
        existing.h = Math.max(existing.h, b.h)
        existing.l = Math.min(existing.l, b.l)
        existing.c = b.c
        existing.v += b.v
      }
    }
    const deduped = Array.from(byTime.entries()).sort((a, b) => a[0] - b[0])

    setUniqueTimestamps(deduped.length)
    if (deduped.length < 2) {
      chart.remove()
      return
    }

    candleSeries.setData(deduped.map(([ts, b]) => ({
      time: ts as any,
      open: b.o, high: b.h, low: b.l, close: b.c,
    })))
    volSeries.setData(deduped.map(([ts, b]) => ({
      time: ts as any,
      value: b.v,
      color: b.c >= b.o ? '#4ade8044' : '#f8717144',
    })))
    chart.timeScale().fitContent()

    const ro = new ResizeObserver(() => {
      if (chartRef.current) chart.applyOptions({ width: chartRef.current.clientWidth })
    })
    ro.observe(chartRef.current)

    return () => { chart.remove(); ro.disconnect() }
  }, [bars])

  return (
    <div>
      <div className="page-title">OHLC Visualization</div>

      <div className="controls">
        <label>Symbol</label>
        <select value={symbol} onChange={e => setSymbol(e.target.value)}>
          {symbols.map(s => <option key={s}>{s}</option>)}
        </select>

        <label>From (UTC)</label>
        <input
          type="text"
          placeholder="YYYY-MM-DD"
          value={fromDate}
          onChange={e => setFromDate(e.target.value)}
          onKeyDown={onKeyDown}
          style={{ width: '7rem', borderColor: isValidDate(fromDate) ? undefined : '#f87171' }}
        />
        <input
          type="text"
          placeholder="HH:MM:SS"
          value={fromTime}
          onChange={e => setFromTime(e.target.value)}
          onKeyDown={onKeyDown}
          style={{ width: '5.5rem', borderColor: isValidTime(fromTime) ? undefined : '#f87171' }}
        />

        <label>To (UTC)</label>
        <input
          type="text"
          placeholder="YYYY-MM-DD"
          value={toDate}
          onChange={e => setToDate(e.target.value)}
          onKeyDown={onKeyDown}
          style={{ width: '7rem', borderColor: isValidDate(toDate) ? undefined : '#f87171' }}
        />
        <input
          type="text"
          placeholder="HH:MM:SS"
          value={toTime}
          onChange={e => setToTime(e.target.value)}
          onKeyDown={onKeyDown}
          style={{ width: '5.5rem', borderColor: isValidTime(toTime) ? undefined : '#f87171' }}
        />
        <button type="button" onClick={clearTo} title="Clear upper bound (open)">×</button>

        <button
          type="button"
          onClick={applyFilter}
          disabled={!draftValid || !draftDirty || isFetching}
          title={!draftValid ? 'Fix invalid inputs' : draftDirty ? 'Apply filter' : 'Already applied'}
          style={{
            background: draftDirty && draftValid ? '#7c83ff' : '#2d3148',
            color: draftDirty && draftValid ? '#fff' : '#94a3b8',
            cursor: draftDirty && draftValid ? 'pointer' : 'default',
            fontWeight: 600,
          }}
        >
          Apply
        </button>

        {isFetching && <span className="muted" style={{ fontSize: '0.8rem' }}>Loading...</span>}
      </div>

      {bars.length === 0 && !isFetching && symbol && (
        <div className="loading">No bars found for {symbol} in this range.</div>
      )}
      {bars.length > 0 && uniqueTimestamps < 2 && !isFetching && (
        <div className="loading">
          {bars.length} bar{bars.length !== 1 ? 's' : ''} found but all share the same timestamp —
          chart requires at least 2 distinct time points. Try a wider date range or use live Alpaca data.
        </div>
      )}

      <div className="section">
        <div className="section-title">{symbol} — {appliedFromISO || '(open)'} to {appliedToISO || '(open)'} ({bars.length} bars)</div>
        <div ref={chartRef} className="chart-wrap" />
      </div>
    </div>
  )
}
