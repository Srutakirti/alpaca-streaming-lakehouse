import { useState, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { createChart, ColorType, CandlestickSeries, HistogramSeries } from 'lightweight-charts'

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

function today(): string {
  return new Date().toISOString().slice(0, 10)
}

function daysAgo(n: number): string {
  const d = new Date()
  d.setDate(d.getDate() - n)
  return d.toISOString().slice(0, 10)
}

export default function Visualization() {
  const [symbol, setSymbol] = useState('')
  const [fromDate, setFromDate] = useState(daysAgo(7))
  const [toDate, setToDate] = useState(today())
  const chartRef = useRef<HTMLDivElement>(null)

  const { data: symbols = [] } = useQuery<string[]>({
    queryKey: ['symbols'],
    queryFn: () => fetch('/api/symbols').then(r => r.json()),
    staleTime: 60_000,
  })

  useEffect(() => {
    if (!symbol && symbols.length > 0) setSymbol(symbols[0])
  }, [symbols, symbol])

  const { data: bars = [], isFetching } = useQuery<Bar[]>({
    queryKey: ['bars', symbol, fromDate, toDate],
    queryFn: () =>
      fetch(`/api/bars?symbol=${symbol}&from=${fromDate}T00:00:00Z&to=${toDate}T23:59:59Z&limit=5000`)
        .then(r => r.json()),
    enabled: !!symbol,
    staleTime: 30_000,
  })

  useEffect(() => {
    if (!chartRef.current || bars.length === 0) return

    const chart = createChart(chartRef.current, {
      layout: { background: { type: ColorType.Solid, color: '#1a1d27' }, textColor: '#94a3b8' },
      grid: { vertLines: { color: '#2d3148' }, horzLines: { color: '#2d3148' } },
      timeScale: { timeVisible: true, secondsVisible: false },
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

    const sorted = [...bars].sort((a, b) => a.t.localeCompare(b.t))
    candleSeries.setData(sorted.map(b => ({
      time: toUnix(b.t) as any,
      open: b.o, high: b.h, low: b.l, close: b.c,
    })))
    volSeries.setData(sorted.map(b => ({
      time: toUnix(b.t) as any,
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

        <label>From</label>
        <input type="date" value={fromDate} onChange={e => setFromDate(e.target.value)} />

        <label>To</label>
        <input type="date" value={toDate} onChange={e => setToDate(e.target.value)} />

        {isFetching && <span className="muted" style={{ fontSize: '0.8rem' }}>Loading...</span>}
      </div>

      {bars.length === 0 && !isFetching && symbol && (
        <div className="loading">No bars found for {symbol} in this range.</div>
      )}

      <div className="section">
        <div className="section-title">{symbol} — {fromDate} to {toDate} ({bars.length} bars)</div>
        <div ref={chartRef} className="chart-wrap" />
      </div>
    </div>
  )
}
