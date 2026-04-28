// All time formatting in this app is UTC. Helpers below derive every component
// via Date.prototype.getUTC*, so the viewer's local timezone never leaks into
// the rendered string.

type DateLike = string | number | Date

const pad = (n: number) => String(n).padStart(2, '0')

function asDate(input: DateLike): Date {
  return input instanceof Date ? input : new Date(input)
}

export function formatUtcDateTime(input: DateLike): string {
  const d = asDate(input)
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())} UTC`
}

export function formatUtcTime(input: DateLike): string {
  const d = asDate(input)
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())} UTC`
}

export function formatUtcDate(input: DateLike): string {
  const d = asDate(input)
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
}

// lightweight-charts feeds time as unix seconds. `withSeconds=true` returns
// HH:MM:SS, otherwise HH:MM. Used as both `timeFormatter` (crosshair label)
// and `tickMarkFormatter` (axis ticks).
export function formatUtcChartLabel(unixSeconds: number, withSeconds = false): string {
  const d = new Date(unixSeconds * 1000)
  const hms = withSeconds
    ? `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`
    : `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${hms} UTC`
}

// Tick-only variant: shorter labels for x-axis ticks (no date prefix).
export function formatUtcChartTick(unixSeconds: number, withSeconds = false): string {
  const d = new Date(unixSeconds * 1000)
  return withSeconds
    ? `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`
    : `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
}
