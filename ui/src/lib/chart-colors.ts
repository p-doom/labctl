export const SERIES_COLORS = [
  "var(--series-0)",
  "var(--series-1)",
  "var(--series-2)",
  "var(--series-3)",
  "var(--series-4)",
  "var(--series-5)",
  "var(--series-6)",
  "var(--series-7)",
];

export function seriesColor(idx: number): string {
  return SERIES_COLORS[idx % SERIES_COLORS.length]!;
}
