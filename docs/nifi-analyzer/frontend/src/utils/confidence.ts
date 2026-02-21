export function normalizeConfidence(value: number): number {
  return value <= 1 ? Math.round(value * 100) : Math.round(value);
}
