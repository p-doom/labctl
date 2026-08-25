const ROLLOUT_WINDOW_RADIUS = 100;

export function rolloutWindow(length: number, current: number): number[] {
  if (!Number.isSafeInteger(length) || length < 0) {
    throw new RangeError("rollout length must be a non-negative integer");
  }
  if (length === 0) return [];
  if (!Number.isSafeInteger(current) || current < 0 || current >= length) {
    throw new RangeError("current rollout frame is outside the trajectory");
  }

  const width = 2 * ROLLOUT_WINDOW_RADIUS + 1;
  const start = Math.min(
    Math.max(0, current - ROLLOUT_WINDOW_RADIUS),
    Math.max(0, length - width),
  );
  const end = Math.min(length, start + width);
  return Array.from({ length: end - start }, (_, index) => start + index);
}
