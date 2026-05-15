// True-async task scheduler.
//
// The browser's event loop runs:
//   task → microtasks → rendering (style/layout/paint) → next task
//
// `queueMicrotask` and `requestAnimationFrame` both fire *before* the
// next paint, in the same frame as the originating task. That means
// work scheduled with them blocks the paint of whatever just happened
// in the original task — defeating the whole point of "let the input
// paint first, let the list catch up later".
//
// `setTimeout(0)` does post a new task, but has a clamp (typically 4ms
// minimum) imposed by browsers. `MessageChannel.postMessage` is the
// idiomatic way to schedule a real task with ~0ms delay — same as
// React's scheduler did pre-Concurrent. Tasks land *after* the current
// paint, so anything queued here is guaranteed not to block the very
// next frame.

const channel = new MessageChannel();
const queue: Array<() => void> = [];

channel.port1.onmessage = () => {
  const fn = queue.shift();
  if (fn) {
    try {
      fn();
    } catch (e) {
      // Don't let a single callback take down subsequent ones.
      // eslint-disable-next-line no-console
      console.error("postTask callback threw", e);
    }
  }
};

/** Schedule a function to run as a separate task — after the current
 *  microtask checkpoint and after any pending paint. */
export function postTask(fn: () => void): void {
  queue.push(fn);
  channel.port2.postMessage(null);
}
