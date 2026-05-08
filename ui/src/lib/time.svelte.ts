// Single global "now" tick. Components read $now() to get a reactive
// timestamp that updates ~1Hz, so "2m ago" stays accurate without each
// component setting up its own interval.

let now = $state(Math.floor(Date.now() / 1000));

if (typeof window !== "undefined") {
  setInterval(() => {
    now = Math.floor(Date.now() / 1000);
  }, 1000);
}

export const nowSecs = {
  get value() {
    return now;
  },
};
