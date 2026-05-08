// Selection state for the cross-run compare flow. Lives in a small
// rune-state set; the URL takes over once the user enters the compare
// view. Selection is ephemeral by design — closing the tab clears it,
// and that's fine because the compare view itself is URL-addressable.

let _selected = $state<Set<string>>(new Set());

export const compareSelection = {
  get ids(): string[] {
    return [..._selected];
  },
  get size(): number {
    return _selected.size;
  },
  has(id: string): boolean {
    return _selected.has(id);
  },
  toggle(id: string): void {
    if (_selected.has(id)) _selected.delete(id);
    else _selected.add(id);
    _selected = new Set(_selected); // trigger reactivity
  },
  add(id: string): void {
    if (_selected.has(id)) return;
    _selected.add(id);
    _selected = new Set(_selected);
  },
  remove(id: string): void {
    if (!_selected.has(id)) return;
    _selected.delete(id);
    _selected = new Set(_selected);
  },
  clear(): void {
    if (_selected.size === 0) return;
    _selected = new Set();
  },
};
