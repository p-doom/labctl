// Shared shape for filter chips — kept out of the component so views can
// import it without poking at module-block exports.

export type ChipDef = {
  /** null = "show everything" — rendered as the leading All chip. */
  key: string | null;
  label: string;
  /** Right-aligned numeric badge. Hidden if undefined. */
  count?: number;
  /** Status group for the colored dot (matches statusGroup() output).
   *  Falsy = no dot (used for repo/policy/kind chips). */
  dot?: "running" | "succeeded" | "failed" | "pending" | "neutral" | null;
  /** Render even when count is 0 / not present. */
  always?: boolean;
};
