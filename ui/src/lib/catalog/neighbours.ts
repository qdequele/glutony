/**
 * What can precede and what can follow an action.
 *
 * The DAG's only compatibility rule is that a step's `accepts` must contain the
 * previous step's `produces`. Surfacing that turns the catalog from a list into
 * a composition aid.
 */
import type { MergedAction } from "./merge";

/** The actions adjacent to one action in a pipeline. */
export interface Neighbours {
  /** Actions whose output this one accepts — they can run before it. */
  canFollow: MergedAction[];
  /** Actions that accept this one's output — they can run after it. */
  canFeed: MergedAction[];
}

/** Compute both neighbour sets, preserving the order of `all`. */
export function neighboursOf(action: MergedAction, all: MergedAction[]): Neighbours {
  const others = all.filter((other) => other.entry.plugin !== action.entry.plugin);
  return {
    canFollow: others.filter((other) =>
      action.accepts.some((kind) => kind === other.produces),
    ),
    canFeed: others.filter((other) =>
      other.accepts.some((kind) => kind === action.produces),
    ),
  };
}
