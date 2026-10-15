export interface LoadedState<T> {
  data: T | undefined;
  // The message of the last failed load. `data` keeps the last good value.
  error: string | undefined;
  // Loads again, replacing `data` when it succeeds.
  reload: () => void;
}
