export interface IsolatedDatabase {
  url: string;
  drop(): Promise<void>;
}
