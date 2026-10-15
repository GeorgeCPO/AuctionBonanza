import type { ApiErrorBody } from "./api.types.js";

export interface HttpRequestOptions {
  method?: "GET" | "POST" | "DELETE";
  body?: unknown;
  // Sent as `Authorization: Bearer <token>` when present.
  accessToken?: string;
}

export interface ApiErrorInit {
  status: number;
  body: ApiErrorBody;
}

export type FetchFunction = typeof fetch;
