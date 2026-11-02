import type { ApiErrorBody } from "./api.types.js";
import type { ApiErrorInit, FetchFunction, HttpRequestOptions } from "./http.types.js";

// A non-2xx answer from a service. `code` is the machine-readable error code
// from the response body, which is what the UI maps to a message.
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly body: ApiErrorBody;

  constructor(init: ApiErrorInit) {
    super(init.body.message);
    this.name = "ApiError";
    this.status = init.status;
    this.code = init.body.code;
    this.body = init.body;
  }
}

// A thin fetch wrapper for one service, reached through a path prefix on the
// dev server (see vite.config.ts). It adds JSON handling and the bearer
// token, and turns error responses into ApiError. It does no retrying: what
// is safe to retry depends on the call.
export class HttpClient {
  constructor(
    private readonly prefix: string,
    private readonly fetchFunction: FetchFunction = (input, init) => fetch(input, init),
  ) {}

  async request<T>(path: string, options: HttpRequestOptions = {}): Promise<T> {
    const headers: Record<string, string> = {};
    if (options.body !== undefined) {
      headers["content-type"] = "application/json";
    }
    if (options.accessToken !== undefined) {
      headers.authorization = `Bearer ${options.accessToken}`;
    }
    const init: RequestInit = { method: options.method ?? "GET", headers };
    if (options.body !== undefined) {
      init.body = JSON.stringify(options.body);
    }

    const response = await this.fetchFunction(`${this.prefix}${path}`, init);
    if (response.status === 204) {
      return undefined as T;
    }
    const parsed: unknown = await response.json().catch(() => undefined);
    if (!response.ok) {
      throw new ApiError({ status: response.status, body: this.toErrorBody(parsed, response) });
    }
    return parsed as T;
  }

  private toErrorBody(parsed: unknown, response: Response): ApiErrorBody {
    const isErrorBody =
      typeof parsed === "object" &&
      parsed !== null &&
      typeof (parsed as { code?: unknown }).code === "string";
    if (isErrorBody) {
      const body = parsed as Partial<ApiErrorBody>;
      return {
        ...body,
        code: body.code ?? "UNKNOWN",
        message: body.message ?? response.statusText,
      };
    }
    return { code: "UNEXPECTED_RESPONSE", message: `HTTP ${response.status}` };
  }
}
