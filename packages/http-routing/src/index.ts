import type { FastifyInstance, FastifySchema, RouteHandlerMethod } from "fastify";

const httpMethods = {
  delete: "DELETE",
  get: "GET",
  head: "HEAD",
  options: "OPTIONS",
  patch: "PATCH",
  post: "POST",
  put: "PUT",
} as const;

type ApiMethod = keyof typeof httpMethods;

export interface ApiRoute {
  handler: RouteHandlerMethod;
  schema?: FastifySchema;
}

export type ApiEndpoint = ApiRoute | RouteHandlerMethod;

export interface ApiTree {
  readonly [segment: string]: ApiTree | ApiEndpoint;
}

function isApiMethod(value: string): value is ApiMethod {
  return Object.hasOwn(httpMethods, value);
}

function isApiRoute(value: ApiTree | ApiEndpoint): value is ApiRoute {
  return typeof value === "object" && value !== null && "handler" in value;
}

function registerBranch(app: FastifyInstance, branch: ApiTree, segments: string[]): void {
  for (const [key, value] of Object.entries(branch)) {
    if (isApiMethod(key)) {
      const route = typeof value === "function" ? { handler: value } : value;

      if (!isApiRoute(route)) {
        throw new Error(`HTTP method ${key} must be the last key in an API branch`);
      }

      app.route({
        method: httpMethods[key],
        url: `/${segments.join("/")}`,
        handler: route.handler,
        ...(route.schema === undefined ? {} : { schema: route.schema }),
      });
      continue;
    }

    if (
      key.length === 0 ||
      key.includes("/") ||
      typeof value !== "object" ||
      value === null ||
      isApiRoute(value)
    ) {
      throw new Error(`Invalid API path segment: ${key}`);
    }

    registerBranch(app, value, [...segments, key]);
  }
}

export function registerApi(app: FastifyInstance, api: ApiTree): void {
  registerBranch(app, api, []);
}
