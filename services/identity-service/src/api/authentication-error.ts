import type { FastifyReply } from "fastify";

export function sendAuthenticationError(reply: FastifyReply): FastifyReply {
  return reply
    .header("WWW-Authenticate", 'Bearer realm="auction-api", error="invalid_token"')
    .code(401)
    .send({ code: "INVALID_AUTHENTICATION", message: "Authentication failed" });
}
