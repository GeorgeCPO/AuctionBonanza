import type { ApiTree } from "@auction-bonanza/http-routing";

import { SessionController } from "../sessions/session.controller.js";
import { UserController } from "../users/user.controller.js";
import type { IdentityRouteDependencies } from "./dependencies.js";

export function createV1Api(dependencies: IdentityRouteDependencies): ApiTree {
  const users = new UserController(dependencies);
  const sessions = new SessionController(dependencies);

  return {
    v1: {
      users: {
        post: users.register,
      },
      sessions: {
        post: sessions.login,
        refresh: {
          post: sessions.refresh,
        },
        current: {
          get: sessions.current,
          delete: sessions.logout,
        },
      },
    },
  };
}
