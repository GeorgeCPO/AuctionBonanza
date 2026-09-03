import type { ApiTree } from "@auction-bonanza/http-routing";

import type { IdentityApiDependencies } from "./dependencies.js";
import { Users } from "./users.js";

export function createV1Api(dependencies: IdentityApiDependencies): ApiTree {
  const users = new Users(dependencies);

  return {
    v1: {
      users: {
        post: users.register,
      },
    },
  };
}
