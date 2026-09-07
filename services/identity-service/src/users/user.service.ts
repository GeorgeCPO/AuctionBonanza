import type { PasswordSecurityPort } from "../security/password-security.js";
import type { RegisteredUser, UserRepository } from "./user.repository.js";

export interface RegisterUserInput {
  email: string;
  password: string;
  username?: string;
}

export type RegisterUserOutcome =
  | { kind: "created"; user: RegisteredUser }
  | { kind: "duplicate_email" };

export class UserService {
  constructor(
    private readonly users: UserRepository,
    private readonly passwordSecurity: PasswordSecurityPort,
  ) {}

  async register(input: RegisterUserInput): Promise<RegisterUserOutcome> {
    const email = input.email.trim().toLowerCase();
    const username = input.username?.trim() ?? null;
    const passwordHash = await this.passwordSecurity.hash(input.password);

    return this.users.create({ email, username, passwordHash });
  }
}
