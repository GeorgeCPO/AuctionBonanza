import { describe, expect, it, vi } from "vitest";

import type { SessionCredentials } from "../../src/api/api.types.js";
import { AuthSession } from "../../src/auth/auth-session.js";
import type { IdentityPort } from "../../src/auth/auth-session.types.js";

function credentials(n: number, expiresIn = 600): SessionCredentials {
  return {
    accessToken: `access-${n}`,
    refreshToken: `refresh-${n}`,
    refreshExpiresAt: "2030-01-01T00:00:00.000Z",
    tokenType: "Bearer",
    expiresIn,
    user: { id: "1", email: "a@example.com", username: null },
  };
}

function setup(overrides: Partial<IdentityPort> = {}) {
  let nowMs = 1_000_000;
  const mocks = {
    register: vi.fn<IdentityPort["register"]>(() =>
      Promise.resolve({ id: "1", email: "a@example.com", username: null }),
    ),
    login: vi.fn<IdentityPort["login"]>(() => Promise.resolve(credentials(1))),
    refresh: vi.fn<IdentityPort["refresh"]>(() => Promise.resolve(credentials(2))),
    logout: vi.fn<IdentityPort["logout"]>(() => Promise.resolve()),
  };
  const identity: IdentityPort = { ...mocks, ...overrides };
  const session = new AuthSession({ identity, now: () => nowMs });
  return {
    identity: mocks,
    session,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

describe("AuthSession", () => {
  it("has no token before signing in", async () => {
    const { session } = setup();

    expect(await session.getAccessToken()).toBeUndefined();
    expect(session.getView().user).toBeNull();
  });

  it("returns the access token while it is fresh, without refreshing", async () => {
    const { session, identity, advance } = setup();
    await session.login("a@example.com", "pw");
    advance(60_000);

    expect(await session.getAccessToken()).toBe("access-1");
    expect(identity.refresh).not.toHaveBeenCalled();
    expect(session.getView().user?.email).toBe("a@example.com");
  });

  it("refreshes shortly before expiry", async () => {
    const { session, identity, advance } = setup();
    await session.login("a@example.com", "pw");
    advance(580_000);

    expect(await session.getAccessToken()).toBe("access-2");
    expect(identity.refresh).toHaveBeenCalledExactlyOnceWith("refresh-1");
  });

  it("shares one refresh between concurrent callers", async () => {
    const { session, identity, advance } = setup();
    await session.login("a@example.com", "pw");
    advance(600_000);

    const tokens = await Promise.all([
      session.getAccessToken(),
      session.getAccessToken(),
      session.getAccessToken(),
    ]);

    expect(tokens).toEqual(["access-2", "access-2", "access-2"]);
    expect(identity.refresh).toHaveBeenCalledTimes(1);
  });

  it("signs the user out when the refresh fails", async () => {
    const { session, advance } = setup({ refresh: () => Promise.reject(new Error("401")) });
    await session.login("a@example.com", "pw");
    advance(600_000);

    expect(await session.getAccessToken()).toBeUndefined();
    expect(session.getView().user).toBeNull();
  });

  it("can refresh again after a refresh finished", async () => {
    const { session, identity, advance } = setup();
    await session.login("a@example.com", "pw");
    advance(600_000);
    await session.getAccessToken();
    advance(600_000);
    await session.getAccessToken();

    expect(identity.refresh).toHaveBeenCalledTimes(2);
    expect(identity.refresh).toHaveBeenLastCalledWith("refresh-2");
  });

  it("discards a refresh that finishes after sign-out", async () => {
    let finishRefresh: (credentials: SessionCredentials) => void = () => {};
    const { session, advance } = setup({
      refresh: () =>
        new Promise<SessionCredentials>((resolve) => {
          finishRefresh = resolve;
        }),
    });
    await session.login("a@example.com", "pw");
    advance(600_000);
    const pending = session.getAccessToken();

    await session.logout();
    finishRefresh(credentials(2));

    expect(await pending).toBeUndefined();
    expect(session.getView().user).toBeNull();
    expect(await session.getAccessToken()).toBeUndefined();
  });

  it("discards a refresh that finishes after a newer login", async () => {
    let finishRefresh: (credentials: SessionCredentials) => void = () => {};
    const { session, identity, advance } = setup({
      refresh: () =>
        new Promise<SessionCredentials>((resolve) => {
          finishRefresh = resolve;
        }),
    });
    await session.login("a@example.com", "pw");
    advance(600_000);
    const pending = session.getAccessToken();
    identity.login.mockResolvedValueOnce(credentials(7));
    await session.login("b@example.com", "pw");

    finishRefresh(credentials(2));
    await pending;

    expect(await session.getAccessToken()).toBe("access-7");
  });

  it("keeps a newer session when a stale refresh fails", async () => {
    let failRefresh: (error: Error) => void = () => {};
    const { session, identity, advance } = setup({
      refresh: () =>
        new Promise<SessionCredentials>((_resolve, reject) => {
          failRefresh = reject;
        }),
    });
    await session.login("a@example.com", "pw");
    advance(600_000);
    const pending = session.getAccessToken();
    identity.login.mockResolvedValueOnce(credentials(7));
    await session.login("b@example.com", "pw");

    failRefresh(new Error("401"));
    await pending;

    expect(session.getView().user).not.toBeNull();
    expect(await session.getAccessToken()).toBe("access-7");
  });

  it("registers, then signs in", async () => {
    const { session, identity } = setup();

    await session.register("a@example.com", "a-long-password");

    expect(identity.register).toHaveBeenCalledWith("a@example.com", "a-long-password", undefined);
    expect(identity.login).toHaveBeenCalledWith("a@example.com", "a-long-password");
    expect(session.getView().user).not.toBeNull();
  });

  it("clears the session on logout even when the server call fails", async () => {
    const { session } = setup({ logout: () => Promise.reject(new Error("offline")) });
    await session.login("a@example.com", "pw");

    await session.logout();

    expect(session.getView().user).toBeNull();
    expect(await session.getAccessToken()).toBeUndefined();
  });

  it("notifies subscribers until they unsubscribe", async () => {
    const { session } = setup();
    const listener = vi.fn<() => void>();
    const unsubscribe = session.subscribe(listener);

    await session.login("a@example.com", "pw");
    unsubscribe();
    await session.logout();

    expect(listener).toHaveBeenCalledTimes(1);
  });
});
