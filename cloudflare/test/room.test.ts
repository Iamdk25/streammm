import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { isPublicEdge, tokenOk } from "../src/index";
import { analyzeNowAllowed, sha256buf, sha256hex, shouldSkipAutoAnalysis, type Env } from "../src/room";
import type { Analysis } from "../src/llm";

async function openPublish(room?: string): Promise<WebSocket> {
  const u = new URL("https://example.com/publish");
  u.searchParams.set("token", "secret");
  if (room) u.searchParams.set("room", room);
  const res = await SELF.fetch(u.toString(), {
    headers: { Upgrade: "websocket" },
  });
  expect(res.status).toBe(101);
  const ws = res.webSocket;
  expect(ws).toBeTruthy();
  ws!.accept();
  return ws!;
}

async function installPin(pub: WebSocket, pin: string, exp = Date.now() + 300_000): Promise<void> {
  const hash = await sha256hex(pin);
  pub.send(JSON.stringify({ type: "otp", hash, exp }));
  await new Promise((r) => setTimeout(r, 30));
}

async function redeem(pin: string, room?: string): Promise<string> {
  const u = new URL("https://example.com/api/otp/redeem");
  if (room) u.searchParams.set("room", room);
  const res = await SELF.fetch(u.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ pin }),
  });
  expect(res.status).toBe(200);
  const body = await res.json<{ session: string }>();
  expect(body.session).toBeTruthy();
  return body.session;
}

async function openWatch(session: string, room?: string): Promise<WebSocket> {
  const u = new URL("https://example.com/watch");
  u.searchParams.set("session", session);
  if (room) u.searchParams.set("room", room);
  const res = await SELF.fetch(u.toString(), {
    headers: { Upgrade: "websocket" },
  });
  expect(res.status).toBe(101);
  const ws = res.webSocket;
  expect(ws).toBeTruthy();
  ws!.accept();
  return ws!;
}

async function viewerSession(room: string, pin = "123456"): Promise<{ pub: WebSocket; session: string }> {
  const pub = await openPublish(room);
  await installPin(pub, pin);
  const session = await redeem(pin, room);
  return { pub, session };
}

describe("StreamRoom", () => {
  it("health is public", async () => {
    const res = await SELF.fetch("https://example.com/health");
    expect(res.status).toBe(200);
  });

  it("EDGE_PUBLIC=off means the public edge is disabled", () => {
    expect(isPublicEdge({ EDGE_PUBLIC: "off" } as Env)).toBe(false);
    expect(isPublicEdge({ EDGE_PUBLIC: "on" } as Env)).toBe(true);
    expect(isPublicEdge({} as Env)).toBe(false);
  });

  it("refuses publish when STREAM_TOKEN is unset", async () => {
    const req = new Request("https://example.com/publish");
    expect(await tokenOk(req, { STREAM_TOKEN: "" } as Env)).toBe(false);
    expect(await tokenOk(req, {} as Env)).toBe(false);
  });

  it("rejects missing/wrong publisher token with 401", async () => {
    const missing = await SELF.fetch("https://example.com/publish", {
      headers: { Upgrade: "websocket" },
    });
    expect(missing.status).toBe(401);
    const wrong = await SELF.fetch("https://example.com/publish?token=nope", {
      headers: { Upgrade: "websocket" },
    });
    expect(wrong.status).toBe(401);
  });

  it("rejects watch without PIN session even with STREAM_TOKEN", async () => {
    const missing = await SELF.fetch("https://example.com/watch", {
      headers: { Upgrade: "websocket" },
    });
    expect(missing.status).toBe(401);
    const withToken = await SELF.fetch("https://example.com/watch?token=secret", {
      headers: { Upgrade: "websocket" },
    });
    expect(withToken.status).toBe(401);
    const api = await SELF.fetch("https://example.com/api/analysis?token=secret");
    expect(api.status).toBe(401);
  });

  it("publisher binary reaches a viewer after PIN redeem (no STREAM_TOKEN on watch URL)", async () => {
    const { pub, session } = await viewerSession("fanout");
    const view = await openWatch(session, "fanout");
    const got = new Promise<ArrayBuffer>((resolve) => {
      view.addEventListener("message", (ev) => {
        const data = ev.data;
        if (data instanceof ArrayBuffer) resolve(data);
        else if (typeof data === "object" && data && "arrayBuffer" in (data as Blob)) {
          (data as Blob).arrayBuffer().then(resolve);
        }
      });
    });
    const payload = new Uint8Array([2, 9, 9, 9]);
    pub.send(payload);
    const buf = new Uint8Array(await Promise.race([
      got,
      new Promise<ArrayBuffer>((_, rej) => setTimeout(() => rej(new Error("timeout")), 3000)),
    ]));
    expect(Array.from(buf)).toEqual([2, 9, 9, 9]);
    pub.close();
    view.close();
  });

  it("late viewer receives cached init then latest fragment", async () => {
    const { pub, session } = await viewerSession("late");
    pub.send(new Uint8Array([1, 1, 1]));
    pub.send(new Uint8Array([2, 2, 2]));
    await new Promise((r) => setTimeout(r, 50));
    const view = await openWatch(session, "late");
    const msgs: number[][] = [];
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("late join timeout")), 3000);
      view.addEventListener("message", (ev) => {
        const raw = ev.data;
        const deliver = (ab: ArrayBuffer) => {
          msgs.push(Array.from(new Uint8Array(ab)));
          if (msgs.length >= 2) {
            clearTimeout(t);
            resolve();
          }
        };
        if (raw instanceof ArrayBuffer) deliver(raw);
        else if (raw instanceof Blob) raw.arrayBuffer().then(deliver);
      });
    });
    expect(msgs[0][0]).toBe(1);
    expect(msgs[1][0]).toBe(2);
    pub.close();
    view.close();
  });

  it("second publisher replaces the first", async () => {
    const a = await openPublish("replace");
    const closed = new Promise<number>((resolve) => {
      a.addEventListener("close", (ev) => resolve((ev as CloseEvent).code));
    });
    const b = await openPublish("replace");
    const code = await Promise.race([
      closed,
      new Promise<number>((_, rej) => setTimeout(() => rej(new Error("no replace")), 3000)),
    ]);
    expect(code).toBe(4000);
    b.close();
  });

  it("exposes analysis APIs with redeemed session and 503 without DeepSeek key", async () => {
    const { session } = await viewerSession("llm-status");
    const res = await SELF.fetch(`https://example.com/api/analysis?session=${session}&room=llm-status`);
    expect(res.status).toBe(200);
    const body = await res.json<{
      last: unknown;
      history: unknown[];
      llm: { configured: boolean; has_snapshot: boolean; model: string };
    }>();
    expect(body.last).toBeNull();
    expect(body.history).toEqual([]);
    expect(body.llm.configured).toBe(false);
    expect(body.llm.has_snapshot).toBe(false);
    expect(body.llm.model).toContain("deepseek");

    const ask = await SELF.fetch(`https://example.com/api/ask?session=${session}&room=llm-status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: "what is on screen?" }),
    });
    expect(ask.status).toBe(503);
    const askBody = await ask.json<{ error: string }>();
    expect(askBody.error).toMatch(/DEEPSEEK_API_KEY/);

    const now = await SELF.fetch(`https://example.com/api/analyze-now?session=${session}&room=llm-status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    expect(now.status).toBe(503);
    await now.text();
  });

  it("does not fan TYPE_SNAP screenshots out to viewers", async () => {
    const { pub, session } = await viewerSession("snap");
    const view = await openWatch(session, "snap");
    let viewerGot = false;
    view.addEventListener("message", () => {
      viewerGot = true;
    });
    pub.send(new Uint8Array([4, 0xff, 0xd8, 0xff, 0xd9, 1, 2, 3, 4, 5]));
    await new Promise((r) => setTimeout(r, 250));
    expect(viewerGot).toBe(false);

    const res = await SELF.fetch(`https://example.com/api/analysis?session=${session}&room=snap`);
    const body = await res.json<{ llm: { has_snapshot: boolean; configured: boolean } }>();
    expect(body.llm.has_snapshot).toBe(true);
    expect(body.llm.configured).toBe(false);
    pub.close();
    view.close();
  });

  it("rejects empty ask questions with 400", async () => {
    const { session } = await viewerSession("ask-empty");
    const empty = await SELF.fetch(`https://example.com/api/ask?session=${session}&room=ask-empty`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: "   " }),
    });
    expect(empty.status).toBe(400);
    const body = await empty.json<{ error: string }>();
    expect(body.error).toMatch(/question/);
  });

  it("forwards authorized viewer control JSON to publisher", async () => {
    const { pub, session } = await viewerSession("ctl");
    pub.send(JSON.stringify({ type: "flags", control: true, ai: false }));
    await new Promise((r) => setTimeout(r, 30));
    const view = await openWatch(session, "ctl");
    const got = new Promise<string>((resolve) => {
      pub.addEventListener("message", (ev) => {
        if (typeof ev.data === "string") resolve(ev.data);
      });
    });
    view.send(JSON.stringify({ type: "control", action: "click", x: 0.5, y: 0.25 }));
    const raw = await Promise.race([
      got,
      new Promise<string>((_, rej) => setTimeout(() => rej(new Error("no control")), 3000)),
    ]);
    const msg = JSON.parse(raw) as { type: string; action: string; x: number; y: number };
    expect(msg.type).toBe("control");
    expect(msg.action).toBe("click");
    expect(msg.x).toBe(0.5);
    expect(msg.y).toBe(0.25);
    pub.close();
    view.close();
  });

  it("does not forward control when host disabled", async () => {
    const { pub, session } = await viewerSession("ctl-off");
    pub.send(JSON.stringify({ type: "flags", control: false, ai: false }));
    await new Promise((r) => setTimeout(r, 30));
    const view = await openWatch(session, "ctl-off");
    let got = false;
    pub.addEventListener("message", () => {
      got = true;
    });
    view.send(JSON.stringify({ type: "control", action: "click", x: 0.1, y: 0.1 }));
    await new Promise((r) => setTimeout(r, 200));
    expect(got).toBe(false);
    pub.close();
    view.close();
  });

  it("computer-use is 403 when AI disabled and forwards when enabled", async () => {
    const { pub, session } = await viewerSession("ai");
    const off = await SELF.fetch(`https://example.com/api/computer-use?session=${session}&room=ai`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task: "click then type" }),
    });
    expect(off.status).toBe(403);

    pub.send(JSON.stringify({ type: "flags", control: true, ai: true }));
    await new Promise((r) => setTimeout(r, 30));
    const got = new Promise<string>((resolve) => {
      pub.addEventListener("message", (ev) => {
        if (typeof ev.data === "string") resolve(ev.data);
      });
    });
    const on = await SELF.fetch(`https://example.com/api/computer-use?session=${session}&room=ai`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ task: "click then type" }),
    });
    expect(on.status).toBe(200);
    const raw = await Promise.race([
      got,
      new Promise<string>((_, rej) => setTimeout(() => rej(new Error("no ai")), 3000)),
    ]);
    const msg = JSON.parse(raw) as { type: string; task: string };
    expect(msg.type).toBe("computer-use");
    expect(msg.task).toBe("click then type");
    pub.close();
  });

  it("redeemed PIN session lasts a day", async () => {
    const pub = await openPublish("day-session");
    await installPin(pub, "123456");
    const u = new URL("https://example.com/api/otp/redeem");
    u.searchParams.set("room", "day-session");
    const res = await SELF.fetch(u.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin: "123456" }),
    });
    expect(res.status).toBe(200);
    const body = await res.json<{ session: string; expires_in_s: number }>();
    expect(body.expires_in_s).toBe(86400);
    const cookie = res.headers.get("set-cookie") ?? "";
    expect(cookie).toMatch(/Max-Age=86400/);
    pub.close();
  });

  it("wrong PIN is 401; STREAM_TOKEN still required to publish", async () => {
    const pub = await openPublish("badpin");
    await installPin(pub, "654321");
    const res = await SELF.fetch("https://example.com/api/otp/redeem?room=badpin", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin: "000000" }),
    });
    expect(res.status).toBe(401);
    pub.close();
  });
});

describe("analysis cost guards", () => {
  it("skips the automatic pass when the frame is unchanged and the last pass succeeded", () => {
    const last: Analysis = {
      ts: "2026-01-01T00:00:00Z",
      summary: "hud",
      questions: [],
      snapHash: "abc123",
    };
    expect(shouldSkipAutoAnalysis(last, "abc123")).toBe(true);
  });

  it("does not skip when the frame changed, the last pass errored, or no prior pass exists", () => {
    const ok: Analysis = { ts: "t", summary: "hud", questions: [], snapHash: "abc123" };
    expect(shouldSkipAutoAnalysis(ok, "different")).toBe(false);
    expect(shouldSkipAutoAnalysis({ ...ok, error: "boom" }, "abc123")).toBe(false);
    expect(shouldSkipAutoAnalysis({ ...ok, snapHash: undefined }, "abc123")).toBe(false);
    expect(shouldSkipAutoAnalysis(null, "abc123")).toBe(false);
  });

  it("rate-limits manual analyze-now within the cooldown window", () => {
    expect(analyzeNowAllowed(0, 4_999, 5_000)).toBe(false);
    expect(analyzeNowAllowed(0, 5_000, 5_000)).toBe(true);
    expect(analyzeNowAllowed(10_000, 12_000, 5_000)).toBe(false);
    expect(analyzeNowAllowed(10_000, 15_000, 5_000)).toBe(true);
  });

  it("hashes raw snapshot bytes, not a string encoding of them", async () => {
    const bytes = new TextEncoder().encode("abc");
    const hash = await sha256buf(bytes);
    // Known-answer test for SHA-256("abc").
    expect(hash).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    // Distinct input must produce a distinct digest: the guard relies on it.
    expect(await sha256buf(new TextEncoder().encode("abd"))).not.toBe(hash);
  });
});
