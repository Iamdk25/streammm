import { DurableObject } from "cloudflare:workers";
import {
  ANALYZE_PROMPT,
  analysisFromModel,
  askFromModel,
  askPrompt,
  completeVision,
  DEFAULT_BASE,
  DEFAULT_MODEL,
  parseJsonObject,
  type Analysis,
} from "./llm";

export type ConnRole = "publisher" | "viewer";

export interface Env {
  STREAM_ROOM: DurableObjectNamespace;
  STREAM_TOKEN?: string;
  EDGE_PUBLIC?: string;
  DEEPSEEK_API_KEY?: string;
  DEEPSEEK_BASE_URL?: string;
  DEEPSEEK_MODEL?: string;
}

type Attachment = { role: ConnRole; session?: string };

const CLOSE_REPLACED = 4000;
const TYPE_INIT = 1;
const TYPE_FRAG = 2;
const TYPE_SNAP = 4;
const FAIL_LIMIT = 5;
const LOCKOUT_MS = 30_000;
const SESSION_TTL_MS = 86_400_000;
const ANALYZE_NOW_COOLDOWN_MS = 5_000;

export async function sha256hex(s: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** SHA-256 over raw bytes (not a string encoding of them). */
export async function sha256buf(u: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", asBuf(u));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * True when the automatic analysis pass can reuse the last result: the frame
 * is byte-identical to the one already analyzed and that pass succeeded.
 */
export function shouldSkipAutoAnalysis(last: Analysis | null, snapHash: string): last is Analysis {
  return !!last && !last.error && last.snapHash === snapHash;
}

/** True when `cooldownMs` has elapsed since the last forced (paid) analysis. */
export function analyzeNowAllowed(lastForcedAt: number, now: number, cooldownMs: number): boolean {
  return now - lastForcedAt >= cooldownMs;
}

function hexRandom(n: number): string {
  const b = new Uint8Array(n);
  crypto.getRandomValues(b);
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

function cookieVal(request: Request, name: string): string {
  const c = request.headers.get("Cookie") ?? "";
  for (const part of c.split(";")) {
    const p = part.trim();
    if (p.startsWith(name + "=")) return decodeURIComponent(p.slice(name.length + 1));
  }
  return "";
}

function requestSession(request: Request, url: URL): string {
  const q = url.searchParams.get("session") ?? "";
  if (q) return q;
  const ck = cookieVal(request, "streamaid_session");
  if (ck) return ck;
  const auth = request.headers.get("Authorization") ?? "";
  if (auth.startsWith("Bearer ")) return auth.slice(7);
  return "";
}

function asBuf(u: Uint8Array): ArrayBuffer {
  return u.buffer.slice(u.byteOffset, u.byteOffset + u.byteLength) as ArrayBuffer;
}

function corsHeaders(): Record<string, string> {
  return {
    "content-type": "application/json",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "Authorization, Content-Type",
    "access-control-allow-methods": "GET, POST, OPTIONS",
  };
}

export class StreamRoom extends DurableObject<Env> {
  private lastInit: ArrayBuffer | null = null;
  private lastFrag: ArrayBuffer | null = null;
  private lastSnap: Uint8Array | null = null;
  private lastAnalysis: Analysis | null = null;
  private history: Analysis[] = [];
  private analyzing = false;
  private snapSeq = 0;
  private analyzedSeq = 0;
  private otpHash = "";
  private otpExp = 0;
  private sessions = new Map<string, number>();
  private fails = 0;
  private lockUntil = 0;
  private lastForcedAt = 0;
  private flags = { control: false, ai: false };
  private flagsHydrated = false;
  private controller: string | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.ctx.setHibernatableWebSocketEventTimeout(60_000);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) {
      return this.handleApi(request, url);
    }
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    const role: ConnRole = url.pathname.endsWith("/publish") ? "publisher" : "viewer";
    if (role === "viewer") {
      const sess = requestSession(request, url);
      if (!(await this.sessionOk(sess))) {
        return Response.json({ error: "unauthorized" }, { status: 401, headers: corsHeaders() });
      }
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    const session = role === "viewer" ? requestSession(request, url) : undefined;
    server.serializeAttachment({ role, session } satisfies Attachment);

    if (role === "publisher") {
      for (const ws of this.ctx.getWebSockets()) {
        if (ws === server) continue;
        const att = ws.deserializeAttachment() as Attachment | null;
        if (att?.role === "publisher") {
          try {
            ws.close(CLOSE_REPLACED, "replaced");
          } catch {
            /* ignore */
          }
        }
      }
    } else {
      if (this.lastInit) {
        try {
          server.send(this.lastInit);
        } catch {
          /* ignore */
        }
      }
      if (this.lastFrag) {
        try {
          server.send(this.lastFrag);
        } catch {
          /* ignore */
        }
      }
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const att = ws.deserializeAttachment() as Attachment | null;
    if (typeof message === "string") {
      await this.handleJson(ws, att, message);
      return;
    }
    if (att?.role !== "publisher") return;
    const bytes = message instanceof ArrayBuffer ? new Uint8Array(message) : new Uint8Array(message);
    if (bytes.length < 1) return;
    const kind = bytes[0];
    if (kind === TYPE_SNAP) {
      this.lastSnap = bytes.slice(1);
      this.snapSeq += 1;
      // Do not persist the JPEG on the live-video path: storage I/O takes
      // the Durable Object input gate and stalls /publish for seconds.
      if (this.apiKey() && !this.analyzing) {
        void this.ctx.storage.setAlarm(Date.now() + 1500);
      }
      return;
    }
    if (kind === TYPE_INIT) this.lastInit = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    if (kind === TYPE_FRAG) this.lastFrag = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    for (const peer of this.ctx.getWebSockets()) {
      if (peer === ws) continue;
      const p = peer.deserializeAttachment() as Attachment | null;
      if (p?.role !== "viewer") continue;
      if (peer.readyState !== WebSocket.OPEN) continue;
      try {
        peer.send(message);
      } catch {
        /* drop-oldest equivalent: skip a full send buffer */
      }
    }
  }

  private async handleJson(ws: WebSocket, att: Attachment | null, raw: string): Promise<void> {
    let v: { type?: string; hash?: string; exp?: number; control?: boolean; ai?: boolean; action?: string; x?: number; y?: number; text?: string; key?: string; dy?: number; task?: string };
    try {
      v = JSON.parse(raw) as typeof v;
    } catch {
      return;
    }
    if (att?.role === "publisher") {
      if (v.type === "otp" && typeof v.hash === "string") {
        this.otpHash = v.hash;
        this.otpExp = Number(v.exp) || Date.now() + 300_000;
        this.fails = 0;
        this.lockUntil = 0;
        await this.ctx.storage.put("otp", { hash: this.otpHash, exp: this.otpExp });
      }
      if (v.type === "flags") {
        this.flags.control = !!v.control;
        this.flags.ai = !!v.ai;
        this.flagsHydrated = true;
        await this.ctx.storage.put("flags", this.flags);
      }
      return;
    }
    if (att?.role !== "viewer") return;
    const session = att.session || "";
    if (!(await this.sessionOk(session))) return;
    if (v.type === "control") {
      if (!this.flags.control) return;
      if (this.controller && this.controller !== session) return;
      this.controller = session;
      this.sendPublisher(JSON.stringify({ ...v, session, type: "control" }));
      return;
    }
    if (v.type === "computer-use") {
      if (!this.flags.ai) return;
      this.sendPublisher(JSON.stringify({ type: "computer-use", task: v.task || "", session }));
    }
  }

  private sendPublisher(msg: string): void {
    for (const peer of this.ctx.getWebSockets()) {
      const p = peer.deserializeAttachment() as Attachment | null;
      if (p?.role !== "publisher" || peer.readyState !== WebSocket.OPEN) continue;
      try {
        peer.send(msg);
      } catch {
        /* ignore */
      }
    }
  }

  private async sessionOk(token: string): Promise<boolean> {
    if (!token) return false;
    await this.hydrateAuth();
    const hash = await sha256hex(token);
    const exp = this.sessions.get(hash);
    if (!exp) return false;
    if (exp <= Date.now()) {
      this.sessions.delete(hash);
      return false;
    }
    return true;
  }

  private async hydrateAuth(): Promise<void> {
    if (!this.otpHash) {
      const otp = await this.ctx.storage.get<{ hash: string; exp: number }>("otp");
      if (otp?.hash) {
        this.otpHash = otp.hash;
        this.otpExp = otp.exp;
      }
    }
    if (this.sessions.size === 0) {
      const sess = await this.ctx.storage.get<Record<string, number>>("sessions");
      if (sess) {
        for (const [k, exp] of Object.entries(sess)) this.sessions.set(k, exp);
      }
    }
    if (!this.flagsHydrated) {
      const flags = await this.ctx.storage.get<{ control: boolean; ai: boolean }>("flags");
      if (flags) this.flags = { control: !!flags.control, ai: !!flags.ai };
      this.flagsHydrated = true;
    }
  }

  private async persistSessions(): Promise<void> {
    const obj: Record<string, number> = {};
    for (const [k, v] of this.sessions) obj[k] = v;
    await this.ctx.storage.put("sessions", obj);
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    const safe = code === 1000 || (code >= 3000 && code <= 4999) ? code : 1000;
    try {
      ws.close(safe, reason);
    } catch {
      /* ignore */
    }
  }

  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error(JSON.stringify({ message: "websocket error", error: String(error) }));
    try {
      ws.close(1011, "error");
    } catch {
      /* ignore */
    }
  }

  async alarm(): Promise<void> {
    await this.hydrate();
    await this.runAnalysis();
  }

  private async hydrate(): Promise<void> {
    if (!this.lastSnap) {
      const snap = await this.ctx.storage.get<ArrayBuffer>("snap");
      if (snap) this.lastSnap = new Uint8Array(snap);
    }
    if (!this.lastAnalysis) {
      const last = await this.ctx.storage.get<string>("last");
      if (last) {
        try {
          this.lastAnalysis = JSON.parse(last) as Analysis;
        } catch {
          this.lastAnalysis = null;
        }
      }
    }
    if (this.history.length === 0) {
      const hist = await this.ctx.storage.get<Analysis[]>("history");
      if (Array.isArray(hist)) this.history = hist;
    }
  }

  private async persistSnap(): Promise<void> {
    // Tiny payloads are test stubs; skip storage so vitest isolated SQLite stays clean.
    if (!this.lastSnap || this.lastSnap.length < 128) return;
    try {
      await this.ctx.storage.put("snap", asBuf(this.lastSnap));
    } catch (e) {
      console.error(JSON.stringify({ message: "persist snap failed", error: String(e) }));
    }
  }

  private async persistAnalysis(a: Analysis): Promise<void> {
    try {
      await this.ctx.storage.put("last", JSON.stringify(a));
      if (!a.error && a.summary) {
        await this.ctx.storage.put("history", this.history.slice(0, 30));
      }
    } catch (e) {
      console.error(JSON.stringify({ message: "persist analysis failed", error: String(e) }));
    }
  }

  private apiKey(): string {
    return (this.env.DEEPSEEK_API_KEY ?? "").trim();
  }

  private llmReady(): { ok: true } | { ok: false; error: string } {
    if (!this.apiKey()) {
      return {
        ok: false,
        error: "DEEPSEEK_API_KEY not set. Run: npx wrangler secret put DEEPSEEK_API_KEY",
      };
    }
    if (!this.lastSnap || this.lastSnap.length < 4) {
      return { ok: false, error: "no screenshot yet — origin sends a snapshot every ~8s while publishing" };
    }
    return { ok: true };
  }

  private async handleApi(request: Request, url: URL): Promise<Response> {
    const headers = corsHeaders();
    if (url.pathname === "/api/otp/redeem" && request.method === "POST") {
      return this.redeem(request, headers);
    }
    const sess = requestSession(request, url);
    if (!(await this.sessionOk(sess))) {
      return Response.json({ error: "unauthorized" }, { status: 401, headers });
    }
    if (url.pathname === "/api/analysis" && request.method === "GET") {
      return Response.json(
        {
          last: this.lastAnalysis,
          history: this.history,
          llm: this.llmStatus(),
          control: { enabled: this.flags.control, ai_enabled: this.flags.ai },
        },
        { headers },
      );
    }
    if (url.pathname === "/api/llm-status" && request.method === "GET") {
      return Response.json(this.llmStatus(), { headers });
    }
    if (url.pathname === "/api/computer-use" && request.method === "POST") {
      if (!this.flags.ai) {
        return Response.json({ error: "ai control disabled" }, { status: 403, headers });
      }
      let task = "";
      try {
        const body = (await request.json()) as { task?: string };
        task = (body.task ?? "").trim();
      } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400, headers });
      }
      if (!task) {
        return Response.json({ error: "missing task" }, { status: 400, headers });
      }
      this.sendPublisher(JSON.stringify({ type: "computer-use", task, session: sess }));
      return Response.json({ ok: true, accepted: true }, { headers });
    }
    if (url.pathname === "/api/analyze-now" && request.method === "POST") {
      try {
        await request.text();
      } catch {
        /* ignore */
      }
      if (this.apiKey()) await this.hydrate();
      const ready = this.llmReady();
      if (!ready.ok) {
        return Response.json({ error: ready.error }, { status: 503, headers });
      }
      const now = Date.now();
      if (!analyzeNowAllowed(this.lastForcedAt, now, ANALYZE_NOW_COOLDOWN_MS)) {
        return Response.json(
          { error: "rate limited — wait a few seconds between manual analyses" },
          { status: 429, headers },
        );
      }
      this.lastForcedAt = now;
      const result = await this.runAnalysis(true);
      return Response.json(result, { headers });
    }
    if (url.pathname === "/api/ask" && request.method === "POST") {
      let question = "";
      try {
        const body = (await request.json()) as { question?: string };
        question = (body.question ?? "").trim();
      } catch {
        return Response.json({ error: "invalid JSON body" }, { status: 400, headers });
      }
      if (!question) {
        return Response.json({ error: "missing question" }, { status: 400, headers });
      }
      if (this.apiKey()) await this.hydrate();
      const ready = this.llmReady();
      if (!ready.ok) {
        return Response.json({ error: ready.error }, { status: 503, headers });
      }
      try {
        const { text, usage } = await completeVision(
          this.lastSnap!,
          askPrompt(question),
          this.apiKey(),
          this.env.DEEPSEEK_BASE_URL || DEFAULT_BASE,
          this.env.DEEPSEEK_MODEL || DEFAULT_MODEL,
        );
        if (usage) console.log(JSON.stringify({ message: "llm ask usage", usage }));
        const parsed = askFromModel(parseJsonObject(text));
        return Response.json(parsed, { headers });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        console.error(JSON.stringify({ message: "llm ask failed", error: msg }));
        return Response.json({ error: msg }, { status: 502, headers });
      }
    }
    return Response.json({ error: "not found" }, { status: 404, headers });
  }

  private async redeem(request: Request, headers: Record<string, string>): Promise<Response> {
    await this.hydrateAuth();
    const now = Date.now();
    if (this.lockUntil && now < this.lockUntil) {
      return Response.json({ error: "rate limited" }, { status: 429, headers });
    }
    let pin = "";
    try {
      const body = (await request.json()) as { pin?: string };
      pin = (body.pin ?? "").trim();
    } catch {
      return Response.json({ error: "invalid JSON body" }, { status: 400, headers });
    }
    const hash = await sha256hex(pin);
    const enc = new TextEncoder();
    const ga = await crypto.subtle.digest("SHA-256", enc.encode(hash));
    const ea = await crypto.subtle.digest("SHA-256", enc.encode(this.otpHash || "none"));
    const match =
      this.otpHash &&
      this.otpExp > now &&
      pin.length === 6 &&
      crypto.subtle.timingSafeEqual(ga, ea);
    if (!match) {
      this.fails += 1;
      if (this.fails >= FAIL_LIMIT) {
        this.lockUntil = now + LOCKOUT_MS;
        this.fails = 0;
      }
      return Response.json({ error: "unauthorized" }, { status: 401, headers });
    }
    this.fails = 0;
    const token = hexRandom(32);
    const sh = await sha256hex(token);
    this.sessions.set(sh, now + SESSION_TTL_MS);
    await this.persistSessions();
    const maxAge = Math.floor(SESSION_TTL_MS / 1000);
    headers = {
      ...headers,
      "set-cookie": `streamaid_session=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`,
    };
    return Response.json({ session: token, expires_in_s: maxAge }, { headers });
  }

  private llmStatus() {
    return {
      configured: !!this.apiKey(),
      model: this.env.DEEPSEEK_MODEL || DEFAULT_MODEL,
      has_snapshot: !!(this.lastSnap && this.lastSnap.length >= 4),
      analyzing: this.analyzing,
      last_error: this.lastAnalysis?.error || "",
    };
  }

  private broadcastAnalysis(a: Analysis): void {
    const payload = JSON.stringify({ type: "analysis", data: a });
    for (const peer of this.ctx.getWebSockets()) {
      const p = peer.deserializeAttachment() as Attachment | null;
      if (p?.role !== "viewer" || peer.readyState !== WebSocket.OPEN) continue;
      try {
        peer.send(payload);
      } catch {
        /* ignore */
      }
    }
  }

  private async runAnalysis(force = false): Promise<Analysis> {
    const ready = this.llmReady();
    if (!ready.ok) {
      const a: Analysis = { ts: new Date().toISOString(), summary: "", questions: [], error: ready.error };
      this.lastAnalysis = a;
      return a;
    }
    if (this.analyzing) {
      return this.lastAnalysis ?? { ts: new Date().toISOString(), summary: "", questions: [] };
    }
    const snap = this.lastSnap!;
    const snapHash = await sha256buf(snap);
    if (!force && shouldSkipAutoAnalysis(this.lastAnalysis, snapHash)) {
      // Frame unchanged since the last pass: reuse that result instead of
      // spending another paid vision call.
      return this.lastAnalysis;
    }
    this.analyzing = true;
    const seq = this.snapSeq;
    try {
      const { text, usage } = await completeVision(
        snap,
        ANALYZE_PROMPT,
        this.apiKey(),
        this.env.DEEPSEEK_BASE_URL || DEFAULT_BASE,
        this.env.DEEPSEEK_MODEL || DEFAULT_MODEL,
      );
      const a = analysisFromModel(parseJsonObject(text));
      a.snapHash = snapHash;
      if (usage) {
        a.usage = usage;
        console.log(JSON.stringify({ message: "llm analyze usage", usage }));
      }
      this.lastAnalysis = a;
      this.history.unshift(a);
      this.history = this.history.slice(0, 30);
      await this.persistAnalysis(a);
      this.broadcastAnalysis(a);
      return a;
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      console.error(JSON.stringify({ message: "llm analyze failed", error: msg }));
      const a: Analysis = { ts: new Date().toISOString(), summary: "", questions: [], error: msg };
      this.lastAnalysis = a;
      await this.persistAnalysis(a);
      return a;
    } finally {
      this.analyzing = false;
      this.analyzedSeq = seq;
      if (this.snapSeq > seq && this.apiKey()) {
        await this.ctx.storage.setAlarm(Date.now() + 500);
      }
    }
  }
}
