/** DeepSeek OpenAI-compatible vision client. Key via wrangler secret DEEPSEEK_API_KEY. */

export const DEFAULT_MODEL = "deepseek-v4-flash-vision-exp";
export const DEFAULT_BASE = "https://api.deepseek.com";

export const ANALYZE_PROMPT =
  "You are a screen-analysis assistant for gaming and streaming. Analyze the provided screen capture. " +
  "1) Summarize what is on screen in 2-3 sentences. " +
  "2) If any question is visible on screen (game UI, chat, quiz, or prompt text), answer it. " +
  "3) For each question give an answer, a confidence score 0-100, and one-sentence reasoning. " +
  'Respond ONLY with valid JSON: {"summary": string, "questions": [{"question": string, "answer": string, "confidence": number, "reasoning": string}]}. If no question is visible, set questions to [].';

export function askPrompt(question: string): string {
  return (
    "Answer this question about the current screen capture.\nQuestion: " +
    question +
    '\nRespond ONLY with valid JSON: {"answer": string, "confidence": number (0-100), "reasoning": string}.'
  );
}

export type Usage = {
  prompt_tokens?: number;
  completion_tokens?: number;
  total_tokens?: number;
};

export type Analysis = {
  ts: string;
  summary: string;
  questions: { question: string; answer: string; confidence: number; reasoning: string }[];
  error?: string;
  /** SHA-256 of the snapshot this pass analyzed; unchanged frames skip re-analysis. */
  snapHash?: string;
  /** Token usage reported by the model API, when present. */
  usage?: Usage;
};

export type AskResult = {
  answer: string;
  confidence: number;
  reasoning: string;
};

export function parseJsonObject(text: string): Record<string, unknown> {
  let s = text.trim();
  if (s.startsWith("```")) {
    const nl = s.indexOf("\n");
    if (nl !== -1) s = s.slice(nl + 1);
    if (s.endsWith("```")) s = s.slice(0, -3);
    s = s.trim();
  }
  const start = s.indexOf("{");
  if (start < 0) throw new Error("invalid JSON from model");
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (inStr) {
      if (esc) esc = false;
      else if (c === "\\") esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') inStr = true;
    else if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) return JSON.parse(s.slice(start, i + 1)) as Record<string, unknown>;
    }
  }
  throw new Error("invalid JSON from model");
}

export function analysisFromModel(obj: Record<string, unknown>): Analysis {
  const qs = Array.isArray(obj.questions) ? obj.questions : [];
  return {
    ts: new Date().toISOString(),
    summary: String(obj.summary ?? ""),
    questions: qs.map((q) => {
      const row = q && typeof q === "object" ? (q as Record<string, unknown>) : {};
      const conf = Number(row.confidence ?? 0);
      return {
        question: String(row.question ?? ""),
        answer: String(row.answer ?? ""),
        confidence: Math.max(0, Math.min(100, Number.isFinite(conf) ? conf : 0)),
        reasoning: String(row.reasoning ?? ""),
      };
    }),
  };
}

export function askFromModel(obj: Record<string, unknown>): AskResult {
  const conf = Number(obj.confidence ?? 0);
  return {
    answer: String(obj.answer ?? ""),
    confidence: Math.max(0, Math.min(100, Number.isFinite(conf) ? conf : 0)),
    reasoning: String(obj.reasoning ?? ""),
  };
}

function b64(bytes: Uint8Array): string {
  let bin = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    bin += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(bin);
}

export async function completeVision(
  jpeg: Uint8Array,
  prompt: string,
  apiKey: string,
  baseUrl: string,
  model: string,
): Promise<{ text: string; usage: Usage | null }> {
  const url = baseUrl.replace(/\/$/, "") + "/chat/completions";
  const body = {
    model,
    temperature: 0.2,
    max_tokens: 1024,
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text: prompt },
          {
            type: "image_url",
            image_url: {
              url: "data:image/jpeg;base64," + b64(jpeg),
            },
          },
        ],
      },
    ],
  };
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`deepseek ${res.status}: ${text.slice(0, 300)}`);
  }
  const data = JSON.parse(text) as {
    choices?: { message?: { content?: string } }[];
    usage?: { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number };
  };
  const content = data.choices?.[0]?.message?.content;
  if (!content || !content.trim()) throw new Error("empty response from model");
  const usage: Usage | null = data.usage
    ? {
        prompt_tokens: typeof data.usage.prompt_tokens === "number" ? data.usage.prompt_tokens : undefined,
        completion_tokens: typeof data.usage.completion_tokens === "number" ? data.usage.completion_tokens : undefined,
        total_tokens: typeof data.usage.total_tokens === "number" ? data.usage.total_tokens : undefined,
      }
    : null;
  return { text: content, usage };
}
