export interface Env {
  AI: Ai;
}

const CHUNK_SIZE = 2 * 1024 * 1024; // 2MB

// Safe ArrayBuffer → base64
function toBase64(buf: ArrayBuffer): string {
  let binary = "";
  const bytes = new Uint8Array(buf);
  const step = 0x8000; // 32KB chunks to avoid stack overflow
  for (let i = 0; i < bytes.length; i += step) {
    const chunk = bytes.subarray(i, i + step);
    binary += String.fromCharCode.apply(null, Array.from(chunk));
  }
  return btoa(binary);
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const { searchParams } = new URL(req.url);
      const url = searchParams.get("url");
      const offsetParam = searchParams.get("offset");

      if (!url) {
        return json({ error: "Missing ?url" }, 400);
      }

      const offset = offsetParam ? parseInt(offsetParam, 10) : 0;
      if (isNaN(offset) || offset < 0) {
        return json({ error: "Invalid offset" }, 400);
      }

      const end = offset + CHUNK_SIZE - 1;
      console.log(`Fetching bytes=${offset}-${end}`);

      // Add fetch timeout (20s max wait)
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 20000);

      let resp: Response;
      try {
        resp = await fetch(url, {
          headers: { Range: `bytes=${offset}-${end}` },
          signal: controller.signal,
        });
      } catch (e: any) {
        clearTimeout(timeout);
        if (e.name === "AbortError") {
          return json({ error: "Origin too slow or unresponsive" }, 504);
        }
        return json({ error: `Failed to fetch origin: ${e.message}` }, 502);
      }
      clearTimeout(timeout);

      // Detect expired signed URLs
      if (resp.status === 403 || resp.status === 401) {
        return json({ error: "URL expired or unauthorized" }, resp.status);
      }

      // Detect server errors from origin
      if (resp.status >= 500) {
        return json({ error: `Origin error ${resp.status}` }, 502);
      }

      // Must support Range
      if (resp.status !== 206 && resp.status !== 200) {
        return json(
          { error: `Origin does not support Range requests (status ${resp.status})` },
          400
        );
      }

      const buffer = await resp.arrayBuffer();
      if (buffer.byteLength === 0) {
        return json({ done: true, message: "No more bytes" });
      }

      const base64Audio = toBase64(buffer);

      const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", {
        audio: base64Audio,
      });

      const nextOffset = offset + buffer.byteLength;

      return json({
        done: false,
        offset,
        nextOffset,
        chunkSize: buffer.byteLength,
        result: aiResp, // full Whisper response
      });
    } catch (err: any) {
      console.error("Worker error:", err);
      return json({ error: err.message || String(err) }, 500);
    }
  },
};

// Helper for JSON responses
function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });
}
