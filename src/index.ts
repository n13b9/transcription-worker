export interface Env {
  AI: Ai;
  HARVEST_KEY: string;
}

const CHUNK_SIZE = 2 * 1024 * 1024;

function toBase64(buf: ArrayBuffer): string {
  let binary = "";
  const bytes = new Uint8Array(buf);
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    const chunk = bytes.subarray(i, i + step);
    binary += String.fromCharCode.apply(null, Array.from(chunk));
  }
  return btoa(binary);
}

async function resolveUrl(inputUrl: string, env: Env): Promise<string> {
  if (/\.(mp4|mp3|m4a|wav|flac|ogg|webm)$/i.test(inputUrl)) {
    return inputUrl;
  }

  const resp = await fetch(
    `https://harvester.satellite.ventures/getDownloadUrl?url=${encodeURIComponent(inputUrl)}`,
    {
      headers: {
        Authorization: `Bearer ${env.HARVEST_KEY}`,
      },
    }
  );

  if (!resp.ok) {
    throw new Error(`Failed to resolve URL (status ${resp.status})`);
  }

  const data: any = await resp.json();

  const directUrl = data.downloadUrl;
  if (!directUrl) {
    throw new Error("Resolver did not return a valid downloadUrl");
  }

  return directUrl;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const { searchParams } = new URL(req.url);
      const inputUrl = searchParams.get("url");
      const offsetParam = searchParams.get("offset");

      if (!inputUrl) {
        return json({ error: "Missing ?url" }, 400);
      }

      const url = await resolveUrl(inputUrl, env);

      const offset = offsetParam ? parseInt(offsetParam, 10) : 0;
      if (isNaN(offset) || offset < 0) {
        return json({ error: "Invalid offset" }, 400);
      }

      const end = offset + CHUNK_SIZE - 1;

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

      if (resp.status === 403 || resp.status === 401) {
        return json({ error: "URL expired or unauthorized" }, resp.status);
      }

      if (resp.status >= 500) {
        return json({ error: `Origin error ${resp.status}` }, 502);
      }

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
        result: aiResp,
      });
    } catch (err: any) {
      console.error("Worker error:", err);
      return json({ error: err.message || String(err) }, 500);
    }
  },
};

function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });
}
