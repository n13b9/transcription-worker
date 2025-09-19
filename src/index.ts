export interface Env {
  AI: Ai;
  HARVEST_KEY: string;
}

const DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024; // 8MB for <50MB files
const SMALL_FILE_LIMIT = 20 * 1024 * 1024;  // single-shot
const MEDIUM_FILE_LIMIT = 50 * 1024 * 1024; // <=50MB uses fast logic
const SAFE_CHUNK_SIZE = 2 * 1024 * 1024;    // 2MB safe for >50MB
const SAFE_PARALLEL = 3;                    // safe parallelism for >50MB
const DEFAULT_PARALLEL = 10;                // fast parallelism for <50MB

function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });
}

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
  if (/\.(mp4|mp3|m4a|wav|flac|ogg|webm)$/i.test(inputUrl)) return inputUrl;

  const resp = await fetch(
    `https://harvester.satellite.ventures/getDownloadUrl?url=${encodeURIComponent(inputUrl)}`,
    { headers: { Authorization: `Bearer ${env.HARVEST_KEY}` } }
  );
  if (!resp.ok) throw new Error(`Failed to resolve URL (status ${resp.status})`);

  const data: any = await resp.json();
  if (!data.downloadUrl) throw new Error("Resolver did not return a valid downloadUrl");

  console.log("Resolved downloadUrl:", data.downloadUrl);
  return data.downloadUrl;
}

type Word = { word: string; start: number; end: number };
type Segment = { start: number; end: number; text: string; words?: Word[] };

function normalizeText(t: string) {
  return t.toLowerCase().replace(/\s+/g, " ").replace(/[.,!?]/g, "").trim();
}

function mergeResults(chunks: Array<{ offset: number; result: any; chunkSize: number }>) {
  let globalSegments: Segment[] = [];
  let globalWords: Word[] = [];

  for (const { offset, result, chunkSize } of chunks) {
    const segs: Segment[] = result.segments || [];
    const wrds: Word[] = result.words || [];
    const chunkDuration = result.transcription_info?.duration ?? 0;
    const lastSegment = globalSegments.length > 0 ? globalSegments[globalSegments.length - 1] : null;
    const timeOffset = (offset / chunkSize) * chunkDuration || (lastSegment ? lastSegment.end : 0);

    for (const s of segs) {
      globalSegments.push({
        start: s.start + timeOffset,
        end: s.end + timeOffset,
        text: s.text,
        words: s.words?.map((w: Word) => ({
          word: w.word,
          start: w.start + timeOffset,
          end: w.end + timeOffset,
        })),
      });
    }

    for (const w of wrds) {
      globalWords.push({
        word: w.word,
        start: w.start + timeOffset,
        end: w.end + timeOffset,
      });
    }
  }

  globalSegments.sort((a, b) => a.start - b.start);
  globalWords.sort((a, b) => a.start - b.start);

  const dedupSegments: Segment[] = [];
  for (const s of globalSegments) {
    const prev = dedupSegments[dedupSegments.length - 1];
    if (prev) {
      const overlap = Math.max(0, Math.min(prev.end, s.end) - Math.max(prev.start, s.start));
      const span = Math.max(prev.end - prev.start, s.end - s.start, 1e-3);
      const similar = normalizeText(prev.text) === normalizeText(s.text);
      if (overlap / span > 0.5 && similar) {
        dedupSegments[dedupSegments.length - 1] = s;
        continue;
      }
    }
    dedupSegments.push(s);
  }

  const fullText = dedupSegments.map((s) => s.text.trim()).join(" ").replace(/\s+/g, " ").trim();
  return { text: fullText, segments: dedupSegments, words: globalWords };
}

async function processChunk(
  env: Env,
  url: string,
  inputUrl: string,
  offset: number,
  end: number
): Promise<{ offset: number; result: any; chunkSize: number }> {
  let resp = await fetch(url, { headers: { Range: `bytes=${offset}-${end}` } });

  if (resp.status === 403 || resp.status === 401) {
    url = await resolveUrl(inputUrl, env);
    resp = await fetch(url, { headers: { Range: `bytes=${offset}-${end}` } });
  }

  if (resp.status !== 206 && resp.status !== 200) {
    throw new Error(`Origin fetch failed: ${resp.status}`);
  }

  const buffer = await resp.arrayBuffer();
  console.log("Chunk byteLength:", buffer.byteLength);

  // Guard: detect non-audio response
  const head = new Uint8Array(buffer.slice(0, 64));
  console.log("Chunk head raw bytes:", Array.from(head));
  console.log("Chunk head decoded:", new TextDecoder().decode(head));

  if (buffer.byteLength === 0 || /<!DOCTYPE|<html|{"error/.test(new TextDecoder().decode(head))) {
    throw new Error(`Harvester returned non-audio data at offset=${offset}`);
  }

  const base64Audio = toBase64(buffer);

  const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", {
    audio: base64Audio,
  });

  return { offset, result: aiResp, chunkSize: buffer.byteLength };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const { searchParams } = new URL(req.url);
      const inputUrl = searchParams.get("url");
      if (!inputUrl) return json({ error: "Missing ?url" }, 400);

      const parallelOverride = parseInt(searchParams.get("parallel") || "");
      const chunkOverride = parseInt(searchParams.get("chunk") || "");

      let url = await resolveUrl(inputUrl, env);
      console.log("Resolved final media URL:", url);

      // probe file size
      let fileSize = 0;
      const probeResp = await fetch(url, { headers: { Range: "bytes=0-0" } });
      console.log(
        "Probe status:", probeResp.status,
        "Content-Range:", probeResp.headers.get("Content-Range"),
        "Content-Type:", probeResp.headers.get("content-type")
      );

      if (probeResp.ok) {
        const contentRange = probeResp.headers.get("Content-Range");
        if (contentRange) {
          const parts = contentRange.split("/");
          if (parts.length === 2) fileSize = parseInt(parts[1], 10);
        }
      }

      const allChunks: { offset: number; result: any; chunkSize: number }[] = [];
      let offset = 0;

      if (fileSize > 0 && fileSize <= SMALL_FILE_LIMIT) {
        const fullResp = await fetch(url);
        const contentType = fullResp.headers.get("content-type") || "";
        console.log("Full fetch content-type:", contentType);
        const buffer = await fullResp.arrayBuffer();
        console.log("Full fetch byteLength:", buffer.byteLength);

        const head = new Uint8Array(buffer.slice(0, 64));
        console.log("Head raw bytes (small file):", Array.from(head));
        console.log("Head decoded (small file):", new TextDecoder().decode(head));

        if (!/audio|video/.test(contentType)) {
          throw new Error(`Invalid media response (content-type=${contentType})`);
        }

        const base64Audio = toBase64(buffer);
        const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", { audio: base64Audio });
        allChunks.push({ offset: 0, result: aiResp, chunkSize: buffer.byteLength });
        offset = buffer.byteLength;
      } else if (fileSize === 0) {
        console.log("File size unknown → fetching full file as fallback");
        const fullResp = await fetch(url);
        const contentType = fullResp.headers.get("content-type") || "";
        console.log("Full fetch content-type:", contentType);
        const buffer = await fullResp.arrayBuffer();
        console.log("Full fetch byteLength:", buffer.byteLength);

        const head = new Uint8Array(buffer.slice(0, 64));
        console.log("Head raw bytes (fallback):", Array.from(head));
        console.log("Head decoded (fallback):", new TextDecoder().decode(head));

        if (!/audio|video/.test(contentType)) {
          throw new Error(`Invalid media response (content-type=${contentType})`);
        }

        const base64Audio = toBase64(buffer);
        const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", { audio: base64Audio });
        allChunks.push({ offset: 0, result: aiResp, chunkSize: buffer.byteLength });
        offset = buffer.byteLength;
        fileSize = buffer.byteLength;
      } else if (fileSize > 0 && fileSize <= MEDIUM_FILE_LIMIT) {
        let chunkSize = DEFAULT_CHUNK_SIZE;
        if (fileSize > 40 * 1024 * 1024) chunkSize = 4 * 1024 * 1024;
        if (!isNaN(chunkOverride) && chunkOverride > 0) chunkSize = chunkOverride;
        const MAX_PARALLEL =
          !isNaN(parallelOverride) && parallelOverride > 0 ? parallelOverride : DEFAULT_PARALLEL;

        while (offset < fileSize) {
          const tasks = [];
          for (let i = 0; i < MAX_PARALLEL && offset < fileSize; i++) {
            const end = Math.min(offset + chunkSize - 1, fileSize - 1);
            tasks.push(processChunk(env, url, inputUrl, offset, end));
            offset += chunkSize;
          }
          const results = await Promise.all(tasks);
          allChunks.push(...results);
        }
      } else {
        let chunkSize = SAFE_CHUNK_SIZE;
        if (!isNaN(chunkOverride) && chunkOverride > 0) chunkSize = chunkOverride;
        const MAX_PARALLEL =
          !isNaN(parallelOverride) && parallelOverride > 0 ? parallelOverride : SAFE_PARALLEL;

        while (offset < fileSize) {
          const tasks = [];
          for (let i = 0; i < MAX_PARALLEL && offset < fileSize; i++) {
            const end = Math.min(offset + chunkSize - 1, fileSize - 1);
            tasks.push(processChunk(env, url, inputUrl, offset, end));
            offset += chunkSize;
          }
          const results = await Promise.all(tasks);
          allChunks.push(...results);
        }
      }

      const merged = mergeResults(allChunks);
      const lastSegment =
        merged.segments.length > 0 ? merged.segments[merged.segments.length - 1] : null;
      const processedDuration = lastSegment ? lastSegment.end : 0;

      const meta = {
        processedDuration,
        segmentCount: merged.segments.length,
        bytesProcessed: offset,
        fileSize: fileSize || null,
        isComplete: fileSize ? offset >= fileSize : null,
        parallelism: parallelOverride || (fileSize > MEDIUM_FILE_LIMIT ? SAFE_PARALLEL : DEFAULT_PARALLEL),
        chunkSize: chunkOverride || (fileSize > MEDIUM_FILE_LIMIT ? SAFE_CHUNK_SIZE : DEFAULT_CHUNK_SIZE),
      };

      return json({ text: merged.text, segments: merged.segments, meta });
    } catch (err: any) {
      console.error("Worker error:", err);
      return json({ error: err.message || String(err) }, 500);
    }
  },
};
