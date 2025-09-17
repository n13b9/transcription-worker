export interface Env {
  AI: Ai;
  HARVEST_KEY: string;
}

const CHUNK_SIZE = 8 * 1024 * 1024; // 8MB safe ceiling for Workers AI
const SMALL_FILE_LIMIT = 20 * 1024 * 1024; // 20MB for single-shot
const MAX_PARALLEL = 5; // increased concurrency

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
  if (resp.status !== 206 && resp.status !== 200) throw new Error(`Origin fetch failed: ${resp.status}`);
  const buffer = await resp.arrayBuffer();
  if (buffer.byteLength === 0) throw new Error("Empty chunk received");
  const base64Audio = toBase64(buffer);
  const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", { audio: base64Audio });
  return { offset, result: aiResp, chunkSize: buffer.byteLength };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const { searchParams } = new URL(req.url);
      const inputUrl = searchParams.get("url");
      if (!inputUrl) return json({ error: "Missing ?url" }, 400);

      let url = await resolveUrl(inputUrl, env);

      const probeResp = await fetch(url, { headers: { Range: "bytes=0-0" } });
      if (!probeResp.ok) throw new Error(`Probe failed: ${probeResp.status}`);
      const contentRange = probeResp.headers.get("Content-Range");
      let fileSize = 0;
      if (contentRange) {
        const parts = contentRange.split("/");
        if (parts.length === 2) fileSize = parseInt(parts[1], 10);
      }

      const allChunks: { offset: number; result: any; chunkSize: number }[] = [];
      let offset = 0;

      if (fileSize > 0 && fileSize <= SMALL_FILE_LIMIT) {
        const fullResp = await fetch(url);
        const buffer = await fullResp.arrayBuffer();
        const base64Audio = toBase64(buffer);
        const aiResp = await env.AI.run("@cf/openai/whisper-large-v3-turbo", { audio: base64Audio });
        allChunks.push({ offset: 0, result: aiResp, chunkSize: buffer.byteLength });
        offset = buffer.byteLength;
      } else {
        while (offset < fileSize) {
          const tasks = [];
          for (let i = 0; i < MAX_PARALLEL && offset < fileSize; i++) {
            const end = Math.min(offset + CHUNK_SIZE - 1, fileSize - 1);
            tasks.push(processChunk(env, url, inputUrl, offset, end));
            offset += CHUNK_SIZE;
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
      };


      return json({ text: merged.text, segments: merged.segments, meta });
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
