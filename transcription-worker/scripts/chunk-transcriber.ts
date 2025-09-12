// scripts/chunk-transcriber.ts
import fetch from "node-fetch";

// Config
const WORKER_URL =
  process.env.WORKER_URL ||
  "https://transcription-worker.h7384285.workers.dev";

function getInputUrl(): string {
  const envUrl = process.env.MEDIA_URL;
  const argUrl = process.argv[2];
  const url = envUrl || argUrl;
  if (!url) {
    throw new Error(
      "Usage: npm run transcribe -- <direct-media-url> OR set MEDIA_URL"
    );
  }
  return url;
}

type Word = { word: string; start: number; end: number };
type Segment = {
  start: number;
  end: number;
  text: string;
  words?: Word[];
};

function normalizeText(t: string) {
  return t.toLowerCase().replace(/\s+/g, " ").replace(/[.,!?]/g, "").trim();
}

function mergeResults(chunks: Array<{ offset: number; result: any }>) {
  let globalSegments: Segment[] = [];
  let globalWords: Word[] = [];

  for (const { offset, result } of chunks) {
    const segs: Segment[] = result.segments || [];
    const wrds: Word[] = result.words || [];

    // Calculate time offset in seconds
    const chunkDuration = result.transcription_info?.duration ?? 0;
    const timeOffset =
      (offset / result.chunkSize) * chunkDuration || globalSegments.at(-1)?.end || 0;

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

  // Sort and dedup
  globalSegments.sort((a, b) => a.start - b.start);
  globalWords.sort((a, b) => a.start - b.start);

  const dedupSegments: Segment[] = [];
  for (const s of globalSegments) {
    const prev = dedupSegments[dedupSegments.length - 1];
    if (prev) {
      const overlap = Math.max(
        0,
        Math.min(prev.end, s.end) - Math.max(prev.start, s.start)
      );
      const span = Math.max(
        prev.end - prev.start,
        s.end - s.start,
        1e-3
      );
      const similar = normalizeText(prev.text) === normalizeText(s.text);
      if (overlap / span > 0.5 && similar) {
        dedupSegments[dedupSegments.length - 1] = s;
        continue;
      }
    }
    dedupSegments.push(s);
  }

  const fullText = dedupSegments
    .map((s) => s.text.trim())
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();

  return { text: fullText, segments: dedupSegments, words: globalWords };
}

async function run() {
  try {
    const mediaUrl = getInputUrl();
    console.log(`🔹 Transcribing: ${mediaUrl}`);

    let offset = 0;
    const allChunks: { offset: number; result: any }[] = [];

    while (true) {
      console.log(`➡️  Requesting offset=${offset}`);
      const res = await fetch(
        `${WORKER_URL}?url=${encodeURIComponent(mediaUrl)}&offset=${offset}`
      );

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Worker failed: ${res.status} ${res.statusText} ${text}`);
      }

      const json:any = await res.json();
      allChunks.push({ offset, result: json.result });

      if (json.done) {
        console.log("✅ Finished all chunks");
        break;
      }
      offset = json.nextOffset;
    }

    const merged = mergeResults(allChunks);

    console.log("\n✅ FINAL TRANSCRIPTION (merged):\n");
    console.log(merged.text);

    // Optionally save full transcript
    // import fs from "fs";
    // import path from "path";
    // fs.writeFileSync(path.join(process.cwd(), "final_transcript.json"), JSON.stringify(merged, null, 2));
  } catch (err) {
    console.error("❌ Error:", err);
    process.exitCode = 1;
  }
}

run();
