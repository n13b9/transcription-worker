export interface Env {
  AI: Ai;
  HARVEST_KEY: string;
  TRANSCRIBER_DO: DurableObjectNamespace;
}

function json(obj: any, status = 200): Response {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const url = new URL(req.url);

      // Main endpoint: GET /?url=VIDEO_URL
      if (url.pathname === "/") {
        const inputUrl = url.searchParams.get("url");
        if (!inputUrl) {
          return json({ error: "Missing ?url" }, 400);
        }

        // Each video gets its own DO instance
        const id = env.TRANSCRIBER_DO.idFromName(inputUrl);
        const stub = env.TRANSCRIBER_DO.get(id);

        // Delegate to DO → merged transcript returned
        return stub.fetch(`https://do/process?url=${encodeURIComponent(inputUrl)}`);
      }

      return json({ error: "Not found" }, 404);
    } catch (err: any) {
      return json({ error: err.message || String(err) }, 500);
    }
  },
};

// 👇 Export Durable Object class so Wrangler can bind it
export { TranscriberDO } from "./TranscriberDO";
