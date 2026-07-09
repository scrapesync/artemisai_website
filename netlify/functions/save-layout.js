// netlify/functions/save-layout.js
// Persists the shared QA dashboard layout to qa_layout.json in the repo, so a
// layout one person publishes becomes everyone's. The GitHub token lives ONLY
// here as a Netlify environment variable and is never sent to the browser.
//
// Reuses the same env var as fetch-live:
//   GH_DISPATCH_TOKEN = a GitHub token with BOTH:
//       Actions: Read and write   (for fetch-live)
//       Contents: Read and write  (for this function)
//   Optional overrides: GH_REPO, GH_LAYOUT_PATH (default 'qa_layout.json'), GH_REF
//
// The page POSTs {layout, by} to /api/save-layout (routed by netlify.toml).

const REPO = process.env.GH_REPO || "scrapesync/artemisai_website";
const LAYOUT_PATH = process.env.GH_LAYOUT_PATH || "qa_layout.json";
const REF = process.env.GH_REF || "main";

exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers };
  if (event.httpMethod !== "POST")
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };

  const token = process.env.GH_DISPATCH_TOKEN;
  if (!token)
    return { statusCode: 500, headers, body: JSON.stringify({ error: "not_configured", message: "GH_DISPATCH_TOKEN is not set in Netlify environment variables." }) };

  let payload;
  try { payload = JSON.parse(event.body || "{}"); }
  catch { return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) }; }

  if (!payload.layout || typeof payload.layout !== "object")
    return { statusCode: 400, headers, body: JSON.stringify({ error: "no_layout", message: "Missing layout." }) };

  // guard against absurd payloads
  const file = {
    published_at: new Date().toISOString(),
    by: (payload.by || "someone").toString().slice(0, 40),
    layout: payload.layout,
  };
  const content = JSON.stringify(file);
  if (content.length > 400000)
    return { statusCode: 413, headers, body: JSON.stringify({ error: "too_large" }) };

  const gh = (path, opts = {}) =>
    fetch(`https://api.github.com/repos/${REPO}/${path}`, {
      ...opts,
      headers: {
        Authorization: "token " + token,
        Accept: "application/vnd.github+json",
        "User-Agent": "artemis-qa-dashboard",
        ...(opts.headers || {}),
      },
    });

  try {
    // current sha (if the file already exists)
    let sha = null;
    const head = await gh(`contents/${LAYOUT_PATH}?ref=${REF}`);
    if (head.ok) { const h = await head.json(); sha = h.sha; }

    const body = {
      message: `chore: publish shared QA dashboard layout (${file.by})`,
      content: Buffer.from(content, "utf8").toString("base64"),
      branch: REF,
      committer: { name: "qa-layout", email: "bot@artemisai.co.uk" },
    };
    if (sha) body.sha = sha;

    const put = await gh(`contents/${LAYOUT_PATH}`, { method: "PUT", body: JSON.stringify(body) });
    if (!put.ok) {
      let e = {}; try { e = await put.json(); } catch {}
      return { statusCode: 502, headers, body: JSON.stringify({ error: "commit_failed", status: put.status, message: e.message || "GitHub rejected the commit." }) };
    }
    return { statusCode: 200, headers, body: JSON.stringify({ ok: true, published_at: file.published_at, by: file.by }) };
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: "server_error", message: err.message }) };
  }
};
