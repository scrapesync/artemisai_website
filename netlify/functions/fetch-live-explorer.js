// netlify/functions/fetch-live-explorer.js
// Triggers the Data Explorer snapshot GitHub Action (workflow_dispatch) so the
// explorer can re-bake its previews + summaries from Redshift on demand.
// Same server-side-token principle as fetch-live.js; reuses GH_DISPATCH_TOKEN.
//
// Env: GH_DISPATCH_TOKEN (Actions: read/write). Optional GH_REPO, GH_REF,
//      GH_EXPLORER_WORKFLOW (default 'explorer-data.yml').

const REPO = process.env.GH_REPO || "scrapesync/artemisai_website";
const WORKFLOW = process.env.GH_EXPLORER_WORKFLOW || "explorer-data.yml";
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

  try {
    const dispatch = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`,
      { method: "POST", headers: { Authorization: "token " + token, Accept: "application/vnd.github+json", "User-Agent": "artemis-data-explorer" }, body: JSON.stringify({ ref: REF }) }
    );
    if (dispatch.status !== 204) {
      let e = {}; try { e = await dispatch.json(); } catch {}
      return { statusCode: 502, headers, body: JSON.stringify({ error: "dispatch_failed", status: dispatch.status, message: e.message || "GitHub rejected the dispatch." }) };
    }
    return { statusCode: 202, headers, body: JSON.stringify({ ok: true, dispatched: true }) };
  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: "server_error", message: err.message }) };
  }
};
