// netlify/functions/refresh-qa-metrics.js
// Triggers the nightly QA metrics GitHub Action (workflow_dispatch) on demand,
// so the NLP accuracy tracker's "Refresh now" button can re-bake qa_metrics.json
// from Redshift without waiting for the 01:00 UTC cron.
// Same server-side-token principle as fetch-live-explorer.js; reuses GH_DISPATCH_TOKEN.
//
// Guard: if the workflow is already queued/in progress, we don't dispatch a
// second run - the caller gets {already_running:true} and just tracks it.
//
// Env: GH_DISPATCH_TOKEN (Actions: read/write). Optional GH_REPO, GH_REF,
//      GH_QA_WORKFLOW (default 'qa-dashboard-metrics.yml').

const REPO = process.env.GH_REPO || "scrapesync/artemisai_website";
const WORKFLOW = process.env.GH_QA_WORKFLOW || "qa-dashboard-metrics.yml";
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

  const gh = { Authorization: "token " + token, Accept: "application/vnd.github+json", "User-Agent": "artemis-qa-refresh" };
  try {
    const runsRes = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?per_page=1`,
      { headers: gh }
    );
    if (runsRes.ok) {
      const latest = ((await runsRes.json()).workflow_runs || [])[0];
      if (latest && (latest.status === "queued" || latest.status === "in_progress"))
        return { statusCode: 200, headers, body: JSON.stringify({ ok: true, already_running: true, run_started_at: latest.run_started_at }) };
    }

    const dispatch = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`,
      { method: "POST", headers: gh, body: JSON.stringify({ ref: REF }) }
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
