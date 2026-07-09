// netlify/functions/fetch-live.js
// Triggers the QA-dashboard metrics GitHub Action (workflow_dispatch) so the
// dashboard can re-query Redshift on demand. The GitHub token lives ONLY here,
// as a Netlify environment variable, and is never sent to the browser.
//
// Setup (one time, in the Netlify dashboard):
//   Site settings -> Environment variables -> add:
//     GH_DISPATCH_TOKEN = a GitHub token with actions:write on the repo
//       (fine-grained: Actions = Read and write; or a classic token with 'workflow' scope)
//   Optional overrides:
//     GH_REPO      (default 'scrapesync/artemisai_website')
//     GH_WORKFLOW  (default 'qa-dashboard-metrics.yml')
//     GH_REF       (default 'main')
//
// The page calls POST /api/fetch-live (routed here by netlify.toml). No body needed.

const REPO = process.env.GH_REPO || "scrapesync/artemisai_website";
const WORKFLOW = process.env.GH_WORKFLOW || "qa-dashboard-metrics.yml";
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
  if (!token) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: "not_configured",
        message: "GH_DISPATCH_TOKEN is not set in Netlify environment variables.",
      }),
    };
  }

  try {
    // 1) dispatch the workflow
    const dispatch = await fetch(
      `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: "token " + token,
          Accept: "application/vnd.github+json",
          "User-Agent": "artemis-qa-dashboard",
        },
        body: JSON.stringify({ ref: REF }),
      }
    );

    if (dispatch.status !== 204) {
      let detail = {};
      try { detail = await dispatch.json(); } catch {}
      return {
        statusCode: 502,
        headers,
        body: JSON.stringify({
          error: "dispatch_failed",
          status: dispatch.status,
          message: detail.message || "GitHub rejected the dispatch.",
        }),
      };
    }

    // 2) best-effort: fetch the latest run id so the page can poll status
    let runId = null, runUrl = null;
    try {
      await new Promise((r) => setTimeout(r, 1500));
      const runs = await fetch(
        `https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?per_page=1`,
        { headers: { Authorization: "token " + token, Accept: "application/vnd.github+json", "User-Agent": "artemis-qa-dashboard" } }
      );
      if (runs.ok) {
        const j = await runs.json();
        if (j.workflow_runs && j.workflow_runs[0]) {
          runId = j.workflow_runs[0].id;
          runUrl = j.workflow_runs[0].html_url;
        }
      }
    } catch { /* non-fatal */ }

    return {
      statusCode: 202,
      headers,
      body: JSON.stringify({ ok: true, dispatched: true, run_id: runId, run_url: runUrl }),
    };
  } catch (err) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: "server_error", message: err.message }),
    };
  }
};
