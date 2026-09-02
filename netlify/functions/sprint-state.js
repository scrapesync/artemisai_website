// netlify/functions/sprint-state.js
// Shared state for the launch sprint tracker: which ticket is in which column,
// which checklist items are ticked, one note per ticket, and who changed what.
//
// WHY THIS EXISTS
// ---------------
// The previous trackers saved every tick to localStorage ('artemis_v6_mvp'),
// which meant each person had a private copy of the board and nobody's ticks
// were anyone else's. A tracker the team runs a launch from has to be one
// board. Same storage choice as charts.js and admin-archive.js: Netlify Blobs,
// because the repo is public and a commit per tick would be absurd.
//
// SHAPE
// -----
// One blob per ticket (key = ticket id), never one document for the board:
// two people updating different tickets at the same moment must not clobber
// each other, and a per-ticket blob makes that structurally impossible.
//
//   { status, checks: {"0":true,...}, note, by, updated_at,
//     history: [{at, by, from, to}] }   // status changes only, newest first
//
// Tickets the team adds in-app live under "custom:<id>" and carry the full
// ticket body, so the page can render them like built-in ones.
//
// No authentication, matching charts.js: the admin sign-in is client-side, so a
// key here would be theatre. Every change keeps a history line with a
// self-declared name, and nothing is destroyed - a dropped ticket is a status.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/sprint-state                      { state: {id: {...}}, custom: [ticket...] }
//   POST   /api/sprint-state  {id, by, status?, checks?, note?, sprint?, layer?}   merge into one ticket
//   POST   /api/sprint-state  {custom: ticket, by}                add a team-authored ticket
//   DELETE /api/sprint-state?id=<custom id>                       remove a custom ticket only

const { connectLambda, getStore } = require("@netlify/blobs");

const STORE = "sprint-state-v2";
const STATUSES = ["todo", "inprog", "blocked", "done", "dropped"];
// A ticket can be moved between sprints, or parked in the backlog ("BL"), by
// anyone on the board. The move is stored as an override on the ticket's state
// rather than by editing the generated data file, so the file stays the plan
// and the board stays the team's.
const SPRINTS = ["N1", "N2", "N3", "N4", "N5", "N6", "LW", "BL"];
const LAYERS = ["foundation", "intelligence", "product", "llm", "launch"];
const KEEP_HISTORY = 30;

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
};
const reply = (statusCode, body) => ({
  statusCode, headers: { ...HEADERS, "Content-Type": "application/json" }, body: JSON.stringify(body),
});
// Ticket ids are "N3-LW-02" or a custom "C-..." id; anything else is not a key.
const okId = (s) => /^[A-Za-z0-9][A-Za-z0-9_-]{1,60}$/.test(String(s || ""));
const now = () => new Date().toISOString();

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };

  let store;
  try { connectLambda(event); store = getStore(STORE); }
  catch (err) {
    return reply(500, { error: "store_unavailable",
      message: "Netlify Blobs is not available here: " + String(err.message || err).slice(0, 160) });
  }
  const qs = event.queryStringParameters || {};

  /* ---------- read the whole board ---------- */
  if (event.httpMethod === "GET") {
    const { blobs } = await store.list();
    const entries = await Promise.all(blobs.map(async (b) => {
      try { return [b.key, await store.get(b.key, { type: "json" })]; } catch { return null; }
    }));
    const state = {}, custom = [];
    for (const e of entries) {
      if (!e || !e[1]) continue;
      if (e[0].startsWith("custom:")) custom.push(e[1]);
      else state[e[0]] = e[1];
    }
    return reply(200, { generated_at: now(), state, custom });
  }

  /* ---------- write ---------- */
  if (event.httpMethod === "POST") {
    let body;
    try { body = JSON.parse(event.body || "{}"); }
    catch { return reply(400, { error: "bad_json" }); }
    const by = String(body.by || "someone").slice(0, 60);

    // a team-authored ticket
    if (body.custom && typeof body.custom === "object") {
      const t = body.custom;
      if (!okId(t.id) || !String(t.id).startsWith("C-")) return reply(400, { error: "custom_id_must_start_with_C-" });
      const clean = {
        id: String(t.id), sprint: SPRINTS.includes(String(t.sprint || "").toUpperCase()) ? String(t.sprint).toUpperCase() : "BL", assignee: String(t.assignee || "").slice(0, 40),
        title: String(t.title || "").slice(0, 140), what: String(t.what || "").slice(0, 1200), why: String(t.why || "").slice(0, 600),
        area: String(t.area || "Ops").slice(0, 40), due: String(t.due || "").slice(0, 10), priority: ["P0","P1","P2"].includes(t.priority) ? t.priority : "P1",
        priority_reason: String(t.priority_reason || "").slice(0, 300), depends_on: [], feeds: [], gate: String(t.gate || "none").slice(0, 8),
        source: "team", acceptance: String(t.acceptance || "").slice(0, 400),
        layer: LAYERS.includes(String(t.layer || "").toLowerCase()) ? String(t.layer).toLowerCase() : null,
        checklist: Array.isArray(t.checklist) ? t.checklist.slice(0, 8).map((s) => String(s).slice(0, 200)) : [],
        created_by: by, created_at: now(),
      };
      if (!clean.title) return reply(400, { error: "title_required" });
      await store.setJSON("custom:" + clean.id, clean);
      return reply(200, { saved: true, id: clean.id });
    }

    if (!okId(body.id)) return reply(400, { error: "bad_id" });
    const key = String(body.id);
    const prev = (await store.get(key, { type: "json" })) || { status: "todo", checks: {}, note: "", history: [] };
    const rec = { ...prev };

    if (body.status !== undefined) {
      const s = String(body.status).toLowerCase();
      if (!STATUSES.includes(s)) return reply(400, { error: "bad_status", expected: STATUSES });
      if (s !== prev.status) {
        rec.history = [{ at: now(), by, from: prev.status || "todo", to: s }, ...(prev.history || [])].slice(0, KEEP_HISTORY);
        // done_at is what the Performance tab reads for "on time or late"; it is
        // set on the transition into done and cleared if the ticket is reopened.
        rec.done_at = s === "done" ? now() : null;
      }
      rec.status = s;
    }
    if (body.checks && typeof body.checks === "object") {
      const c = {};
      for (const k of Object.keys(body.checks).slice(0, 12)) if (body.checks[k]) c[String(k)] = true;
      rec.checks = c;
    }
    if (body.note !== undefined) rec.note = String(body.note).slice(0, 1000);
    if (body.sprint !== undefined) {
      const s = String(body.sprint).toUpperCase();
      if (!SPRINTS.includes(s)) return reply(400, { error: "bad_sprint", expected: SPRINTS });
      if (s !== (prev.sprint || null)) {
        rec.history = [{ at: now(), by, from: "sprint:" + (prev.sprint || "plan"), to: "sprint:" + s }, ...(prev.history || [])].slice(0, KEEP_HISTORY);
      }
      rec.sprint = s;
    }
    if (body.layer !== undefined) {
      const l = String(body.layer).toLowerCase();
      if (!LAYERS.includes(l)) return reply(400, { error: "bad_layer", expected: LAYERS });
      rec.layer = l;
    }
    rec.by = by; rec.updated_at = now();
    await store.setJSON(key, rec);
    return reply(200, { saved: true, id: key, rec });
  }

  /* ---------- delete a custom ticket ---------- */
  if (event.httpMethod === "DELETE") {
    const id = String(qs.id || "");
    if (!okId(id) || !id.startsWith("C-")) return reply(400, { error: "only_custom_tickets_can_be_deleted" });
    await store.delete("custom:" + id);
    await store.delete(id).catch(() => {});
    return reply(200, { deleted: true, id });
  }
  return reply(405, { error: "method_not_allowed" });
};
