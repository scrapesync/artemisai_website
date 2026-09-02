// netlify/functions/sprint-state.mjs
// Shared state for the launch sprint tracker: which ticket is in which column,
// which checklist items are ticked, one note per ticket, moves between sprints
// or into the backlog, and who changed what.
//
// WHY THIS EXISTS
// ---------------
// The previous trackers saved every tick to localStorage, so each person had a
// private copy of the board. A tracker the team runs a launch from has to be
// one board. Storage is Netlify Blobs, as in charts.js and admin-archive.mjs.
//
// WHY A v2 FUNCTION
// -----------------
// The legacy exports.handler form bootstraps Blobs via connectLambda, which
// never supplies uncachedEdgeURL, so strong-consistency reads silently fall
// back to eventual ones and a tick can vanish on the next refresh. The v2 form
// gets the runtime's full context. Every response says which consistency was
// actually used.
//
// SHAPE
// -----
// One blob per ticket (key = ticket id): two people updating different tickets
// at the same moment cannot clobber each other.
//   { status, checks: {"0":true}, note, sprint?, layer?, by, updated_at, done_at,
//     history: [{at, by, from, to}] }   // newest first
// Team-added tickets live under "custom:<id>" with the full ticket body.
//
// No authentication, matching charts.js. Nothing is destroyed: dropped is a
// status, and custom tickets are the only things that can be deleted.
//
// Routes (via the /api/* redirect in netlify.toml):
//   GET    /api/sprint-state                       { state, custom, consistency }
//   POST   /api/sprint-state  {id, by, status?, checks?, note?, sprint?, layer?}
//   POST   /api/sprint-state  {custom: ticket, by}
//   DELETE /api/sprint-state?id=<custom id>

import { getStore } from "@netlify/blobs";

const STORE = "sprint-state-v2";
const STATUSES = ["todo", "inprog", "blocked", "done", "dropped"];
const SPRINTS = ["N1", "N2", "N3", "N4", "N5", "N6", "LW", "BL"];
const LAYERS = ["foundation", "intelligence", "product", "llm", "launch"];
const KEEP_HISTORY = 30;
const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Cache-Control": "no-store",
  "Content-Type": "application/json",
};
const reply = (status, body) => new Response(JSON.stringify(body), { status, headers: HEADERS });
const okId = (s) => /^[A-Za-z0-9][A-Za-z0-9_-]{1,60}$/.test(String(s || ""));
const now = () => new Date().toISOString();

export default async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: HEADERS });

  let weak, strong, used = "strong";
  try {
    weak = getStore(STORE); strong = weak;
    try { strong = getStore({ name: STORE, consistency: "strong" }); } catch { used = "eventual"; }
  } catch (err) {
    return reply(500, { error: "store_unavailable", message: String(err?.message || err).slice(0, 160) });
  }
  const read = async (fn) => {
    try { return await fn(strong); }
    catch (err) { if (String(err?.name) !== "BlobsConsistencyError") throw err; used = "eventual"; return await fn(weak); }
  };

  if (req.method === "GET") {
    const { blobs } = await read((s) => s.list());
    const entries = await Promise.all(blobs.map(async (b) => {
      try { return [b.key, await read((s) => s.get(b.key, { type: "json" }))]; } catch { return null; }
    }));
    const state = {}, custom = [];
    for (const e of entries) {
      if (!e || !e[1]) continue;
      if (e[0].startsWith("custom:")) custom.push(e[1]); else state[e[0]] = e[1];
    }
    return reply(200, { generated_at: now(), state, custom, consistency: used });
  }

  if (req.method === "POST") {
    let body;
    try { body = await req.json(); } catch { return reply(400, { error: "bad_json" }); }
    const by = String(body?.by || "someone").slice(0, 60);

    if (body?.custom && typeof body.custom === "object") {
      const t = body.custom;
      if (!okId(t.id) || !String(t.id).startsWith("C-")) return reply(400, { error: "custom_id_must_start_with_C-" });
      const clean = {
        id: String(t.id),
        sprint: SPRINTS.includes(String(t.sprint || "").toUpperCase()) ? String(t.sprint).toUpperCase() : "BL",
        layer: LAYERS.includes(String(t.layer || "").toLowerCase()) ? String(t.layer).toLowerCase() : null,
        assignee: String(t.assignee || "").slice(0, 40),
        title: String(t.title || "").slice(0, 140), what: String(t.what || "").slice(0, 1200), why: String(t.why || "").slice(0, 600),
        area: String(t.area || "Ops").slice(0, 40), due: String(t.due || "").slice(0, 10),
        priority: ["P0", "P1", "P2"].includes(t.priority) ? t.priority : "P1",
        priority_reason: String(t.priority_reason || "").slice(0, 300), depends_on: [], feeds: [],
        gate: String(t.gate || "none").slice(0, 8), source: "team", acceptance: String(t.acceptance || "").slice(0, 400),
        checklist: Array.isArray(t.checklist) ? t.checklist.slice(0, 8).map((s) => String(s).slice(0, 200)) : [],
        created_by: by, created_at: now(),
      };
      if (!clean.title) return reply(400, { error: "title_required" });
      await weak.setJSON("custom:" + clean.id, clean);
      return reply(200, { saved: true, id: clean.id, consistency: used });
    }

    if (!okId(body?.id)) return reply(400, { error: "bad_id" });
    const key = String(body.id);
    const prev = (await read((s) => s.get(key, { type: "json" }))) || { status: "todo", checks: {}, note: "", history: [] };
    const rec = { ...prev };
    if (body.status !== undefined) {
      const s = String(body.status).toLowerCase();
      if (!STATUSES.includes(s)) return reply(400, { error: "bad_status", expected: STATUSES });
      if (s !== prev.status) {
        rec.history = [{ at: now(), by, from: prev.status || "todo", to: s }, ...(prev.history || [])].slice(0, KEEP_HISTORY);
        // done_at is what the Performance tab reads for "on time or late".
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
    await weak.setJSON(key, rec);
    return reply(200, { saved: true, id: key, rec, consistency: used });
  }

  if (req.method === "DELETE") {
    const id = String(new URL(req.url).searchParams.get("id") || "");
    if (!okId(id) || !id.startsWith("C-")) return reply(400, { error: "only_custom_tickets_can_be_deleted" });
    await weak.delete("custom:" + id);
    await weak.delete(id).catch(() => {});
    return reply(200, { deleted: true, id });
  }
  return reply(405, { error: "method_not_allowed" });
};
