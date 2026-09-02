# Alerts Page — Deployment Guide

How the scheduled checks in AWS end up as cards on `alerts.html`.

**Estimated time:** 15–20 minutes for the first alert, ~2 minutes for each one after.

---

## Overview

```
┌──────────────────┐  cron   ┌──────────────────┐  POST   ┌──────────────────┐
│ EventBridge      │────────▶│ Lambda           │────────▶│ /api/alert-status│
│ rule (schedule)  │         │ runs ONE check   │  +key   │ Netlify function │
└──────────────────┘         └──────────────────┘         └────────┬─────────┘
                                                                   │ Netlify Blobs
                                                                   ▼
                                                          ┌──────────────────┐
                                                          │ alerts.html      │
                                                          │ one card each    │
                                                          └──────────────────┘
```

The Lambda decides whether the alert passed. This site only remembers what it was
told. That split matters: the browser cannot hold AWS credentials, and this repo
is public, so the page can never call EventBridge or CloudWatch itself.

---

## Why the write path needs a key

`/api/charts` is deliberately open — the worst case there is clutter a human can
delete. This is different. Anything that can POST to `/api/alert-status` can make
a broken pipeline look green, which is the exact failure the page exists to catch.

So writes require the `X-Alert-Key` header to match `ALERT_INGEST_KEY`.

**The key lives in two places only:** Netlify environment variables, and the
Lambda's environment. Never in this repo — it is public.

---

## Step 1 — Set the key on Netlify

Netlify dashboard → Site configuration → Environment variables → Add:

| Key | Value |
|---|---|
| `ALERT_INGEST_KEY` | a long random string (`openssl rand -hex 32`) |

Redeploy for it to take effect. Until it is set, the endpoint refuses all writes
with `503 ingest_not_configured` — deliberately, so a missing key fails loudly
rather than accepting unauthenticated writes.

---

## Step 2 — The POST contract

```
POST https://<your-site>/api/alert-status
Content-Type: application/json
X-Alert-Key: <ALERT_INGEST_KEY>

{
  "name": "No load today - page_posts",   // required, becomes the card title
  "status": "ok" | "fired" | "error",     // required
  "message": "page_posts loaded 09:14.",  // shown on the card, <= 400 chars
  "ms": 640,                              // how long the check took
  "every_minutes": 60,                    // how often the rule runs - see below
  "detail": { }                           // anything; stored, not displayed
}
```

`status` means:

- **ok** — the check ran and the thing it guards is healthy
- **fired** — the check ran and found the problem it was built to find
- **error** — the check could not complete (timeout, permissions, bad SQL)

`error` is not `fired`. A check that cannot run is not a check that passed, and
the page colours them differently for exactly that reason.

**`every_minutes` is the important one.** It is how the page knows a check has
*stopped running*. Without it a dead alert shows only "last run 4 days ago" and
looks like any other quiet card. With it, the card turns violet and says so.

---

## Step 3 — The Lambda

One Lambda can host many checks, or you can have one per check. Node 20 runtime.
Environment: `ALERT_INGEST_KEY`, `SITE_URL`.

```js
const post = async (payload) => {
  const res = await fetch(`${process.env.SITE_URL}/api/alert-status`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Alert-Key": process.env.ALERT_INGEST_KEY,
    },
    body: JSON.stringify(payload),
  });
  if (!res.ok) console.error("alert post failed", res.status, await res.text());
};

exports.handler = async () => {
  const started = Date.now();
  const NAME = "No load today - page_posts";
  try {
    const stale = await checkPagePostsLoadedToday(); // your Redshift query
    await post({
      name: NAME,
      status: stale ? "fired" : "ok",
      message: stale
        ? "page_posts has not loaded today."
        : "page_posts loaded within the last 24h.",
      ms: Date.now() - started,
      every_minutes: 60,
    });
  } catch (err) {
    // Report the failure rather than letting the Lambda die silently. A check
    // that throws and says nothing is indistinguishable from a healthy system,
    // which is how NEBULA's guard sat dead from June to August.
    await post({
      name: NAME,
      status: "error",
      message: String(err.message || err).slice(0, 400),
      ms: Date.now() - started,
      every_minutes: 60,
    });
  }
};
```

**Always report, including on failure.** The `catch` block posting `error` is the
whole point — it is what turns a silent breakage into a visible card.

---

## Step 4 — The EventBridge rule

Amazon EventBridge → Rules → Create rule:

- **Rule type:** Schedule
- **Schedule:** `rate(1 hour)` — must match the `every_minutes` you post
- **Target:** the Lambda from Step 3

Keep the two in step. If you change the rule to every 4 hours, change
`every_minutes` to 240, or the page will start reporting a healthy check as stopped.

---

## Step 5 — Check it worked

Post a test card from your machine:

```bash
curl -X POST https://<your-site>/api/alert-status \
  -H 'Content-Type: application/json' \
  -H "X-Alert-Key: $ALERT_INGEST_KEY" \
  -d '{"name":"Setup test","status":"ok","message":"Reached the store.","ms":12,"every_minutes":60}'
```

Open `alerts.html` and hit Refresh. Remove it when you are done:

```bash
curl -X DELETE 'https://<your-site>/api/alert-status?name=Setup%20test'
```

---

## What the cards tell you

Cards sort by how much they want attention, and **silence outranks noise**:

1. **Stopped running** — missed twice its own schedule. The rule or the Lambda is
   dead, so this check is protecting nothing.
2. **Never run** — registered but has never reported.
3. **Errored** — ran, could not complete.
4. **Fired** — ran, found the problem. Working as designed.
5. **Passing** — ran, all clear.

A passing card can still carry a warning: **"has never fired since <date>"**. That
is not reassurance. A check that has never once fired is usually a check that
*cannot* fire, and it looks exactly like a system with no problems.

---

## Notes

- History is capped at the last 20 runs per alert — enough to see a pattern, not
  enough to make a card slow.
- `last_fired_at` is stored outside the run list, so "has it ever fired?" survives
  the history rolling over.
- Deleting an alert only removes the card. Delete the EventBridge rule too, or it
  reappears on the next run.
