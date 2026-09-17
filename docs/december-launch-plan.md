> Superseded scope reduction: see [restoration of the full plan](scope-restoration.md). The broad essential-only cut was reversed; full workstreams are restored.

# Current scope correction

The 17 September [essential-scope review](business-scope-review.md) supersedes the earlier workload totals, ticket lists and fixed Meta submission assumption below. Current plan: 177 open tasks, 96 completed history records; launch 15 December, N1 end 25 September. Meta review follows a working integration.

# December launch plan — 16 September 2026

The user has fixed public launch at **15 December 2026**, with **15 December 2026 also the final deadline**. Use the existing eight-person team. The February/March plan and assumed new contractors or outside-counsel capacity are superseded.

This is a revised ticket calendar and management plan, not a capacity sign-off. No product delivery, external approval, spending, hiring, or launch announcement has been executed by this change.

## Milestones

| Milestone | Date |
|---|---|
| Meta App Review submission | 2 October |
| Beta readiness review | 27 November |
| Invited beta target | 1 December |
| Public readiness review | 11 December |
| Public launch target | 15 December |
| Final deadline | 15 December |

The beta target is a planning choice, not a completed gate. Required evidence windows, regulatory obligations, holiday dates and external review durations do not become shorter when sprint dates move. The Meta review-clock ticket explicitly requires verification against the new gates.

## Team execution

- Jill runs the daily blocker review and weekly owner workload review (N1-JL-20). Keep one primary build item in progress per owner; review and finish it before pulling the next.
- Alex resolves priority, scope and capacity conflicts within one working day (N1-AX-20), without moving the deadline. A smaller implementation or later release must be written down, with dependency and acceptance impacts.
- Asad owns the app and integration; Muteeb owns platform and infrastructure; Faheem owns models and data; Saad owns design and design QA.
- Jill owns coordination and evidence collection; Lewis owns pilot coordination and feedback; Filza retains legal sign-off; Alex owns commercial and scope decisions. Administrative support is not counted as replacement engineering or legal capacity.
- Required security, legal and release checks remain. An unmet gate is a blocker to resolve, never permission to mark a check passed.
- There is no later launch contingency window. No launch after 15 December is authorised.

## Workload still to resolve

Audited scope with inherited estimates and focus rates, including allocated reviewer effort; excludes deferred/merged work and recorded done/Phase 0. Shared-board status was read on 16 September at 12:15 UTC; remaining effort, leave and availability require owner confirmation. No new hires, overtime or assumed external counsel. Weekends add no capacity.

| Owner | Estimated work days | Focus days to 15 Dec | Unresolved gap |
|---|---:|---:|---:|
| Asad | 115.25 | 39 | 76.25 |
| Muteeb | 157.25 | 45.5 | 111.75 |
| Faheem | 97.75 | 45.5 | 52.25 |
| Saad | 76.5 | 45.5 | 31 |
| Alex | 24.5 | 26 | 0 |
| Jill | 42 | 39 | 3 |
| Lewis | 38 | 39 | 0 |
| Filza | 58 | 32.5 | 25.5 |

After the [second ticket audit](ticket-audit-round-2.md) and [live-board reconciliation](all-tabs-reconciliation.md), estimated work is **609.25 days**, capacity **312 days**, and the unresolved gap **297.25 days**. Fourteen deferrals and three consolidations across both passes preserve all original IDs. The fixed December date is not a capacity sign-off.

## Validation and publication state

- `node scripts/check_launch_plan.js`: fixed dates, sprint bounds, ticket IDs, dependency order/cycles, Meta anchors and existing owners.
- `node scripts/report_launch_capacity.js`: reproducible baseline workload report.
- Original IDs and Phase 0 history preserved; consolidated dependencies remapped and ghost-feature prerequisites removed with recorded reasons.
- Local browser preview checked. The live shared-board API was read separately; 14 completed active tickets are now included in the local baseline. Production shared edits and status have not been overwritten.
- Changes are in the local `codex/december-launch-plan` branch. No commit, push or deployment performed.

## Saad’s supplied design dates

See the [handoff plan](saad-design-handoff.md): landing mockups and tokens hand over 19 September in N1; app Home redesign is 22–23 September in N2. Other app dates remain provisional. Landing mockup completion is not implementation completion.

## 17 September calendar correction

N1 ends Friday25 September. All33 open ordinary N1 tickets use that deadline;31 due dates changed and two were already25 September. Three urgent security/incident tickets retain earlier deadlines (N1-AS-17, N1-MT-22, N1-FZ-13). Completed ticket dates remain historical. Eight dependent dates moved to the N1 gate to preserve ordering. Launch phase now ends15 December; public and final deadline agree. See friday-launch-correction.json for the date-change receipt.
