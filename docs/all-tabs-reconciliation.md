# All-tab reconciliation — 16 September 2026

The live shared board was read at 2026-09-16T12:15:43.591Z using GET https://artemisai.co.uk/api/sprint-state. Its 24 matching ticket statuses were incorporated into the local baseline; two unknown test records were ignored. Fourteen active tickets were recorded done, removing 22 estimated days of already completed work. Private notes and checkbox data were not copied into generated public data. These are team-recorded completions, not new independent acceptance sign-offs.

The tracker has 96 completed / 627 scheduled records, including 82 historical Phase 0 records. Seven scheduled tickets are in progress and two blocked. All 906 original IDs remain: 627 scheduled, 276 parked and three consolidated history records.

| Tab | Reconciled information |
|---|---|
| Overview | Shared completion counts, remaining owner effort, 15 December target and 20 December hard limit |
| Sprint Tracker | Current statuses, scheduled dates, Saad handoffs and checklist revision handling |
| MVP Features | Current scope, shared progress; deferred surfaces excluded from active commitments |
| Plain English | Same ticket scope and completion as feature and sprint views |
| Dependencies | Current sprint dates and remapped prerequisites; unresolved prose dependencies remain visible |
| Backlog | 276 unscheduled tickets and three consolidated records; excluded from active effort |
| Performance | Shared status; completion timeliness explicitly compared with current planned due dates |
| Build | Landing mockups separate from product designs; existing work reflected in progress |
| App Review | 2 October submission target and actual open ticket state; no assumed approval |
| Costing | Ticket-backed progress; inherited estimates remain planning inputs, not new measured costs |
| Models | Post/comment NLP foundations complete; quality/integration follow-ups separate; video freshness limitation explicit |
| Launch | Fixed December dates and current rollout ticket state; waitlist opening still conditional |
| QA Control Center | Existing dashboard reused; gate dates derived from current records, not copied old calendar |

The exact saved 7 September N1 calendar override (18 September gate) is superseded locally; unrelated or newer overrides still apply. The saved source record was not deleted. Older cached statuses cannot erase verified baseline completion; newer team updates still apply. Revised checklist wording requires fresh confirmation rather than inheriting old positional ticks.

Remaining estimated work: **609.25 days** against **312 focus days** to 15 December. This is not balanced or capacity-approved. Remaining estimates, leave and availability need owner validation; no invented overtime or hiring closes the gap.

Evidence: live shared board above; https://artemisai.co.uk/nlp_accuracy_tracker.html; https://artemisai.co.uk/qa_metrics.json (snapshot 2026-09-16T05:35:25.238828Z); Marvel repository checks documented in nlp-completion-reconciliation.md. Historical audit documents retain their earlier snapshots; this receipt and december-launch-plan.md supersede their current totals.

Validation: plan invariants, actual view-filter tests, stale/new state and calendar override regression tests passed. All 13 tabs were opened in the local browser and their rendered content checked. The static preview cannot call the same-origin production API; it displays the verified snapshot. No production API writes, commit, push or deployment were performed.
