# Ticket audit — 16 September 2026

**First-pass record.** See the [second audit](ticket-audit-round-2.md) for the latest workload and dispositions.

Public launch remains **15 December**, with **20 December the absolute limit**, using the existing team.

Reviewed the 906 planning records: 82 historical Phase 0 records, 562 previously active tickets and 262 previously parked records. The revised board has **550 active tickets**, 82 historical records, 271 unscheduled backlog tickets and 3 consolidated historical records. No IDs or historical acceptance requirements were deleted. This is a planning audit, not a verification of completed code or the live team's progress.

## What changed

- Deferred nine stranded tickets tied to already-deferred Discovery, composite health, and the second pilot cohort used for Discovery. Five core pilots and their release checks remain.
- Consolidated three overlapping tickets into canonical deliverables: N1-AS-04 → N1-AS-03, N2-AS-08 → N2-AS-06, N4-AS-24 → N4-AS-14. Acceptance, checklists, dependency references and the full combined estimates are retained. Consolidation alone claims no capacity saving.
- Narrowed the remaining design packs to shipped surfaces and removed composite health from briefing implementation. These explicit scope changes and the nine deferrals remove 12 estimated person-days, from 651.5 to 639.5. Other wording fixes claim no effort saving.
- Corrected tickets that promised a standalone QA app, certification, contractor onboarding, or SOC 2 work despite their existing descriptions already deferring those deliverables. Existing dashboard, account/device security, repository controls and vendor review remain.
- Reassigned component bootstrap and mobile chrome to Saad, who already owns the component library, with one total day of Asad review retained. Lewis drafts support guides, with half a day of Saad review retained. These are proposed board assignments, not confirmed team commitments.
- Corrected the beta recruitment acceptance from 25 to the ticket's intended 10 and restored seven complete days of public-readiness shadow evidence. Preserved distinct prototype, integration and final-build tests; repeating a check on a different build is not automatically waste.

## Remaining workload

Audited scope with inherited estimates and focus rates, including allocated reviewer effort; excludes deferred/merged work and recorded done/Phase 0. Live team status, leave, estimates and availability require owner confirmation. No new hires, overtime or assumed external counsel. Weekends add no capacity.

| Owner | Estimated work days | Focus days to 15 Dec | Remaining gap |
|---|---:|---:|---:|
| Asad | 115.25 | 39 | 76.25 |
| Muteeb | 159.25 | 45.5 | 113.75 |
| Faheem | 118.5 | 45.5 | 73 |
| Saad | 78.5 | 45.5 | 33 |
| Alex | 28.5 | 26 | 2.5 |
| Jill | 42.5 | 39 | 3.5 |
| Lewis | 39 | 39 | 0 |
| Filza | 58 | 32.5 | 25.5 |

**639.5 estimated work days versus 312 available focus-days: 327.5 days remain unresolved.** The backlog is not included. Saad's component work is within his recorded role; administrative capacity is not treated as interchangeable with engineering or legal review. No overtime, hiring or shortened evidence windows is assumed. The plan is cleaner, but it is not yet capacity-balanced and this audit cannot certify that no unnecessary work remains.

The immediate execution rule is one primary build ticket per owner, with reviewers and dependencies named before work starts. Jill's existing N1-JL-20 collects actual remaining estimates and current completion; Alex's N1-AX-20 resolves scope/priority conflicts within one working day. Completion is supported by evidence; changing ticket counts never substitutes for finishing work. The unresolved gap requires verified remaining effort and further explicit scope decisions within the fixed deadline.

## Changed-ticket ledger

| Ticket | Disposition | Reason |
|---|---|---|
| N1-AS-03 | consolidated | Single deliverable with N1-AS-04; combined estimate retained, no assumed efficiency saving. |
| N1-AS-12 | reassigned | Saad already owns the component library; retain Asad integration review, without claiming team acceptance. |
| N1-SD-09 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N2-AS-06 | consolidated | Single deliverable with N2-AS-08; combined estimate retained, no assumed efficiency saving. |
| N2-SD-09 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N3-AS-10 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N3-AS-12 | reassigned | Saad already owns the component library; retain Asad integration review, without claiming team acceptance. |
| N3-MT-07 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N4-AS-05 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N4-AS-14 | consolidated | Single deliverable with N4-AS-24; combined estimate retained, no assumed efficiency saving. |
| N4-SD-10 | reassigned | Use pilot/support expertise for drafting, retain Saad review; total effort unchanged. |
| N5-AS-06 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N5-FH-03 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N5-SD-09 | reassigned | Use pilot/support expertise for drafting, retain Saad review; total effort unchanged. |
| N5-SD-16 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N2-SD-19 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N2-SD-20 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N2-FZ-14 | clarified | Keep data-reuse review; Discovery implementation is not its prerequisite. |
| N3-JL-15 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N3-SD-17 | clarified | Remove ghost-feature implementation obligations while retaining shipped-flow and disabled-route tests. |
| N3-MT-23 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N3-FZ-19 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N3-AS-24 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N3-FH-26 | narrowed | Align the ticket with the existing launch scope; preserve the shipped feature checks. |
| N5-LW-10 | clarified | Acceptance now matches the existing ten-person beta ticket, instead of requiring 25. |
| N1-FZ-04 | deferred | Discovery implementation is already deferred; resume its privacy, signal and gate work together with that feature. |
| N2-FH-18 | deferred | Discovery implementation is already deferred; resume its privacy, signal and gate work together with that feature. |
| N2-FH-19 | deferred | Discovery implementation is already deferred; resume its privacy, signal and gate work together with that feature. |
| N2-AS-14 | deferred | Discovery implementation is already deferred; resume its privacy, signal and gate work together with that feature. |
| N3-AS-15 | deferred | Discovery implementation is already deferred; resume its privacy, signal and gate work together with that feature. |
| N2-FH-15 | deferred | Composite health implementation N3-FH-06 is already deferred; do not specify or display an unimplemented score. |
| N2-FH-16 | deferred | Composite health implementation N3-FH-06 is already deferred; do not specify or display an unimplemented score. |
| N3-AS-05 | deferred | Composite health implementation N3-FH-06 is already deferred; do not specify or display an unimplemented score. |
| N3-LW-03 | deferred | Second pilot cohort exists to widen the deferred Discovery graph; retain the five core pilots and their launch acceptance checks. |
| N1-AS-04 | merged | All acceptance and effort transferred to N1-AS-03. |
| N2-AS-08 | merged | All acceptance and effort transferred to N2-AS-06. |
| N4-AS-24 | merged | All acceptance and effort transferred to N4-AS-14. |

The accompanying [full audit ledger](ticket-audit.json) classifies all 906 records and stores before/after records for the 37 changed tickets. Generic retained/backlog classifications preserve distinct work without claiming implementation verification. Consolidated source records remain readable under backlog history and do not appear as new work.

## Verification and publication

Fixed-date, Meta anchor, dependency ordering/cycle, ticket conservation, merge acceptance, reviewer-allocation and capacity-total checks pass. The updated board is a local proposal; it has not been committed, pushed or deployed. Shared team edits were not fetched or overwritten. The offline preview cannot validate server synchronization. Reconcile those edits before publication.
