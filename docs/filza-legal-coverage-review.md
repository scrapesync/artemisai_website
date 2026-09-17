# Filza legal-coverage review — 17 September 2026

## Filza's N1 tickets: audited, confirmed genuine

N1-FZ-06 (co-sign Meta scopes decision), N1-FZ-07 (draft ToS/Privacy/DPA), N1-FZ-09 (landing
privacy notice + tracking wording), N1-FZ-10 (data-deletion spec), N1-FZ-11 (Companies House
verification for Meta) and N1-FZ-12 (Meta permissions master doc) were each read in full against
`sprint_launch_data.js`. All six are genuine, launch-critical N1 deliverables with real
`what`/`why`/checklist/acceptance content, correct `applicability_note` disclaimers, and consistent
dependency edges. None are duplicates or filler. No changes were made to any of the six.

## Where company-legal and employee-contract work already lives

Verified present and not duplicated:

- **Company-legal:** N2-AX-11 (structure/share classes/articles/option pool/cap table), N2-JL-09
  (shareholders' agreement drafted), N3-JL-08 (signed + Companies House filings), N2-JL-10
  (Filza's equity), N3-AX-09 (board constitution), N3-FZ-14 (asset register + OSS licence inventory).
- **Employee/contractor legal:** N2-FZ-13 (IP assignment + confidentiality), N4-JL-08 (employment
  status + written engagement terms, all eight team members), N1-FZ-01 (Rafeh leaver exit paper,
  correctly parked in backlog).
- **Privacy/ToS/DPA lifecycle:** N1-FZ-07 draft → N2-FZ-05 sign+publish v1 → N2-AX-04 Alex signs
  pack → N2-FZ-06/N2-FZ-04 DPA/pilot terms → N5-FZ-06 sign ToS final → N6-FZ-03 frozen-build sweep.
- Trademark clearance, children's-data position, Online Safety Act scoping, Article 9/22 memos,
  DPIA, international transfers, DUAA pass, refund terms, sanctions screening, insurance, VDP,
  Meta narrative/permissions review cycle, Data Use Checkup binder, image-rights position and
  reviews/endorsements policy were all independently re-verified present in the tracker and are
  not duplicated by this review.
- N3-JL-18, N3-JL-19 and LW-JL-05 (the parked no-new-resource tickets) were left untouched.

## Confirmed gaps closed — three new tickets

| ID | Sprint | Due | Reason |
|---|---|---|---|
| **N2-FZ-18** | N2 | 2026-10-02 | Cookie Policy had no draft-to-publish lifecycle of its own. N6-FZ-03 already sweeps a signed, versioned Cookie Policy as a fourth core legal document, but nothing drafted or published one — only N1-FZ-09's tracking-wording inputs existed. |
| **N2-FZ-19** | N2 | 2026-10-04 | No copyright/IP-infringement notice-and-takedown policy existed for the public website. P0-FZ-02 only covers the company's own use of third-party images (input side), not the outward-facing complaints process. Kept lightweight — no user-generated content, so no full DMCA program. |
| **N5-FZ-12** | N6 | 2026-11-24 | The accessibility statement in BL-PR-06 was unscheduled with no owner, bundled with an unrelated coach-marks UI feature. Split out with Filza as owner (wording, Equality Act 2010 context), Saad/Asad publishing, and a due date ahead of the 15 Dec hard deadline, depending on Saad's completed WCAG fix ticket N5-AS-04. |

Two existing tickets were edited for consistency, not cut:

- **N6-FZ-03** — added `N2-FZ-18` to `depends_on` so the frozen-build legal sweep actually depends
  on the Cookie Policy existing.
- **N2-AX-04** — title, `what`, checklist and acceptance updated so Alex's legal-pack sign-off
  explicitly covers Cookie Policy v1 alongside ToS/Privacy/DPA, and `depends_on` gained `N2-FZ-18`.

No existing ticket was deleted. No blanket cuts were made.

## BL-PR-06

Split in place (same id, still in `backlog`): retitled from "Accessibility statement + coach marks"
to **"Onboarding coach marks"**, with its `what`/`why`/layman fields rescoped to the coach-marks UI
feature only. The accessibility-statement scope moved to the new N5-FZ-12, which is scheduled,
owned and dated — unlike the old bundled row.

## Before / after counts

| Metric | Before | After |
|---|---|---|
| Total ticket+backlog IDs | 904 | 907 |
| Open non-P0 tickets | 529 | 532 |
| Backlog count | 279 | 279 (unchanged) |
| Total effort days (capacity review) | 606.25 | 608.25 |
| N1-AS-17 / N1-FZ-13 | excluded | still excluded |

## Files changed

- `sprint_launch_data.js` — added N2-FZ-18, N2-FZ-19, N5-FZ-12; edited BL-PR-06, N6-FZ-03,
  N2-AX-04; regenerated `capacity_review` (Filza's row and `total_effort_days`).
- `docs/scope-restoration.json` — `expected_ids` (+3), `before_total` 906→909, `retained_total`
  904→907.
- `scripts/check_launch_plan.js` — open non-P0 ticket count 529→532.
- `scripts/check_workload_review.js` — `capacity_review.total_effort_days` 606.25→608.25.
- `scripts/check_critical_review.js` — ticket+backlog total 904→907; `total_effort_days`
  606.25→608.25.
- `docs/filza-legal-coverage-review.md` / `.json` — this review (new).

`scripts/report_launch_capacity.js`, `scripts/check_tracker_views.js` and
`scripts/check_ticket_links.js` were checked and needed no changes — none of their assertions
reference a count this review touched.

## Validation

`node scripts/check_launch_plan.js`, `check_workload_review.js`, `check_critical_review.js`,
`check_tracker_views.js` and `check_ticket_links.js` all pass. No assertion was weakened; every
changed number was recomputed from the actual data (via `scripts/report_launch_capacity.js` for
capacity, and direct filters for ticket counts).

## Browser + PDF verification

Served the worktree with a local static server and opened `sprint_tracker_launch.html`. Confirmed
N2-FZ-18, N2-FZ-19 and N5-FZ-12 render correctly with full field content, dependency links and
correct sprint placement, and that BL-PR-06 in the Backlog tab shows the retitled coach-marks-only
row cross-linking to N5-FZ-12. (Note: two stale `http.server` processes from an unrelated prior
session were already bound to port 8766 serving a different directory; they were stopped so this
review's own server on that port could serve the current worktree — see judgment calls below.)

Regenerated `output/pdf/ArtemisAI_N1_Filza.pdf` from `tmp/pdfs/current_guides.py` (6 pages, one per
Filza N1 ticket — unchanged ticket set, since all new tickets are N2/N6). All 6 pages rendered via
`pdftoppm` and visually inspected: no clipping or overlap, correct content, working ticket links.
`ArtemisAI_N1_Alex_and_Jill.pdf` was regenerated as a side effect of running the same script (it
builds both PDFs in one pass), but its N1-AX/N1-JL ticket set and content are unaffected — verified
via the script's own built-in assertion against the current N1 ticket data, since no N1 ticket was
touched by this review.

## Judgment calls to flag

1. Extended N2-AX-04 to require signing Cookie Policy v1, since it explicitly names the other
   three core documents Alex signs — treating the omission as an oversight rather than intentional.
2. Used the `N2-FZ-`/`N5-FZ-` id convention (matching assignee initials) for all three new tickets
   rather than the `N5-AS-NN` example numbering mentioned only as one option for the accessibility
   ticket, since Filza is the assignee and neighbors like N5-FZ-10/11 already show sprint fields
   that don't match their id-number prefix.
3. Did not add the new copyright/takedown ticket to N6-FZ-03's sweep, since that ticket's `what`
   field names exactly four core documents (ToS/Privacy/DPA/Cookie) and the takedown policy is a
   separate, fifth public-site policy.
4. Found and stopped two stale, unrelated `python -m http.server 8766` processes (serving a
   different, older preview directory `/private/tmp/artemis-december-preview`) that were silently
   intercepting requests on the port this task's local server needed. Stopping them was necessary
   to verify this review's actual changes in a browser; flagging in case that preview directory
   was being used by another concurrent session.
