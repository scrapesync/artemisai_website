# Workload allocation and dependency handoffs

17 September 2026. Launch stays **15 December**. N1 stays **Friday 25 September**. All 904 retained records and their acceptance requirements remain.

This is a skills-based planning allocation using the existing roles, not confirmation of individual availability. No new hires, assumed overtime or invented engineering skills. **15 tickets now have explicit preparation/delivery and review shares; 13 lead assignments changed.**

## Allocation rules

- Asad owns technical architecture and frontend integration; Muteeb backend, infrastructure and APIs; Faheem AI/ML and data quality; Saad product design, components and acceptance.
- Alex takes more release evidence and commercial coordination. Jill handles operational and financial records. Lewis handles customer scenarios, walkthrough recordings and documentation capture. Filza retains legal judgement and sign-off.
- A lead collects the deliverable; that does not grant specialist authority. Every listed contributor has reserved effort, and required review is part of completion.
- Use one primary delivery item per person. Complete and review it before pulling the next; a waiting dependency should be resolved or clearly identified, not bypassed.

## Concrete handoffs

| Ticket | Delivery lead | Reserved work/review days | Responsibility split |
|---|---|---|---|
| [N5-MT-09](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N5-MT-09) | Alex | Alex 0.25; Muteeb 0.25 | Alex assembles the review pack and chases missing links; Muteeb validates the platform results and unresolved defects. |
| [N6-MT-08](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N6-MT-08) | Alex | Alex 0.25; Muteeb 0.25 | Alex fills the go/no-go evidence slots; Muteeb enforces the technical freeze and attests to the evidence. |
| [N6-SD-06](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N6-SD-06) | Alex | Alex 0.5; Saad 0.5 | Alex maintains the decision/defect log and escalates blockers; Saad validates product acceptance and UAT evidence. |
| [N5-SD-06](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N5-SD-06) | Alex | Alex 0.5; Saad 0.25; Muteeb 0.25 | Alex assembles launch roles and the run-of-show; Saad signs product gates and Muteeb validates rollback instructions. |
| [N2-SD-04](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N2-SD-04) | Alex | Alex 1; Saad 0.5; Asad 0.25; Filza 0.25 | Alex assembles the submission evidence; Saad retains the Meta submission, Asad confirms the working journey and Filza approves claims. |
| [N2-AS-09](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N2-AS-09) | Lewis | Lewis 0.5; Asad 0.5 | Lewis records the scripted customer journey on approved test accounts; Asad verifies permission coverage and freezes the implemented UI. Lewis does not change auth or permission configuration. |
| [N6-SD-03](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N6-SD-03) | Lewis | Lewis 0.25; Saad 0.25 | Lewis captures screenshots and checks links and dates; Saad approves visual and product accuracy. Legal wording is reused from approved documents. |
| [N3-FH-09](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N3-FH-09) | Lewis | Lewis 0.5; Faheem 0.5 | Lewis assembles question coverage and missing customer scenarios; Faheem decides evaluation validity and technical coverage gaps. |
| [N3-FZ-14](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N3-FZ-14) | Jill | Jill 0.5; Filza 0.25; Asad 0.25 | Jill collects company asset and account ownership records; Asad supplies repository licence evidence; Filza interprets licences and approves legal ownership conclusions. |
| [N3-FZ-19](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N3-FZ-19) | Jill | Jill 0.5; Filza 0.25; Muteeb 0.25 | Jill collects current vendor evidence in the existing register; Muteeb assesses technical gaps and Filza signs the data/vendor implications. |
| [N2-JL-08](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N2-JL-08) | Alex | Alex 0.75; Jill 0.25 | Alex resolves product-direction decisions and maintains their outcomes; Jill checks follow-up and unresolved age in the existing team board. |
| [N5-JL-08](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N5-JL-08) | Alex | Alex 0.5 | Alex owns the marketing scoreboard, targets and channel decisions, using existing analytics exports; no new engineering dashboard is commissioned. |
| [N3-JL-12](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N3-JL-12) | Alex | Alex 0.25; Jill 0.25 | Alex owns spending approval limits and decisions; Jill maintains vendor commitments and checks the financial records. |
| [N5-MT-05](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N5-MT-05) | Muteeb | Muteeb 1.5; Alex 0.5 | Muteeb implements the monitoring view and writes technical recovery steps. Alex assembles the failure table, roles and communication links from those instructions; technical acceptance stays with Muteeb. |
| [N6-AS-10](https://artemisai.co.uk/sprint_tracker_launch.html?ticket=N6-AS-10) | Asad | Asad 0.5; Alex 0.5 | Asad enforces the code freeze and signs engineering evidence; Alex collects the existing evidence links into the release memo. |

## Capacity after redistribution

The total remains **606.25 workdays**: no savings have been manufactured. Available capacity is now **307.2 days**, refreshed to 17 September (the previous 312-day baseline started on 16 September). Estimates and focus rates remain inherited, not owner-confirmed. Conditional work remains reserved until a decision is recorded.

| Person | Previous work days | New work + review days | Available days | Unresolved gap |
|---|---:|---:|---:|---:|
| Asad | 113.25 | 112.75 | 38.4 | 74.35 |
| Muteeb | 157.25 | 156.75 | 44.8 | 111.95 |
| Faheem | 97.75 | 97.25 | 44.8 | 52.45 |
| Saad | 76.5 | 73.5 | 44.8 | 28.7 |
| Alex | 24.5 | 29.5 | 25.6 | 3.9 |
| Jill | 42 | 41.5 | 38.4 | 3.1 |
| Lewis | 38 | 39.25 | 38.4 | 0.85 |
| Filza | 57 | 55.75 | 32 | 23.75 |

Redistribution gives Alex a larger coordination queue and reduces some specialist overhead. It does **not** solve the technical shortfall. Alex, Jill and Lewis also have limited spare time; adding every administrative task to them would create a new bottleneck. The current plan is not resource-feasible and no ticket dates have been moved to pretend otherwise.

## How to use the live queue

Open **Performance → a person → Workload and next actions**.

1. If work is already in progress, finish or resolve it before pulling another item. Otherwise, the first three suggestions have no unfinished ticket-ID prerequisites, are in a started sprint, and are not conditional or explicitly blocked. They are candidates to pull, not automatic permission to start.
2. Confirm listed external/unnumbered inputs and required access before starting. Unknown inputs remain visible.
3. The full delivery queue lists waiting ticket IDs and how many downstream records depend on each item. Ordering uses earliest due date, priority and downstream impact among available work. Blocked items are never included in the pull suggestions.
4. The review/support list shows work on other people's tickets, so their involvement is not hidden behind ticket counts.
5. Saved statuses, assignee edits and due dates are read when the view renders. After a lead reassignment, recheck contributor allocations as well; changing an assignee alone does not transfer specialist review effort.

No automatic status changes, customer messages, permission grants or vendor bookings were made. A reviewer reservation is not a new approval system; the lead must obtain the required review evidence before completing the ticket.

## Dependency handoff contract

- Design → frontend: approved pack, states and API contract; fixtures allow preparation only.
- Model → serving → frontend: versioned artifact and output contract, tested endpoint, then real-data UI acceptance.
- Legal → pilot onboarding: applicable signed/countersigned terms and required data-risk clearance before connection.
- Backend → Art-E frontend: working retrieval/orchestration, citations, quotas and failure behaviour before integrated acceptance.
- Build → QA → release: versioned test evidence, accepted fixes, stable evaluation window, deployment/rollback proof, then go/no-go.

The existing dependency graph is preserved and checked for missing IDs, cycles and completion ordering. The queue does not infer that free-text/external dependencies are complete. Parallel preparation does not satisfy a prerequisite. Same-day handoffs still require a real order and review time; this view is not a resource-levelled delivery schedule.

## Next operational action

Jill uses N1-JL-20 to collect actual remaining effort and availability. Alex resolves the conflicts in N1-AX-20. Prioritise a complete vertical customer journey and its legal/security gates; record any implementation simplification explicitly without losing acceptance. The engineering shortfall cannot be eliminated by giving non-engineers coding work or changing estimated days without evidence.
