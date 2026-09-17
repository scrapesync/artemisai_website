# NLP completion reconciliation —16 September2026

The earlier Comment NLP0% was a grouping error. Its lane contained only three future tasks and omitted the four existing Phase0 records: P0-FH-07 (shipped pipeline), P0-FH-08 (five single-task classifiers), P0-FH-09 (training set) and P0-FH-10 (rubric). Post NLP also mixed its shipped foundation with later quality/data tasks, and included a comment record.

## Evidence checked

- Published [NLP tracker](https://artemisai.co.uk/nlp_accuracy_tracker.html): post model versions and five locked comment classifiers; comment locked entry dated1 June2026. Inspected delivered page source; database-backed historical entries were not separately fetched because the page’s get-all path can seed records.
- Live [QA snapshot](https://artemisai.co.uk/qa_metrics.json), generated2026-09-16T05:35:25.238828Z, powering the [QA dashboard](https://artemisai.co.uk/qa_dashboard.html): NEBULA posts28,105 and NEBULA-C comments38,181, both latest pipeline date14 September and reported pass. These establish existing output, not all model quality thresholds.
- [Marvel main](https://github.com/scrapesync/marvel/commit/e41795ab6e7a): remote head checked; recursive tree contains prediction schemas, collectors and OCR/fusion code. Local checkout is older atd063240. Complete NLP training/serving implementation is not present in the inspected main-tree paths; its absence is not evidence that production models are unfinished. No repository modification or pipeline execution performed.

## Corrections

Post and Comment NLP now each have a **built and running** lane containing their existing completed foundation records. Separate lanes hold quality/data follow-ups and crisis integration. Existing shipped OCR/fusion and video records remain distinct from unscheduled feature extensions. No new completion tickets were invented and no quality follow-up was marked done without evidence.

N1-MT-14 and N2-FH-09 now explicitly reuse the existing Comment NLP models for the crisis fast-lane. N2-FH-05 starts by checking whether the recorded toxic-recall gap has already been resolved, avoiding repeated training when evidence exists. The current QA aggregate does not measure that toxic-recall gate or the new endpoint latency budget.

No ticket effort or completion status changed in this correction. The apparent0% was a missing/misplaced foundation and mixed-purpose denominator, not proof Comment NLP needs building. Earlier workload estimates remain provisional until actual remaining acceptance work is reconciled.

The user screenshot shows the published March-date tracker. These fixes and the December calendar are local only, not deployed. Live shared-board state was not overwritten.

Current status update: see [all-tab reconciliation](all-tabs-reconciliation.md) for the later successful live-board read and revised workload totals.
