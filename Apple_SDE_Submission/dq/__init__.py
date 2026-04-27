"""
Data Quality module — production SQL implementation.

Production rules live in `rules.sql` (`dq_check_*` functions + `dq_run_batch`
orchestrator) and `rules_split.sql` (INGEST / PRE_FACT / SEMANTIC stage-aware
variants used by the /load-data pipeline).

Three stages, by design:
  * INGEST   — runs on raw stg_price_offer (parse / format / null checks)
  * PRE_FACT — runs on enriched stg AFTER harmonise; HIGH-severity gate
               (country↔currency, partner↔country, harmonise unmatched).
               Failing rows do NOT enter fact_price_offer.
  * SEMANTIC — runs on fact_price_offer AFTER load (soft signals only;
               low-confidence harmonise + category sanity bounds).
               Failing rows STAY in fact, just flagged for triage.

The SQL functions are deployed once via `psql -f rules.sql` (and rules_split.sql)
and are called from `api/services.py` through `dq_run_batch_ingest($1)`,
`dq_run_batch_pre_fact($1)`, and `dq_run_batch_semantic($1)`.
"""
