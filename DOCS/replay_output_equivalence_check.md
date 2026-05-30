# Replay Output Equivalence Check

Developer-only V7.2.1.1 smoke check for local replay performance changes.

1. Run the previous V7.2.1 code on the same 10 minute or 1 hour sample.
2. Run the V7.2.1.1 code on the identical inputs and time window.
3. Compare these core outputs:
   - `research/phase1_candidates.jsonl` line count
   - `research/a1_reaction_events.jsonl` line count
   - `research_events.jsonl` `ICEBERG_ABSORPTION` row count
   - `research_events.jsonl` `SPOOFING_WITHDRAWAL` row count
   - `summary.json` `stats.trades`
   - `summary.json` `stats.books`
   - `summary.json` `stats.raw_book_rows`
   - `summary.json` `stats.a1_iceberg_events`

These values should match. By default V7.2.1.1 does not write ignored A1 engine debug returns to `research_events.jsonl`, so total `research_events.jsonl` rows may be lower. Use `--write-ignored-engine-returns` when comparing full debug-row output with the old behavior.
