# Research Reports Runbook

After `main.py` has produced logs, generate all research reports with:

```bash
python tools/generate_research_reports.py \
  --run-name <RUN_NAME> \
  --phase1-candidates logs/research/phase1_candidates.jsonl \
  --a1-reactions logs/research/a1_reaction_events.jsonl \
  --kline <KLINE_CSV> \
  --nohup <NOHUP_LOG> \
  --timezone Asia/Shanghai \
  --snapshot \
  --zip
```

Output:

```text
reports/<RUN_NAME>/
  phase1_truth/
  a1_edge/
  zone_truth/
  manifest.json
  research_report_index.md
```

Send:

```text
reports/<RUN_NAME>.zip
```

For 4h forward metrics, export Kline until at least last zone time + 4 hours.

Report set is frozen:

- no new independent report packages
- add future research fields into `zone_truth_events.csv` when possible
