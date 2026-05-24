# Research Reports Runbook

After `main.py` has produced logs, generate all research reports with:

```bash
python tools/generate_research_reports.py \
  --run-name <RUN_NAME> \
  --phase1-candidates logs/research/phase1_candidates.jsonl \
  --a1-reactions logs/research/a1_reaction_events.jsonl \
  --kline data/history_k/<KLINE_CSV> \
  --nohup logs/<NOHUP_LOG> \
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


e.g.
```text
python tools/generate_research_reports.py --run-name V6311_5 --phase1-candidates logs/research_V6311_5/phase1_candidates.jsonl   --a1-reactions logs/research_V6311_5/a1_reaction_events.jsonl --kline data/history_k/ETH-USDT-SWAP_1m_20260523_2200_20260524_1500.csv --nohup logs/nohup_V6311_5.out --timezone Asia/Shanghai   --snapshot   --zip
```
