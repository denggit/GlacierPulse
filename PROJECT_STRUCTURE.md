# 项目目录架构

> 此文件由 `tools/generate_project_tree.py` 自动生成，请勿手动编辑。
> 手动刷新：`python tools/generate_project_tree.py --output PROJECT_STRUCTURE.md`
> 安装提交前自动刷新：`python tools/generate_project_tree.py --install-hook --output PROJECT_STRUCTURE.md`

```text
GlacierPulse/
|-- config/
|   |-- __init__.py
|   |-- env_loader.py
|   `-- research_evaluator.py
|-- DOCS/
|   |-- PROJECT_PLAN.md
|   |-- V6.2_SHADOW_RUN_GUIDE.md
|   |-- V6.2总体定位
|   |-- V6.3.0_PREFLIGHT_RESTRUCTURE_PLAN.md
|   |-- 三个阶段.txt
|   |-- 冰山&流动性扫损（无VIP）.docx
|   `-- 冰山&流动性扫损（有VIP）.docx
|-- src/
|   |-- context/
|   |   |-- __init__.py
|   |   `-- market_context.py
|   |-- data_feed/
|   |   |-- __init__.py
|   |   |-- okx_books_stream.py
|   |   |-- okx_loader.py
|   |   `-- okx_stream.py
|   |-- detectors/
|   |   |-- __init__.py
|   |   |-- iceberg_detector.py
|   |   `-- sweep_detector.py
|   |-- execution/
|   |   |-- __init__.py
|   |   `-- trader.py
|   |-- monitoring/
|   |   |-- __init__.py
|   |   `-- research_runtime_monitor.py
|   |-- research/
|   |   |-- __init__.py
|   |   `-- a1_frozen_metadata.py
|   |-- strategy/
|   |   |-- a1_absorption/
|   |   |   |-- __init__.py
|   |   |   |-- engine.py
|   |   |   |-- event_schema.py
|   |   |   |-- metadata.py
|   |   |   |-- outcome_evaluator.py
|   |   |   |-- pending_event_manager.py
|   |   |   |-- reaction_evaluator.py
|   |   |   |-- research_report.py
|   |   |   `-- zone_tracker.py
|   |   |-- a2_accumulation/
|   |   |   |-- __init__.py
|   |   |   |-- accumulation_evaluator.py
|   |   |   |-- auction_balance_tracker.py
|   |   |   |-- compression_detector.py
|   |   |   |-- liquidity_vacuum_detector.py
|   |   |   `-- README.md
|   |   |-- a3_aggression/
|   |   |   |-- __init__.py
|   |   |   |-- breakout_validator.py
|   |   |   |-- ignition_evaluator.py
|   |   |   |-- imbalance_detector.py
|   |   |   |-- momentum_escape_detector.py
|   |   |   `-- README.md
|   |   |-- execution_research/
|   |   |   |-- __init__.py
|   |   |   |-- candidate_risk_evaluator.py
|   |   |   |-- trade_outcome_evaluator.py
|   |   |   `-- virtual_position_manager.py
|   |   |-- iceberg/
|   |   |-- triplea/
|   |   |   `-- __init__.py
|   |   `-- __init__.py
|   |-- utils/
|   |   |-- __init__.py
|   |   |-- email_sender.py
|   |   |-- log.py
|   |   `-- log_noise.py
|   `-- __init__.py
|-- tests/
|   |-- __init__.py
|   |-- test_market_context.py
|   |-- test_phase1_live.py
|   |-- test_v62_logging_controls.py
|   |-- test_v62_research_evaluator.py
|   |-- test_v638_a1_reaction_no_direct_virtual_by_default.py
|   |-- test_v638_final_layout_imports.py
|   |-- test_v638_no_legacy_strategy_paths.py
|   |-- test_v638_research_report_no_score_model.py
|   |-- test_v638_safety_real_trading_disabled.py
|   |-- test_v63_a1_research_report.py
|   |-- test_v63_a1_schema_adapters.py
|   |-- test_v63_a1_virtual_chain_switch.py
|   `-- test_v63_phase1_engine_semantic_imports.py
|-- tools/
|   |-- __init__.py
|   |-- export_history_k.py
|   |-- generate_project_tree.py
|   `-- parse_iceberg_log.py
|-- .gitattributes
|-- .gitignore
|-- delete_table.py
|-- main.py
|-- README.md
`-- requirements.txt
```
