# 项目目录架构

> 此文件由 `tools/generate_project_tree.py` 自动生成，请勿手动编辑。
> 手动刷新：`python tools/generate_project_tree.py --output PROJECT_STRUCTURE.md`
> 安装提交前自动刷新：`python tools/generate_project_tree.py --install-hook --output PROJECT_STRUCTURE.md`

```text
GlacierPulse/
|-- config/
|   |-- run_profiles/
|   |   |-- a1_edge_capture.json
|   |   `-- shadow_research_safe.json
|   |-- __init__.py
|   |-- env_loader.py
|   |-- research_evaluator.py
|   `-- runtime_profile.json
|-- DOCS/
|   |-- local_okx_backtest_runbook.md
|   |-- PROJECT_PLAN.md
|   |-- RESEARCH_REPORTS_RUNBOOK.md
|   |-- V6.2_SHADOW_RUN_GUIDE.md
|   |-- V6.2总体定位
|   |-- V6.3.0_PREFLIGHT_RESTRUCTURE_PLAN.md
|   |-- V6.3.10.2_RUNTIME_PROFILE.md
|   |-- V6.3.10.3_TIMEZONE_GUARD.md
|   |-- V6.3.10.4_FEE_AWARE_AND_METADATA.md
|   |-- V6.3.10_A1_EDGE_VALIDATION.md
|   |-- V6.3.11.5_ZONE_TRUTH_AGGREGATION.md
|   |-- V6.3.11_A1_ICEBERG_TRUTH_SHADOW.md
|   |-- V6.3.8_HARD_RESTRUCTURE.md
|   |-- V6.3.9_A1_REACTION_COVERAGE.md
|   |-- 三个阶段.txt
|   |-- 冰山&流动性扫损（无VIP）.docx
|   `-- 冰山&流动性扫损（有VIP）.docx
|-- src/
|   |-- config/
|   |   |-- __init__.py
|   |   `-- runtime_profile_loader.py
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
|   |   |-- a1_dynamic_params/
|   |   |   |-- __init__.py
|   |   |   |-- previewer.py
|   |   |   `-- session.py
|   |   |-- a1_edge/
|   |   |   |-- __init__.py
|   |   |   |-- dataset_exporter.py
|   |   |   |-- forward_metrics.py
|   |   |   |-- hypothesis_simulator.py
|   |   |   |-- io_utils.py
|   |   |   |-- metadata.py
|   |   |   |-- random_baseline.py
|   |   |   |-- report_builder.py
|   |   |   `-- schema.py
|   |   |-- phase1_truth/
|   |   |   |-- __init__.py
|   |   |   |-- analyzer.py
|   |   |   |-- models.py
|   |   |   |-- recorder.py
|   |   |   |-- scorer.py
|   |   |   `-- tracker.py
|   |   |-- zone_truth/
|   |   |   |-- __init__.py
|   |   |   |-- aggregator.py
|   |   |   |-- analyzer.py
|   |   |   |-- forward.py
|   |   |   |-- market_context.py
|   |   |   `-- models.py
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
|   |   |   |-- reaction_event_recorder.py
|   |   |   |-- reaction_taxonomy.py
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
|   |-- test_backtest_local_data_book_cleaner.py
|   |-- test_backtest_local_data_cache.py
|   |-- test_backtest_local_data_coverage.py
|   |-- test_backtest_local_data_time_alignment.py
|   |-- test_market_context.py
|   |-- test_phase1_live.py
|   |-- test_v62_logging_controls.py
|   |-- test_v62_research_evaluator.py
|   |-- test_v63102_main_loads_runtime_profile_early.py
|   |-- test_v63102_runtime_profile_loader.py
|   |-- test_v63103_kline_timestamp_units.py
|   |-- test_v63103_kline_timezone.py
|   |-- test_v63104_fee_aware_metrics.py
|   |-- test_v63104_report_metadata.py
|   |-- test_v6310_a1_edge_dataset_exporter.py
|   |-- test_v6310_a1_edge_report_builder.py
|   |-- test_v6310_a1_edge_schema.py
|   |-- test_v6310_a1_forward_metrics.py
|   |-- test_v6310_a1_hypothesis_simulator.py
|   |-- test_v6310_a1_random_baseline.py
|   |-- test_v6310_analyze_a1_edge_cli.py
|   |-- test_v63115_zone_forward_metrics.py
|   |-- test_v63115_zone_truth_aggregation.py
|   |-- test_v63115_zone_truth_analyzer.py
|   |-- test_v63116_generate_research_reports.py
|   |-- test_v63116_zone_market_context.py
|   |-- test_v63117_enhanced_trend_context.py
|   |-- test_v6311_dynamic_param_preview.py
|   |-- test_v6311_phase1_candidate_recorder.py
|   |-- test_v6311_phase1_parameter_grid.py
|   |-- test_v6311_phase1_truth_scorer.py
|   |-- test_v638_a1_reaction_no_direct_virtual_by_default.py
|   |-- test_v638_candidate_log_a1_fields.py
|   |-- test_v638_event_schema_a1_fields.py
|   |-- test_v638_final_layout_imports.py
|   |-- test_v638_no_legacy_strategy_paths.py
|   |-- test_v638_research_report_bool_parsing.py
|   |-- test_v638_research_report_no_score_model.py
|   |-- test_v638_safety_real_trading_disabled.py
|   |-- test_v639_a1_reaction_coverage.py
|   |-- test_v639_a1_reaction_event_recorder.py
|   |-- test_v639_a1_reaction_taxonomy.py
|   |-- test_v639_candidate_counters.py
|   |-- test_v639_engine_drains_research_events.py
|   |-- test_v639_monitor_a1_reaction_counters.py
|   |-- test_v63_a1_research_report.py
|   |-- test_v63_a1_schema_adapters.py
|   |-- test_v63_a1_virtual_chain_switch.py
|   `-- test_v63_phase1_engine_semantic_imports.py
|-- tools/
|   |-- __init__.py
|   |-- analyze_a1_edge.py
|   |-- analyze_phase1_candidates.py
|   |-- analyze_zone_truth.py
|   |-- backtest_local_data.py
|   |-- download_okx_historical_data.py
|   |-- export_history_k.py
|   |-- generate_project_tree.py
|   |-- generate_research_reports.py
|   `-- parse_iceberg_log.py
|-- .gitattributes
|-- .gitignore
|-- delete_table.py
|-- main.py
|-- README.md
`-- requirements.txt
```
