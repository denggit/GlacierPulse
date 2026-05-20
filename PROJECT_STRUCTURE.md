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
|   |   |-- iceberg/
|   |   |   |-- __init__.py
|   |   |   |-- outcome_evaluator.py
|   |   |   `-- zone_tracker.py
|   |   |-- __init__.py
|   |   |-- phase1_zone_engine.py
|   |   |-- phase2_orderflow_evaluator.py
|   |   |-- phase3_candidate_evaluator.py
|   |   |-- phase3_trade_outcome_evaluator.py
|   |   `-- virtual_position_manager.py
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
|   `-- test_v62_research_evaluator.py
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
