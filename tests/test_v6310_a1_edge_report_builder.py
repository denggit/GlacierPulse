import json

from src.research.a1_edge.report_builder import A1EdgeReportBuilder


def _build(tmp_path, events, random_summary, hypothesis_summary, random_rows=None):
    return A1EdgeReportBuilder(min_group_sample_size=1, min_total_events=1).build(
        events=events,
        forward_metrics=[],
        random_baseline=random_rows if random_rows is not None else [{"baseline_id": f"r{i}"} for i in range(5 * max(1, len(events)))],
        random_summary=random_summary,
        hypothesis_results=[],
        hypothesis_summary=hypothesis_summary,
        out_dir=tmp_path,
    )


def test_empty_data_insufficient_and_markdown_sections(tmp_path):
    report = A1EdgeReportBuilder(min_group_sample_size=1, min_total_events=200).build([], [], [], [], [], [], tmp_path)
    text = (tmp_path / "a1_go_no_go_report.md").read_text(encoding="utf-8")
    assert report.decision == "INSUFFICIENT_SAMPLE"
    assert "Run Summary" in text and "Go / No-Go Decision" in text


def test_go_partial_and_no_go_decisions(tmp_path):
    events = [{"zone_id": "z1", "direction": "BUY", "event_ts": 1, "a1_reaction_type": "A"}]
    go = _build(
        tmp_path / "go",
        events,
        [{"dimension": "ALL", "edge_label": "STRONG_DIRECTIONAL_EDGE", "a1_sample_count": 1}],
        [{"valid_count": 1, "avg_realized_r_proxy": 0.2}],
    )
    assert go.decision == "A1_GO"
    partial = _build(
        tmp_path / "partial",
        events,
        [{"dimension": "a1_reaction_type", "edge_label": "STRONG_DIRECTIONAL_EDGE", "a1_sample_count": 1}],
        [],
    )
    assert partial.decision == "A1_PARTIAL_GO"
    no_go = _build(tmp_path / "nog", events, [{"dimension": "ALL", "edge_label": "NO_EDGE", "a1_sample_count": 1}], [])
    assert no_go.decision == "A1_NO_GO"
    assert (tmp_path / "go" / "a1_edge_summary.json").exists()


def test_report_builder_does_not_overwrite_dataset_summary_and_event_key_is_safe(tmp_path):
    dataset_summary = tmp_path / "a1_edge_dataset_summary.json"
    dataset_summary.write_text(json.dumps({"dataset": True}), encoding="utf-8")
    event = {"zone_id": "z1", "event_key": "z1|A|CONFIRMED|1.0", "direction": "BUY", "event_ts": 1, "a1_reaction_type": "A"}
    report = _build(
        tmp_path,
        [event],
        [{"dimension": "ALL", "edge_label": "NO_EDGE", "a1_sample_count": 1}],
        [],
    )
    assert report.decision == "A1_NO_GO"
    assert json.loads(dataset_summary.read_text(encoding="utf-8")) == {"dataset": True}
    summary = json.loads((tmp_path / "a1_edge_summary.json").read_text(encoding="utf-8"))
    assert summary["decision"] == "A1_NO_GO"
    assert "Run Summary" in (tmp_path / "a1_go_no_go_report.md").read_text(encoding="utf-8")
