#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.a1_absorption.schema import (
    A1AbsorptionContext,
    A1OutcomeRecord,
    A1ReactionSnapshot,
)
from src.strategy.a1_absorption.score_model import A1UnifiedScoreModel


def test_a1_absorption_context_from_public_zone_legacy_keys():
    zone = {
        "zone_id": "iz-1",
        "direction": "BUY",
        "is_frozen": True,
        "frozen_ts": 100.0,
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "DISCOVERED",
        "frozen_event_id": "evt-1",
        "frozen_zone_lower": 3000.0,
        "frozen_zone_upper": 3001.5,
        "live_zone_lower": 2999.8,
        "live_zone_upper": 3001.8,
        "event_count": 3,
        "iceberg_count": 2,
        "high_count": 1,
        "medium_count": 1,
        "low_count": 1,
        "positive_score": 5.0,
        "negative_score": 1.0,
        "net_score": 4.0,
    }

    ctx = A1AbsorptionContext.from_public_zone(zone)
    assert ctx.frozen_low == 3000.0
    assert ctx.frozen_high == 3001.5
    assert ctx.live_low == 2999.8
    assert ctx.live_high == 3001.8
    assert ctx.frozen_reason == "HIGH_ICEBERG"
    assert ctx.iceberg_count == 2
    assert ctx.net_score == 4.0

    as_mapping = ctx.to_dict()
    assert "frozen_low" in as_mapping
    assert "frozen_zone_lower" not in as_mapping


def test_a1_reaction_snapshot_from_phase2_confirmed_event():
    event = {
        "zone_id": "iz-1",
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "confirmed_ts": 103.0,
        "last_price": 3000.3,
        "frozen_low": 3000.0,
        "frozen_high": 3001.5,
        "live_low": 3000.0,
        "live_high": 3001.5,
        "sweep_extreme": 2999.0,
        "suggested_stop": 2998.5,
        "risk_to_stop_u": 1.8,
        "risk_to_stop_pct": 0.0006,
        "phase2_total_score": 0.83,
        "absorption_score": 0.9,
        "pressure_decay_score": 0.8,
        "reclaim_score": 0.9,
        "retest_score": 0.85,
        "book_absorption_score": 0.4,
        "relevant_book_depth_available": True,
        "reload_score": 0.3,
        "has_swept_boundary": True,
        "has_absorbed_after_sweep": True,
        "has_reclaimed_boundary": True,
        "has_retested_inside_zone": True,
    }

    snapshot = A1ReactionSnapshot.from_phase2_confirmed_event(event)
    assert snapshot.reaction_type == "SWEEP_RECLAIM"
    assert snapshot.legacy_phase2_type == "SWEEP_RECLAIM"
    assert snapshot.reaction_total_score == 0.83
    assert snapshot.legacy_phase2_total_score == 0.83
    assert snapshot.state == "PHASE2_CONFIRMED"
    assert snapshot.relevant_book_depth_available is True

    as_mapping = snapshot.to_dict()
    assert "reaction_type" in as_mapping
    assert "legacy_phase2_type" in as_mapping


def test_a1_outcome_record_from_mapping():
    record = {
        "zone_id": "iz-1",
        "direction": "BUY",
        "outcome_label": "CLEAN_HOLD",
        "close_reason": "TAKE_PROFIT",
        "phase2_type": "SWEEP_RECLAIM",
        "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
        "realized_pnl_u": 12.5,
        "realized_r_multiple": 1.5,
        "mfe_u": 5.0,
        "mae_u": -1.0,
        "frozen_reason": "STATE_RELOADING",
        "frozen_state": "RELOADING",
        "iceberg_count": 3,
        "high_count": 1,
        "medium_count": 2,
        "low_count": 0,
        "net_score": 4.3,
    }

    outcome = A1OutcomeRecord.from_mapping(record)
    assert outcome.outcome_label == "CLEAN_HOLD"
    assert outcome.reaction_type == "SWEEP_RECLAIM"
    assert outcome.legacy_phase2_type == "SWEEP_RECLAIM"
    assert outcome.candidate_type == "SWEEP_RECLAIM_RETEST_ENTRY"
    assert outcome.realized_r_multiple == 1.5
    assert outcome.iceberg_count == 3


def test_missing_fields_do_not_raise_and_default_zone_id_empty():
    ctx = A1AbsorptionContext.from_public_zone({})
    snapshot = A1ReactionSnapshot.from_phase2_confirmed_event({})
    outcome = A1OutcomeRecord.from_mapping({})

    assert ctx.zone_id == ""
    assert snapshot.zone_id == ""
    assert outcome.zone_id == ""


def test_string_number_fields_convert_safely():
    zone = {
        "zone_id": "iz-str",
        "frozen_zone_lower": "3000.1",
        "iceberg_count": "2",
        "net_score": "3.5",
    }

    ctx = A1AbsorptionContext.from_public_zone(zone)
    assert ctx.frozen_low == 3000.1
    assert ctx.iceberg_count == 2
    assert ctx.net_score == 3.5


def test_a1_absorption_context_boundary_fields_fallback_when_new_keys_are_none():
    zone = {
        "zone_id": "iz-boundary-fallback",
        "frozen_low": None,
        "frozen_high": None,
        "live_low": None,
        "live_high": None,
        "frozen_zone_lower": "3000.1",
        "frozen_zone_upper": "3001.2",
        "live_zone_lower": "2999.9",
        "live_zone_upper": "3001.5",
    }

    ctx = A1AbsorptionContext.from_public_zone(zone)

    assert ctx.frozen_low == 3000.1
    assert ctx.frozen_high == 3001.2
    assert ctx.live_low == 2999.9
    assert ctx.live_high == 3001.5


def test_a1_outcome_record_from_iceberg_outcome_fields():
    record = {
        "zone_id": "iz-iceberg",
        "direction": "BUY",
        "label": "SWEPT_THEN_RECLAIMED",
        "mfe": 4.2,
        "mae": -1.3,
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "DISCOVERED",
        "iceberg_count": 2,
        "high_count": 1,
        "medium_count": 1,
        "low_count": 0,
        "net_score": 3.5,
    }

    outcome = A1OutcomeRecord.from_mapping(record)
    assert outcome.outcome_label == "SWEPT_THEN_RECLAIMED"
    assert outcome.mfe_u == 4.2
    assert outcome.mae_u == -1.3
    assert outcome.reaction_type == ""
    assert outcome.legacy_phase2_type == ""


def test_a1_outcome_record_from_virtual_position_outcome_fields():
    record = {
        "zone_id": "iz-vp",
        "direction": "LONG",
        "outcome_bucket": "WIN",
        "close_reason": "TAKE_PROFIT_R_MULTIPLE",
        "phase2_type": "SWEEP_RECLAIM",
        "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
        "realized_pnl_u": 15.0,
        "realized_r_multiple": 1.5,
        "max_favorable_u": 18.0,
        "max_adverse_u": -2.0,
        "frozen_reason": "STATE_RELOADING",
        "frozen_state": "RELOADING",
        "iceberg_count": 3,
        "high_count": 1,
        "medium_count": 2,
        "low_count": 0,
        "net_score": 4.3,
    }

    outcome = A1OutcomeRecord.from_mapping(record)
    assert outcome.outcome_label == "WIN"
    assert outcome.reaction_type == "SWEEP_RECLAIM"
    assert outcome.legacy_phase2_type == "SWEEP_RECLAIM"
    assert outcome.candidate_type == "SWEEP_RECLAIM_RETEST_ENTRY"
    assert outcome.mfe_u == 18.0
    assert outcome.mae_u == -2.0
    assert outcome.realized_r_multiple == 1.5


def test_a1_outcome_record_field_precedence():
    record = {
        "mfe_u": 10.0,
        "mfe": 4.0,
        "max_favorable_u": 6.0,
        "mae_u": -5.0,
        "mae": -2.0,
        "max_adverse_u": -3.0,
        "outcome_label": "PRIMARY",
        "label": "SECONDARY",
        "outcome_bucket": "THIRD",
    }

    outcome = A1OutcomeRecord.from_mapping(record)
    assert outcome.mfe_u == 10.0
    assert outcome.mae_u == -5.0
    assert outcome.outcome_label == "PRIMARY"


def test_a1_outcome_record_field_precedence_skips_none_values():
    record = {
        "outcome_label": None,
        "label": "LABEL_FALLBACK",
        "outcome_bucket": "BUCKET_FALLBACK",
        "reaction_type": None,
        "phase2_type": "CLEAN_HOLD",
        "mfe_u": None,
        "mfe": 4.0,
        "max_favorable_u": 6.0,
        "mae_u": None,
        "mae": -2.0,
        "max_adverse_u": -3.0,
    }

    outcome = A1OutcomeRecord.from_mapping(record)

    assert outcome.outcome_label == "LABEL_FALLBACK"
    assert outcome.reaction_type == "CLEAN_HOLD"
    assert outcome.legacy_phase2_type == "CLEAN_HOLD"
    assert outcome.mfe_u == 4.0
    assert outcome.mae_u == -2.0


def test_as_int_supports_float_like_string_values():
    zone = {
        "zone_id": "iz-float-int",
        "iceberg_count": "2.0",
        "event_count": 3.0,
    }

    ctx = A1AbsorptionContext.from_public_zone(zone)
    assert ctx.iceberg_count == 2
    assert ctx.event_count == 3


def test_a1_score_model_accepts_schema_adapter_outputs():
    absorption = A1AbsorptionContext.from_public_zone(
        {
            "zone_id": "iz-integ",
            "direction": "BUY",
            "frozen_reason": "HIGH_ICEBERG",
            "frozen_state": "DISCOVERED",
            "iceberg_count": 2,
            "high_count": 1,
            "medium_count": 1,
            "positive_score": 4,
            "negative_score": 1,
            "net_score": 3,
        }
    )
    reaction = A1ReactionSnapshot.from_phase2_confirmed_event(
        {
            "zone_id": "iz-integ",
            "direction": "BUY",
            "phase2_type": "SWEEP_RECLAIM",
            "phase2_total_score": 0.7,
            "absorption_score": 0.7,
            "reclaim_score": 0.6,
            "retest_score": 0.6,
            "reload_score": 0.2,
            "risk_to_stop_pct": 0.002,
            "relevant_book_depth_available": True,
        }
    )
    record = A1UnifiedScoreModel.score(absorption, reaction)
    assert record.zone_id == absorption.zone_id == reaction.zone_id
    assert record.reaction_type == reaction.reaction_type
    assert record.legacy_phase2_type == reaction.legacy_phase2_type
