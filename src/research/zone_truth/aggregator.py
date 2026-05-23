#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping

from src.research.a1_edge.schema import parse_float, parse_timestamp
from src.research.phase1_truth.analyzer import normalize_record

from .models import (
    MATCH_EXACT,
    MATCH_FUZZY,
    MATCH_UNMATCHED,
    SOURCE_CANDIDATE,
    SOURCE_REACTION,
    SOURCE_SYNTHETIC,
    ZoneReaction,
    ZoneTruthEvent,
    first_present,
    local_session,
    normalize_direction,
    score_warnings,
    truth_label,
    truth_score,
)


EXACT_ZONE_ID_FIELDS = ("zone_id", "assigned_zone_id", "frozen_zone_id", "matched_zone_id")
HARD_CAP_WARNINGS = {
    "negative_hidden_volume_cap": "negative_hidden_cap_count",
    "strong_negative_hidden_volume_cap": "strong_negative_hidden_cap_count",
    "negative_absorption_rate_cap": "negative_absorption_rate_cap_count",
    "spoofing_withdrawal_cap": "spoofing_withdrawal_cap_count",
    "spoofing_result_cap": "spoofing_result_cap_count",
    "excessive_book_reduction_cap": "excessive_book_reduction_cap_count",
}


class ZoneTruthAggregator:
    def __init__(
        self,
        price_tolerance_usdt: float = 1.5,
        time_tolerance_sec: float = 300.0,
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self.price_tolerance_usdt = float(price_tolerance_usdt)
        self.time_tolerance_sec = float(time_tolerance_sec)
        self.timezone = str(timezone)
        self.unmatched_pie_count = 0

    def aggregate(
        self,
        phase1_records: Iterable[Mapping[str, Any]],
        reaction_records: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        self.unmatched_pie_count = 0
        pies = self._dedupe_candidates(phase1_records)
        reactions = self._dedupe_reactions(reaction_records)
        by_zone: dict[str, dict[str, Any]] = {}
        reaction_by_zone = {r.zone_id: r for r in reactions if r.zone_id}

        for reaction in reactions:
            if not reaction.zone_id:
                continue
            by_zone.setdefault(
                reaction.zone_id,
                {"reaction": reaction, "pies": [], "method": MATCH_UNMATCHED, "score": 0.0, "source": SOURCE_REACTION},
            )

        for pie in pies:
            match_zone_id, method, score, source = self._match_pie(pie, reactions, reaction_by_zone)
            if match_zone_id:
                entry = by_zone.setdefault(
                    match_zone_id,
                    {"reaction": reaction_by_zone.get(match_zone_id), "pies": [], "method": method, "score": score, "source": source},
                )
                entry["pies"].append(pie)
                entry["method"] = self._merge_method(str(entry.get("method") or ""), method)
                entry["score"] = max(float(entry.get("score") or 0.0), float(score or 0.0))
                if entry.get("source") == SOURCE_REACTION and source != SOURCE_REACTION and not entry.get("reaction"):
                    entry["source"] = source
                continue

            result = str(pie.get("result") or "").upper()
            if result == "ICEBERG":
                synthetic_id = f"synthetic-{pie.get('event_key') or pie.get('event_id')}"
                by_zone[synthetic_id] = {
                    "reaction": None,
                    "pies": [pie],
                    "method": MATCH_UNMATCHED,
                    "score": 0.0,
                    "source": SOURCE_SYNTHETIC,
                }
            else:
                self.unmatched_pie_count += 1

        rows = [self._build_zone_event(zone_id, entry).to_dict() for zone_id, entry in by_zone.items()]
        rows.sort(key=lambda row: (float(row.get("first_seen_ts") or 0.0), str(row.get("zone_id") or "")))
        return rows

    def _dedupe_candidates(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        by_key: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for raw in records or []:
            if not isinstance(raw, Mapping):
                continue
            record = normalize_record(raw)
            record_type = str(record.get("record_type") or "")
            if record_type not in {"candidate_finalized", "candidate_settled"}:
                continue
            key = str(record.get("event_key") or record.get("event_id") or "")
            if not key:
                key = f"pie-{len(order) + 1}"
                record["event_key"] = key
            if key not in by_key:
                order.append(key)
                by_key[key] = record
                continue
            current_type = str(by_key[key].get("record_type") or "")
            if current_type != "candidate_finalized" and record_type == "candidate_finalized":
                by_key[key] = record
            elif current_type == record_type:
                merged = dict(by_key[key])
                merged.update({k: v for k, v in record.items() if v not in (None, "")})
                by_key[key] = merged
        return [by_key[key] for key in order]

    def _dedupe_reactions(self, records: Iterable[Mapping[str, Any]]) -> list[ZoneReaction]:
        by_zone: dict[str, ZoneReaction] = {}
        no_id: list[ZoneReaction] = []
        for raw in records or []:
            if not isinstance(raw, Mapping):
                continue
            reaction = ZoneReaction.from_mapping(raw)
            if not reaction.zone_id:
                no_id.append(reaction)
                continue
            current = by_zone.get(reaction.zone_id)
            if current is None:
                by_zone[reaction.zone_id] = reaction
                continue
            by_zone[reaction.zone_id] = self._merge_reaction(current, reaction)
        return list(by_zone.values()) + no_id

    def _merge_reaction(self, left: ZoneReaction, right: ZoneReaction) -> ZoneReaction:
        left_score = self._reaction_rank(left)
        right_score = self._reaction_rank(right)
        primary = right if right_score >= left_score else left
        secondary = left if primary is right else right
        lower_values = [x for x in (primary.zone_lower, secondary.zone_lower) if x > 0]
        upper_values = [x for x in (primary.zone_upper, secondary.zone_upper) if x > 0]
        primary.zone_lower = min(lower_values) if lower_values else 0.0
        primary.zone_upper = max(upper_values) if upper_values else 0.0
        primary.first_seen_ts = min([x for x in (primary.first_seen_ts, secondary.first_seen_ts) if x > 0] or [0.0])
        primary.last_seen_ts = max(primary.last_seen_ts, secondary.last_seen_ts)
        primary.frozen_ts = primary.frozen_ts or secondary.frozen_ts
        primary.reaction_event_ts = primary.reaction_event_ts or secondary.reaction_event_ts
        return primary

    @staticmethod
    def _reaction_rank(reaction: ZoneReaction) -> tuple[int, float]:
        reaction_type = reaction.a1_reaction_type.upper()
        if "CLEAN_HOLD" in reaction_type:
            rank = 3
        elif "FAILED" in reaction_type or "BREAKOUT" in reaction_type or "SWEEP_NO_RECLAIM" in reaction_type:
            rank = 2
        else:
            rank = 1
        return rank, reaction.reaction_event_ts or reaction.frozen_ts

    def _match_pie(
        self,
        pie: Mapping[str, Any],
        reactions: list[ZoneReaction],
        reaction_by_zone: Mapping[str, ZoneReaction],
    ) -> tuple[str, str, float, str]:
        for field in EXACT_ZONE_ID_FIELDS:
            zone_id = str(pie.get(field) or "").strip()
            if zone_id and zone_id in reaction_by_zone:
                return zone_id, MATCH_EXACT, 100.0, SOURCE_REACTION
        for field in EXACT_ZONE_ID_FIELDS:
            zone_id = str(pie.get(field) or "").strip()
            if zone_id:
                return zone_id, MATCH_EXACT, 100.0, SOURCE_CANDIDATE

        best_zone = ""
        best_score = 0.0
        for reaction in reactions:
            score = self._fuzzy_score(pie, reaction)
            if score > best_score:
                best_score = score
                best_zone = reaction.zone_id
        if best_zone and best_score >= 50.0:
            return best_zone, MATCH_FUZZY, best_score, SOURCE_REACTION
        return "", MATCH_UNMATCHED, best_score, ""

    def _fuzzy_score(self, pie: Mapping[str, Any], reaction: ZoneReaction) -> float:
        if not reaction.zone_id:
            return 0.0
        score = 0.0
        direction = normalize_direction(pie.get("direction"))
        if direction != "UNKNOWN" and direction == reaction.direction:
            score += 20.0
        else:
            return 0.0

        price = parse_float(first_present(pie, "settle_price", "trigger_price", "price"))
        if price > 0 and reaction.zone_lower - self.price_tolerance_usdt <= price <= reaction.zone_upper + self.price_tolerance_usdt:
            score += 30.0

        pie_lower = parse_float(first_present(pie, "assigned_zone_lower", "zone_lower"))
        pie_upper = parse_float(first_present(pie, "assigned_zone_upper", "zone_upper"))
        if pie_lower > pie_upper:
            pie_lower, pie_upper = pie_upper, pie_lower
        if pie_lower > 0 and pie_upper > 0:
            if pie_lower <= reaction.zone_upper + self.price_tolerance_usdt and reaction.zone_lower <= pie_upper + self.price_tolerance_usdt:
                score += 25.0

        time_distance = self._time_distance(pie, reaction)
        if 0 <= time_distance <= 60:
            score += 20.0
        elif 0 <= time_distance <= self.time_tolerance_sec:
            score += 10.0
        else:
            return 0.0

        if str(pie.get("result") or "").upper() == "ICEBERG":
            score += 10.0
        return min(score, 100.0)

    def _time_distance(self, pie: Mapping[str, Any], reaction: ZoneReaction) -> float:
        pie_ts = parse_timestamp(first_present(pie, "settle_ts", "settle_recv_ts", "trigger_ts"))
        reaction_times = [
            reaction.reaction_event_ts,
            reaction.frozen_ts,
            reaction.first_seen_ts,
            reaction.last_seen_ts,
        ]
        values = [abs(pie_ts - ts) for ts in reaction_times if pie_ts > 0 and ts > 0]
        return min(values) if values else float("inf")

    @staticmethod
    def _merge_method(current: str, new: str) -> str:
        order = {MATCH_UNMATCHED: 0, MATCH_FUZZY: 1, MATCH_EXACT: 2}
        return new if order.get(new, 0) > order.get(current, 0) else current

    def _build_zone_event(self, zone_id: str, entry: Mapping[str, Any]) -> ZoneTruthEvent:
        reaction = entry.get("reaction")
        pies = list(entry.get("pies") or [])
        if isinstance(reaction, ZoneReaction):
            base = self._base_from_reaction(zone_id, reaction)
        else:
            base = self._base_from_pies(zone_id, pies)
        base.zone_source = str(entry.get("source") or (SOURCE_REACTION if isinstance(reaction, ZoneReaction) else SOURCE_SYNTHETIC))
        base.zone_match_method = str(entry.get("method") or MATCH_UNMATCHED)
        base.match_score = round(float(entry.get("score") or 0.0), 4)
        self._attach_pie_stats(base, pies)
        if base.zone_source == SOURCE_SYNTHETIC:
            base.reaction_type = "SYNTHETIC"
            base.a1_reaction_type = "SYNTHETIC"
        return base

    def _base_from_reaction(self, zone_id: str, reaction: ZoneReaction) -> ZoneTruthEvent:
        lower, upper = reaction.zone_lower, reaction.zone_upper
        if lower > upper:
            lower, upper = upper, lower
        first_seen = reaction.first_seen_ts or reaction.frozen_ts or reaction.reaction_event_ts
        last_seen = reaction.last_seen_ts or reaction.reaction_event_ts or reaction.frozen_ts or first_seen
        event_ts = reaction.reaction_event_ts or reaction.frozen_ts or first_seen
        session = local_session(event_ts, self.timezone)
        return ZoneTruthEvent(
            zone_id=zone_id,
            zone_source=SOURCE_REACTION,
            symbol=reaction.symbol,
            direction=reaction.direction,
            zone_lower=lower,
            zone_upper=upper,
            zone_mid=(lower + upper) / 2.0 if lower > 0 and upper > 0 else 0.0,
            zone_width=max(0.0, upper - lower),
            first_seen_ts=first_seen,
            last_seen_ts=last_seen,
            frozen_ts=reaction.frozen_ts,
            reaction_event_ts=reaction.reaction_event_ts,
            local_time=session["local_time"],
            session_tag=session["session_tag"],
            is_weekend=session["is_weekend"],
            reaction_type=reaction.reaction_type,
            a1_reaction_type=reaction.a1_reaction_type,
            a1_reaction_reason=reaction.a1_reaction_reason,
            frozen_reason=reaction.frozen_reason,
            zone_state=reaction.zone_state,
        )

    def _base_from_pies(self, zone_id: str, pies: list[Mapping[str, Any]]) -> ZoneTruthEvent:
        best = self._best_pie(pies)
        lower_values = [parse_float(first_present(p, "assigned_zone_lower", "zone_lower")) for p in pies]
        upper_values = [parse_float(first_present(p, "assigned_zone_upper", "zone_upper")) for p in pies]
        lower_values = [x for x in lower_values if x > 0]
        upper_values = [x for x in upper_values if x > 0]
        lower = min(lower_values) if lower_values else 0.0
        upper = max(upper_values) if upper_values else 0.0
        ts_values = [parse_timestamp(first_present(p, "settle_ts", "trigger_ts")) for p in pies]
        ts_values = [x for x in ts_values if x > 0]
        event_ts = parse_timestamp(first_present(best, "settle_ts", "trigger_ts")) if best else 0.0
        session = local_session(event_ts, self.timezone)
        return ZoneTruthEvent(
            zone_id=zone_id,
            zone_source=SOURCE_SYNTHETIC,
            symbol=str(first_present(best, "symbol") or "") if best else "",
            direction=normalize_direction(first_present(best, "direction")) if best else "UNKNOWN",
            zone_lower=lower,
            zone_upper=upper,
            zone_mid=(lower + upper) / 2.0 if lower > 0 and upper > 0 else 0.0,
            zone_width=max(0.0, upper - lower),
            first_seen_ts=min(ts_values) if ts_values else 0.0,
            last_seen_ts=max(ts_values) if ts_values else 0.0,
            local_time=session["local_time"],
            session_tag=session["session_tag"],
            is_weekend=session["is_weekend"],
            zone_state=str(first_present(best, "zone_state", "assigned_zone_state") or "") if best else "",
        )

    def _attach_pie_stats(self, row: ZoneTruthEvent, pies: list[Mapping[str, Any]]) -> None:
        row.pie_count = len(pies)
        counts = Counter(str(p.get("result") or "UNKNOWN").upper() for p in pies)
        row.iceberg_pie_count = counts.get("ICEBERG", 0)
        row.ignore_pie_count = counts.get("IGNORE", 0)
        row.spoofing_pie_count = counts.get("SPOOFING", 0)
        row.cancel_pie_count = counts.get("CANCEL", 0)

        scores = [truth_score(p) for p in pies]
        if scores:
            row.truth_score_max = max(scores)
            row.truth_score_avg = sum(scores) / len(scores)
            row.truth_score_median = round(statistics.median(scores), 6)
            row.truth_score_min = min(scores)
            row.truth_ge50_count = sum(1 for s in scores if s >= 50)
            row.truth_ge65_count = sum(1 for s in scores if s >= 65)
            row.truth_ge80_count = sum(1 for s in scores if s >= 80)
        row.truth_not_iceberg_count = sum(1 for p in pies if truth_label(p).upper() == "NOT_ICEBERG")
        row.truth_insufficient_count = sum(1 for p in pies if truth_label(p).upper() == "INSUFFICIENT_POST_DATA")

        best = self._best_pie(pies)
        if best:
            row.best_pie_event_key = str(best.get("event_key") or best.get("event_id") or "")
            row.best_pie_ts = parse_timestamp(first_present(best, "settle_ts", "trigger_ts"))
            row.best_pie_price = parse_float(first_present(best, "settle_price", "trigger_price"))
            row.best_pie_truth_score = truth_score(best)
            row.best_pie_truth_label = truth_label(best)
            row.best_pie_quality = str(best.get("quality") or "")
            row.best_pie_behavior = str(best.get("behavior") or "")
            if not row.symbol:
                row.symbol = str(best.get("symbol") or "")
            if row.direction == "UNKNOWN":
                row.direction = normalize_direction(best.get("direction"))

        active = [parse_float(p.get("active_notional")) for p in pies]
        hidden = [parse_float(p.get("hidden_volume")) for p in pies]
        absorption = [parse_float(p.get("absorption_rate")) for p in pies]
        row.sum_active_notional = sum(active)
        row.max_active_notional = max(active) if active else 0.0
        row.avg_active_notional = round(sum(active) / len(active), 6) if active else 0.0
        row.sum_hidden_volume = sum(hidden)
        row.max_hidden_volume = max(hidden) if hidden else 0.0
        row.avg_hidden_volume = round(sum(hidden) / len(hidden), 6) if hidden else 0.0
        row.avg_absorption_rate = round(sum(absorption) / len(absorption), 6) if absorption else 0.0
        row.max_absorption_rate = max(absorption) if absorption else 0.0

        warning_counts: defaultdict[str, int] = defaultdict(int)
        for pie in pies:
            for warning in score_warnings(pie):
                if warning in HARD_CAP_WARNINGS:
                    warning_counts[warning] += 1
        for warning, field_name in HARD_CAP_WARNINGS.items():
            setattr(row, field_name, warning_counts.get(warning, 0))
        row.hard_cap_warning_count = sum(warning_counts.values())
        row.has_any_hard_cap = row.hard_cap_warning_count > 0
        row.a2_pre_pool_eligible = row.iceberg_pie_count >= 1
        row.a2_pre_pool_reason = "HAS_ICEBERG_PIE" if row.a2_pre_pool_eligible else "NO_ICEBERG_PIE"

    @staticmethod
    def _best_pie(pies: list[Mapping[str, Any]]) -> Mapping[str, Any]:
        if not pies:
            return {}
        return max(
            pies,
            key=lambda p: (
                truth_score(p),
                parse_float(p.get("active_notional")),
                parse_timestamp(first_present(p, "settle_ts", "trigger_ts")),
            ),
        )
