#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime
from statistics import median
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from src.research.a1_edge.io_utils import normalize_klines
from src.research.a1_edge.schema import parse_bool, parse_float


DEFAULT_WINDOWS_SEC = (900, 3600, 14400)
A3_PREVIEW_BREAKOUT_WINDOW_SEC = 3600
WINDOW_LABELS = {
    900: "15m",
    3600: "1h",
    14400: "4h",
}
ROUNDTRIP_FEE_PCT = 0.001


class ZoneForwardMetricsCalculator:
    def __init__(self, windows_sec: Iterable[int] | None = None, kline_timezone: str = "Asia/Shanghai") -> None:
        self.windows_sec = [int(w) for w in (windows_sec or DEFAULT_WINDOWS_SEC)]
        self.kline_timezone = kline_timezone

    def attach_forward_metrics(
        self,
        zone_rows: Iterable[Mapping[str, Any]],
        kline_rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        bars = normalize_klines(kline_rows, kline_timezone=self.kline_timezone)
        return [self.attach_to_row(row, bars) for row in zone_rows or []]

    def attach_to_row(self, row: Mapping[str, Any], bars: list[dict[str, float]]) -> dict[str, Any]:
        result = dict(row)
        anchor_ts, anchor_source = _resolve_event_ts(result)
        entry_price, entry_source = _resolve_entry_price(result)
        reaction_event_ts = parse_float(result.get("reaction_event_ts"))
        outside_kline = _outside_kline_range(reaction_event_ts, bars) if reaction_event_ts > 0 else False
        result["reaction_event_ts_valid"] = parse_bool(
            result.get("reaction_event_ts_valid"),
            default=reaction_event_ts > 0,
        )
        result["reaction_event_ts_outside_kline_range"] = outside_kline
        result["forward_anchor_ts"] = anchor_ts
        result["forward_anchor_source"] = anchor_source
        result["forward_anchor_local_time"] = _local_time(anchor_ts, self.kline_timezone)
        result["forward_entry_price"] = entry_price
        result["forward_entry_price_source"] = entry_source
        for window_sec in self.windows_sec:
            label = WINDOW_LABELS.get(int(window_sec), f"{int(window_sec)}s")
            metric = compute_zone_forward_metric(result, bars, int(window_sec))
            result[f"mfe_{label}_u"] = metric["mfe_u"]
            result[f"mae_{label}_u"] = metric["mae_u"]
            result[f"end_{label}_u"] = metric["end_u"]
            result[f"is_complete_{label}"] = metric["is_complete"]

        result.update(compute_a3_preview_breakout(result, bars))
        result.update(compute_a2_fee_metrics(result))
        result.update(compute_a2_pre_ignition_metrics(result, bars))
        result["a3_preview_net_mfe_1h_bucket"] = bucket_a3_net_mfe_1h(result)
        result["a3_preview_realized_r_proxy_1h_bucket"] = bucket_a3_realized_r_1h(result)
        return result


def compute_zone_forward_metric(zone: Mapping[str, Any], bars: list[dict[str, float]], window_sec: int) -> dict[str, Any]:
    event_ts, _anchor_source = _resolve_event_ts(zone)
    entry, _entry_source = _resolve_entry_price(zone)
    direction = str(zone.get("direction") or "").upper()
    if not bars or event_ts <= 0 or entry <= 0 or direction not in {"BUY", "SELL"}:
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}
    if _outside_kline_range(event_ts, bars):
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    timestamps = [float(bar["timestamp"]) for bar in bars]
    start_idx = bisect_right(timestamps, event_ts)
    if start_idx >= len(bars):
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    interval = infer_bar_interval_sec(bars)
    expected_count = max(1, int(round(float(window_sec) / max(interval, 1.0))))
    start_ts = float(bars[start_idx]["timestamp"])
    end_ts = start_ts + float(window_sec)
    future = [bar for bar in bars[start_idx:] if float(bar["timestamp"]) < end_ts]
    if len(future) > expected_count:
        future = future[:expected_count]
    is_complete = len(future) >= expected_count
    if not future:
        return {"mfe_u": 0.0, "mae_u": 0.0, "end_u": 0.0, "is_complete": False, "future_bar_count": 0}

    high = max(float(bar["high"]) for bar in future)
    low = min(float(bar["low"]) for bar in future)
    close = float(future[-1]["close"])
    if direction == "BUY":
        mfe = max(0.0, high - entry)
        mae = min(0.0, low - entry)
        end = close - entry
    else:
        mfe = max(0.0, entry - low)
        mae = min(0.0, entry - high)
        end = entry - close
    return {
        "mfe_u": round(mfe, 8),
        "mae_u": round(mae, 8),
        "end_u": round(end, 8),
        "is_complete": bool(is_complete),
        "future_bar_count": len(future),
    }


def infer_bar_interval_sec(bars: list[dict[str, float]], default: float = 60.0) -> float:
    diffs = [
        float(bars[i]["timestamp"]) - float(bars[i - 1]["timestamp"])
        for i in range(1, len(bars))
        if float(bars[i]["timestamp"]) > float(bars[i - 1]["timestamp"])
    ]
    return float(median(diffs)) if diffs else float(default)



def _risk_u(zone: Mapping[str, Any]) -> float:
    zone_low = parse_float(zone.get("zone_lower"))
    zone_high = parse_float(zone.get("zone_upper"))
    return max(parse_float(zone.get("zone_width")), abs(zone_high-zone_low), 1.0)


def _latency_bucket(flag: bool, latency_sec: float) -> str:
    if not flag:
        return "NO_IGNITION"
    if latency_sec <= 60:
        return "FAST_IGNITION"
    if latency_sec <= 900:
        return "NORMAL_IGNITION"
    if latency_sec <= 3600:
        return "LATE_IGNITION"
    return "OUT_OF_WINDOW"


def _first_hit(direction: str, bars: list[dict[str,float]], entry: float, risk_u: float, fee_share_r: float) -> tuple[float,str,bool]:
    if not bars:
        return -fee_share_r, "CLOSE_EXIT", False
    target = entry + risk_u if direction=="BUY" else entry-risk_u
    stop = entry - risk_u if direction=="BUY" else entry+risk_u
    first_hit=False
    for bar in bars:
        lo=float(bar["low"]); hi=float(bar["high"])
        if direction=="BUY":
            hit_stop = lo<=stop
            hit_target = hi>=target
        else:
            hit_stop = hi>=stop
            hit_target = lo<=target
        if hit_stop and hit_target:
            return -1.0-fee_share_r, "AMBIGUOUS_BOTH_HIT", True
        if hit_target:
            return 1.0-fee_share_r, "TARGET_1R_FIRST", True
        if hit_stop:
            return -1.0-fee_share_r, "STOP_1R_FIRST", True
    close=float(bars[-1]["close"])
    close_r=(close-entry)/risk_u if direction=="BUY" else (entry-close)/risk_u
    return close_r-fee_share_r, "CLOSE_EXIT", False


def _volume_value(bar: Mapping[str, Any]) -> float:
    for k in ("volume","vol","Volume","base_volume"):
        v=parse_float(bar.get(k))
        if v>0:
            return v
    return 0.0


def compute_a3_preview_breakout(zone: Mapping[str, Any], bars: list[dict[str, float]]) -> dict[str, Any]:
    """Offline A3 watch preview only; not a runtime signal."""
    default = {
        "a3_preview_breakout_raw_flag": False,
        "a3_preview_breakout_raw_latency_sec": 0.0,
        "a3_preview_breakout_direction": "UNKNOWN",
        "a3_preview_breakout_threshold_u": 0.0,
        "a3_preview_breakout_price": 0.0,
        "a3_preview_max_extension_15m_u": 0.0,
        "a3_preview_max_extension_1h_u": 0.0,
        "a3_preview_latency_bucket": "NO_IGNITION",
        "a3_preview_entry_ts": 0.0, "a3_preview_entry_price": 0.0, "a3_preview_entry_time_utc": "",
        "a3_preview_risk_u": 0.0, "a3_preview_fee_u": 0.0, "a3_preview_fee_share_r": 0.0,
        "a3_preview_net_mfe_15m_r": 0.0, "a3_preview_net_mae_15m_r": 0.0,
        "a3_preview_net_mfe_1h_r": 0.0, "a3_preview_net_mae_1h_r": 0.0,
        "a3_preview_first_hit_1r_15m": False, "a3_preview_first_hit_1r_1h": False,
        "a3_preview_realized_r_proxy_15m": 0.0, "a3_preview_realized_r_proxy_1h": 0.0,
        "a3_preview_realized_outcome_15m": "NO_BREAKOUT", "a3_preview_realized_outcome_1h": "NO_BREAKOUT",
        "a3_preview_breakout_volume": 0.0, "a3_preview_volume_median_20": 0.0, "a3_preview_volume_boost": 0.0,
        "a3_preview_body_strength": 0.0, "a3_preview_breakout_strength_r": 0.0,
        "a3_preview_persistence_3m_flag": False, "a3_preview_persistence_5m_flag": False,
        "a3_preview_no_quick_return_3m_flag": False, "a3_preview_no_quick_return_5m_flag": False,
        "a3_preview_ignition_quality": "NO_IGNITION",
    }
    anchor_ts, _anchor_source = _resolve_event_ts(zone)
    direction = str(zone.get("direction") or "").upper()
    zone_low = parse_float(zone.get("zone_lower"))
    zone_high = parse_float(zone.get("zone_upper"))
    if zone_low > zone_high:
        zone_low, zone_high = zone_high, zone_low
    zone_width = max(
        parse_float(zone.get("zone_width")),
        zone_high - zone_low,
        1.0,
    )
    if not bars or anchor_ts <= 0 or direction not in {"BUY", "SELL"} or zone_low <= 0 or zone_high <= 0:
        return default
    if anchor_ts < float(bars[0]["timestamp"]) or anchor_ts > float(bars[-1]["timestamp"]):
        return default

    threshold_u = max(zone_width * 0.5, 1.0)
    timestamps = [float(bar["timestamp"]) for bar in bars]
    start_idx = bisect_right(timestamps, anchor_ts)
    if start_idx >= len(bars):
        return {
            **default,
            "a3_preview_breakout_direction": direction,
            "a3_preview_breakout_threshold_u": round(threshold_u, 8),
        }

    breakout_price = zone_high + threshold_u if direction == "BUY" else zone_low - threshold_u
    future = bars[start_idx:]
    future_window = [
        bar for bar in future
        if float(bar["timestamp"]) <= anchor_ts + A3_PREVIEW_BREAKOUT_WINDOW_SEC
    ]
    breakout_ts = 0.0
    for bar in future_window:
        if direction == "BUY" and float(bar["high"]) >= breakout_price:
            breakout_ts = float(bar["timestamp"])
            break
        if direction == "SELL" and float(bar["low"]) <= breakout_price:
            breakout_ts = float(bar["timestamp"])
            break

    out = {
        "a3_preview_breakout_raw_flag": breakout_ts > 0,
        "a3_preview_breakout_raw_latency_sec": round(max(0.0, breakout_ts - anchor_ts), 6) if breakout_ts > 0 else 0.0,
        "a3_preview_breakout_direction": direction,
        "a3_preview_breakout_threshold_u": round(threshold_u, 8),
        "a3_preview_breakout_price": round(breakout_price, 8),
        "a3_preview_max_extension_15m_u": _max_zone_extension(zone_low, zone_high, direction, future, anchor_ts, 900),
        "a3_preview_max_extension_1h_u": _max_zone_extension(zone_low, zone_high, direction, future, anchor_ts, 3600),
    }
    out["a3_preview_latency_bucket"] = _latency_bucket(out["a3_preview_breakout_raw_flag"], out["a3_preview_breakout_raw_latency_sec"])
    if not out["a3_preview_breakout_raw_flag"]:
        out["a3_preview_ignition_quality"] = "NO_IGNITION"
        return {**default, **out}
    entry_ts = breakout_ts
    entry_price = breakout_price
    risk_u = _risk_u(zone)
    fee_u = entry_price * ROUNDTRIP_FEE_PCT
    fee_share_r = fee_u / risk_u
    post = [b for b in future if float(b["timestamp"]) >= entry_ts]
    w15 = [b for b in post if float(b["timestamp"]) <= entry_ts + 900]
    w1h = [b for b in post if float(b["timestamp"]) <= entry_ts + 3600]
    def mfe_mae(window):
        if not window: return (0.0,0.0)
        if direction=="BUY":
            return max(float(b["high"]) for b in window)-entry_price, min(float(b["low"]) for b in window)-entry_price
        return entry_price-min(float(b["low"]) for b in window), entry_price-max(float(b["high"]) for b in window)
    mfe15,mae15=mfe_mae(w15); mfe1h,mae1h=mfe_mae(w1h)
    r15,o15,h15=_first_hit(direction,w15,entry_price,risk_u,fee_share_r)
    r1h,o1h,h1h=_first_hit(direction,w1h,entry_price,risk_u,fee_share_r)
    bidx = next((i for i,b in enumerate(bars) if float(b["timestamp"])==entry_ts), -1)
    bbar = bars[bidx] if bidx>=0 else {}
    prev20 = bars[max(0,bidx-20):bidx] if bidx>0 else []
    med20 = median([_volume_value(b) for b in prev20 if _volume_value(b)>0]) if len(prev20)>=20 else 0.0
    bvol = _volume_value(bbar)
    vol_boost = (bvol/med20) if med20>0 else 0.0
    span = max(parse_float(bbar.get("high"))-parse_float(bbar.get("low")),1e-9)
    body = abs(parse_float(bbar.get("close"))-parse_float(bbar.get("open")))/span
    breakout_strength = (max(float(b["high"]) for b in w15)-entry_price)/risk_u if (direction=="BUY" and w15) else ((entry_price-min(float(b["low"]) for b in w15))/risk_u if w15 else 0.0)
    p3 = (parse_float(post[3]["close"])>=entry_price) if direction=="BUY" and len(post)>3 else ((parse_float(post[3]["close"])<=entry_price) if len(post)>3 else False)
    p5 = (parse_float(post[5]["close"])>=entry_price) if direction=="BUY" and len(post)>5 else ((parse_float(post[5]["close"])<=entry_price) if len(post)>5 else False)
    q3 = (min(float(b["low"]) for b in post[:3])>=zone_high) if direction=="BUY" and len(post)>=1 else ((max(float(b["high"]) for b in post[:3])<=zone_low) if len(post)>=1 else False)
    q5 = (min(float(b["low"]) for b in post[:5])>=zone_high) if direction=="BUY" and len(post)>=1 else ((max(float(b["high"]) for b in post[:5])<=zone_low) if len(post)>=1 else False)
    net_mfe15 = mfe15/risk_u-fee_share_r; net_mae15 = mae15/risk_u-fee_share_r
    if net_mfe15>=1.0 and net_mae15>-1.0 and p3 and q3 and out["a3_preview_latency_bucket"] in {"FAST_IGNITION","NORMAL_IGNITION"}: iq="STRONG_IGNITION"
    elif net_mfe15>=0.5 and net_mae15>-1.5 and p3 and out["a3_preview_latency_bucket"]!="OUT_OF_WINDOW": iq="MEDIUM_IGNITION"
    else: iq="WEAK_IGNITION"
    out.update({"a3_preview_entry_ts":entry_ts,"a3_preview_entry_price":round(entry_price,8),"a3_preview_entry_time_utc":_local_time(entry_ts,'UTC'),"a3_preview_risk_u":round(risk_u,8),"a3_preview_fee_u":round(fee_u,8),"a3_preview_fee_share_r":round(fee_share_r,8),"a3_preview_net_mfe_15m_r":round(net_mfe15,8),"a3_preview_net_mae_15m_r":round(net_mae15,8),"a3_preview_net_mfe_1h_r":round(mfe1h/risk_u-fee_share_r,8),"a3_preview_net_mae_1h_r":round(mae1h/risk_u-fee_share_r,8),"a3_preview_first_hit_1r_15m":h15,"a3_preview_first_hit_1r_1h":h1h,"a3_preview_realized_r_proxy_15m":round(r15,8),"a3_preview_realized_r_proxy_1h":round(r1h,8),"a3_preview_realized_outcome_15m":o15,"a3_preview_realized_outcome_1h":o1h,"a3_preview_breakout_volume":round(bvol,8),"a3_preview_volume_median_20":round(med20,8),"a3_preview_volume_boost":round(vol_boost,8),"a3_preview_body_strength":round(body,8),"a3_preview_breakout_strength_r":round(breakout_strength,8),"a3_preview_persistence_3m_flag":bool(p3),"a3_preview_persistence_5m_flag":bool(p5),"a3_preview_no_quick_return_3m_flag":bool(q3),"a3_preview_no_quick_return_5m_flag":bool(q5),"a3_preview_ignition_quality":iq})
    return {**default, **out}


def _max_zone_extension(
    zone_low: float,
    zone_high: float,
    direction: str,
    future: list[dict[str, float]],
    anchor_ts: float,
    window_sec: int,
) -> float:
    window = [bar for bar in future if float(bar["timestamp"]) <= anchor_ts + float(window_sec)]
    if not window:
        return 0.0
    if direction == "BUY":
        return round(max(0.0, max(float(bar["high"]) for bar in window) - zone_high), 8)
    if direction == "SELL":
        return round(max(0.0, zone_low - min(float(bar["low"]) for bar in window)), 8)
    return 0.0


def _resolve_event_ts(zone: Mapping[str, Any]) -> tuple[float, str]:
    for name in ("reaction_event_ts", "frozen_ts", "best_pie_ts", "first_seen_ts"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value, name
    return 0.0, "none"


def _resolve_entry_price(zone: Mapping[str, Any]) -> tuple[float, str]:
    for name in ("zone_mid", "best_pie_price", "settle_price"):
        value = parse_float(zone.get(name))
        if value > 0:
            return value, name
    return 0.0, "none"


def _local_time(ts: float, timezone: str) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(float(ts), tz=ZoneInfo(str(timezone))).isoformat()


def _outside_kline_range(ts: float, bars: list[dict[str, float]]) -> bool:
    if not bars or ts <= 0:
        return False
    return ts < float(bars[0]["timestamp"]) or ts > float(bars[-1]["timestamp"])


def compute_a2_fee_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    zone_low=parse_float(row.get("zone_lower")); zone_high=parse_float(row.get("zone_upper"))
    risk_u=max(parse_float(row.get("zone_width")), abs(zone_high-zone_low), 1.0)
    ref=0.0
    for n in ("trigger_price","reaction_price","zone_mid","zone_center"):
        v=parse_float(row.get(n));
        if v>0: ref=v; break
    if ref<=0: ref=(zone_low+zone_high)/2 if (zone_low or zone_high) else 0.0
    fee_u=ref*ROUNDTRIP_FEE_PCT
    fee_share=fee_u/risk_u
    out={"a2_fee_reference_price":round(ref,8),"a2_risk_u":round(risk_u,8),"a2_fee_u":round(fee_u,8),"a2_fee_share_r":round(fee_share,8)}
    for lbl in ("15m","1h","4h"):
        mfe=parse_float(row.get(f"mfe_{lbl}_u")); mae=parse_float(row.get(f"mae_{lbl}_u"))
        out[f"a2_net_mfe_{lbl}_r"]=round(mfe/risk_u-fee_share,8)
        out[f"a2_net_mae_{lbl}_r"]=round(mae/risk_u-fee_share,8)
    out["a2_net_hit_1r_15m"]=out["a2_net_mfe_15m_r"]>=1.0
    out["a2_net_hit_1r_1h"]=out["a2_net_mfe_1h_r"]>=1.0
    out["a2_net_hit_2r_1h"]=out["a2_net_mfe_1h_r"]>=2.0
    return out

def compute_a2_pre_ignition_metrics(row: Mapping[str, Any], bars:list[dict[str,float]])->dict[str,Any]:
    start=next((parse_float(row.get(n)) for n in ("reaction_event_ts","a2_state_ts","frozen_ts","best_pie_ts") if parse_float(row.get(n))>0),0.0)
    if start<=0 or not bars: return {"a2_pre_ignition_bar_count":0,"a2_pre_ignition_window_sec":0.0,"a2_pre_ignition_range_u":0.0,"a2_pre_ignition_range_ratio":0.0,"a2_pre_ignition_zone_stay_ratio":0.0,"a2_pre_ignition_compression_state":"INSUFFICIENT_BARS"}
    end=min(start+3600,float(bars[-1]["timestamp"]))
    window=[b for b in bars if start<=float(b["timestamp"])<=end]
    cnt=len(window); risk=max(parse_float(row.get("a2_risk_u")),1.0)
    if cnt==0: return {"a2_pre_ignition_bar_count":0,"a2_pre_ignition_window_sec":max(0,end-start),"a2_pre_ignition_range_u":0.0,"a2_pre_ignition_range_ratio":0.0,"a2_pre_ignition_zone_stay_ratio":0.0,"a2_pre_ignition_compression_state":"INSUFFICIENT_BARS"}
    rng=max(float(b["high"]) for b in window)-min(float(b["low"]) for b in window); ratio=rng/risk
    zl=parse_float(row.get("zone_lower")); zh=parse_float(row.get("zone_upper")); zle=zl-0.5*risk; zhe=zh+0.5*risk
    stay=sum(1 for b in window if zle<=float(b["close"])<=zhe)/cnt
    if cnt<3: st="INSUFFICIENT_BARS"
    elif cnt>=5 and ratio<=3.0 and stay>=0.6: st="PRE_IGNITION_COMPRESSED"
    elif cnt>=5 and ratio<=5.0 and stay>=0.4: st="PRE_IGNITION_RANGING"
    elif cnt>=3 and ratio>5.0: st="PRE_IGNITION_EXPANDING"
    else: st="UNKNOWN"
    return {"a2_pre_ignition_bar_count":cnt,"a2_pre_ignition_window_sec":round(max(0,end-start),6),"a2_pre_ignition_range_u":round(rng,8),"a2_pre_ignition_range_ratio":round(ratio,8),"a2_pre_ignition_zone_stay_ratio":round(stay,8),"a2_pre_ignition_compression_state":st}

def bucket_a3_net_mfe_1h(row):
    if not parse_bool(row.get("a3_preview_breakout_raw_flag")): return "NO_BREAKOUT"
    v=parse_float(row.get("a3_preview_net_mfe_1h_r"))
    if v<0:return "NET_MFE_LT_0"
    if v<1:return "NET_MFE_0_TO_1R"
    if v<2:return "NET_MFE_1R_TO_2R"
    return "NET_MFE_GE_2R"

def bucket_a3_realized_r_1h(row):
    if not parse_bool(row.get("a3_preview_breakout_raw_flag")): return "NO_BREAKOUT"
    v=parse_float(row.get("a3_preview_realized_r_proxy_1h"))
    if v<-1:return "REALIZED_LT_-1R"
    if v<0:return "REALIZED_-1R_TO_0"
    if v<1:return "REALIZED_0_TO_1R"
    return "REALIZED_GE_1R"
