#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CLI wrapper for the V7.3 runtime_events cache builder.

Normal local research runs build missing daily cache shards through
tools/backtest_local_data.py. Use this tool only for explicit cache rebuilds,
debugging, or trades-only runtime_events generation.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.runtime_three_a import runtime_event_builder as _builder

if __name__ != "__main__":
    sys.modules[__name__] = _builder
else:
    raise SystemExit(_builder.main())
