"""Pending A1 absorption event manager placeholder.

V6.3.8 keeps pending-event runtime state inside A1AbsorptionEngine while the
final package layout settles. This module is reserved for a future extraction.
"""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class PendingA1AbsorptionEvent:
    event: Dict[str, Any]


__all__ = ["PendingA1AbsorptionEvent"]
