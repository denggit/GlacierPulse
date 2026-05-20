#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Execution research virtual position aliases."""

from src.strategy.virtual_position_manager import VirtualPosition, VirtualPositionManager

ResearchVirtualPositionManager = VirtualPositionManager
ResearchVirtualPosition = VirtualPosition

__all__ = [
    "ResearchVirtualPositionManager",
    "ResearchVirtualPosition",
    "VirtualPositionManager",
    "VirtualPosition",
]

