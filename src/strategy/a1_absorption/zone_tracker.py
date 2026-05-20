#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A1 zone tracker alias for the current iceberg zone tracker."""

from src.strategy.iceberg.zone_tracker import IcebergZoneTracker

A1ZoneTracker = IcebergZoneTracker

__all__ = ["A1ZoneTracker", "IcebergZoneTracker"]

