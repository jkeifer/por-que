"""A lightweight, internal replacement for the `intervaltree` library."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Interval[T]:
    """Represents an interval with a start, end, and associated data."""

    begin: int
    end: int
    data: T

    def __lt__(self, other) -> bool:
        if not isinstance(other, Interval):
            return NotImplemented
        return self.begin < other.begin


class BinningIntervalTree[T]:
    """
    An interval tree implementation that uses binning to speed up queries.
    This is a special-purpose implementation for FileReadCache.
    """

    def __init__(self, bin_size: int) -> None:
        if bin_size <= 0:
            raise ValueError('Bin size must be a positive integer')
        self.bin_size = bin_size
        self.bins: defaultdict[int, set[Interval[T]]] = defaultdict(set)
        self.all_intervals: set[Interval[T]] = set()

    def clear(self) -> None:
        """Removes all intervals from the tree."""
        self.bins.clear()
        self.all_intervals.clear()

    def add(self, interval: Interval[T]) -> None:
        """Adds a new interval to the tree."""
        self.all_intervals.add(interval)
        for bin_key in self._get_bin_keys_for_interval(interval):
            self.bins[bin_key].add(interval)

    def remove(self, interval: Interval[T]) -> None:
        """Removes a specific interval from the tree."""
        self.all_intervals.discard(interval)
        for bin_key in self._get_bin_keys_for_interval(interval):
            if bin_key in self.bins:
                self.bins[bin_key].discard(interval)

    def __iter__(self):
        return iter(self.all_intervals)

    def find_overlapping(
        self,
        start: int,
        end: int,
        sort_by_end: bool = False,
    ) -> list[Interval]:
        """Finds all intervals that overlap with the given range."""
        if start >= end:
            return []

        candidates: set[Interval[T]] = set()
        for bin_key in self._get_bin_keys_for_range(start, end):
            candidates.update(self.bins.get(bin_key, set()))

        overlapping = {iv for iv in candidates if iv.begin < end and iv.end > start}

        if sort_by_end:
            return sorted(overlapping, key=lambda iv: iv.end, reverse=True)
        return sorted(overlapping)

    def _get_bin_keys_for_interval(self, interval: Interval) -> range:
        start_bin = interval.begin // self.bin_size
        end_bin = (interval.end - 1) // self.bin_size
        return range(start_bin, end_bin + 1)

    def _get_bin_keys_for_range(self, start: int, end: int) -> range:
        start_bin = start // self.bin_size
        end_bin = (end - 1) // self.bin_size
        return range(start_bin, end_bin + 1)
