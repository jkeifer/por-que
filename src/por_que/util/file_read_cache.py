import io
import logging

from collections.abc import Callable

from intervaltree import Interval, IntervalTree

logger = logging.getLogger(__name__)


class FileReadCache:
    """
    A memory-efficient class to manage caching chunks of a file.
    Typically used to cache data in a remote file to prevent
    many small reads, or repeated reads of the same bytes.
    """

    def __init__(
        self,
        file_size: int,
        fetch_range: Callable[[int, int], bytes],
        minimum_request_size: int = 8192,
    ) -> None:
        if file_size < 0:
            raise ValueError('File size cannot be less than zero')

        self.file_size = file_size
        self.cache = IntervalTree()
        self._fetcher = fetch_range
        self._minimum_request_size = minimum_request_size

    def clear(self):
        """
        Resets the cache, clearing all stored data and intervals.
        """
        logger.debug('Clearing all cached data')
        self.cache = IntervalTree()

    def _store_and_merge(self, start, end, chunk):
        """
        Stores a new chunk of data and merges it with any adjacent or
        overlapping chunks in the cache.
        """
        overlapping = self.cache[start - 1 : end + 1]
        min_start = start
        max_end = end
        parts = {start: chunk}

        for iv in overlapping:
            min_start = min(min_start, iv.begin)
            max_end = max(max_end, iv.end)
            parts[iv.begin] = iv.data
            self.cache.remove(iv)

        merged_data = io.BytesIO()
        for offset in sorted(parts.keys()):
            merged_data.write(parts[offset])

        self.cache.add(Interval(min_start, max_end, merged_data.getvalue()))
        logger.debug(
            '--- CACHE UPDATE: Cache now contains: %s',
            [iv[:2] for iv in sorted(self.cache)],
        )

    def _fetch_range(self, start: int, end: int) -> None:
        """
        Fetches a range of bytes, applying minimum request size logic
        in a cache-aware manner to avoid re-downloading.
        """
        more = self._minimum_request_size - (end - start)

        if more > 0 and not self.cache[end]:
            # stretch request forward to get more bytes up to max
            # but not encroaching into data we already have
            _next = sorted(self.cache[end:])
            right_wall = _next[0].begin if _next else self.file_size
            end = min(end + more, right_wall)
            more = self._minimum_request_size - (end - start)

        if more > 0 and not self.cache[start]:
            # stretch request backwards to get more bytes up to max
            # but not encroaching into data we already have
            _previous = sorted(self.cache[:start], reverse=True)
            left_wall = _previous[0].end if _previous else 0
            start = max(start - more, left_wall)

        chunk = self._fetcher(start, end)
        self._store_and_merge(start, end, chunk)

    def _find_missing_ranges(self, start: int, end: int) -> list[Interval]:
        """
        A simple, robust method to find "holes" in the cache.
        """
        missing: list[Interval] = []
        relevant_intervals = sorted(self.cache[start:end])
        pos = start

        for interval in relevant_intervals:
            if pos < interval.begin:
                missing.append(Interval(pos, interval.begin))
            pos = max(pos, interval.end)

        if pos < end:
            missing.append(Interval(pos, end))

        return missing

    def read(self, start: int, end: int) -> bytes:
        """
        Reads a range of bytes, utilizing the cache and fetching if necessary.
        """
        if end > self.file_size:
            raise ValueError('Read request extends beyond the end of the file.')

        logger.debug(
            'Read Request for bytes %s-%s',
            start,
            end - 1,
        )

        missing_intervals = self._find_missing_ranges(start, end)

        if not missing_intervals:
            logger.debug('CACHE HIT: All requested data already in cache.')
        else:
            logger.debug(
                'CACHE MISS: Missing intervals are: %s',
                [iv[:2] for iv in missing_intervals],
            )
            for interval in missing_intervals:
                # Check if the interval (or part of it) has been filled
                # by a previous larger fetch in this same read() call.
                still_missing = self._find_missing_ranges(
                    interval.begin,
                    interval.end,
                )
                for gap in still_missing:
                    self._fetch_range(gap.begin, gap.end)

        result_buffer = io.BytesIO()
        cached_chunks = sorted(self.cache[start:end])

        for interval in cached_chunks:
            read_start = max(start, interval.begin)
            read_end = min(end, interval.end)

            slice_start = read_start - interval.begin
            slice_end = read_end - interval.begin

            result_buffer.write(interval.data[slice_start:slice_end])

        return result_buffer.getvalue()
