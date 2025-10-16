import asyncio
import io
import logging

from collections.abc import Callable

from por_que.util.interval_tree import BinningIntervalTree, Interval

logger = logging.getLogger(__name__)


class AsyncFileReadCache:
    """
    An async-safe cache for file chunks that supports concurrent reads.

    Key Features:
    - Stores bytes | asyncio.Task[bytes] in intervals
    - Multiple reads of the same range share the same fetch task
    - Lock-free: uses asyncio's single-threaded execution model
    """

    def __init__(
        self,
        file_size: int,
        fetch_range: Callable[[int, int], asyncio.Task[bytes]],
        minimum_request_size: int = 8192,
    ) -> None:
        """
        Initialize async file read cache.

        Args:
            file_size: Total size of the file being cached
            fetch_range: Async function that returns a Task to fetch a byte range
            minimum_request_size: Minimum bytes to fetch in a single request
        """
        if file_size < 0:
            raise ValueError('File size cannot be less than zero')

        self.file_size = file_size
        self.cache = BinningIntervalTree[bytes | asyncio.Task[bytes]](
            bin_size=minimum_request_size,
        )
        self._fetcher = fetch_range
        self._minimum_request_size = minimum_request_size

    def clear(self):
        """
        Resets the cache, clearing all stored data and intervals.
        """
        logger.debug('Clearing all cached data')
        self.cache.clear()

    def _store_and_merge(self, start: int, end: int, chunk: bytes) -> None:
        """
        Stores a new chunk of data and merges it with any adjacent or
        overlapping chunks that contain completed bytes (not Tasks).
        """
        # Find adjacent/overlapping intervals that have completed (bytes only)
        overlapping = [
            iv
            for iv in self.cache.find_overlapping(start - 1, end + 1)
            if isinstance(iv.data, bytes)
        ]

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
            [(iv.begin, iv.end, type(iv.data).__name__) for iv in sorted(self.cache)],
        )

    def _create_fetch_task(self, start: int, end: int) -> asyncio.Task[bytes] | None:
        """
        Creates a fetch task and stores it in the cache synchronously.

        Returns:
            Task to await, or None if range is already covered
        """
        # Apply minimum request size logic (same as sync version)
        more = self._minimum_request_size - (end - start)

        if more > 0:
            overlapping = self.cache.find_overlapping(
                end,
                min(self.file_size, end + self._minimum_request_size),
            )
            right_wall = overlapping[0].begin if overlapping else self.file_size
            end = min(end + more, right_wall)
            more = self._minimum_request_size - (end - start)

        if more > 0:
            overlapping = self.cache.find_overlapping(
                max(0, start - self._minimum_request_size),
                start,
                sort_by_end=True,
            )
            left_wall = overlapping[0].end if overlapping else 0
            start = max(start - more, left_wall)

        # Check if already covered (bytes or task)
        # Not needed in sync version due to no concurrency
        existing = self.cache.find_overlapping(start, end)
        if existing:
            logger.debug(
                'FETCH SKIPPED: bytes %s-%s (already in cache)',
                start,
                end - 1,
            )
            return None

        # Create task for the actual fetch operation
        async def do_fetch() -> bytes:
            chunk = await self._fetcher(start, end)
            # Remove task interval and store bytes
            self.cache.remove(task_interval)
            self._store_and_merge(start, end, chunk)
            return chunk

        task = asyncio.create_task(do_fetch())
        task_interval: Interval[bytes | asyncio.Task[bytes]] = Interval(
            start,
            end,
            task,
        )
        self.cache.add(task_interval)

        logger.debug(
            'FETCH INITIATED: bytes %s-%s (task created)',
            start,
            end - 1,
        )

        return task

    def _find_missing_ranges(self, start: int, end: int) -> list[Interval]:
        """
        Find 'holes' in the cache that need to be fetched.
        """
        missing: list[Interval] = []
        # All intervals (bytes or tasks) are considered "covered"
        relevant_intervals = sorted(self.cache.find_overlapping(start, end))
        pos = start

        for interval in relevant_intervals:
            if pos < interval.begin:
                missing.append(Interval(pos, interval.begin, None))
            pos = max(pos, interval.end)

        if pos < end:
            missing.append(Interval(pos, end, None))

        return missing

    async def read(self, start: int, end: int) -> bytes:
        """
        Reads a range of bytes asynchronously, utilizing the cache and
        fetching if necessary.
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
                [(iv.begin, iv.end) for iv in missing_intervals],
            )
            # Synchronously create all fetch tasks
            fetch_tasks = []
            for interval in missing_intervals:
                # Check if the interval (or part of it) has been filled
                # by a previous larger fetch in this same read() call.
                still_missing = self._find_missing_ranges(
                    interval.begin,
                    interval.end,
                )
                for gap in still_missing:
                    task = self._create_fetch_task(gap.begin, gap.end)
                    if task:
                        fetch_tasks.append(task)

        # Assemble result, awaiting any fetch tasks
        result_buffer = io.BytesIO()
        cached_chunks = sorted(self.cache.find_overlapping(start, end))

        for interval in cached_chunks:
            read_start = max(start, interval.begin)
            read_end = min(end, interval.end)

            # If this is still a task, await it to get bytes
            if isinstance(interval.data, asyncio.Task):
                data = await interval.data
            else:
                data = interval.data

            slice_start = read_start - interval.begin
            slice_end = read_end - interval.begin

            result_buffer.write(data[slice_start:slice_end])

        return result_buffer.getvalue()
