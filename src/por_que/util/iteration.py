from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Iterable
from typing import Self


class PeekableAsyncIterator[T]:
    """
    A wrapper around an async iterator that allows peeking at the next item
    without consuming it.
    """

    def __init__(self, iterator: AsyncIterator[T]) -> None:
        self.iterator = iterator
        self._peeked: T | None = None
        self._finished = False

    async def peek(self) -> T | None:
        """
        Returns the next item from the iterator without consuming it.
        Returns None if the iterator is exhausted.
        """
        if self._peeked is None and not self._finished:
            try:
                self._peeked = await self.iterator.__anext__()
            except StopAsyncIteration:
                self._finished = True
                return None
        return self._peeked

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        """
        Returns the next item from the iterator, consuming it.
        """
        if self._peeked is not None:
            item = self._peeked
            self._peeked = None
            return item

        if self._finished:
            raise StopAsyncIteration

        try:
            return await self.iterator.__anext__()
        except StopAsyncIteration:
            self._finished = True
            raise


class AsyncChain[T](AsyncIterable[T]):
    __slots__ = '_iterables'

    async def __aiter__(self) -> AsyncIterator[T]:
        for iterable in self._iterables:
            match iterable:
                case AsyncIterable():
                    async for item in aiter(iterable):
                        yield item
                case Iterable():
                    for item in iter(iterable):
                        yield item

    def __init__(self, *iterables: AsyncIterable[T] | Iterable[T]) -> None:
        self._iterables = list(iterables)

    def add(self, *iterables: AsyncIterable[T] | Iterable[T]) -> None:
        self._iterables.extend(iterables)
