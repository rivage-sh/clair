"""The connection pool. It gives one warehouse connection to each thread."""

from __future__ import annotations

import queue
import threading

from clair.adapters.base import WarehouseAdapter


class AdapterPool:
    """Hold one warehouse connection for each thread of a parallel run.

    A warehouse session holds the role and the warehouse, and `USE ROLE` changes
    them for each later query on that session. Two threads that share one
    connection thus change the context of each other. The pool prevents this: a
    thread that asks for a connection keeps the same one until the run ends.

    Clair opens each connection when it makes the pool, before the first query.
    A lazy connection is worse, because SSO authentication opens a browser
    window, and the user must then answer in the middle of a run.
    """

    def __init__(self, adapter: WarehouseAdapter, size: int) -> None:
        """Make the pool.

        Args:
            adapter: A warehouse adapter with an open connection. The pool uses
                it as the first connection, and the caller keeps the ownership
                of it. `AdapterPool.close()` does not close this adapter.
            size: The number of connections, and thus the number of threads.

        Raises:
            ValueError: If *size* is less than 1.
        """
        if size < 1:
            raise ValueError(f"The pool size must be 1 or more, but it is {size}")

        # The pool opens size - 1 connections, because the caller supplies one.
        self._opened: list[WarehouseAdapter] = [
            adapter.new_connection() for _ in range(size - 1)
        ]

        self._free: queue.SimpleQueue[WarehouseAdapter] = queue.SimpleQueue()
        self._free.put(adapter)
        for extra_adapter in self._opened:
            self._free.put(extra_adapter)

        self._thread_adapter = threading.local()

    def acquire(self) -> WarehouseAdapter:
        """Give the connection of the thread that calls this method.

        The first call in a thread takes a free connection and keeps it. Each
        later call in that thread gives the same connection.

        Raises:
            RuntimeError: If more threads ask for a connection than the pool
                holds. The caller must limit the thread count to the pool size.
        """
        adapter: WarehouseAdapter | None = getattr(self._thread_adapter, "adapter", None)
        if adapter is not None:
            return adapter

        try:
            adapter = self._free.get_nowait()
        except queue.Empty:
            raise RuntimeError(
                "The pool has no free connection. The thread count is more than "
                "the pool size."
            ) from None

        self._thread_adapter.adapter = adapter
        return adapter

    def close(self) -> None:
        """Close each connection that the pool opened.

        The pool does not close the adapter that the caller supplied, because
        the caller keeps the ownership of it.
        """
        for extra_adapter in self._opened:
            extra_adapter.close()
        self._opened = []
