from __future__ import annotations

from typing import Any

from ..file_metadata import SchemaRoot
from ..util.iteration import PeekableAsyncIterator
from .factory import make_assembler


async def reconstruct(
    schema_root: SchemaRoot,
    tuple_streams: dict[str, Any],
) -> dict[str, list[Any]]:
    """
    The main entry point for reconstructing Parquet data.

    This function orchestrates the entire streaming and compositional reconstruction
    process.

    Args:
        schema_root: The root schema node for the Parquet file.
        tuple_streams: A dictionary mapping the full path of each column to an
                       async iterator of its (value, dl, rl) tuples.

    Returns:
        A list of dictionaries, where each dictionary represents one fully
        reconstructed top-level record.
    """
    # 1. Wrap all raw tuple streams to make them peekable.
    peekable_streams = {
        path: PeekableAsyncIterator(s) for path, s in tuple_streams.items()
    }

    # 2. Create the single root assembler for the whole file schema.
    # The root of a Parquet file is always a struct.
    root_assembler = make_assembler(schema_root, peekable_streams)

    # 3. Loop until the stream is exhausted, collecting one record at a time.
    records = []
    try:
        while True:
            # The root is not contained in anything, so its parent_max_rl is 0.
            # The root assembler will raise StopAsyncIteration when there are
            # no more records.
            record, boundary = await root_assembler.next_item(parent_max_rl=0)

            if boundary is not None:
                # This should never happen at root level - indicates a bug
                dl, rl = boundary
                raise ValueError(
                    'Root assembler returned boundary for tuple '
                    f'with dl={dl}, rl={rl}. '
                    f'This should never happen at the root level.',
                )

            records.append(record)
    except StopAsyncIteration:
        # This is the normal termination condition when the file is fully processed.
        pass

    # Transpose the list of records (row-oriented) into a dictionary of
    # lists (column-oriented) to match the expected output format of the tests.
    if not records:
        return {}

    columnar_result: dict[str, list] = {key: [] for key in records[0]}
    for record in records:
        for key, value in record.items():
            columnar_result[key].append(value)

    return columnar_result
