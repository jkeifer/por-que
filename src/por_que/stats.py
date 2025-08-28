from dataclasses import dataclass


@dataclass
class CompressionStats:
    """Compression statistics for data."""

    total_compressed: int
    total_uncompressed: int

    @property
    def ratio(self) -> float:
        """Compression ratio (compressed/uncompressed)."""
        return (
            self.total_compressed / self.total_uncompressed
            if self.total_uncompressed > 0
            else 0.0
        )

    @property
    def space_saved_percent(self) -> float:
        """Percentage of space saved by compression."""
        return (1 - self.ratio) * 100 if self.total_uncompressed > 0 else 0.0

    @property
    def compressed_mb(self) -> float:
        """Compressed size in MB."""
        return self.total_compressed / (1024 * 1024)

    @property
    def uncompressed_mb(self) -> float:
        """Uncompressed size in MB."""
        return self.total_uncompressed / (1024 * 1024)


@dataclass
class RowGroupStats:
    """Statistics for a single row group."""

    num_rows: int
    num_columns: int
    total_byte_size: int
    compression: CompressionStats

    @property
    def avg_column_size(self) -> int:
        """Average column size in bytes."""
        return self.total_byte_size // self.num_columns if self.num_columns > 0 else 0


@dataclass
class FileStats:
    """Overall file statistics."""

    version: int
    created_by: str | None
    total_rows: int
    num_row_groups: int
    num_schema_elements: int
    total_columns: int
    min_rows_per_group: int
    max_rows_per_group: int
    compression: CompressionStats
    row_group_stats: list[RowGroupStats]
