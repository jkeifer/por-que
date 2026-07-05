/**
 * Explicit interfaces for the por-que JSON dump shapes.
 *
 * NOTE: These model only what the app actually reads today (enum codes are
 * ints). A follow-up will replace these with schema-generated types, so keep
 * them permissive rather than exhaustive.
 */

export interface LogicalType {
    logical_type?: number;
    precision?: number;
    scale?: number;
    bit_width?: number;
    is_signed?: boolean;
}

export interface ColumnStatistics {
    min_value?: unknown;
    max_value?: unknown;
    null_count?: number | null;
    distinct_count?: number | null;
}

/** A dictionary/data/index page. */
export interface PageMeta {
    start_offset: number;
    header_size?: number;
    compressed_page_size?: number;
    uncompressed_page_size?: number;
    page_type?: number;
    encoding?: number;
    num_values?: number;
    num_rows?: number;
    crc?: number;
    is_sorted?: boolean;
}

/** Detailed column metadata (the `.metadata` inside a file-level column chunk). */
export interface ColumnMetadata {
    type?: number;
    logical_type?: LogicalType | null;
    converted_type?: number | null;
    type_length?: number;
    precision?: number;
    scale?: number;
    total_compressed_size?: number;
    total_uncompressed_size?: number;
    encodings?: number[];
    data_page_offset?: number;
    dictionary_page_offset?: number;
    index_page_offset?: number;
    num_values?: number;
    statistics?: ColumnStatistics | null;
}

/** File-level column chunk keyed under `metadata.row_groups[].column_chunks`. */
export interface FileColumnChunk {
    metadata: ColumnMetadata;
    column_index_offset?: number | null;
    column_index_length?: number | null;
    // Present on ColumnMetadata, not here at runtime; kept optional so the
    // builder's fallback path type-checks (always undefined -> falls back).
    total_compressed_size?: number;
    total_uncompressed_size?: number;
}

/** Top-level physical column chunk (`column_chunks[]`). */
export interface PhysicalColumnChunk {
    start_offset: number;
    total_byte_size: number;
    path_in_schema: string;
    row_group?: number;
    codec?: number;
    num_values?: number;
    dictionary_page?: PageMeta | null;
    data_pages?: PageMeta[];
    index_pages?: PageMeta[];
}

export interface CompressionStats {
    total_compressed?: number;
    total_uncompressed?: number;
    ratio?: number;
    space_saved_percent?: number;
}

export interface SchemaNode {
    name?: string;
    element_type?: string;
    children?: Record<string, SchemaNode>;
    start_offset?: number;
    byte_length?: number;
    num_children?: number;
    type?: number;
    logical_type?: LogicalType | null;
    converted_type?: number | null;
    repetition?: number;
    field_id?: number | null;
    type_length?: number | null;
    precision?: number | null;
    scale?: number | null;
}

export interface RowGroup {
    row_count?: number;
    byte_length?: number;
    column_chunks?: Record<string, FileColumnChunk>;
    compression_stats?: CompressionStats;
}

export interface KeyValueEntry {
    key: string;
    value?: string;
    byte_length?: number;
    start_offset?: number;
}

export interface Metadata {
    version?: number | string;
    created_by?: string;
    schema_root?: SchemaNode;
    row_groups?: RowGroup[];
    key_value_metadata?: KeyValueEntry[];
    start_offset?: number;
    total_byte_size?: number;
    compression_stats?: CompressionStats;
    column_count?: number;
    row_count?: number;
    row_group_count?: number;
    /** Known-broken nested path some builders reach for; usually absent. */
    metadata?: { key_value_metadata?: KeyValueEntry[] };
}

export interface FileData {
    source?: string;
    filesize: number;
    column_chunks: PhysicalColumnChunk[];
    metadata: Metadata;
}

/**
 * Permissive metadata carried on a segment. A segment's metadata may be a
 * schema node, a row group, a page, a file-level column chunk, or one of a few
 * synthetic container/entry objects the builder constructs. All fields are
 * optional so consumers can probe with optional access.
 */
export interface SegmentMetadata {
    // schema node
    name?: string;
    element_type?: string;
    children?: Record<string, SchemaNode>;
    start_offset?: number;
    byte_length?: number;
    num_children?: number;
    type?: number;
    logical_type?: LogicalType | null;
    converted_type?: number | null;
    repetition?: number;
    field_id?: number | null;
    type_length?: number | null;
    precision?: number | null;
    scale?: number | null;
    // row group
    row_count?: number;
    column_chunks?: Record<string, FileColumnChunk>;
    compression_stats?: CompressionStats;
    // synthetic containers
    row_groups?: RowGroup[];
    num_row_groups?: number;
    indices_count?: number;
    total_size?: number;
    entries?: KeyValueEntry[];
    num_entries?: number;
    structured_format?: boolean;
    // index element
    index_type?: string;
    column_path?: string;
    row_group_index?: number;
    // key-value entry
    key?: string;
    value?: string;
    entry_index?: number;
    // page
    page_type?: number;
    encoding?: number;
    header_size?: number;
    compressed_page_size?: number;
    uncompressed_page_size?: number;
    num_values?: number;
    num_rows?: number;
    crc?: number;
    is_sorted?: boolean;
    statistics?: ColumnStatistics | null;
    total_compressed_size?: number;
    total_uncompressed_size?: number;
    encodings?: number[];
    data_page_offset?: number;
    dictionary_page_offset?: number;
    index_page_offset?: number;
}
