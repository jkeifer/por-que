/**
 * App-facing aliases for the schema-generated dump types.
 *
 * The dump shapes themselves are GENERATED from schema/por-que.schema.json into
 * src/generated/por-que.d.ts (see `npm run generate`). This module only gives
 * them friendly, stable names for the rest of the app to import.
 *
 * ponytail: RowGroup1 / KeyValueMetadata1 are the generator's collision-renamed
 * names (the plain names became the scalar `row_group` field and the KV array).
 * If a schema change reshuffles the numbering, typecheck fails loudly here —
 * fix the alias, don't work around it.
 */
export type {
    ParquetFile as Dump,
    MetadataExport as MetadataDump,
    FileMetadata,
    PhysicalColumnChunk,
    ColumnMetadata,
    ColumnChunk,
    DictionaryPage,
    DataPageV1,
    DataPageV2,
    IndexPage,
    ColumnIndex,
    OffsetIndex,
    PageLocation,
    ColumnStatistics,
    SchemaRoot,
    SchemaGroup,
    SchemaLeaf,
    RowGroup1 as RowGroup,
    KeyValueMetadata1 as KeyValueEntry,
} from './generated/por-que';

import type {
    ParquetFile,
    MetadataExport,
    DataPageV1,
    DataPageV2,
    SchemaLeaf,
} from './generated/por-que';

/** A data page is either the v1 or v2 shape. */
export type DataPage = DataPageV1 | DataPageV2;

/** Either dump root. Discriminate on `_meta.model` ('file' | 'metadata'). */
export type AnyDump = ParquetFile | MetadataExport;

/** The logical-type info object carried on schema elements (discriminated by `logical_type`). */
export type LogicalTypeInfo = NonNullable<SchemaLeaf['logical_type']>;
