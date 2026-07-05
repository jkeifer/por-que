/**
 * Segment tree projection.
 *
 * ONE recursive pass turns a validated dump into a tree of physical segments.
 * Every span is a REAL byte offset read off the wire — nothing is estimated or
 * fabricated. Depth = drill-down level; a node's children ARE the next level;
 * the selection path is the ancestor chain. "What is this segment" is the
 * node's `kind`, not a re-derivation from id strings or metadata shapes.
 */
import type {
    AnyDump,
    Dump,
    MetadataDump,
    FileMetadata,
    PhysicalColumnChunk,
    ColumnMetadata,
    ColumnChunk,
    DataPage,
    DictionaryPage,
    IndexPage,
    ColumnIndex,
    OffsetIndex,
    SchemaRoot,
    SchemaGroup,
    SchemaLeaf,
    RowGroup,
    KeyValueEntry,
} from '../types';

export type Kind =
    | 'file'
    | 'magic_header'
    | 'data_region'
    | 'row_group'
    | 'column_chunk'
    | 'dictionary_page'
    | 'data_page'
    | 'index_page'
    | 'column_index'
    | 'offset_index'
    | 'bloom_filter'
    | 'metadata_region'
    | 'schema_root'
    | 'schema_group'
    | 'schema_leaf'
    | 'row_groups_meta'
    | 'row_group_meta'
    | 'chunk_meta'
    | 'kv_meta'
    | 'kv_entry'
    | 'footer'
    | 'magic_footer';

/** Every kind, for exhaustiveness checks and iteration. */
export const KINDS: Kind[] = [
    'file',
    'magic_header',
    'data_region',
    'row_group',
    'column_chunk',
    'dictionary_page',
    'data_page',
    'index_page',
    'column_index',
    'offset_index',
    'bloom_filter',
    'metadata_region',
    'schema_root',
    'schema_group',
    'schema_leaf',
    'row_groups_meta',
    'row_group_meta',
    'chunk_meta',
    'kv_meta',
    'kv_entry',
    'footer',
    'magic_footer',
];

interface Base {
    id: string;
    name: string;
    start: number;
    end: number;
    children: SegmentNode[];
}

/**
 * A physical segment. Discriminated by `kind`; each variant carries the typed
 * source slice needed to describe it. Narrow on `node.kind` to read the extras.
 */
export type SegmentNode =
    | (Base & { kind: 'file'; dump: AnyDump })
    | (Base & { kind: 'magic_header'; text: string })
    | (Base & { kind: 'magic_footer'; text: string })
    | (Base & { kind: 'footer' })
    | (Base & { kind: 'data_region' })
    | (Base & { kind: 'metadata_region'; meta: FileMetadata })
    | (Base & { kind: 'row_group'; index: number; group: RowGroup | null })
    | (Base & {
          kind: 'column_chunk';
          // null in a metadata-only export: no materialized page structure,
          // only the footer's ColumnMetadata (`meta`).
          chunk: PhysicalColumnChunk | null;
          meta: ColumnMetadata | null;
          leaf: SchemaLeaf | null;
      })
    | (Base & { kind: 'dictionary_page'; page: DictionaryPage; path: string })
    | (Base & { kind: 'data_page'; page: DataPage; path: string })
    | (Base & { kind: 'index_page'; page: IndexPage; path: string })
    // index is null in a metadata-only export: the byte span comes from the
    // footer, but the parsed index contents aren't in the dump.
    | (Base & { kind: 'column_index'; index: ColumnIndex | null; path: string })
    | (Base & { kind: 'offset_index'; index: OffsetIndex | null; path: string })
    | (Base & { kind: 'bloom_filter'; path: string; rowGroup: number })
    | (Base & { kind: 'schema_root'; node: SchemaRoot })
    | (Base & { kind: 'schema_group'; node: SchemaGroup })
    | (Base & { kind: 'schema_leaf'; node: SchemaLeaf })
    | (Base & { kind: 'row_groups_meta'; groups: RowGroup[] })
    | (Base & { kind: 'row_group_meta'; index: number; group: RowGroup })
    | (Base & { kind: 'chunk_meta'; meta: ColumnMetadata; path: string })
    | (Base & { kind: 'kv_meta'; entries: KeyValueEntry[] })
    | (Base & { kind: 'kv_entry'; entry: KeyValueEntry });

const byStart = (a: SegmentNode, b: SegmentNode): number => a.start - b.start;

/** True when a nullable/optional field is actually present. */
function isSet<T>(v: T | null | undefined): v is T {
    return v !== null && v !== undefined;
}

/** Project either dump root into the physical segment tree. */
export function project(dump: AnyDump): SegmentNode {
    // ParquetFile carries `column_chunks`; MetadataExport does not. That
    // structural difference is the reliable discriminator.
    return 'column_chunks' in dump ? projectDump(dump) : projectMetadataExport(dump);
}

/** Project a validated full dump into the physical segment tree. */
export function projectDump(dump: Dump): SegmentNode {
    return assemble(dump, buildDataRegion(dump, dump.magic_header?.length ?? 4));
}

/**
 * Project a metadata-only export. The byte map comes from `filesize`; the data
 * region's row groups and column chunks are derived from footer metadata ONLY
 * (real offsets/sizes), with NO page-level nodes -- that data isn't in the
 * export. Magic/footer/metadata-region segments are identical to the full path.
 */
export function projectMetadataExport(dump: MetadataDump): SegmentNode {
    return assemble(dump, buildMetadataDataRegion(dump.metadata, 4));
}

/**
 * Assemble the five top-level file segments around a pre-built data region.
 * magic_header/magic_footer are optional in the schema (and absent on a
 * metadata export) but always "PAR1" (4 bytes) in a real parquet file.
 */
function assemble(dump: AnyDump, dataRegion: SegmentNode): SegmentNode {
    const meta = dump.metadata;
    const headerMagic = ('magic_header' in dump ? dump.magic_header : null) ?? 'PAR1';
    const footerMagic = ('magic_footer' in dump ? dump.magic_footer : null) ?? 'PAR1';
    const magicLen = headerMagic.length;
    const metaEnd = meta.start_offset + meta.total_byte_size;
    const footerMagicLen = footerMagic.length;

    const children: SegmentNode[] = [
        {
            kind: 'magic_header',
            id: 'magic_header',
            name: 'MAGIC',
            start: 0,
            end: magicLen,
            text: headerMagic,
            children: [],
        },
        dataRegion,
        buildMetadataRegion(meta),
        {
            kind: 'footer',
            id: 'footer',
            name: 'FOOTER',
            start: metaEnd,
            end: dump.filesize - footerMagicLen,
            children: [],
        },
        {
            kind: 'magic_footer',
            id: 'magic_footer',
            name: 'MAGIC',
            start: dump.filesize - footerMagicLen,
            end: dump.filesize,
            text: footerMagic,
            children: [],
        },
    ];

    return {
        kind: 'file',
        id: 'file',
        name: dump.source,
        start: 0,
        end: dump.filesize,
        dump,
        children: children.sort(byStart),
    };
}

/** The data portion: everything between the header magic and the footer. */
function buildDataRegion(dump: Dump, start: number): SegmentNode {
    const end = dump.metadata.start_offset;
    const byGroup = new Map<number, PhysicalColumnChunk[]>();
    for (const chunk of dump.column_chunks) {
        const list = byGroup.get(chunk.row_group) ?? [];
        list.push(chunk);
        byGroup.set(chunk.row_group, list);
    }

    const children: SegmentNode[] = [];
    for (const [index, chunks] of byGroup) {
        children.push(buildRowGroup(dump, index, chunks));
    }

    // Page-index blocks live between the data and the footer, not inside any
    // row group's byte span, so they hang directly off the data region. Bloom
    // filter bitsets sit in that same between-chunks-and-footer territory
    // (verified against real fixtures), so they follow the same pattern.
    for (const chunk of dump.column_chunks) {
        if (chunk.column_index) {
            children.push(buildColumnIndex(chunk.column_index, chunk.path_in_schema));
        }
        if (chunk.offset_index) {
            children.push(buildOffsetIndex(chunk.offset_index, chunk.path_in_schema));
        }
        const meta =
            dump.metadata.row_groups[chunk.row_group]?.column_chunks[chunk.path_in_schema]
                ?.metadata;
        // Real byte spans only: older files may record an offset without a
        // length (no bitset size persisted), so skip rather than estimate.
        if (meta && isSet(meta.bloom_filter_offset) && isSet(meta.bloom_filter_length)) {
            children.push(
                buildBloomFilter(
                    meta.bloom_filter_offset,
                    meta.bloom_filter_length,
                    chunk.path_in_schema,
                    chunk.row_group
                )
            );
        }
    }

    return {
        kind: 'data_region',
        id: 'data_region',
        name: 'DATA',
        start,
        end,
        children: children.sort(byStart),
    };
}

function buildRowGroup(dump: Dump, index: number, chunks: PhysicalColumnChunk[]): SegmentNode {
    const start = Math.min(...chunks.map(c => c.start_offset));
    const end = Math.max(...chunks.map(c => c.start_offset + c.total_byte_size));
    const group = dump.metadata.row_groups[index] ?? null;

    return {
        kind: 'row_group',
        id: `rg_${index}`,
        name: `RG${index}`,
        start,
        end,
        index,
        group,
        children: chunks.map(c => buildColumnChunk(dump, index, c)).sort(byStart),
    };
}

function buildColumnChunk(dump: Dump, rgIndex: number, chunk: PhysicalColumnChunk): SegmentNode {
    const meta =
        dump.metadata.row_groups[rgIndex]?.column_chunks[chunk.path_in_schema]?.metadata ?? null;
    const leaf = findSchemaLeaf(dump.metadata.schema_root, chunk.path_in_schema);

    const pages: SegmentNode[] = [];
    if (chunk.dictionary_page) {
        pages.push(buildDictPage(chunk.dictionary_page, chunk.path_in_schema));
    }
    chunk.data_pages.forEach((p, i) => pages.push(buildDataPage(p, chunk.path_in_schema, i)));
    chunk.index_pages.forEach((p, i) => pages.push(buildIndexPage(p, chunk.path_in_schema, i)));

    return {
        kind: 'column_chunk',
        id: `rg_${rgIndex}_col_${chunk.path_in_schema}`,
        name: chunk.path_in_schema,
        start: chunk.start_offset,
        end: chunk.start_offset + chunk.total_byte_size,
        chunk,
        meta,
        leaf,
        children: pages.sort(byStart),
    };
}

const pageEnd = (p: {
    start_offset: number;
    header_size: number;
    compressed_page_size: number;
}): number => p.start_offset + p.header_size + p.compressed_page_size;

function buildDictPage(page: DictionaryPage, path: string): SegmentNode {
    return {
        kind: 'dictionary_page',
        id: `${path}_dict`,
        name: 'DICT',
        start: page.start_offset,
        end: pageEnd(page),
        page,
        path,
        children: [],
    };
}

function buildDataPage(page: DataPage, path: string, i: number): SegmentNode {
    return {
        kind: 'data_page',
        id: `${path}_data_${i}`,
        name: `DATA${i}`,
        start: page.start_offset,
        end: pageEnd(page),
        page,
        path,
        children: [],
    };
}

function buildIndexPage(page: IndexPage, path: string, i: number): SegmentNode {
    return {
        kind: 'index_page',
        id: `${path}_idx_${i}`,
        name: `IDX${i}`,
        start: page.start_offset,
        end: pageEnd(page),
        page,
        path,
        children: [],
    };
}

function buildColumnIndex(index: ColumnIndex, path: string): SegmentNode {
    return {
        kind: 'column_index',
        id: `colidx_${path}`,
        name: `${path} column index`,
        start: index.start_offset,
        end: index.start_offset + index.byte_length,
        index,
        path,
        children: [],
    };
}

function buildOffsetIndex(index: OffsetIndex, path: string): SegmentNode {
    return {
        kind: 'offset_index',
        id: `offidx_${path}`,
        name: `${path} offset index`,
        start: index.start_offset,
        end: index.start_offset + index.byte_length,
        index,
        path,
        children: [],
    };
}

function buildBloomFilter(
    offset: number,
    length: number,
    path: string,
    rowGroup: number
): SegmentNode {
    return {
        kind: 'bloom_filter',
        id: `bloomfilter_rg${rowGroup}_${path}`,
        name: `${path} bloom filter (RG${rowGroup})`,
        start: offset,
        end: offset + length,
        path,
        rowGroup,
        children: [],
    };
}

/**
 * Data region derived from footer metadata only (metadata-only export). Row
 * groups and column chunks come from real ColumnMetadata offsets/sizes; page
 * indexes and bloom filters from the footer's ColumnChunk spans. No page-level
 * nodes -- that data isn't in a metadata export.
 */
function buildMetadataDataRegion(meta: FileMetadata, start: number): SegmentNode {
    const end = meta.start_offset;
    const children: SegmentNode[] = [];

    meta.row_groups.forEach((group, rgIndex) => {
        const chunks = Object.entries(group.column_chunks).map(([path, cc]) =>
            buildMetadataColumnChunk(meta, rgIndex, path, cc)
        );
        if (chunks.length === 0) {
            return;
        }
        children.push({
            kind: 'row_group',
            id: `rg_${rgIndex}`,
            name: `RG${rgIndex}`,
            start: Math.min(...chunks.map(c => c.start)),
            end: Math.max(...chunks.map(c => c.end)),
            index: rgIndex,
            group,
            children: chunks.sort(byStart),
        });
    });

    // Page-index and bloom-filter blocks sit between the chunks and the footer,
    // hanging directly off the data region (same as the full path). Their byte
    // spans are all footer metadata, so they survive a metadata-only export.
    meta.row_groups.forEach((group, rgIndex) => {
        for (const [path, cc] of Object.entries(group.column_chunks)) {
            if (isSet(cc.column_index_offset) && isSet(cc.column_index_length)) {
                children.push(
                    buildMetadataIndex(
                        'column_index',
                        cc.column_index_offset,
                        cc.column_index_length,
                        path
                    )
                );
            }
            if (isSet(cc.offset_index_offset) && isSet(cc.offset_index_length)) {
                children.push(
                    buildMetadataIndex(
                        'offset_index',
                        cc.offset_index_offset,
                        cc.offset_index_length,
                        path
                    )
                );
            }
            const m = cc.metadata;
            // Real byte spans only: skip when the length wasn't persisted.
            if (isSet(m.bloom_filter_offset) && isSet(m.bloom_filter_length)) {
                children.push(
                    buildBloomFilter(m.bloom_filter_offset, m.bloom_filter_length, path, rgIndex)
                );
            }
        }
    });

    return {
        kind: 'data_region',
        id: 'data_region',
        name: 'DATA',
        start,
        end,
        children: children.sort(byStart),
    };
}

function buildMetadataColumnChunk(
    meta: FileMetadata,
    rgIndex: number,
    path: string,
    cc: ColumnChunk
): SegmentNode {
    const m = cc.metadata;
    // Chunk start = first page written; matches PhysicalColumnChunk.start_offset.
    const start = isSet(m.dictionary_page_offset)
        ? Math.min(m.data_page_offset, m.dictionary_page_offset)
        : m.data_page_offset;
    return {
        kind: 'column_chunk',
        id: `rg_${rgIndex}_col_${path}`,
        name: path,
        start,
        end: start + m.total_compressed_size,
        chunk: null,
        meta: m,
        leaf: findSchemaLeaf(meta.schema_root, path),
        children: [],
    };
}

/** Page-index node (metadata mode): real byte span, no parsed contents. */
function buildMetadataIndex(
    kind: 'column_index' | 'offset_index',
    offset: number,
    length: number,
    path: string
): SegmentNode {
    const isCol = kind === 'column_index';
    const base = {
        id: `${isCol ? 'colidx' : 'offidx'}_${path}`,
        name: `${path} ${isCol ? 'column' : 'offset'} index`,
        start: offset,
        end: offset + length,
        index: null,
        path,
        children: [],
    };
    return isCol ? { kind: 'column_index', ...base } : { kind: 'offset_index', ...base };
}

/** The footer thrift metadata: schema, per-row-group metadata, key/value pairs. */
function buildMetadataRegion(meta: FileMetadata): SegmentNode {
    const children: SegmentNode[] = [buildSchemaRoot(meta.schema_root)];

    if (meta.row_groups.length > 0) {
        children.push(buildRowGroupsMeta(meta.row_groups));
    }
    if (meta.key_value_metadata && meta.key_value_metadata.length > 0) {
        children.push(buildKvMeta(meta.key_value_metadata));
    }

    return {
        kind: 'metadata_region',
        id: 'metadata_region',
        name: 'METADATA',
        start: meta.start_offset,
        end: meta.start_offset + meta.total_byte_size,
        meta,
        children: children.sort(byStart),
    };
}

/** Physical extent of a schema element and all its descendants. */
function schemaExtent(node: SchemaRoot | SchemaGroup | SchemaLeaf): [number, number] {
    let end = node.start_offset + node.byte_length;
    const children = 'children' in node ? node.children : undefined;
    for (const child of Object.values(children ?? {})) {
        end = Math.max(end, schemaExtent(child)[1]);
    }
    return [node.start_offset, end];
}

function buildSchemaRoot(root: SchemaRoot): SegmentNode {
    const [start, end] = schemaExtent(root);
    return {
        kind: 'schema_root',
        id: 'schema_root',
        name: 'SCHEMA',
        start,
        end,
        node: root,
        children: buildSchemaChildren(root, 'schema'),
    };
}

function buildSchemaChildren(parent: SchemaRoot | SchemaGroup, parentId: string): SegmentNode[] {
    const out: SegmentNode[] = [];
    for (const [name, child] of Object.entries(parent.children ?? {})) {
        const id = `${parentId}_${name}`;
        if (child.element_type === 'group') {
            const group = child as SchemaGroup;
            const [start, end] = schemaExtent(group);
            out.push({
                kind: 'schema_group',
                id,
                name,
                start,
                end,
                node: group,
                children: buildSchemaChildren(group, id),
            });
        } else {
            const leaf = child as SchemaLeaf;
            out.push({
                kind: 'schema_leaf',
                id,
                name,
                start: leaf.start_offset,
                end: leaf.start_offset + leaf.byte_length,
                node: leaf,
                children: [],
            });
        }
    }
    return out.sort(byStart);
}

function buildRowGroupsMeta(groups: RowGroup[]): SegmentNode {
    const start = Math.min(...groups.map(g => g.start_offset));
    const end = Math.max(...groups.map(g => g.start_offset + g.byte_length));
    return {
        kind: 'row_groups_meta',
        id: 'row_groups_meta',
        name: 'ROW GROUP METADATA',
        start,
        end,
        groups,
        children: groups.map((g, i) => buildRowGroupMeta(g, i)).sort(byStart),
    };
}

function buildRowGroupMeta(group: RowGroup, index: number): SegmentNode {
    const chunks: SegmentNode[] = Object.entries(group.column_chunks).map(([path, cc]) => ({
        kind: 'chunk_meta',
        id: `rgm_${index}_${path}`,
        name: path,
        start: cc.metadata.start_offset,
        end: cc.metadata.start_offset + cc.metadata.byte_length,
        meta: cc.metadata,
        path,
        children: [],
    }));

    return {
        kind: 'row_group_meta',
        id: `rgm_${index}`,
        name: `RG${index} META`,
        start: group.start_offset,
        end: group.start_offset + group.byte_length,
        index,
        group,
        children: chunks.sort(byStart),
    };
}

function buildKvMeta(entries: KeyValueEntry[]): SegmentNode {
    const start = Math.min(...entries.map(e => e.start_offset));
    const end = Math.max(...entries.map(e => e.start_offset + e.byte_length));
    return {
        kind: 'kv_meta',
        id: 'kv_meta',
        name: 'KEY-VALUE METADATA',
        start,
        end,
        entries,
        children: entries
            .map((e, i) => ({
                kind: 'kv_entry' as const,
                id: `kv_${i}`,
                name: e.key,
                start: e.start_offset,
                end: e.start_offset + e.byte_length,
                entry: e,
                children: [],
            }))
            .sort(byStart),
    };
}

/** Find a schema leaf by its full dotted path (matches path_in_schema). */
export function findSchemaLeaf(
    node: SchemaRoot | SchemaGroup | SchemaLeaf,
    path: string
): SchemaLeaf | null {
    // `type` (physical type) is required on SchemaLeaf and absent on
    // root/group, so it discriminates the leaf reliably.
    if ('type' in node) {
        return node.full_path === path ? node : null;
    }
    for (const child of Object.values(node.children ?? {})) {
        const found = findSchemaLeaf(child, path);
        if (found) {
            return found;
        }
    }
    return null;
}

/** Short human label for a node, used for tooltips and panel headings. */
export function describe(node: SegmentNode): string {
    switch (node.kind) {
        case 'file':
            return node.name;
        case 'magic_header':
        case 'magic_footer':
            return `${node.text} magic number`;
        case 'footer':
            return 'Footer metadata length';
        case 'data_region':
            return 'Data region';
        case 'metadata_region':
            return 'File metadata (footer)';
        case 'row_group':
            return `Row Group ${node.index}`;
        case 'column_chunk':
            return `Column Chunk ${node.name}`;
        case 'dictionary_page':
            return 'Dictionary page';
        case 'data_page':
            return `Data page ${node.name}`;
        case 'index_page':
            return `Index page ${node.name}`;
        case 'column_index':
            return `Column index (${node.path})`;
        case 'offset_index':
            return `Offset index (${node.path})`;
        case 'bloom_filter':
            return `Bloom filter (${node.path}, RG${node.rowGroup})`;
        case 'schema_root':
            return 'Schema';
        case 'schema_group':
            return `Schema group ${node.name}`;
        case 'schema_leaf':
            return `Schema column ${node.name}`;
        case 'row_groups_meta':
            return 'Row group metadata';
        case 'row_group_meta':
            return `Row Group ${node.index} metadata`;
        case 'chunk_meta':
            return `Column metadata ${node.name}`;
        case 'kv_meta':
            return 'Key-value metadata';
        case 'kv_entry':
            return node.name;
    }
}

/** Recursively find a node by id (selection restore, tests). */
export function findNode(root: SegmentNode, id: string): SegmentNode | null {
    if (root.id === id) {
        return root;
    }
    for (const child of root.children) {
        const found = findNode(child, id);
        if (found) {
            return found;
        }
    }
    return null;
}
