/**
 * Info Panel Manager
 *
 * One declarative registry maps each segment `kind` to the sections it shows.
 * A single renderer turns sections into HTML. No per-kind generate* methods, no
 * shape-sniffing dispatch — the node's kind already says what it is.
 */
import { formatBytes, formatNumber, formatOffset } from '../format';
import { logicalTypeLabel, displayType } from '../domain/parquet-type-resolver';
import { describe, type Kind, type SegmentNode } from '../business/segment-tree';
import type { AnyDump, ColumnStatistics, SchemaGroup, SchemaLeaf, SchemaRoot } from '../types';

/** A metadata-only export lacks the physical `column_chunks` array. */
function isMetadataOnly(dump: AnyDump): boolean {
    return !('column_chunks' in dump);
}

/** Column-chunk count from either root (physical array or footer row groups). */
function columnChunkCount(dump: AnyDump): number {
    if ('column_chunks' in dump) {
        return dump.column_chunks.length;
    }
    return dump.metadata.row_groups.reduce((n, g) => n + Object.keys(g.column_chunks).length, 0);
}

const METADATA_ONLY_NOTE =
    'Page detail is not in a metadata-only dump — load the original .parquet to see pages.';

type Row = [string, string | number];
interface Section {
    title: string;
    rows?: Row[];
    html?: string;
}

type Handler<K extends Kind> = (
    node: Extract<SegmentNode, { kind: K }>,
    dump: AnyDump
) => Section[];
type Registry = { [K in Kind]: Handler<K> };

/** Standard Start/End/Size section every physical segment shows. */
function layout(node: SegmentNode): Section {
    return {
        title: 'Physical Layout',
        rows: [
            ['Start Offset', formatOffset(node.start)],
            ['End Offset', formatOffset(node.end)],
            ['Size', formatBytes(node.end - node.start)],
        ],
    };
}

/** True when a nullable/optional value is actually set. */
function present<T>(v: T | null | undefined): v is T {
    return v !== null && v !== undefined;
}

function ratio(compressed: number, uncompressed: number): string {
    return uncompressed > 0 ? `${((compressed / uncompressed) * 100).toFixed(1)}%` : 'N/A';
}

function countColumns(node: SchemaRoot | SchemaGroup | SchemaLeaf): number {
    const children = 'children' in node ? node.children : undefined;
    if (!children) {
        return node.element_type === 'group' || node.element_type === 'root' ? 0 : 1;
    }
    return Object.values(children).reduce((sum, c) => sum + countColumns(c), 0);
}

function statRows(stats: ColumnStatistics): Row[] {
    const val = (v: unknown): string => {
        if (v === null || v === undefined) {
            return 'N/A';
        }
        const s = String(v);
        return s.length > 50 ? `${s.slice(0, 47)}...` : s;
    };
    return [
        ['Min Value', val(stats.min_value)],
        ['Max Value', val(stats.max_value)],
        ['Null Count', stats.null_count === null ? 'N/A' : formatNumber(stats.null_count)],
        [
            'Distinct Count',
            stats.distinct_count === null || stats.distinct_count === undefined
                ? 'N/A'
                : formatNumber(stats.distinct_count),
        ],
    ];
}

/** Aggregate page-type / encoding counts across all data pages (overview). */
function pageSummary(dump: AnyDump): Section | null {
    // A metadata-only export carries no page structure to summarize.
    if (!('column_chunks' in dump)) {
        return { title: 'Data Pages', rows: [['Detail', METADATA_ONLY_NOTE]] };
    }
    if (dump.column_chunks.length === 0) {
        return null;
    }
    const pageTypes: Record<string, number> = {};
    const encodings: Record<string, number> = {};
    let total = 0;
    let bytes = 0;
    const bump = (m: Record<string, number>, k: string): void => {
        m[k] = (m[k] ?? 0) + 1;
    };
    for (const chunk of dump.column_chunks) {
        if (chunk.dictionary_page) {
            total++;
            bytes += chunk.dictionary_page.compressed_page_size;
            bump(pageTypes, 'DICTIONARY_PAGE');
            bump(encodings, chunk.dictionary_page.encoding);
        }
        for (const p of chunk.data_pages) {
            total++;
            bytes += p.compressed_page_size;
            bump(pageTypes, p.page_type ?? 'DATA_PAGE');
            bump(encodings, p.encoding);
        }
        for (const p of chunk.index_pages) {
            total++;
            bytes += p.compressed_page_size;
            bump(pageTypes, 'INDEX_PAGE');
        }
    }
    const pct = (n: number): string => (total > 0 ? ((n / total) * 100).toFixed(1) : '0.0');
    const rows: Row[] = [
        ['Total Pages', formatNumber(total)],
        ['Average Page Size', formatBytes(total > 0 ? bytes / total : 0)],
    ];
    const list = (m: Record<string, number>): string =>
        Object.entries(m)
            .map(([k, n]) => `${k}: ${formatNumber(n)} (${pct(n)}%)`)
            .join('<br>');
    if (Object.keys(pageTypes).length > 0) {
        rows.push(['Page Types', list(pageTypes)]);
    }
    if (Object.keys(encodings).length > 0) {
        rows.push(['Encodings', list(encodings)]);
    }
    return { title: 'Data Page Summary', rows };
}

// -- JSON value viewer (kept for key-value entries) --------------------------

function escapeHtml(text: string): string {
    return text.replace(
        /[&<>"']/g,
        c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!
    );
}

function renderJson(obj: unknown, depth = 0): string {
    const indent = '  '.repeat(depth);
    if (Array.isArray(obj)) {
        if (obj.length === 0) {
            return '<span class="json-empty-array">[]</span>';
        }
        const items = obj
            .map(
                (item, i) =>
                    `${indent}  ${renderJson(item, depth + 1)}${i < obj.length - 1 ? '<span class="json-comma">,</span>' : ''}`
            )
            .join('<br>');
        return `<span class="json-bracket">[</span><br>${items}<br>${indent}<span class="json-bracket">]</span>`;
    }
    if (obj !== null && typeof obj === 'object') {
        const rec = obj as Record<string, unknown>;
        const keys = Object.keys(rec);
        if (keys.length === 0) {
            return '<span class="json-empty-object">{}</span>';
        }
        const items = keys
            .map(
                (k, i) =>
                    `${indent}  <span class="json-key">"${escapeHtml(k)}"</span><span class="json-colon">: </span>${renderJson(rec[k], depth + 1)}${i < keys.length - 1 ? '<span class="json-comma">,</span>' : ''}`
            )
            .join('<br>');
        return `<span class="json-bracket">{</span><br>${items}<br>${indent}<span class="json-bracket">}</span>`;
    }
    if (obj === null) {
        return '<span class="json-null">null</span>';
    }
    if (typeof obj === 'string') {
        return `<span class="json-string">"${escapeHtml(obj)}"</span>`;
    }
    return `<span class="json-${typeof obj}">${escapeHtml(String(obj))}</span>`;
}

function valueViewer(value: string): Section {
    try {
        const parsed: unknown = JSON.parse(value);
        if (parsed && typeof parsed === 'object') {
            return {
                title: 'Value (JSON)',
                html: `<div class="value-viewer json-viewer"><div class="json-tree">${renderJson(parsed)}</div></div>`,
            };
        }
    } catch {
        // not JSON; fall through to plain text
    }
    return {
        title: 'Value',
        html: `<div class="value-viewer code-viewer"><pre><code>${escapeHtml(value)}</code></pre></div>`,
    };
}

// -- The registry ------------------------------------------------------------

const PANELS: Registry = {
    file: (_node, dump) => {
        const m = dump.metadata;
        const sections: Section[] = [
            {
                title: 'File Information',
                rows: [
                    ['Source', dump.source],
                    ['Total Size', formatBytes(dump.filesize)],
                    ['Mode', isMetadataOnly(dump) ? 'Metadata-only export' : 'Full dump'],
                    ['Parquet Version', m.version],
                    ['Created By', m.created_by ?? 'Unknown'],
                ],
            },
            {
                title: 'Schema Summary',
                rows: [
                    ['Total Columns', formatNumber(m.column_count)],
                    ['Total Rows', formatNumber(m.row_count)],
                    ['Row Groups', formatNumber(m.row_group_count)],
                ],
            },
            {
                title: 'Compression',
                rows: [
                    ['Compressed Size', formatBytes(m.compression_stats.total_compressed)],
                    ['Uncompressed Size', formatBytes(m.compression_stats.total_uncompressed)],
                    ['Compression Ratio', `${(m.compression_stats.ratio * 100).toFixed(1)}%`],
                    ['Space Saved', `${m.compression_stats.space_saved_percent.toFixed(1)}%`],
                ],
            },
        ];
        const summary = pageSummary(dump);
        if (summary) {
            sections.push(summary);
        }
        return sections;
    },

    magic_header: node => [layout(node), { title: 'Magic', rows: [['Bytes', node.text]] }],
    magic_footer: node => [layout(node), { title: 'Magic', rows: [['Bytes', node.text]] }],

    footer: node => [
        layout(node),
        {
            title: 'Footer',
            rows: [['Purpose', 'Length of the thrift file metadata, in bytes']],
        },
    ],

    data_region: (node, dump) => [
        layout(node),
        {
            title: 'Data Region',
            rows: [
                ['Row Groups', formatNumber(dump.metadata.row_group_count)],
                ['Column Chunks', formatNumber(columnChunkCount(dump))],
            ],
        },
    ],

    metadata_region: node => {
        const m = node.meta;
        return [
            layout(node),
            {
                title: 'File Metadata',
                rows: [
                    ['Version', m.version],
                    ['Created By', m.created_by ?? 'Unknown'],
                    ['Columns', m.column_count],
                    ['Rows', formatNumber(m.row_count)],
                    ['Row Groups', m.row_group_count],
                ],
            },
        ];
    },

    row_group: node => {
        const rows: Row[] = [['Row Group Index', node.index]];
        if (node.group) {
            const cs = node.group.compression_stats;
            rows.push(
                ['Row Count', formatNumber(node.group.row_count)],
                ['Column Count', Object.keys(node.group.column_chunks).length],
                ['Compressed Size', formatBytes(cs.total_compressed)],
                ['Uncompressed Size', formatBytes(cs.total_uncompressed)],
                ['Compression Ratio', ratio(cs.total_compressed, cs.total_uncompressed)]
            );
        }
        return [layout(node), { title: 'Row Group', rows }];
    },

    column_chunk: node => {
        const { chunk, meta, leaf } = node;
        const sections: Section[] = [
            layout(node),
            {
                title: 'Data Types',
                rows: [
                    ['Physical Type', meta?.type ?? leaf?.type ?? 'Unknown'],
                    ['Logical Type', (leaf && logicalTypeLabel(leaf.logical_type)) ?? 'None'],
                    ['Converted Type', leaf?.converted_type ?? 'None'],
                    ['Display Type', leaf ? displayType(leaf) : (meta?.type ?? 'Unknown')],
                ],
            },
            {
                title: 'Compression & Encoding',
                rows: [
                    ['Codec', chunk?.codec ?? meta?.codec ?? 'Unknown'],
                    ...(meta
                        ? ([
                              ['Compressed Size', formatBytes(meta.total_compressed_size)],
                              ['Uncompressed Size', formatBytes(meta.total_uncompressed_size)],
                              [
                                  'Compression Ratio',
                                  ratio(meta.total_compressed_size, meta.total_uncompressed_size),
                              ],
                              ['Encodings', meta.encodings.join(', ')],
                          ] as Row[])
                        : []),
                ],
            },
        ];
        const values = chunk?.num_values ?? meta?.num_values;
        const pageRows: Row[] = present(values) ? [['Values', formatNumber(values)]] : [];
        if (meta) {
            pageRows.push(['Data Page Offset', formatOffset(meta.data_page_offset)]);
            if (present(meta.dictionary_page_offset)) {
                pageRows.push([
                    'Dictionary Page Offset',
                    formatOffset(meta.dictionary_page_offset),
                ]);
            }
        }
        // chunk === null => metadata-only export: page structure isn't in the dump.
        if (chunk) {
            const pageCount =
                (chunk.dictionary_page ? 1 : 0) +
                chunk.data_pages.length +
                chunk.index_pages.length;
            pageRows.push(['Total Pages', formatNumber(pageCount)]);
            sections.push({ title: 'Page Layout', rows: pageRows });
        } else {
            sections.push({ title: 'Page Layout', rows: pageRows });
            sections.push({ title: 'Pages', rows: [['Detail', METADATA_ONLY_NOTE]] });
        }
        return sections;
    },

    dictionary_page: node => [
        layout(node),
        {
            title: 'Dictionary Page',
            rows: [
                ['Page Type', node.page.page_type ?? 'DICTIONARY_PAGE'],
                ['Encoding', node.page.encoding],
                ['Values', formatNumber(node.page.num_values)],
                ['Header Size', formatBytes(node.page.header_size)],
                ['Compressed Size', formatBytes(node.page.compressed_page_size)],
                ['Uncompressed Size', formatBytes(node.page.uncompressed_page_size)],
                [
                    'Compression Ratio',
                    ratio(node.page.compressed_page_size, node.page.uncompressed_page_size),
                ],
            ],
        },
    ],

    data_page: node => {
        const p = node.page;
        const rows: Row[] = [
            ['Page Type', p.page_type ?? 'DATA_PAGE'],
            ['Encoding', p.encoding],
            ['Values', formatNumber(p.num_values)],
            ['Header Size', formatBytes(p.header_size)],
            ['Compressed Size', formatBytes(p.compressed_page_size)],
            ['Uncompressed Size', formatBytes(p.uncompressed_page_size)],
            ['Compression Ratio', ratio(p.compressed_page_size, p.uncompressed_page_size)],
        ];
        const sections: Section[] = [layout(node), { title: 'Data Page', rows }];
        if (p.statistics) {
            sections.push({ title: 'Page Statistics', rows: statRows(p.statistics) });
        }
        return sections;
    },

    index_page: node => [
        layout(node),
        {
            title: 'Index Page',
            rows: [
                ['Page Type', node.page.page_type ?? 'INDEX_PAGE'],
                ['Header Size', formatBytes(node.page.header_size)],
                ['Compressed Size', formatBytes(node.page.compressed_page_size)],
                ['Uncompressed Size', formatBytes(node.page.uncompressed_page_size)],
            ],
        },
    ],

    column_index: node => [
        layout(node),
        {
            title: 'Column Index',
            rows: [
                ['Column', node.path],
                // index === null in a metadata-only export: span only, no contents.
                ...(node.index
                    ? ([
                          ['Boundary Order', node.index.boundary_order],
                          ['Pages', formatNumber(node.index.null_pages.length)],
                      ] as Row[])
                    : ([['Detail', METADATA_ONLY_NOTE]] as Row[])),
                ['Purpose', 'Per-page min/max and null stats for predicate push-down'],
            ],
        },
    ],

    offset_index: node => [
        layout(node),
        {
            title: 'Offset Index',
            rows: [
                ['Column', node.path],
                ...(node.index
                    ? ([
                          ['Page Locations', formatNumber(node.index.page_locations.length)],
                      ] as Row[])
                    : ([['Detail', METADATA_ONLY_NOTE]] as Row[])),
                ['Purpose', 'Page byte offsets and row ranges for seeking'],
            ],
        },
    ],

    bloom_filter: node => [
        layout(node),
        {
            title: 'Bloom Filter',
            rows: [
                ['Column', node.path],
                ['Row Group', node.rowGroup],
                ['Purpose', 'Probabilistic membership test to skip row groups on equality lookups'],
            ],
        },
    ],

    schema_root: node => [
        layout(node),
        {
            title: 'Schema Root',
            rows: [
                ['Name', node.node.name],
                ['Children', node.node.num_children],
                ['Total Columns', formatNumber(countColumns(node.node))],
            ],
        },
    ],

    schema_group: node => {
        const g = node.node;
        const rows: Row[] = [
            ['Name', g.name],
            ['Repetition', g.repetition],
            ['Children', g.num_children],
        ];
        if (present(g.field_id)) {
            rows.push(['Field ID', g.field_id]);
        }
        if (g.converted_type) {
            rows.push(['Converted Type', g.converted_type]);
        }
        const logical = logicalTypeLabel(g.logical_type);
        if (logical) {
            rows.push(['Logical Type', logical]);
        }
        return [layout(node), { title: 'Schema Group', rows }];
    },

    schema_leaf: node => {
        const l = node.node;
        const rows: Row[] = [
            ['Name', l.name],
            ['Physical Type', l.type],
            ['Repetition', l.repetition],
            ['Logical Type', logicalTypeLabel(l.logical_type) ?? 'None'],
            ['Converted Type', l.converted_type ?? 'None'],
        ];
        if (present(l.field_id)) {
            rows.push(['Field ID', l.field_id]);
        }
        if (present(l.type_length)) {
            rows.push(['Type Length', l.type_length]);
        }
        if (present(l.precision)) {
            rows.push(['Precision', l.precision]);
        }
        if (present(l.scale)) {
            rows.push(['Scale', l.scale]);
        }
        return [layout(node), { title: 'Schema Column', rows }];
    },

    row_groups_meta: node => {
        const totalRows = node.groups.reduce((s, g) => s + g.row_count, 0);
        const compressed = node.groups.reduce(
            (s, g) => s + g.compression_stats.total_compressed,
            0
        );
        const uncompressed = node.groups.reduce(
            (s, g) => s + g.compression_stats.total_uncompressed,
            0
        );
        return [
            layout(node),
            {
                title: 'Row Group Metadata',
                rows: [
                    ['Row Groups', node.groups.length],
                    ['Total Rows', formatNumber(totalRows)],
                    ['Total Compressed', formatBytes(compressed)],
                    ['Total Uncompressed', formatBytes(uncompressed)],
                    ['Compression Ratio', ratio(compressed, uncompressed)],
                ],
            },
        ];
    },

    row_group_meta: node => {
        const cs = node.group.compression_stats;
        return [
            layout(node),
            {
                title: `Row Group ${node.index}`,
                rows: [
                    ['Row Count', formatNumber(node.group.row_count)],
                    ['Column Count', Object.keys(node.group.column_chunks).length],
                    ['Compressed Size', formatBytes(cs.total_compressed)],
                    ['Uncompressed Size', formatBytes(cs.total_uncompressed)],
                ],
            },
        ];
    },

    chunk_meta: node => {
        const m = node.meta;
        const sections: Section[] = [
            layout(node),
            {
                title: 'Column Metadata',
                rows: [
                    ['Column', node.path],
                    ['Physical Type', m.type],
                    ['Codec', m.codec],
                    ['Encodings', m.encodings.join(', ')],
                    ['Values', formatNumber(m.num_values)],
                    ['Compressed Size', formatBytes(m.total_compressed_size)],
                    ['Uncompressed Size', formatBytes(m.total_uncompressed_size)],
                    ['Data Page Offset', formatOffset(m.data_page_offset)],
                ],
            },
        ];
        if (m.statistics) {
            sections.push({ title: 'Statistics', rows: statRows(m.statistics) });
        }
        return sections;
    },

    kv_meta: node => [
        layout(node),
        {
            title: 'Key-Value Metadata',
            rows: node.entries.map(e => [
                e.key,
                e.value.length > 50 ? `${e.value.slice(0, 47)}...` : e.value,
            ]),
        },
    ],

    kv_entry: node => [
        layout(node),
        { title: 'Key-Value Metadata', rows: [['Key', node.entry.key]] },
        valueViewer(node.entry.value),
    ],
};

/** Kinds that have a panel handler (exhaustiveness checks). */
export const PANEL_KINDS = new Set<Kind>(Object.keys(PANELS) as Kind[]);

export class InfoPanelManager {
    private container: HTMLElement;
    private infoPanel: HTMLElement;

    constructor(container: HTMLElement) {
        this.container = container;
        this.container.innerHTML = '';
        this.infoPanel = document.createElement('div');
        this.infoPanel.className = 'info-panel';
        this.infoPanel.style.display = 'none';
        this.container.appendChild(this.infoPanel);
    }

    /** Render the panel for a segment node (the root node renders the overview). */
    show(node: SegmentNode, dump: AnyDump): void {
        // ponytail: registry keys are correlated with node.kind by construction;
        // the mapped type can't prove that at the call site, so widen once here.
        const handler = PANELS[node.kind] as (n: SegmentNode, d: AnyDump) => Section[];
        const heading = node.kind === 'file' ? 'File Overview' : describe(node);
        this.infoPanel.style.display = 'block';
        this.infoPanel.innerHTML = `<h3>${heading}</h3><div class="info-sections">${handler(
            node,
            dump
        )
            .map(s => this.renderSection(s))
            .join('')}</div>`;
    }

    hide(): void {
        this.infoPanel.style.display = 'none';
    }

    private renderSection(section: Section): string {
        const body =
            section.html ??
            `<div class="info-grid">${(section.rows ?? [])
                .map(
                    ([label, value]) =>
                        `<div class="info-item"><span class="info-label">${label}:</span><span class="info-value">${value}</span></div>`
                )
                .join('')}</div>`;
        const card = section.html ? 'large-card' : 'regular-card';
        return `<div class="info-section ${card}"><h5 class="info-section-title">${section.title}</h5>${body}</div>`;
    }
}
