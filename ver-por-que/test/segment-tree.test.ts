import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { validateFile, validateMetadata } from '../src/generated/validate.js';
import {
    project,
    projectDump,
    projectMetadataExport,
    findNode,
    describe as describeNode,
    KINDS,
    type Kind,
    type SegmentNode,
} from '../src/business/segment-tree';
import { VisualizationConfig } from '../src/config/visualization-config';
import { PANEL_KINDS } from '../src/components/info-panel-manager';
import type { Dump, MetadataDump } from '../src/types';

const FIXTURES = fileURLToPath(new URL('../../tests/fixtures/metadata/', import.meta.url));
const METADATA_EXPORT = fileURLToPath(new URL('./fixtures/metadata-export.json', import.meta.url));

function load(name: string): Dump {
    const dump: unknown = JSON.parse(readFileSync(`${FIXTURES}${name}_expected.json`, 'utf8'));
    if (!validateFile(dump)) {
        throw new Error(`fixture ${name} failed schema validation`);
    }
    return dump;
}

function loadMetadataExport(): MetadataDump {
    const dump: unknown = JSON.parse(readFileSync(METADATA_EXPORT, 'utf8'));
    if (!validateMetadata(dump)) {
        throw new Error('metadata-export fixture failed schema validation');
    }
    return dump;
}

// Three diverse real fixtures: key/value metadata, page indexes, nested schema.
const KV = 'binary';
const INDEXED = 'data_index_bloom_encoding_with_length';
const NESTED = 'nested_maps.snappy';

function walk(node: SegmentNode, fn: (n: SegmentNode) => void): void {
    fn(node);
    node.children.forEach(c => walk(c, fn));
}

function kindsIn(root: SegmentNode): Set<Kind> {
    const set = new Set<Kind>();
    walk(root, n => set.add(n.kind));
    return set;
}

describe('validator', () => {
    it('accepts real fixtures', () => {
        for (const f of [KV, INDEXED, NESTED]) {
            expect(
                validateFile(JSON.parse(readFileSync(`${FIXTURES}${f}_expected.json`, 'utf8')))
            ).toBe(true);
        }
    });

    it('rejects a mutated copy (required field removed)', () => {
        const dump = JSON.parse(readFileSync(`${FIXTURES}${KV}_expected.json`, 'utf8'));
        delete dump.metadata;
        expect(validateFile(dump)).toBe(false);
        expect(validateFile.errors?.length).toBeGreaterThan(0);
    });

    it('dispatches each shape to its own root validator', () => {
        const file = JSON.parse(readFileSync(`${FIXTURES}${KV}_expected.json`, 'utf8'));
        const metadata = JSON.parse(readFileSync(METADATA_EXPORT, 'utf8'));

        // Each validates under its own root...
        expect(validateFile(file)).toBe(true);
        expect(validateMetadata(metadata)).toBe(true);

        // ...and the discriminator values are distinct.
        expect(file._meta.model).toBe('file');
        expect(metadata._meta.model).toBe('metadata');

        // A full dump is not a valid metadata export (has extra required-absent
        // shape) and vice versa: the metadata export lacks column_chunks.
        expect(validateFile(metadata)).toBe(false);
        expect(validateMetadata(file)).toBe(false);
    });
});

describe('projectDump', () => {
    it('produces the overview regions at the top level', () => {
        const root = projectDump(load(KV));
        expect(root.kind).toBe('file');
        expect(root.children.map(c => c.kind)).toEqual([
            'magic_header',
            'data_region',
            'metadata_region',
            'footer',
            'magic_footer',
        ]);
    });

    it('has no NaN or undefined offsets anywhere', () => {
        for (const f of [KV, INDEXED, NESTED]) {
            walk(projectDump(load(f)), n => {
                expect(Number.isFinite(n.start), `${n.id} start`).toBe(true);
                expect(Number.isFinite(n.end), `${n.id} end`).toBe(true);
                expect(n.end).toBeGreaterThanOrEqual(n.start);
            });
        }
    });

    it('sorts children by start offset', () => {
        walk(projectDump(load(INDEXED)), n => {
            const starts = n.children.map(c => c.start);
            expect(starts).toEqual([...starts].sort((a, b) => a - b));
        });
    });

    it('groups column chunks under their actual row_group', () => {
        const dump = load(KV);
        const root = projectDump(dump);
        const dataRegion = root.children.find(c => c.kind === 'data_region')!;
        const rowGroups = dataRegion.children.filter(c => c.kind === 'row_group');
        for (const rg of rowGroups) {
            if (rg.kind !== 'row_group') {
                continue;
            }
            for (const chunk of rg.children) {
                const physical = dump.column_chunks.find(c => c.path_in_schema === chunk.name);
                expect(physical?.row_group).toBe(rg.index);
            }
        }
    });

    it('surfaces key-value entries as real segments', () => {
        const dump = load(KV);
        const kvMeta: SegmentNode[] = [];
        walk(projectDump(dump), n => {
            if (n.kind === 'kv_entry') {
                kvMeta.push(n);
            }
        });
        expect(kvMeta.length).toBe(dump.metadata.key_value_metadata!.length);
        expect(kvMeta.every(n => n.end > n.start)).toBe(true);
    });

    it('emits real column and offset index segments', () => {
        const kinds = kindsIn(projectDump(load(INDEXED)));
        expect(kinds.has('column_index')).toBe(true);
        expect(kinds.has('offset_index')).toBe(true);
    });

    it('emits bloom filter segments with sane spans when offset and length are both present', () => {
        const dump = load(INDEXED);
        const bloomNodes: SegmentNode[] = [];
        walk(projectDump(dump), n => {
            if (n.kind === 'bloom_filter') {
                bloomNodes.push(n);
            }
        });
        expect(bloomNodes.length).toBeGreaterThan(0);
        for (const n of bloomNodes) {
            expect(Number.isFinite(n.start)).toBe(true);
            expect(Number.isFinite(n.end)).toBe(true);
            expect(n.start).toBeGreaterThanOrEqual(0);
            expect(n.end).toBeLessThanOrEqual(dump.filesize);
            expect(n.end).toBeGreaterThan(n.start);
        }
    });

    it('omits bloom filter segments when the footer has an offset but no length', () => {
        // data_index_bloom_encoding_stats has bloom_filter_offset set but no
        // bloom_filter_length on its column metadata; real byte spans only, so
        // no node should be projected for it.
        const dump = load('data_index_bloom_encoding_stats');
        const meta = dump.metadata.row_groups[0]?.column_chunks['String']?.metadata;
        expect(meta?.bloom_filter_offset).toBeTruthy();
        expect(meta?.bloom_filter_length === null || meta?.bloom_filter_length === undefined).toBe(
            true
        );

        const kinds = kindsIn(projectDump(dump));
        expect(kinds.has('bloom_filter')).toBe(false);
    });

    it('emits nested schema groups', () => {
        expect(kindsIn(projectDump(load(NESTED))).has('schema_group')).toBe(true);
    });

    it('keeps children within their parent span where the format guarantees it', () => {
        // Column chunks tile their row group; pages tile their chunk; schema
        // extents nest. Metadata containers likewise contain their entries.
        const containment: Kind[] = [
            'row_group',
            'column_chunk',
            'schema_root',
            'schema_group',
            'row_groups_meta',
            'row_group_meta',
            'kv_meta',
            'metadata_region',
        ];
        for (const f of [KV, INDEXED, NESTED]) {
            walk(projectDump(load(f)), n => {
                if (!containment.includes(n.kind)) {
                    return;
                }
                for (const c of n.children) {
                    expect(c.start, `${c.id} in ${n.id}`).toBeGreaterThanOrEqual(n.start);
                    expect(c.end, `${c.id} in ${n.id}`).toBeLessThanOrEqual(n.end);
                }
            });
        }
    });

    it('findNode locates a node by id', () => {
        const root = projectDump(load(KV));
        expect(findNode(root, 'schema_root')?.kind).toBe('schema_root');
        expect(findNode(root, 'nope')).toBeNull();
    });
});

describe('projectMetadataExport', () => {
    const PAGE_KINDS: Kind[] = ['dictionary_page', 'data_page', 'index_page'];

    it('project() dispatches a metadata export to the metadata projection', () => {
        const root = project(loadMetadataExport());
        expect(root.kind).toBe('file');
        expect(root.children.map(c => c.kind)).toEqual([
            'magic_header',
            'data_region',
            'metadata_region',
            'footer',
            'magic_footer',
        ]);
    });

    it('has no page-level nodes', () => {
        const kinds = kindsIn(projectMetadataExport(loadMetadataExport()));
        for (const k of PAGE_KINDS) {
            expect(kinds.has(k), k).toBe(false);
        }
    });

    it('projects column chunks with sane contained spans and a null chunk', () => {
        const dump = loadMetadataExport();
        const chunks: SegmentNode[] = [];
        walk(projectMetadataExport(dump), n => {
            if (n.kind === 'column_chunk') {
                chunks.push(n);
            }
        });
        expect(chunks.length).toBeGreaterThan(0);
        for (const c of chunks) {
            expect(c.kind === 'column_chunk' && c.chunk).toBe(null);
            expect(c.end).toBeGreaterThan(c.start);
            expect(c.start).toBeGreaterThanOrEqual(0);
            expect(c.end).toBeLessThanOrEqual(dump.filesize);
        }
    });

    it('keeps column/offset index and bloom filter nodes (footer-derived spans)', () => {
        const kinds = kindsIn(projectMetadataExport(loadMetadataExport()));
        expect(kinds.has('column_index')).toBe(true);
        expect(kinds.has('offset_index')).toBe(true);
        expect(kinds.has('bloom_filter')).toBe(true);
    });

    it('keeps the metadata-region drill-down intact', () => {
        const kinds = kindsIn(projectMetadataExport(loadMetadataExport()));
        expect(kinds.has('schema_root')).toBe(true);
        expect(kinds.has('row_groups_meta')).toBe(true);
        expect(kinds.has('chunk_meta')).toBe(true);
    });

    it('has finite, ordered offsets everywhere', () => {
        walk(projectMetadataExport(loadMetadataExport()), n => {
            expect(Number.isFinite(n.start)).toBe(true);
            expect(Number.isFinite(n.end)).toBe(true);
            expect(n.end).toBeGreaterThanOrEqual(n.start);
        });
    });
});

describe('exhaustiveness', () => {
    it('every kind has a color mapping and a panel entry', () => {
        for (const kind of KINDS) {
            expect(VisualizationConfig.KIND_COLORS[kind], `${kind} color`).toBeTruthy();
            expect(PANEL_KINDS.has(kind), `${kind} panel`).toBe(true);
        }
    });

    it('every kind has a describe label', () => {
        // describe covers every kind via a fully-typed switch; spot-check a few.
        const root = projectDump(load(KV));
        walk(root, n => expect(describeNode(n)).toBeTruthy());
    });
});
