import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import validate from '../src/generated/validate.js';
import {
    projectDump,
    findNode,
    describe as describeNode,
    KINDS,
    type Kind,
    type SegmentNode,
} from '../src/business/segment-tree';
import { VisualizationConfig } from '../src/config/visualization-config';
import { PANEL_KINDS } from '../src/components/info-panel-manager';
import type { Dump } from '../src/types';

const FIXTURES = fileURLToPath(new URL('../../tests/fixtures/metadata/', import.meta.url));

function load(name: string): Dump {
    const dump: unknown = JSON.parse(readFileSync(`${FIXTURES}${name}_expected.json`, 'utf8'));
    if (!validate(dump)) {
        throw new Error(`fixture ${name} failed schema validation`);
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
                validate(JSON.parse(readFileSync(`${FIXTURES}${f}_expected.json`, 'utf8')))
            ).toBe(true);
        }
    });

    it('rejects a mutated copy (required field removed)', () => {
        const dump = JSON.parse(readFileSync(`${FIXTURES}${KV}_expected.json`, 'utf8'));
        delete dump.metadata;
        expect(validate(dump)).toBe(false);
        expect(validate.errors?.length).toBeGreaterThan(0);
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
