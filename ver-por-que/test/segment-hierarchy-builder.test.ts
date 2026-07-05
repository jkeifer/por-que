import { describe, it, expect } from 'vitest';
import { SegmentHierarchyBuilder } from '../src/business/segment-hierarchy-builder';
import type { FileData } from '../src/types';

/** Small hand-written fixture: 1 row group, 2 column chunks, pages incl. dict. */
const fixture: FileData = {
    source: 'test',
    filesize: 1000,
    column_chunks: [
        {
            path_in_schema: 'a',
            start_offset: 4,
            total_byte_size: 100,
            codec: 1,
            num_values: 10,
            dictionary_page: { start_offset: 4, header_size: 2, compressed_page_size: 20 },
            data_pages: [{ start_offset: 26, header_size: 2, compressed_page_size: 40 }],
            index_pages: [],
        },
        {
            path_in_schema: 'b',
            start_offset: 104,
            total_byte_size: 100,
            codec: 1,
            num_values: 10,
            dictionary_page: null,
            data_pages: [{ start_offset: 104, header_size: 2, compressed_page_size: 50 }],
            index_pages: [],
        },
    ],
    metadata: {
        version: 2,
        created_by: 'test',
        start_offset: 500,
        total_byte_size: 200,
        schema_root: {
            name: 'root',
            element_type: 'root',
            start_offset: 500,
            byte_length: 10,
            num_children: 2,
            children: {
                a: {
                    name: 'a',
                    element_type: 'column',
                    type: 6,
                    start_offset: 510,
                    byte_length: 5,
                },
                b: {
                    name: 'b',
                    element_type: 'column',
                    type: 1,
                    start_offset: 515,
                    byte_length: 5,
                },
            },
        },
        row_groups: [
            {
                row_count: 10,
                byte_length: 50,
                column_chunks: {
                    a: { metadata: { type: 6, encodings: [0, 8], total_compressed_size: 40 } },
                    b: { metadata: { type: 1, encodings: [0] } },
                },
            },
        ],
    },
};

describe('buildOverviewSegments', () => {
    const overview = SegmentHierarchyBuilder.buildOverviewSegments(fixture);

    it('produces the five file-structure segments', () => {
        expect(overview.map(s => s.id)).toEqual([
            'header_magic',
            'rowgroups',
            'metadata',
            'footer',
            'footer_magic',
        ]);
    });

    it('anchors magic + footer boundaries to the file size', () => {
        const header = overview[0]!;
        const footerMagic = overview[4]!;
        const footer = overview[3]!;
        expect([header.start, header.end]).toEqual([0, 4]);
        expect([footerMagic.start, footerMagic.end]).toEqual([996, 1000]);
        expect([footer.start, footer.end]).toEqual([992, 996]);
    });

    it('spans data pages across the column chunk offsets', () => {
        const dataPages = overview[1]!;
        expect(dataPages.start).toBe(4);
        expect(dataPages.end).toBe(204);
    });
});

describe('buildAll', () => {
    const cache = SegmentHierarchyBuilder.buildAll(fixture);

    it('builds one row group segment', () => {
        expect(cache.rowgroups.map(s => s.id)).toEqual(['rowgroup_0']);
    });

    it('orders column chunks by start offset', () => {
        const chunks = cache.columnchunks[0]!;
        expect(chunks.map(s => s.name)).toEqual(['a', 'b']);
    });

    it('builds pages including the dictionary page first with correct offsets', () => {
        const pages = cache.pages['0_0']!;
        expect(pages.map(s => s.name)).toEqual(['DICT', 'DATA0']);
        expect([pages[0]!.start, pages[0]!.end]).toEqual([4, 26]);
        expect([pages[1]!.start, pages[1]!.end]).toEqual([26, 68]);
    });

    it('resolves child levels through getSegmentsForLevel', () => {
        const chunks = SegmentHierarchyBuilder.getSegmentsForLevel(
            cache,
            'columnchunks',
            'rowgroup_0'
        );
        expect(chunks).toHaveLength(2);

        const pages = SegmentHierarchyBuilder.getSegmentsForLevel(cache, 'pages', 'chunk_0_0');
        expect(pages).toHaveLength(2);
    });

    it('finds a segment by id across levels', () => {
        expect(SegmentHierarchyBuilder.findSegment(cache, 'rowgroup_0')?.id).toBe('rowgroup_0');
        expect(SegmentHierarchyBuilder.findSegment(cache, 'nope')).toBeNull();
    });
});
