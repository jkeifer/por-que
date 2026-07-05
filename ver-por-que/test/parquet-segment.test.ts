import { describe, it, expect } from 'vitest';
import { ParquetSegment } from '../src/domain/parquet-segment';
import { formatBytes } from '../src/format';

describe('ParquetSegment', () => {
    it('computes size from start/end', () => {
        const seg = new ParquetSegment({ id: 's', name: 'S', start: 100, end: 250 });
        expect(seg.size).toBe(150);
        expect(seg.formattedSize).toBe(formatBytes(150));
    });

    it('clamps size to zero when end === start', () => {
        const seg = new ParquetSegment({ id: 's', name: 'S', start: 10, end: 10 });
        expect(seg.size).toBe(0);
    });

    it('throws when a required property is missing', () => {
        expect(() => new ParquetSegment({ name: 'S', start: 0, end: 1 } as never)).toThrow(
            /Required property 'id'/
        );
    });

    it('throws when end < start', () => {
        expect(() => new ParquetSegment({ id: 's', name: 'S', start: 10, end: 5 })).toThrow(
            /cannot be less than start/
        );
    });

    it('throws on a negative start offset', () => {
        expect(() => new ParquetSegment({ id: 's', name: 'S', start: -1, end: 5 })).toThrow(
            /cannot be negative/
        );
    });

    it('clones with overrides while preserving other fields', () => {
        const seg = new ParquetSegment({
            id: 's',
            name: 'S',
            start: 0,
            end: 100,
            rowGroupIndex: 2,
        });
        const clone = seg.clone({ name: 'S2' });
        expect(clone.name).toBe('S2');
        expect(clone.id).toBe('s');
        expect(clone.rowGroupIndex).toBe(2);
        expect(clone).not.toBe(seg);
    });
});
