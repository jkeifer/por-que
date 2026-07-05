import { describe, it, expect } from 'vitest';
import { isParquet } from '../src/detect';

const PAR1 = new Uint8Array([0x50, 0x41, 0x52, 0x31]);
const JSON_HEAD = new Uint8Array([0x7b, 0x22, 0x73, 0x22]); // '{"s'

describe('isParquet', () => {
    it('detects the PAR1 magic number', () => {
        expect(isParquet(PAR1)).toBe(true);
    });

    it('rejects JSON bytes', () => {
        expect(isParquet(JSON_HEAD, 'dump.json')).toBe(false);
    });

    it('magic wins even with a .json name', () => {
        expect(isParquet(PAR1, 'mislabeled.json')).toBe(true);
    });

    it('falls back to the .parquet extension when the header is inconclusive', () => {
        expect(isParquet(JSON_HEAD, 'data.parquet')).toBe(true);
        expect(isParquet(new Uint8Array([]), 'data.parquet')).toBe(true);
    });

    it('is case-insensitive on the extension', () => {
        expect(isParquet(JSON_HEAD, 'DATA.PARQUET')).toBe(true);
    });

    it('rejects a short buffer with no name', () => {
        expect(isParquet(new Uint8Array([0x50, 0x41]))).toBe(false);
    });
});
