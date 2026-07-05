import { describe, it, expect } from 'vitest';
import { formatBytes, formatNumber, formatOffset } from '../src/format';

describe('formatBytes', () => {
    it('returns "0 Bytes" for zero', () => {
        expect(formatBytes(0)).toBe('0 Bytes');
    });

    it('formats exact KB', () => {
        expect(formatBytes(1024)).toBe('1 KB');
    });

    it('formats fractional KB', () => {
        expect(formatBytes(1536)).toBe('1.5 KB');
    });

    it('formats MB', () => {
        expect(formatBytes(1024 * 1024)).toBe('1 MB');
    });

    it('respects the decimals argument', () => {
        expect(formatBytes(1536, 0)).toBe('2 KB');
    });
});

describe('formatNumber', () => {
    it('returns N/A for nullish', () => {
        expect(formatNumber(null)).toBe('N/A');
        expect(formatNumber(undefined)).toBe('N/A');
    });

    it('groups thousands with locale separators', () => {
        expect(formatNumber(1000)).toBe('1,000');
    });
});

describe('formatOffset', () => {
    it('leaves small numbers untouched', () => {
        expect(formatOffset(0)).toBe('0');
        expect(formatOffset(42)).toBe('42');
    });

    it('groups thousands with underscores', () => {
        expect(formatOffset(1234567)).toBe('1_234_567');
        expect(formatOffset(1000)).toBe('1_000');
    });
});
