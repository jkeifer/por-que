import { describe, it, expect } from 'vitest';
import { SegmentLayoutCalculator } from '../src/business/segment-layout-calculator';
import { VisualizationConfig } from '../src/config/visualization-config';
import type { SegmentNode } from '../src/business/segment-tree';

const config = VisualizationConfig.LAYOUT;

/** Build contiguous leaf segments with the given byte sizes. */
function segmentsOfSizes(sizes: number[]): SegmentNode[] {
    let offset = 0;
    return sizes.map((size, i) => {
        const seg: SegmentNode = {
            kind: 'footer',
            id: `s${i}`,
            name: `s${i}`,
            start: offset,
            end: offset + size,
            children: [],
        };
        offset += size;
        return seg;
    });
}

describe('calculateSegmentWidths', () => {
    it('returns [] for no segments', () => {
        expect(SegmentLayoutCalculator.calculateSegmentWidths([], 1000, config)).toEqual([]);
    });

    it('proportional widths roughly sum to the container width', () => {
        const layouts = SegmentLayoutCalculator.calculateSegmentWidths(
            segmentsOfSizes([250, 250, 250, 250]),
            1000,
            config
        );
        const total = layouts.reduce((sum, l) => sum + l.width, 0);
        expect(total).toBeCloseTo(1000, 5);
    });

    it('expands a tiny segment below its minimum width', () => {
        const layouts = SegmentLayoutCalculator.calculateSegmentWidths(
            segmentsOfSizes([1, 1000, 1000, 1000]),
            1000,
            config
        );
        expect(layouts[0]!.isExpanded).toBe(true);
        expect(layouts[0]!.width).toBeGreaterThan(layouts[0]!.naturalWidth);
    });
});

describe('calculateLogarithmicMinWidths', () => {
    it('is monotonic in segment size', () => {
        const segments = segmentsOfSizes([10, 100, 1000, 10000]);
        const widths = SegmentLayoutCalculator.calculateLogarithmicMinWidths(
            segments,
            11110,
            1000,
            config
        );
        for (let i = 1; i < widths.length; i++) {
            expect(widths[i]!).toBeGreaterThanOrEqual(widths[i - 1]!);
        }
    });

    it('falls back to uniform widths when all sizes are equal', () => {
        const segments = segmentsOfSizes([100, 100, 100]);
        const widths = SegmentLayoutCalculator.calculateLogarithmicMinWidths(
            segments,
            300,
            1000,
            config
        );
        expect(new Set(widths).size).toBe(1);
    });
});

describe('computeLevelLayout', () => {
    it('produces an empty level layout when there are no segments', () => {
        const layout = SegmentLayoutCalculator.computeLevelLayout(
            'overview',
            null,
            [],
            0,
            1000,
            config
        );
        expect(layout.segments).toEqual([]);
        expect(layout.width).toBe(1000);
    });
});
