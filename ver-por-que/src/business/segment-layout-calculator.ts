/**
 * Segment Layout Calculator
 * Pure functions for calculating segment layouts and positioning.
 */
import { VisualizationConfig, type LayoutConfig } from '../config/visualization-config';
import type { ParquetSegment } from '../domain/parquet-segment';

export interface SegmentLayout {
    segment: ParquetSegment;
    x: number;
    y: number;
    width: number;
    height: number;
    widthPercent: number;
    isExpanded: boolean;
    naturalWidth: number;
    minWidth: number;
}

export interface LevelLayout {
    x: number;
    y: number;
    width: number;
    height: number;
    segments: SegmentLayout[];
    totalSize?: number;
    minStart?: number;
    maxEnd?: number;
    levelName: string;
    parentSegmentId: string | null;
    segmentCount?: number;
}

interface SegmentWidthData {
    segment: ParquetSegment;
    naturalWidthPercent: number;
    naturalWidthPixels: number;
    finalWidthPercent: number;
    finalWidthPixels: number;
    isExpanded: boolean;
    minWidth: number;
}

export class SegmentLayoutCalculator {
    /** Calculate proportional widths for segments. */
    static calculateSegmentWidths(
        segments: ParquetSegment[],
        containerWidth: number,
        config: LayoutConfig = VisualizationConfig.LAYOUT
    ): SegmentLayout[] {
        if (!segments || segments.length === 0) {
            return [];
        }

        const minStart = Math.min(...segments.map(s => s.start));
        const maxEnd = Math.max(...segments.map(s => s.end));
        const totalSize = maxEnd - minStart;

        const minWidths = this.calculateLogarithmicMinWidths(
            segments,
            totalSize,
            containerWidth,
            config
        );

        const segmentData: SegmentWidthData[] = segments.map((segment, index) => ({
            segment: segment,
            naturalWidthPercent: (segment.size / totalSize) * 100,
            naturalWidthPixels: (segment.size / totalSize) * containerWidth,
            finalWidthPercent: 0,
            finalWidthPixels: 0,
            isExpanded: false,
            minWidth: minWidths[index] ?? 0,
        }));

        segmentData.forEach(data => {
            const segmentMinWidthPercent = (data.minWidth / containerWidth) * 100;

            if (data.naturalWidthPixels < data.minWidth) {
                data.isExpanded = true;
                data.finalWidthPercent = segmentMinWidthPercent;
                data.finalWidthPixels = data.minWidth;
            } else {
                data.finalWidthPercent = data.naturalWidthPercent;
                data.finalWidthPixels = data.naturalWidthPixels;
            }
        });

        const expandedSegments = segmentData.filter(d => d.isExpanded);
        const extraSpaceUsed = expandedSegments.reduce((sum, data) => {
            return sum + (data.finalWidthPercent - data.naturalWidthPercent);
        }, 0);

        if (extraSpaceUsed > 0) {
            const availableSpace =
                100 - expandedSegments.reduce((sum, d) => sum + d.finalWidthPercent, 0);
            const naturalSpaceForNonExpanded = segmentData
                .filter(d => !d.isExpanded)
                .reduce((sum, d) => sum + d.naturalWidthPercent, 0);

            if (naturalSpaceForNonExpanded > 0) {
                const scaleFactor = availableSpace / naturalSpaceForNonExpanded;

                segmentData.forEach(data => {
                    if (!data.isExpanded) {
                        data.finalWidthPercent = data.naturalWidthPercent * scaleFactor;
                        data.finalWidthPixels = (data.finalWidthPercent / 100) * containerWidth;
                    }
                });
            }
        }

        return this.calculateSegmentPositions(segmentData, config);
    }

    /** Calculate logarithmic minimum widths for segments. */
    static calculateLogarithmicMinWidths(
        segments: ParquetSegment[],
        _totalSize: number,
        containerWidth: number,
        config: LayoutConfig
    ): number[] {
        const startingBaseline = config.STARTING_BASELINE || 5;
        const adjustedBaseline = Math.min(startingBaseline, containerWidth / segments.length);

        if (adjustedBaseline < config.MIN_SEGMENT_WIDTH) {
            return segments.map(() => containerWidth / segments.length);
        }

        const sizes = segments.map(s => Math.max(1, s.size));
        const minSize = Math.min(...sizes);
        const maxSize = Math.max(...sizes);

        if (minSize === maxSize) {
            return segments.map(() => adjustedBaseline);
        }

        const logMinSize = Math.log10(minSize);
        const logMaxSize = Math.log10(maxSize);
        const logRange = logMaxSize - logMinSize;

        const logarithmicWidths = sizes.map(size => {
            const logSize = Math.log10(size);
            const logPosition = (logSize - logMinSize) / logRange;
            const scaledPosition = Math.pow(logPosition, 1 / config.LOG_SCALE_FACTOR);
            return (
                adjustedBaseline +
                (config.MAX_MIN_SEGMENT_WIDTH - adjustedBaseline) * scaledPosition
            );
        });

        const totalLogarithmicWidth = logarithmicWidths.reduce((sum, width) => sum + width, 0);

        if (totalLogarithmicWidth > containerWidth) {
            const scalingFactor = containerWidth / totalLogarithmicWidth;
            return logarithmicWidths.map(width => width * scalingFactor);
        }

        return logarithmicWidths;
    }

    /** Calculate segment positions based on calculated widths. */
    static calculateSegmentPositions(
        segmentData: SegmentWidthData[],
        config: LayoutConfig
    ): SegmentLayout[] {
        let currentLeft = 0;

        return segmentData.map(data => {
            const layout: SegmentLayout = {
                segment: data.segment,
                x: currentLeft,
                y: config.SEGMENT_MARGIN,
                width: data.finalWidthPixels,
                height: config.LEVEL_HEIGHT - 2 * config.SEGMENT_MARGIN,
                widthPercent: data.finalWidthPercent,
                isExpanded: data.isExpanded,
                naturalWidth: data.naturalWidthPixels,
                minWidth: data.minWidth,
            };

            currentLeft += data.finalWidthPixels;
            return layout;
        });
    }

    /** Calculate layout for a complete level. */
    static computeLevelLayout(
        levelName: string,
        parentSegmentId: string | null,
        segments: ParquetSegment[],
        levelIndex: number,
        containerWidth: number,
        config: LayoutConfig
    ): LevelLayout {
        const y = levelIndex * (config.LEVEL_HEIGHT + config.LEVEL_SPACING);

        if (!segments || segments.length === 0) {
            return {
                x: 0,
                y,
                width: containerWidth,
                height: config.LEVEL_HEIGHT,
                segments: [],
                levelName: levelName,
                parentSegmentId: parentSegmentId,
            };
        }

        const segmentLayouts = this.calculateSegmentWidths(segments, containerWidth, config);

        const minStart = Math.min(...segments.map(s => s.start));
        const maxEnd = Math.max(...segments.map(s => s.end));
        const totalSize = maxEnd - minStart;

        return {
            x: 0,
            y: y,
            width: containerWidth,
            height: config.LEVEL_HEIGHT,
            segments: segmentLayouts,
            totalSize: totalSize,
            minStart: minStart,
            maxEnd: maxEnd,
            levelName: levelName,
            parentSegmentId: parentSegmentId,
            segmentCount: segments.length,
        };
    }
}
