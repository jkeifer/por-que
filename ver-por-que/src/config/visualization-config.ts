/**
 * Visualization Configuration
 * Centralized layout, color, and behavior constants plus segment color logic.
 */
import type { ParquetSegment } from '../domain/parquet-segment';

export interface LayoutConfig {
    LEVEL_HEIGHT: number;
    LEVEL_SPACING: number;
    TOP_MARGIN: number;
    SEGMENT_MARGIN: number;
    CORNER_RADIUS: number;
    MIN_SEGMENT_WIDTH: number;
    MAX_MIN_SEGMENT_WIDTH: number;
    LOG_SCALE_FACTOR: number;
    STARTING_BASELINE: number;
    DEFAULT_HEIGHT: number;
    MIN_WIDTH: number;
    MAX_WIDTH: number;
    CONNECTION_OPACITY: number;
    HIT_TEST_TOLERANCE: number;
}

interface RGB {
    r: number;
    g: number;
    b: number;
}

export class VisualizationConfig {
    /** Layout constants for the SVG visualizer. */
    static LAYOUT: LayoutConfig = {
        LEVEL_HEIGHT: 40,
        LEVEL_SPACING: 32,
        TOP_MARGIN: 20,
        SEGMENT_MARGIN: 2,
        CORNER_RADIUS: 0,
        MIN_SEGMENT_WIDTH: 3,
        MAX_MIN_SEGMENT_WIDTH: 25,
        LOG_SCALE_FACTOR: 2,
        STARTING_BASELINE: 5,
        DEFAULT_HEIGHT: 400,
        MIN_WIDTH: 300,
        MAX_WIDTH: 1200,
        CONNECTION_OPACITY: 0.3,
        HIT_TEST_TOLERANCE: 2,
    };

    /** Color scheme (only the default fallback is used directly). */
    static COLORS = {
        DEFAULT: '--text-secondary',
    };

    /** Tooltip positioning configuration. */
    static TOOLTIP = {
        OFFSET_X: 10,
        OFFSET_Y: -10,
        BOUNDARY_PADDING: 10,
        MIN_WIDTH: 200,
    };

    /**
     * Get CSS custom property name for a segment based on its semantic type.
     */
    static getSegmentColor(segment: ParquetSegment, segmentIndex = 0): string {
        const elementType = this._getSemanticElementType(segment);
        const colorFamily = this._getColorFamilyForElementType(elementType);
        const shadeIndex = segmentIndex % colorFamily.length;
        return colorFamily[shadeIndex] ?? VisualizationConfig.COLORS.DEFAULT;
    }

    private static _getSemanticElementType(segment: ParquetSegment): string {
        if (segment.id === 'header_magic' || segment.id === 'footer_magic') {
            return 'magic';
        }
        if (segment.id === 'footer') {
            return 'footer';
        }
        if (segment.id === 'metadata') {
            return 'metadata_container';
        }
        if (segment.id === 'rowgroups') {
            return 'rowgroups_container';
        }
        if (segment.id === 'schema_root') {
            return 'schema_group';
        }
        if (segment.metadata && segment.metadata.element_type === 'group') {
            return 'schema_group';
        }
        if (segment.metadata && segment.metadata.element_type === 'column') {
            return 'schema_element';
        }
        if (segment.id === 'row_groups_metadata') {
            return 'row_group_metadata';
        }
        if (segment.id === 'column_indices') {
            return 'column_index';
        }
        if (segment.metadata && segment.metadata.index_type) {
            return 'column_index';
        }
        if (segment.rowGroupIndex !== undefined && segment.chunkIndex === undefined) {
            return 'row_group';
        }
        if (segment.columnPath && segment.chunkIndex !== undefined) {
            return 'column_chunk';
        }
        if (segment.pageIndex !== undefined) {
            if (segment.name && segment.name.includes('DICT')) {
                return 'dictionary_page';
            } else if (segment.name && segment.name.includes('DATA')) {
                return 'data_page';
            } else if (segment.name && segment.name.includes('IDX')) {
                return 'index_page';
            }
            return 'page';
        }
        if (segment.id === 'key_value_metadata') {
            return 'key_value_metadata_container';
        }
        if (segment.id.startsWith('kv_') && segment.metadata?.key) {
            return 'key_value_metadata_entry';
        }
        if (segment.metadata && typeof segment.metadata === 'object') {
            return 'metadata_element';
        }
        return 'generic';
    }

    private static _getColorFamilyForElementType(elementType: string): string[] {
        const colorFamilies = {
            magic: ['--magic-color', '--magic-color', '--magic-color'],
            footer: ['--footer-color', '--footer-color', '--footer-color'],
            metadata_container: [
                '--metadata-container-color',
                '--metadata-container-color',
                '--metadata-container-color',
            ],
            rowgroups_container: ['--row-groups', '--row-groups', '--row-groups'],
            schema_group: ['--schema-group-light', '--schema-group-medium', '--schema-group-dark'],
            schema_element: [
                '--schema-element-light',
                '--schema-element-medium',
                '--schema-element-dark',
            ],
            row_group: ['--row-group-light', '--row-group-medium', '--row-group-dark'],
            column_chunk: ['--column-chunk-light', '--column-chunk-medium', '--column-chunk-dark'],
            data_page: ['--data-page-light', '--data-page-medium', '--data-page-dark'],
            dictionary_page: [
                '--dictionary-page-color',
                '--dictionary-page-color',
                '--dictionary-page-color',
            ],
            index_page: ['--index-page-light', '--index-page-medium', '--index-page-dark'],
            page: ['--generic-page-light', '--generic-page-medium', '--generic-page-dark'],
            row_group_metadata: ['--row-groups', '--row-groups', '--row-groups'],
            column_index: ['--column-index-light', '--column-index-medium', '--column-index-dark'],
            metadata_element: [
                '--metadata-element-light',
                '--metadata-element-medium',
                '--metadata-element-dark',
            ],
            key_value_metadata_container: ['--red-medium', '--red-medium', '--red-medium'],
            key_value_metadata_entry: ['--green-light', '--green-medium', '--green-dark'],
            generic: [
                '--generic-segment-color',
                '--generic-segment-color',
                '--generic-segment-color',
            ],
        };

        return colorFamilies[elementType as keyof typeof colorFamilies] ?? colorFamilies.generic;
    }

    /**
     * Get contrast class name based on background color luminance.
     */
    static getContrastClass(backgroundColor: string): string {
        let actualColor = backgroundColor;
        if (backgroundColor.startsWith('--')) {
            const computedColor = getComputedStyle(document.documentElement).getPropertyValue(
                backgroundColor
            );
            actualColor = computedColor.trim();
        }

        const rgb = this._parseColor(actualColor);
        if (!rgb) {
            return 'segment-on-light';
        }

        const luminance = this._calculateLuminance(rgb.r, rgb.g, rgb.b);
        return luminance < 0.4 ? 'segment-on-dark' : 'segment-on-light';
    }

    private static _parseColor(colorStr: string): RGB | null {
        if (colorStr.startsWith('#')) {
            const hex = colorStr.slice(1);
            if (hex.length === 3) {
                return {
                    r: parseInt(hex[0]! + hex[0]!, 16),
                    g: parseInt(hex[1]! + hex[1]!, 16),
                    b: parseInt(hex[2]! + hex[2]!, 16),
                };
            } else if (hex.length === 6) {
                return {
                    r: parseInt(hex.slice(0, 2), 16),
                    g: parseInt(hex.slice(2, 4), 16),
                    b: parseInt(hex.slice(4, 6), 16),
                };
            }
        }

        const rgbMatch = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
            return {
                r: parseInt(rgbMatch[1]!),
                g: parseInt(rgbMatch[2]!),
                b: parseInt(rgbMatch[3]!),
            };
        }

        return null;
    }

    private static _calculateLuminance(r: number, g: number, b: number): number {
        const rs = r / 255;
        const gs = g / 255;
        const bs = b / 255;

        const rLin = rs <= 0.03928 ? rs / 12.92 : Math.pow((rs + 0.055) / 1.055, 2.4);
        const gLin = gs <= 0.03928 ? gs / 12.92 : Math.pow((gs + 0.055) / 1.055, 2.4);
        const bLin = bs <= 0.03928 ? bs / 12.92 : Math.pow((bs + 0.055) / 1.055, 2.4);

        return 0.2126 * rLin + 0.7152 * gLin + 0.0722 * bLin;
    }
}
