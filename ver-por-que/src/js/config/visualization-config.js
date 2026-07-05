/**
 * Visualization Configuration
 * Centralized configuration for all layout, styling, and behavior constants
 * Single source of truth for visualization parameters across all components
 */
class VisualizationConfig {
    /**
     * Layout constants for Canvas visualizer
     * Controls spacing, sizing, and positioning of visualization elements
     */
    static LAYOUT = {
        // Level dimensions
        LEVEL_HEIGHT: 40, // Height of each visualization level in pixels
        LEVEL_SPACING: 32, // Vertical spacing between levels in pixels
        TOP_MARGIN: 20, // Top margin for first level in pixels

        // Segment styling
        SEGMENT_MARGIN: 2, // Margin around each segment in pixels
        CORNER_RADIUS: 0, // Corner radius for segment rectangles in pixels

        // Width calculation parameters
        MIN_SEGMENT_WIDTH: 3, // Absolute minimum width for any segment in pixels
        MAX_MIN_SEGMENT_WIDTH: 25, // Maximum minimum width allocation in pixels
        LOG_SCALE_FACTOR: 2, // Logarithmic scaling factor for width calculations
        STARTING_BASELINE: 5, // Starting baseline for minimum width calculations

        // Canvas dimensions
        DEFAULT_HEIGHT: 400, // Default Canvas height in pixels
        MIN_WIDTH: 300, // Minimum Canvas width in pixels
        MAX_WIDTH: 1200, // Maximum Canvas width for responsive fallback

        // Connection styling
        CONNECTION_OPACITY: 0.3, // Opacity for funnel connections between levels

        // Hit testing
        HIT_TEST_TOLERANCE: 2, // Pixel tolerance for hit testing interactions
    };

    /**
     * Color scheme for different segment types
     * Maps segment types to CSS custom properties for consistent theming
     */
    static COLORS = {
        // Segment type colors (maps to CSS custom properties)
        HEADER: '--text-secondary',
        FOOTER: '--text-secondary',
        METADATA: '--viz-brown',

        // Row group colors (cycle through these)
        ROWGROUP_COLORS: ['--viz-blue', '--viz-orange', '--viz-green', '--viz-red'],

        // Column chunk colors (cycle through these)
        COLUMN_COLORS: ['--viz-purple', '--viz-brown', '--viz-pink', '--viz-gray'],

        // Page type colors
        DATA_PAGE: '--viz-green',
        DICTIONARY_PAGE: '--viz-red',
        INDEX_PAGE: '--viz-purple',

        // Default colors
        DEFAULT: '--text-secondary',

        // Interaction states
        HOVER_OPACITY: 0.8,
        SELECTED_OPACITY: 0.9,
        DEFAULT_OPACITY: 1.0,

        // Connection colors
        CONNECTION: '--border-dark',
    };

    /**
     * Typography settings for labels and text
     */
    static TYPOGRAPHY = {
        // Font settings
        FONT_FAMILY: 'var(--font-sans)',
        FONT_SIZE: 12, // Base font size in pixels
        FONT_WEIGHT: 400, // Normal font weight

        // Text styling
        TEXT_COLOR: '--text-primary',
        TEXT_ALIGN: 'center',
        TEXT_BASELINE: 'middle',

        // Label constraints
        MIN_WIDTH_FOR_LABEL: 30, // Minimum segment width to show labels
        LABEL_PADDING: 8, // Padding inside segments for labels
        LABEL_TRUNCATE_SUFFIX: '...', // Suffix for truncated labels
    };

    /**
     * Animation and interaction settings
     */
    static ANIMATION = {
        // Transition durations (in milliseconds)
        HOVER_TRANSITION: 150,
        SELECTION_TRANSITION: 200,
        LEVEL_ADD_TRANSITION: 300,

        // Easing functions
        EASE_IN_OUT: 'cubic-bezier(0.4, 0, 0.2, 1)',
        EASE_OUT: 'cubic-bezier(0, 0, 0.2, 1)',

        // Animation frame rate
        TARGET_FPS: 60,
        FRAME_TIME: 16.67, // 1000ms / 60fps
    };

    /**
     * Tooltip configuration
     */
    static TOOLTIP = {
        // Positioning
        OFFSET_X: 10, // Horizontal offset from cursor
        OFFSET_Y: -10, // Vertical offset from cursor
        BOUNDARY_PADDING: 10, // Padding from viewport edges

        // Styling
        MAX_WIDTH: 300, // Maximum tooltip width
        MIN_WIDTH: 200, // Minimum tooltip width

        // Content
        SHOW_DRILL_DOWN_HINT: true, // Show "Click to drill down" for expandable segments
    };

    /**
     * Responsive breakpoints for different screen sizes
     */
    static RESPONSIVE = {
        // Breakpoints (in pixels)
        MOBILE_MAX: 768,
        TABLET_MAX: 1024,
        DESKTOP_MIN: 1025,

        // Mobile adjustments
        MOBILE_LAYOUT: {
            LEVEL_SPACING: 24,
            MIN_SEGMENT_WIDTH: 2,
        },

        // Tablet adjustments
        TABLET_LAYOUT: {
            LEVEL_SPACING: 28,
            MIN_SEGMENT_WIDTH: 2.5,
        },
    };

    /**
     * Performance settings
     */
    static PERFORMANCE = {
        // Rendering optimization
        USE_DEVICE_PIXEL_RATIO: true, // Enable high-DPI rendering
        DEBOUNCE_RESIZE: 100, // Resize debounce time in milliseconds
        MAX_SEGMENTS_FOR_LABELS: 50, // Max segments before disabling labels

        // Memory management
        CACHE_LAYOUTS: true, // Cache calculated layouts
        CLEAR_CACHE_ON_RESIZE: true, // Clear layout cache on window resize
    };

    /**
     * Schema tree configuration (for schema visualization component)
     */
    static SCHEMA_TREE = {
        // Indentation
        INDENT_SIZE: 20, // Pixels of indentation per level

        // Node styling
        NODE_HEIGHT: 24, // Height of each schema node
        ICON_SIZE: 16, // Size of expand/collapse icons

        // Expand/collapse icons
        EXPAND_ICON: '▶',
        COLLAPSE_ICON: '▼',
        LEAF_ICON: '•',
    };

    /**
     * Info panel configuration
     */
    static INFO_PANEL = {
        // Statistics display
        MAX_STAT_VALUE_LENGTH: 50, // Maximum length for displayed statistic values
        STAT_TRUNCATE_LENGTH: 47, // Length before truncation

        // Numeric formatting
        DECIMAL_PLACES: {
            COMPRESSION_RATIO: 1, // Decimal places for compression ratios
            PERCENTAGE: 1, // Decimal places for percentages
            STATISTICS: 2, // Default decimal places for statistics
        },

        // Section organization
        SHOW_EMPTY_SECTIONS: false, // Whether to show sections with no data
    };

    /**
     * Get layout configuration adjusted for current screen size
     * @param {number} screenWidth - Current screen width in pixels
     * @returns {object} Adjusted layout configuration
     */
    static getResponsiveLayout(screenWidth) {
        const baseLayout = { ...this.LAYOUT };

        if (screenWidth <= this.RESPONSIVE.MOBILE_MAX) {
            return { ...baseLayout, ...this.RESPONSIVE.MOBILE_LAYOUT };
        }

        if (screenWidth <= this.RESPONSIVE.TABLET_MAX) {
            return { ...baseLayout, ...this.RESPONSIVE.TABLET_LAYOUT };
        }

        return baseLayout;
    }

    /**
     * Get color for segment based on semantic element type
     * @param {object} segment - Segment object with semantic info
     * @param {number} segmentIndex - Position of segment within the row (determines shade within color family)
     * @param {object[]} allSegmentsInLevel - All segments in this level (optional, for compatibility)
     * @returns {string} CSS custom property name
     */
    static getSegmentColor(segment, segmentIndex = 0, _allSegmentsInLevel = []) {
        // Determine semantic element type and get appropriate color family
        const elementType = this._getSemanticElementType(segment);
        const colorFamily = this._getColorFamilyForElementType(elementType);

        // Use segment index to pick shade within the color family
        const shadeIndex = segmentIndex % colorFamily.length;
        return colorFamily[shadeIndex];
    }

    /**
     * Determine what kind of element this segment represents semantically
     * @private
     */
    static _getSemanticElementType(segment) {
        // File structure elements
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

        // Schema root and container
        if (segment.id === 'schema_root') {
            return 'schema_group';
        }

        // Schema elements based on metadata
        if (segment.metadata && segment.metadata.element_type === 'group') {
            return 'schema_group';
        }
        if (segment.metadata && segment.metadata.element_type === 'column') {
            return 'schema_element';
        }

        // Row group metadata container
        if (segment.id === 'row_groups_metadata') {
            return 'row_group_metadata';
        }

        // Column indices container
        if (segment.id === 'column_indices') {
            return 'column_index';
        }

        // Individual index elements
        if (segment.metadata && segment.metadata.index_type) {
            return 'column_index';
        }

        // Row group elements (both physical data row groups AND individual metadata elements)
        // Use the same orange colors for both types of individual row group segments
        if (segment.rowGroupIndex !== undefined && segment.chunkIndex === undefined) {
            return 'row_group';
        }

        // Column chunk elements
        if (segment.columnPath && segment.chunkIndex !== undefined) {
            return 'column_chunk';
        }

        // Page elements
        if (segment.pageIndex !== undefined) {
            if (segment.name && segment.name.includes('DICT')) {
                return 'dictionary_page';
            } else if (segment.name && segment.name.includes('DATA')) {
                return 'data_page';
            } else if (segment.name && segment.name.includes('IDX')) {
                return 'index_page';
            }
            return 'page'; // Generic page
        }

        // Key-value metadata container
        if (segment.id === 'key_value_metadata') {
            return 'key_value_metadata_container';
        }

        // Individual key-value metadata entries
        if (segment.id && segment.id.startsWith('kv_') && segment.metadata?.key) {
            return 'key_value_metadata_entry';
        }

        // Generic metadata elements (have metadata but don't match other patterns)
        if (segment.metadata && typeof segment.metadata === 'object') {
            return 'metadata_element';
        }

        // Default
        return 'generic';
    }

    /**
     * Get color family (light/medium/dark) for each semantic element type
     * @private
     */
    static _getColorFamilyForElementType(elementType) {
        const colorFamilies = {
            // File structure
            magic: ['--magic-color', '--magic-color', '--magic-color'],
            footer: ['--footer-color', '--footer-color', '--footer-color'],
            metadata_container: [
                '--metadata-container-color',
                '--metadata-container-color',
                '--metadata-container-color',
            ],
            rowgroups_container: ['--row-groups', '--row-groups', '--row-groups'],

            // Schema hierarchy - different colors for each level
            schema_group: ['--schema-group-light', '--schema-group-medium', '--schema-group-dark'],
            schema_element: [
                '--schema-element-light',
                '--schema-element-medium',
                '--schema-element-dark',
            ],

            // Data structure
            row_group: ['--row-group-light', '--row-group-medium', '--row-group-dark'],
            column_chunk: ['--column-chunk-light', '--column-chunk-medium', '--column-chunk-dark'],

            // Pages - each page type gets distinct color
            data_page: ['--data-page-light', '--data-page-medium', '--data-page-dark'],
            dictionary_page: [
                '--dictionary-page-color',
                '--dictionary-page-color',
                '--dictionary-page-color',
            ], // Usually only one per column
            index_page: ['--index-page-light', '--index-page-medium', '--index-page-dark'],
            page: ['--generic-page-light', '--generic-page-medium', '--generic-page-dark'],

            // Metadata
            row_group_metadata: ['--row-groups', '--row-groups', '--row-groups'], // Same blue as container
            column_index: ['--column-index-light', '--column-index-medium', '--column-index-dark'],
            metadata_element: [
                '--metadata-element-light',
                '--metadata-element-medium',
                '--metadata-element-dark',
            ],
            key_value_metadata_container: ['--red-medium', '--red-medium', '--red-medium'],
            key_value_metadata_entry: ['--green-light', '--green-medium', '--green-dark'],

            // Default
            generic: [
                '--generic-segment-color',
                '--generic-segment-color',
                '--generic-segment-color',
            ],
        };

        return colorFamilies[elementType] || colorFamilies.generic;
    }

    /**
     * Get optimal text color (light or dark) based on background color contrast
     * @param {string} backgroundColor - CSS color value (hex, rgb, or CSS variable)
     * @returns {string} Either '#ffffff' for light text or '#2c3e50' for dark text
     */
    static getOptimalTextColor(backgroundColor) {
        // Convert CSS variable to actual color value if needed
        let actualColor = backgroundColor;
        if (backgroundColor.startsWith('--')) {
            // Get computed value from CSS
            const computedColor = getComputedStyle(document.documentElement).getPropertyValue(
                backgroundColor
            );
            actualColor = computedColor.trim();
        }

        // Convert to RGB
        const rgb = this._parseColor(actualColor);
        if (!rgb) {
            console.warn(`Failed to parse color: ${backgroundColor} -> ${actualColor}`);
            // Return resolved CSS variable value
            return (
                getComputedStyle(document.documentElement)
                    .getPropertyValue('--text-on-light')
                    .trim() || '#2c3e50'
            );
        }

        // Calculate relative luminance using WCAG formula
        const luminance = this._calculateLuminance(rgb.r, rgb.g, rgb.b);

        // Use light text for dark backgrounds (luminance < 0.4)
        // Use dark text for light backgrounds (luminance >= 0.4)
        const textColorVar = luminance < 0.4 ? '--text-on-dark' : '--text-on-light';

        // Resolve CSS variable to actual color value for SVG compatibility
        const resolvedTextColor = getComputedStyle(document.documentElement)
            .getPropertyValue(textColorVar)
            .trim();

        return resolvedTextColor || '#2c3e50'; // Fallback if CSS var not found
    }

    /**
     * Get contrast class name based on background color luminance
     * @param {string} backgroundColor - CSS color value (hex, rgb, or CSS variable)
     * @returns {string} Either 'segment-on-light' or 'segment-on-dark'
     */
    static getContrastClass(backgroundColor) {
        // Convert CSS variable to actual color value if needed
        let actualColor = backgroundColor;
        if (backgroundColor.startsWith('--')) {
            // Get computed value from CSS
            const computedColor = getComputedStyle(document.documentElement).getPropertyValue(
                backgroundColor
            );
            actualColor = computedColor.trim();
        }

        // Convert to RGB
        const rgb = this._parseColor(actualColor);
        if (!rgb) {
            return 'segment-on-light'; // Default to light background styling
        }

        // Calculate relative luminance using WCAG formula
        const luminance = this._calculateLuminance(rgb.r, rgb.g, rgb.b);

        // Return appropriate class based on luminance
        return luminance < 0.4 ? 'segment-on-dark' : 'segment-on-light';
    }

    /**
     * Parse color string to RGB values
     * @private
     */
    static _parseColor(colorStr) {
        // Handle hex colors
        if (colorStr.startsWith('#')) {
            const hex = colorStr.slice(1);
            if (hex.length === 3) {
                return {
                    r: parseInt(hex[0] + hex[0], 16),
                    g: parseInt(hex[1] + hex[1], 16),
                    b: parseInt(hex[2] + hex[2], 16),
                };
            } else if (hex.length === 6) {
                return {
                    r: parseInt(hex.slice(0, 2), 16),
                    g: parseInt(hex.slice(2, 4), 16),
                    b: parseInt(hex.slice(4, 6), 16),
                };
            }
        }

        // Handle rgb() and rgba() colors
        const rgbMatch = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
            return {
                r: parseInt(rgbMatch[1]),
                g: parseInt(rgbMatch[2]),
                b: parseInt(rgbMatch[3]),
            };
        }

        return null; // Parsing failed
    }

    /**
     * Calculate relative luminance according to WCAG 2.1
     * @private
     */
    static _calculateLuminance(r, g, b) {
        // Convert to 0-1 range
        const rs = r / 255;
        const gs = g / 255;
        const bs = b / 255;

        // Apply gamma correction
        const rLin = rs <= 0.03928 ? rs / 12.92 : Math.pow((rs + 0.055) / 1.055, 2.4);
        const gLin = gs <= 0.03928 ? gs / 12.92 : Math.pow((gs + 0.055) / 1.055, 2.4);
        const bLin = bs <= 0.03928 ? bs / 12.92 : Math.pow((bs + 0.055) / 1.055, 2.4);

        // Calculate luminance
        return 0.2126 * rLin + 0.7152 * gLin + 0.0722 * bLin;
    }

    /**
     * Get configuration for specific component
     * @param {string} component - Component name
     * @returns {object} Component-specific configuration
     */
    static getComponentConfig(component) {
        const configs = {
            'canvas-visualizer': {
                layout: this.LAYOUT,
                colors: this.COLORS,
                typography: this.TYPOGRAPHY,
                animation: this.ANIMATION,
                tooltip: this.TOOLTIP,
                performance: this.PERFORMANCE,
            },
            'schema-tree': {
                ...this.SCHEMA_TREE,
                typography: this.TYPOGRAPHY,
            },
            'info-panel': {
                ...this.INFO_PANEL,
                typography: this.TYPOGRAPHY,
            },
        };

        return configs[component] || {};
    }

    /**
     * Validate and sanitize configuration values
     * @param {object} config - Configuration object to validate
     * @returns {object} Validated and sanitized configuration
     */
    static validateConfig(config) {
        const validated = { ...config };

        // Ensure positive values for dimensions
        if (validated.LEVEL_HEIGHT && validated.LEVEL_HEIGHT < 10) {
            validated.LEVEL_HEIGHT = 10;
        }

        if (validated.MIN_SEGMENT_WIDTH && validated.MIN_SEGMENT_WIDTH < 1) {
            validated.MIN_SEGMENT_WIDTH = 1;
        }

        // Ensure valid opacity values
        Object.keys(validated).forEach(key => {
            if (key.includes('OPACITY') && (validated[key] < 0 || validated[key] > 1)) {
                validated[key] = Math.max(0, Math.min(1, validated[key]));
            }
        });

        return validated;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = VisualizationConfig;
}
