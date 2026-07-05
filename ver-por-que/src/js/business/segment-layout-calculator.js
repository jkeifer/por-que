/**
 * Segment Layout Calculator
 * Pure functions for calculating segment layouts and positioning
 * Extracts layout algorithms from CanvasByteVisualizer for better separation of concerns
 */
class SegmentLayoutCalculator {
    /**
     * Calculate proportional widths for segments using sophisticated algorithm
     * @param {ParquetSegment[]} segments - Array of segments to calculate widths for
     * @param {number} containerWidth - Available container width in pixels
     * @param {object} config - Layout configuration (from VisualizationConfig)
     * @returns {object[]} Array of layout objects with calculated positions and sizes
     */
    static calculateSegmentWidths(segments, containerWidth, config = VisualizationConfig.LAYOUT) {
        if (!segments || segments.length === 0) {
            return [];
        }

        // Calculate total range for this level
        const minStart = Math.min(...segments.map(s => s.start));
        const maxEnd = Math.max(...segments.map(s => s.end));
        const totalSize = maxEnd - minStart;

        // Calculate minimum widths using logarithmic scaling
        const minWidths = this.calculateLogarithmicMinWidths(
            segments,
            totalSize,
            containerWidth,
            config
        );

        // Calculate natural proportional widths
        const segmentData = segments.map((segment, index) => ({
            segment: segment,
            naturalWidthPercent: (segment.size / totalSize) * 100,
            naturalWidthPixels: (segment.size / totalSize) * containerWidth,
            finalWidthPercent: 0,
            finalWidthPixels: 0,
            isExpanded: false,
            minWidth: minWidths[index],
        }));

        // Identify segments that need expansion using calculated minimum widths
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

        // Calculate space taken by expanded segments beyond their natural size
        const expandedSegments = segmentData.filter(d => d.isExpanded);
        const extraSpaceUsed = expandedSegments.reduce((sum, data) => {
            return sum + (data.finalWidthPercent - data.naturalWidthPercent);
        }, 0);

        // If we used extra space, proportionally reduce non-expanded segments
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

        // Create final layout objects with positions
        return this.calculateSegmentPositions(segmentData, config);
    }

    /**
     * Calculate logarithmic minimum widths for segments
     * @param {ParquetSegment[]} segments - Array of segments
     * @param {number} totalSize - Total size of all segments
     * @param {number} containerWidth - Available container width
     * @param {object} config - Layout configuration
     * @returns {number[]} Array of minimum widths in pixels
     */
    static calculateLogarithmicMinWidths(segments, totalSize, containerWidth, config) {
        // Start with a reasonable baseline minimum width
        const startingBaseline = config.STARTING_BASELINE || 5;

        // Adjust baseline downward if there's not enough space for all segments
        const adjustedBaseline = Math.min(startingBaseline, containerWidth / segments.length);

        // If adjusted baseline is too small, fall back to uniform distribution
        if (adjustedBaseline < config.MIN_SEGMENT_WIDTH) {
            return segments.map(() => containerWidth / segments.length);
        }

        // Get segment sizes and calculate logarithmic scaling
        const sizes = segments.map(s => Math.max(1, s.size)); // Avoid log(0)
        const minSize = Math.min(...sizes);
        const maxSize = Math.max(...sizes);

        // If all segments are the same size, use adjusted baseline for all
        if (minSize === maxSize) {
            return segments.map(() => adjustedBaseline);
        }

        // Calculate logarithmic widths independently for each segment
        const logMinSize = Math.log10(minSize);
        const logMaxSize = Math.log10(maxSize);
        const logRange = logMaxSize - logMinSize;

        const logarithmicWidths = sizes.map(size => {
            // Calculate position on logarithmic scale (0 to 1)
            const logSize = Math.log10(size);
            const logPosition = (logSize - logMinSize) / logRange;

            // Apply logarithmic scaling with configurable factor
            const scaledPosition = Math.pow(logPosition, 1 / config.LOG_SCALE_FACTOR);

            // Map to width range from adjusted baseline to MAX_MIN_WIDTH_PX
            const minWidth =
                adjustedBaseline +
                (config.MAX_MIN_SEGMENT_WIDTH - adjustedBaseline) * scaledPosition;

            return minWidth;
        });

        // Calculate total width of all logarithmic segments
        const totalLogarithmicWidth = logarithmicWidths.reduce((sum, width) => sum + width, 0);

        // If total exceeds container width, scale everything down proportionally
        if (totalLogarithmicWidth > containerWidth) {
            const scalingFactor = containerWidth / totalLogarithmicWidth;
            return logarithmicWidths.map(width => width * scalingFactor);
        }

        // Otherwise, return the logarithmic widths as calculated
        return logarithmicWidths;
    }

    /**
     * Calculate segment positions based on calculated widths
     * @param {object[]} segmentData - Array of segment data with calculated widths
     * @param {object} config - Layout configuration
     * @returns {object[]} Array of layout objects with positions
     */
    static calculateSegmentPositions(segmentData, config) {
        let currentLeft = 0; // Start at left edge

        return segmentData.map(data => {
            const layout = {
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

    /**
     * Calculate layout for a complete level
     * @param {string} levelName - Name of the level
     * @param {string} parentSegmentId - ID of parent segment (for child levels)
     * @param {ParquetSegment[]} segments - Array of segments in this level
     * @param {number} levelIndex - Index of this level in the visualization
     * @param {number} containerWidth - Available container width
     * @param {object} config - Layout configuration
     * @returns {object} Complete level layout
     */
    static computeLevelLayout(
        levelName,
        parentSegmentId,
        segments,
        levelIndex,
        containerWidth,
        config
    ) {
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

        // Calculate proportional widths with minimum width allocation
        const segmentLayouts = this.calculateSegmentWidths(segments, containerWidth, config);

        // Calculate actual range for statistics
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

    /**
     * Calculate optimal Canvas height based on number of levels
     * @param {number} levelCount - Number of levels to display
     * @param {object} config - Layout configuration
     * @returns {number} Optimal Canvas height in pixels
     */
    static calculateCanvasHeight(levelCount, config) {
        if (levelCount === 0) {
            // Minimum height when no content
            return config.LEVEL_HEIGHT + config.TOP_MARGIN * 2;
        }

        // Calculate height based purely on content, no minimum enforced
        return levelCount * (config.LEVEL_HEIGHT + config.LEVEL_SPACING) + config.TOP_MARGIN * 2;
    }

    /**
     * Calculate connection points between parent segment and child level
     * @param {object} parentSegmentLayout - Layout of parent segment
     * @param {object} parentLevelLayout - Layout of parent level
     * @param {object} childLevelLayout - Layout of child level
     * @param {number} containerWidth - Container width
     * @returns {object} Connection geometry
     */
    static calculateConnectionGeometry(
        parentSegmentLayout,
        parentLevelLayout,
        childLevelLayout,
        containerWidth
    ) {
        const parentY = parentLevelLayout.y + parentLevelLayout.height;
        const childY = childLevelLayout.y;
        const connectionHeight = childY - parentY;

        if (connectionHeight <= 0) {
            return null;
        }

        // Calculate trapezoid points for funnel shape
        return {
            parentLeft: parentSegmentLayout.x,
            parentRight: parentSegmentLayout.x + parentSegmentLayout.width,
            parentY: parentY,
            childLeft: 0, // Left edge of container
            childRight: containerWidth, // Right edge of container
            childY: childY,
            height: connectionHeight,
        };
    }

    /**
     * Perform hit testing to find segment at given coordinates
     * @param {number} x - X coordinate
     * @param {number} y - Y coordinate
     * @param {object[]} levelLayouts - Array of level layouts
     * @param {object} config - Layout configuration
     * @returns {object|null} Hit test result or null
     */
    static hitTest(x, y, levelLayouts, config) {
        // Test all segments in all levels
        for (let levelIndex = 0; levelIndex < levelLayouts.length; levelIndex++) {
            const level = levelLayouts[levelIndex];

            if (!level.segments) {
                continue;
            }

            for (const segmentLayout of level.segments) {
                const segmentX = segmentLayout.x;
                const segmentY = level.y + segmentLayout.y;
                const segmentWidth = segmentLayout.width;
                const segmentHeight = segmentLayout.height;

                // Add tolerance for easier selection
                const tolerance = config.HIT_TEST_TOLERANCE || 0;

                if (
                    x >= segmentX - tolerance &&
                    x <= segmentX + segmentWidth + tolerance &&
                    y >= segmentY - tolerance &&
                    y <= segmentY + segmentHeight + tolerance
                ) {
                    return {
                        segment: segmentLayout.segment,
                        level: level,
                        levelIndex: levelIndex,
                        segmentLayout: segmentLayout,
                        localX: x - segmentX,
                        localY: y - segmentY,
                    };
                }
            }
        }

        return null;
    }

    /**
     * Calculate responsive layout adjustments
     * @param {number} containerWidth - Current container width
     * @param {number} containerHeight - Current container height
     * @param {object} baseConfig - Base configuration
     * @returns {object} Responsive configuration adjustments
     */
    static getResponsiveAdjustments(containerWidth, containerHeight, baseConfig) {
        const adjustments = { ...baseConfig };

        // Mobile adjustments
        if (containerWidth <= VisualizationConfig.RESPONSIVE.MOBILE_MAX) {
            Object.assign(adjustments, VisualizationConfig.RESPONSIVE.MOBILE_LAYOUT);
        }
        // Tablet adjustments
        else if (containerWidth <= VisualizationConfig.RESPONSIVE.TABLET_MAX) {
            Object.assign(adjustments, VisualizationConfig.RESPONSIVE.TABLET_LAYOUT);
        }

        // Adjust for very narrow containers
        if (containerWidth < 300) {
            adjustments.MIN_SEGMENT_WIDTH = Math.max(1, containerWidth / 100);
            adjustments.MAX_MIN_SEGMENT_WIDTH = Math.max(
                adjustments.MIN_SEGMENT_WIDTH,
                containerWidth / 20
            );
        }

        return adjustments;
    }

    /**
     * Validate layout configuration
     * @param {object} config - Configuration to validate
     * @returns {object} Validated configuration
     */
    static validateConfig(config) {
        const validated = { ...config };

        // Ensure positive values for dimensions
        const positiveProperties = [
            'LEVEL_HEIGHT',
            'LEVEL_SPACING',
            'TOP_MARGIN',
            'SEGMENT_MARGIN',
            'MIN_SEGMENT_WIDTH',
            'MAX_MIN_SEGMENT_WIDTH',
            'STARTING_BASELINE',
        ];

        positiveProperties.forEach(prop => {
            if (validated[prop] !== undefined && validated[prop] <= 0) {
                console.warn(`SegmentLayoutCalculator: ${prop} must be positive, using default`);
                validated[prop] = VisualizationConfig.LAYOUT[prop];
            }
        });

        // Ensure min width is less than max min width
        if (validated.MIN_SEGMENT_WIDTH > validated.MAX_MIN_SEGMENT_WIDTH) {
            validated.MAX_MIN_SEGMENT_WIDTH = validated.MIN_SEGMENT_WIDTH * 2;
        }

        // Ensure log scale factor is reasonable
        if (validated.LOG_SCALE_FACTOR <= 0 || validated.LOG_SCALE_FACTOR > 10) {
            validated.LOG_SCALE_FACTOR = 2;
        }

        return validated;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SegmentLayoutCalculator;
}
