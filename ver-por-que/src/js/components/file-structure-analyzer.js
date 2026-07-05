/**
 * File Structure Analyzer
 * Provides access to parquet file structure hierarchy
 * Refactored to use SegmentHierarchyBuilder for better separation of concerns
 */
class FileStructureAnalyzer {
    constructor(data) {
        this.data = data;

        // Performance optimization: Precalculate all hierarchy levels using the builder
        this.cache = SegmentHierarchyBuilder.buildAll(data);
    }

    /**
     * Get segments for a specific level and parent (delegates to SegmentHierarchyBuilder)
     */
    getSegmentsForLevel(level, parentId = null) {
        return SegmentHierarchyBuilder.getSegmentsForLevel(this.cache, level, parentId);
    }

    /**
     * Find a segment by ID across all cached levels (delegates to SegmentHierarchyBuilder)
     */
    findSegment(segmentId) {
        return SegmentHierarchyBuilder.findSegment(this.cache, segmentId);
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FileStructureAnalyzer;
}
