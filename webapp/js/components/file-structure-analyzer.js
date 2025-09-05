/**
 * File Structure Analyzer
 * Parses parquet metadata into hierarchical byte range structure
 */
class FileStructureAnalyzer {
    constructor(data) {
        this.data = data;
        this.fileSize = data.filesize;
        this.metadata = data.metadata?.metadata;
        this.columnChunks = data.column_chunks || [];
    }

    /**
     * Get the complete hierarchical structure
     */
    getHierarchy() {
        return {
            overview: this.getOverviewLevel(),
            rowGroups: this.getRowGroupsLevel(),
            columnChunks: this.getColumnChunksLevel(),
            pages: this.getPagesLevel()
        };
    }

    /**
     * Level 1: Overview (MAGIC | ROWGROUPS | METADATA | FOOTER | MAGIC)
     */
    getOverviewLevel() {
        const segments = [];
        const magicSize = 4; // PAR1
        const footerSize = 8; // Footer length + PAR1

        // Use actual metadata size if available
        const metadataSize = this.data.metadata?.total_byte_size ||
            (this.metadata ? JSON.stringify(this.metadata).length : 1024);

        // Calculate actual row groups range based on column chunk data
        let rowGroupsStart = magicSize;
        let rowGroupsEnd = this.fileSize - metadataSize - footerSize;

        if (this.columnChunks.length > 0) {
            const allOffsets = this.columnChunks.map(chunk => chunk.start_offset);
            const allSizes = this.columnChunks.map(chunk => chunk.total_byte_size);
            const allEnds = this.columnChunks.map((chunk, i) => allOffsets[i] + allSizes[i]);

            rowGroupsStart = Math.min(...allOffsets);
            rowGroupsEnd = Math.max(...allEnds);
        }

        segments.push({
            id: 'header_magic',
            name: 'MAGIC',
            type: 'header',
            start: 0,
            end: magicSize,
            size: magicSize,
            description: 'PAR1 magic number'
        });

        segments.push({
            id: 'rowgroups',
            name: 'ROWGROUPS',
            type: 'rowgroups',
            start: rowGroupsStart,
            end: rowGroupsEnd,
            size: rowGroupsEnd - rowGroupsStart,
            description: `Row group data sections (${formatBytes(rowGroupsEnd - rowGroupsStart)})`,
            hasChildren: true,
            children: this.getRowGroupsLevel()
        });

        segments.push({
            id: 'metadata',
            name: 'METADATA',
            type: 'metadata',
            start: rowGroupsEnd,
            end: this.fileSize - footerSize,
            size: metadataSize,
            description: `File metadata (${formatBytes(metadataSize)})`
        });

        segments.push({
            id: 'footer',
            name: 'FOOTER',
            type: 'footer',
            start: this.fileSize - footerSize,
            end: this.fileSize - magicSize,
            size: footerSize - magicSize,
            description: 'Footer metadata length (4 bytes)'
        });

        segments.push({
            id: 'footer_magic',
            name: 'MAGIC',
            type: 'footer',
            start: this.fileSize - magicSize,
            end: this.fileSize,
            size: magicSize,
            description: 'PAR1 magic number'
        });

        return segments;
    }

    /**
     * Level 2: Row Groups breakdown
     */
    getRowGroupsLevel() {
        if (!this.metadata?.row_groups) {
            return [];
        }

        const segments = [];

        // Calculate actual row group boundaries based on column chunk offsets
        this.metadata.row_groups.forEach((rowGroup, index) => {
            const rowGroupChunks = this.getChunksForRowGroup(index);

            if (rowGroupChunks.length === 0) return;

            // Find the actual range of this row group's data
            const chunkOffsets = rowGroupChunks.map(chunk => chunk.start_offset);
            const chunkSizes = rowGroupChunks.map(chunk => chunk.total_byte_size);
            const chunkEnds = rowGroupChunks.map((chunk, i) => chunkOffsets[i] + chunkSizes[i]);

            const start = Math.min(...chunkOffsets);
            const end = Math.max(...chunkEnds);
            const size = end - start;

            segments.push({
                id: `rowgroup_${index}`,
                name: `RG${index}`,
                type: `rowgroup`,
                start: start,
                end: end,
                size: size,
                description: `Row Group ${index} (${formatNumber(rowGroup.row_count)} rows, ${formatBytes(size)})`,
                hasChildren: true,
                children: this.getColumnChunksLevel(index),
                rowGroupIndex: index,
                metadata: rowGroup
            });
        });

        return segments;
    }

    /**
     * Level 3: Column Chunks within a row group
     */
    getColumnChunksLevel(rowGroupIndex = 0) {
        const segments = [];
        const rowGroupChunks = this.getChunksForRowGroup(rowGroupIndex);

        rowGroupChunks.forEach((chunk, chunkIndex) => {
            const columnPath = chunk.path_in_schema;
            const chunkSize = chunk.total_byte_size || 0;

            // Use actual start_offset from the data
            const chunkStart = chunk.start_offset || 0;

            // Find the corresponding metadata for this column chunk
            const columnMetadata = this.findColumnMetadata(rowGroupIndex, columnPath);

            // Create enhanced description with metadata
            const description = this.buildColumnChunkDescription(columnPath, chunkSize, columnMetadata, chunk);

            segments.push({
                id: `chunk_${rowGroupIndex}_${chunkIndex}`,
                name: columnPath,
                type: 'column',
                start: chunkStart,
                end: chunkStart + chunkSize,
                size: chunkSize,
                description: description,
                hasChildren: true,
                children: this.getPagesLevel(rowGroupIndex, chunkIndex),
                rowGroupIndex: rowGroupIndex,
                chunkIndex: chunkIndex,
                columnPath: columnPath,
                physicalMetadata: chunk,
                logicalMetadata: columnMetadata
            });
        });

        // Sort by actual start offset to show proper file order
        segments.sort((a, b) => a.start - b.start);
        return segments;
    }

    /**
     * Level 4: Pages within a column chunk
     */
    getPagesLevel(rowGroupIndex = 0, chunkIndex = 0) {
        const segments = [];
        const chunk = this.getChunkData(rowGroupIndex, chunkIndex);

        if (!chunk) return segments;

        let pageOffset = this.getChunkStartOffset(rowGroupIndex, chunkIndex);

        // Dictionary page
        if (chunk.dictionary_page) {
            const dictPage = chunk.dictionary_page;
            segments.push({
                id: `page_dict_${rowGroupIndex}_${chunkIndex}`,
                name: 'DICT',
                type: 'dictionary',
                start: dictPage.start_offset,
                end: dictPage.start_offset + dictPage.compressed_page_size,
                size: dictPage.compressed_page_size,
                description: 'Dictionary page',
                hasChildren: false,
                metadata: dictPage
            });
        }

        // Data pages
        if (chunk.data_pages) {
            chunk.data_pages.forEach((page, pageIndex) => {
                segments.push({
                    id: `page_data_${rowGroupIndex}_${chunkIndex}_${pageIndex}`,
                    name: `DATA${pageIndex}`,
                    type: 'data',
                    start: page.start_offset,
                    end: page.start_offset + page.compressed_page_size,
                    size: page.compressed_page_size,
                    description: `Data page ${pageIndex} (${formatNumber(page.num_values || page.num_rows || 0)} values)`,
                    hasChildren: false,
                    pageIndex: pageIndex,
                    metadata: page
                });
            });
        }

        // Index pages
        if (chunk.index_pages) {
            chunk.index_pages.forEach((page, pageIndex) => {
                segments.push({
                    id: `page_index_${rowGroupIndex}_${chunkIndex}_${pageIndex}`,
                    name: `IDX${pageIndex}`,
                    type: 'index',
                    start: page.start_offset,
                    end: page.start_offset + page.compressed_page_size,
                    size: page.compressed_page_size,
                    description: `Index page ${pageIndex}`,
                    hasChildren: false,
                    pageIndex: pageIndex,
                    metadata: page
                });
            });
        }

        return segments;
    }

    /**
     * Helper methods
     */
    getRowGroupDataSize() {
        const magicSize = 4;
        const footerSize = 8;
        const metadataSize = this.metadata ?
            JSON.stringify(this.metadata).length : 1024;

        return this.fileSize - (2 * magicSize) - footerSize - metadataSize;
    }

    getRowGroupStartOffset(rowGroupIndex) {
        const totalSize = this.getRowGroupDataSize();
        const numRowGroups = this.metadata?.row_groups?.length || 1;
        const singleRowGroupSize = totalSize / numRowGroups;
        return 4 + (rowGroupIndex * singleRowGroupSize);
    }

    getChunksForRowGroup(rowGroupIndex) {
        // In a real implementation, this would map row groups to chunks
        // For now, we'll distribute column chunks across row groups
        const numRowGroups = this.metadata?.row_groups?.length || 1;
        const chunksPerRowGroup = Math.ceil(this.columnChunks.length / numRowGroups);
        const startIndex = rowGroupIndex * chunksPerRowGroup;
        const endIndex = Math.min(startIndex + chunksPerRowGroup, this.columnChunks.length);

        return this.columnChunks.slice(startIndex, endIndex);
    }

    getChunkData(rowGroupIndex, chunkIndex) {
        const rowGroupChunks = this.getChunksForRowGroup(rowGroupIndex);
        return rowGroupChunks[chunkIndex];
    }

    getChunkStartOffset(rowGroupIndex, chunkIndex) {
        const rowGroupStart = this.getRowGroupStartOffset(rowGroupIndex);
        const chunk = this.getChunkData(rowGroupIndex, chunkIndex);
        const chunkSize = chunk?.total_byte_size || 0;
        return rowGroupStart + (chunkIndex * chunkSize);
    }

    /**
     * Get segments for a specific level and parent
     */
    getSegmentsForLevel(level, parentId = null) {
        switch (level) {
            case 'overview':
                return this.getOverviewLevel();
            case 'rowgroups':
                return this.getRowGroupsLevel();
            case 'columnchunks':
                if (parentId && parentId.includes('rowgroup_')) {
                    const rowGroupIndex = parseInt(parentId.split('_')[1]);
                    return this.getColumnChunksLevel(rowGroupIndex);
                }
                return [];
            case 'pages':
                if (parentId && parentId.includes('chunk_')) {
                    const parts = parentId.split('_');
                    const rowGroupIndex = parseInt(parts[1]);
                    const chunkIndex = parseInt(parts[2]);
                    return this.getPagesLevel(rowGroupIndex, chunkIndex);
                }
                return [];
            default:
                return [];
        }
    }

    /**
     * Find a segment by ID across all levels
     */
    findSegment(segmentId) {
        const hierarchy = this.getHierarchy();

        for (const [levelName, segments] of Object.entries(hierarchy)) {
            for (const segment of segments) {
                if (segment.id === segmentId) {
                    return segment;
                }
                // Search in children recursively
                if (segment.children) {
                    const found = this.findInChildren(segment.children, segmentId);
                    if (found) return found;
                }
            }
        }

        return null;
    }

    findInChildren(segments, segmentId) {
        for (const segment of segments) {
            if (segment.id === segmentId) {
                return segment;
            }
            if (segment.children) {
                const found = this.findInChildren(segment.children, segmentId);
                if (found) return found;
            }
        }
        return null;
    }

    /**
     * Find column metadata for a specific column in a row group
     */
    findColumnMetadata(rowGroupIndex, columnPath) {
        if (!this.metadata?.row_groups?.[rowGroupIndex]) {
            return null;
        }

        const rowGroup = this.metadata.row_groups[rowGroupIndex];
        return rowGroup.column_chunks?.[columnPath] || null;
    }

    /**
     * Build enhanced description for column chunk
     */
    buildColumnChunkDescription(columnPath, chunkSize, columnMetadata, physicalChunk) {
        const parts = [`Column: ${columnPath}`];

        // Add size
        parts.push(`Size: ${formatBytes(chunkSize)}`);

        // Add compression info
        if (physicalChunk.codec !== undefined) {
            const compressionNames = {
                0: 'UNCOMPRESSED', 1: 'SNAPPY', 2: 'GZIP', 3: 'LZO',
                4: 'BROTLI', 5: 'LZ4', 6: 'ZSTD'
            };
            const compressionName = compressionNames[physicalChunk.codec] || `CODEC_${physicalChunk.codec}`;
            parts.push(`Compression: ${compressionName}`);
        }

        // Add value count
        if (physicalChunk.num_values) {
            parts.push(`Values: ${formatNumber(physicalChunk.num_values)}`);
        }

        // Add metadata info if available
        if (columnMetadata?.metadata) {
            const meta = columnMetadata.metadata;

            // Add data type
            if (meta.type !== undefined) {
                const typeNames = {
                    0: 'BOOLEAN', 1: 'INT32', 2: 'INT64', 3: 'INT96',
                    4: 'FLOAT', 5: 'DOUBLE', 6: 'BYTE_ARRAY', 7: 'FIXED_LEN_BYTE_ARRAY'
                };
                const typeName = typeNames[meta.type] || `TYPE_${meta.type}`;
                parts.push(`Type: ${typeName}`);
            }

            // Add encodings
            if (meta.encodings && meta.encodings.length > 0) {
                const encodingNames = {
                    0: 'PLAIN', 2: 'DICTIONARY', 3: 'RLE', 4: 'BIT_PACKED',
                    5: 'DELTA_BINARY_PACKED', 6: 'DELTA_LENGTH_BYTE_ARRAY',
                    7: 'DELTA_BYTE_ARRAY', 8: 'RLE_DICTIONARY', 9: 'BYTE_STREAM_SPLIT'
                };
                const encodings = meta.encodings.map(enc =>
                    encodingNames[enc] || `ENC_${enc}`
                ).join(', ');
                parts.push(`Encodings: ${encodings}`);
            }

            // Add compression ratio if available
            if (meta.total_compressed_size && meta.total_uncompressed_size) {
                const ratio = (meta.total_compressed_size / meta.total_uncompressed_size * 100).toFixed(1);
                parts.push(`Compression: ${ratio}%`);
            }
        }

        return parts.join(' • ');
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FileStructureAnalyzer;
}
