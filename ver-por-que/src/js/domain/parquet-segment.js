/**
 * Parquet Segment
 * Domain object representing a segment in the parquet file structure
 * Encapsulates segment data with computed properties and validation
 */
class ParquetSegment {
    /**
     * Create a new parquet segment
     * @param {object} config - Segment configuration
     * @param {string} config.id - Unique identifier for the segment
     * @param {string} config.name - Display name for the segment
     * @param {number} config.start - Start byte offset in the file
     * @param {number} config.end - End byte offset in the file
     * @param {object} [config.metadata] - Optional metadata object
     * @param {string} [config.description] - Optional description
     * @param {number} [config.rowGroupIndex] - Row group index (for row group segments)
     * @param {number} [config.chunkIndex] - Chunk index (for column segments)
     * @param {number} [config.pageIndex] - Page index (for page segments)
     * @param {string} [config.columnPath] - Column path (for column segments)
     * @param {object} [config.physicalMetadata] - Physical metadata
     * @param {object} [config.logicalMetadata] - Logical metadata
     */
    constructor(config) {
        // Validate required properties
        this._validateRequired(config, ['id', 'name', 'start', 'end']);

        // Core properties
        this.id = config.id;
        this.name = config.name;
        this.start = config.start;
        this.end = config.end;

        // Optional properties
        this.metadata = config.metadata || null;
        this.rowGroupIndex = config.rowGroupIndex;
        this.chunkIndex = config.chunkIndex;
        this.pageIndex = config.pageIndex;
        this.columnPath = config.columnPath;
        this.description = config.description || this._generateDescription();
        this.physicalMetadata = config.physicalMetadata || null;
        this.logicalMetadata = config.logicalMetadata || null;

        // Validate data integrity
        this._validate();
    }

    /**
     * Get the size of this segment in bytes
     * @returns {number} Size in bytes
     */
    get size() {
        return Math.max(0, this.end - this.start);
    }

    /**
     * Get formatted size string
     * @returns {string} Human-readable size string
     */
    get formattedSize() {
        return formatBytes(this.size);
    }

    /**
     * Get formatted start offset string
     * @returns {string} Human-readable start offset string
     */
    get formattedStart() {
        return formatBytes(this.start);
    }

    /**
     * Get formatted end offset string
     * @returns {string} Human-readable end offset string
     */
    get formattedEnd() {
        return formatBytes(this.end);
    }

    /**
     * Check if this segment can have child segments based on metadata
     * @returns {boolean} True if segment can be expanded
     */
    get canHaveChildren() {
        // Check if metadata object has a property indicating it can have children
        if (this.metadata && typeof this.metadata === 'object') {
            // Schema elements with children
            if (this.metadata.children && Object.keys(this.metadata.children).length > 0) {
                return true;
            }
            // Row group metadata container
            if (this.metadata.row_groups && Array.isArray(this.metadata.row_groups)) {
                return true;
            }
            // Column indices container
            if (this.metadata.indices_count && this.metadata.indices_count > 0) {
                return true;
            }
            // Key-value metadata container (structured format)
            if (
                this.metadata.entries &&
                Array.isArray(this.metadata.entries) &&
                this.metadata.entries.length > 0
            ) {
                return true;
            }
        }
        // Physical file structure that can have children based on the file data
        if (this.id === 'metadata' || this.id === 'rowgroups') {
            return true;
        }
        if (this.rowGroupIndex !== undefined || this.chunkIndex !== undefined) {
            return true;
        }
        return false;
    }

    /**
     * Get the expected child level name for drill-down based on metadata
     * @returns {string|null} Child level name or null if no children
     */
    get childLevelName() {
        // Use ID-based mapping for core file structure
        if (this.id === 'metadata') {
            return 'metadatastructure';
        }
        if (this.id === 'rowgroups') {
            return 'rowgroups';
        }
        if (this.id === 'schema_root') {
            return 'schemaelements';
        }
        if (this.id === 'row_groups_metadata') {
            return 'rowgroupelements';
        }
        if (this.id === 'column_indices') {
            return 'indexelements';
        }
        if (this.id === 'key_value_metadata') {
            return 'keyvaluemetadata';
        }

        // Physical row group -> physical column chunks
        if (
            this.rowGroupIndex !== undefined &&
            this.chunkIndex === undefined &&
            !this.id.includes('meta')
        ) {
            return 'columnchunks';
        }
        // Row group metadata -> column chunk metadata
        if (this.id && this.id.startsWith('rowgroup_meta_')) {
            return 'columnchunkmetadata';
        }
        // Column chunk -> pages
        if (this.chunkIndex !== undefined && this.pageIndex === undefined) {
            return 'pages';
        }
        // Schema groups -> schema elements
        if (this.metadata && this.metadata.children && this.metadata.element_type === 'group') {
            return 'schemaelements';
        }

        return null;
    }

    /**
     * Get index for color cycling based on segment position
     * @returns {number} Index for color cycling
     */
    get colorIndex() {
        return this.rowGroupIndex ?? this.chunkIndex ?? this.pageIndex ?? 0;
    }

    /**
     * Get comprehensive type information (for column segments)
     * @returns {object|null} Type information or null if not applicable
     */
    get typeInfo() {
        if (!this.logicalMetadata?.metadata) {
            return null;
        }

        return ParquetTypeResolver.getColumnTypeInfo(this.logicalMetadata.metadata);
    }

    /**
     * Get compression information (for column segments)
     * @returns {object|null} Compression information or null if not applicable
     */
    get compressionInfo() {
        if (!this.physicalMetadata && !this.logicalMetadata) {
            return null;
        }

        const info = {
            algorithm: 'Unknown',
            ratio: null,
            compressedSize: null,
            uncompressedSize: null,
        };

        // Get compression algorithm
        if (this.physicalMetadata?.codec !== undefined) {
            info.algorithm = ParquetTypeResolver.getCompressionName(this.physicalMetadata.codec);
        }

        // Get compression statistics
        const logicalMeta = this.logicalMetadata?.metadata;
        if (logicalMeta?.total_compressed_size && logicalMeta?.total_uncompressed_size) {
            info.compressedSize = logicalMeta.total_compressed_size;
            info.uncompressedSize = logicalMeta.total_uncompressed_size;
            info.ratio = ((info.compressedSize / info.uncompressedSize) * 100).toFixed(1) + '%';
        }

        return info;
    }

    /**
     * Get encoding information (for column segments)
     * @returns {string|null} Encoding information or null if not applicable
     */
    get encodingInfo() {
        if (!this.logicalMetadata?.metadata?.encodings) {
            return null;
        }

        return ParquetTypeResolver.getEncodingNames(this.logicalMetadata.metadata.encodings);
    }

    /**
     * Get statistics information (for column segments)
     * @returns {object|null} Statistics information or null if not available
     */
    get statisticsInfo() {
        if (!this.logicalMetadata?.metadata?.statistics) {
            return null;
        }

        const stats = this.logicalMetadata.metadata.statistics;
        const totalValues = this.physicalMetadata?.num_values || 0;

        return {
            minValue: stats.min_value,
            maxValue: stats.max_value,
            nullCount: stats.null_count,
            distinctCount: stats.distinct_count,
            nullPercentage:
                totalValues > 0 ? (((stats.null_count || 0) / totalValues) * 100).toFixed(1) : '0',
            distinctPercentage:
                totalValues > 0
                    ? (((stats.distinct_count || 0) / totalValues) * 100).toFixed(1)
                    : '0',
        };
    }

    /**
     * Create a copy of this segment with updated properties
     * @param {object} updates - Properties to update
     * @returns {ParquetSegment} New segment instance
     */
    clone(updates = {}) {
        const config = {
            id: this.id,
            name: this.name,
            start: this.start,
            end: this.end,
            metadata: this.metadata,
            description: this.description,
            rowGroupIndex: this.rowGroupIndex,
            chunkIndex: this.chunkIndex,
            pageIndex: this.pageIndex,
            columnPath: this.columnPath,
            physicalMetadata: this.physicalMetadata,
            logicalMetadata: this.logicalMetadata,
            ...updates,
        };

        return new ParquetSegment(config);
    }

    /**
     * Convert to plain object for serialization
     * @returns {object} Plain object representation
     */
    toJSON() {
        return {
            id: this.id,
            name: this.name,
            start: this.start,
            end: this.end,
            size: this.size,
            description: this.description,
            rowGroupIndex: this.rowGroupIndex,
            chunkIndex: this.chunkIndex,
            pageIndex: this.pageIndex,
            columnPath: this.columnPath,
            metadata: this.metadata,
            physicalMetadata: this.physicalMetadata,
            logicalMetadata: this.logicalMetadata,
        };
    }

    /**
     * Create ParquetSegment from plain object
     * @param {object} obj - Plain object representation
     * @returns {ParquetSegment} New segment instance
     */
    static fromJSON(obj) {
        return new ParquetSegment(obj);
    }

    /**
     * Validate required properties are present
     * @private
     */
    _validateRequired(config, required) {
        for (const prop of required) {
            if (config[prop] === undefined || config[prop] === null) {
                throw new Error(`ParquetSegment: Required property '${prop}' is missing`);
            }
        }
    }

    /**
     * Validate segment data integrity
     * @private
     */
    _validate() {
        // Validate byte offsets
        if (this.start < 0) {
            throw new Error(`ParquetSegment: Start offset cannot be negative (${this.start})`);
        }

        if (this.end < this.start) {
            throw new Error(
                `ParquetSegment: End offset (${this.end}) cannot be less than start offset (${this.start})`
            );
        }

        // Validate indices are non-negative if present
        const indices = ['rowGroupIndex', 'chunkIndex', 'pageIndex'];
        for (const index of indices) {
            if (this[index] !== undefined && this[index] < 0) {
                throw new Error(`ParquetSegment: ${index} cannot be negative (${this[index]})`);
            }
        }
    }

    /**
     * Generate default description based on segment properties
     * @private
     */
    _generateDescription() {
        // Use ID-based descriptions for core file structure
        if (this.id === 'header_magic' || this.id === 'footer_magic') {
            return '<code>PAR1</code> magic number';
        }
        if (this.id === 'footer') {
            return 'Footer metadata length (4 bytes)';
        }
        if (this.id === 'metadata') {
            return 'File metadata';
        }
        if (this.id === 'rowgroups') {
            return 'Data pages (by row group)';
        }
        if (this.id === 'schema_root') {
            return 'Schema Root';
        }
        if (this.id === 'row_groups_metadata') {
            return 'Row Group Metadata';
        }
        if (this.id === 'column_indices') {
            return 'Column Index';
        }

        // Row group segments
        if (this.rowGroupIndex !== undefined && this.chunkIndex === undefined) {
            return `Row Group <code>${this.rowGroupIndex}</code>`;
        }

        // Column chunk segments
        if (this.columnPath) {
            return `Column Chunk <code>${this.columnPath}</code>`;
        }

        // Page segments
        if (this.pageIndex !== undefined) {
            if (this.name.includes('DICT')) {
                return 'Dictionary page';
            } else if (this.name.includes('DATA')) {
                return `Data Page <code>${this.name}</code>`;
            } else if (this.name.includes('IDX')) {
                return `Index page <code>${this.pageIndex}</code>`;
            }
        }

        // Schema elements
        if (this.metadata && this.metadata.element_type === 'group') {
            return `Schema Group <code>${this.name}</code>`;
        } else if (this.metadata && this.metadata.element_type === 'column') {
            return `Schema Element <code>${this.name}</code>`;
        }

        // Generic fallback
        return `${this.name} (${this.formattedSize})`;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ParquetSegment;
}
