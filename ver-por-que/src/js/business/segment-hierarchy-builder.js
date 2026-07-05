/**
 * Segment Hierarchy Builder
 * Pure functions for building parquet file segment hierarchies
 * Extracts hierarchy construction logic from FileStructureAnalyzer for better separation of concerns
 */
class SegmentHierarchyBuilder {
    /**
     * Build complete hierarchy for a parquet file
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Complete hierarchy cache
     */
    static buildAll(fileData) {
        const cache = {
            overview: [],
            rowgroups: [],
            columnchunks: {},
            pages: {},
            metadatastructure: [],
            schemaelements: {},
        };

        // Build each level of the hierarchy
        cache.overview = this.buildOverviewSegments(fileData);
        cache.rowgroups = this.buildRowGroupSegments(fileData);

        // Build metadata structure
        cache.metadatastructure = this.buildMetadataStructureSegments(fileData);

        // Build schema elements for schema groups
        cache.schemaelements = this.buildAllSchemaElementSegments(fileData);

        // Build row group metadata elements
        cache.rowgroupelements = this.buildRowGroupMetadataSegments(fileData);

        // Build column index elements
        cache.indexelements = this.buildColumnIndexSegments(fileData);

        // Build key-value metadata elements
        cache.keyvaluemetadata = this.buildKeyValueMetadataSegments(fileData);

        // Build column chunk metadata elements
        cache.columnchunkmetadata = this.buildColumnChunkMetadataSegments(fileData);

        // Build column chunks for each row group
        const metadata = fileData.metadata;
        if (metadata?.row_groups) {
            metadata.row_groups.forEach((_, index) => {
                cache.columnchunks[index] = this.buildColumnChunkSegments(fileData, index);
            });
        }

        // Build pages for each column chunk
        for (const [rowGroupIndex, chunks] of Object.entries(cache.columnchunks)) {
            chunks.forEach((chunk, chunkIndex) => {
                const key = `${rowGroupIndex}_${chunkIndex}`;
                cache.pages[key] = this.buildPageSegments(
                    fileData,
                    parseInt(rowGroupIndex),
                    chunkIndex
                );
            });
        }

        return cache;
    }

    /**
     * Build overview segments (file structure: MAGIC | ROWGROUPS | METADATA | FOOTER | MAGIC)
     * @param {object} fileData - Raw parquet file data
     * @returns {ParquetSegment[]} Array of overview segments
     */
    static buildOverviewSegments(fileData) {
        const segments = [];
        const constants = ParquetConstants.FILE_STRUCTURE;

        // Use actual metadata size if available
        const metadataSize =
            fileData.metadata?.total_byte_size ||
            (fileData.metadata?.metadata
                ? JSON.stringify(fileData.metadata.metadata).length
                : 1024);

        // Calculate actual row groups range based on column chunk data
        let rowGroupsStart = constants.MAGIC_SIZE;
        let rowGroupsEnd = fileData.filesize - metadataSize - constants.FOOTER_SIZE;

        if (fileData.column_chunks && fileData.column_chunks.length > 0) {
            const allOffsets = fileData.column_chunks.map(chunk => chunk.start_offset);
            const allSizes = fileData.column_chunks.map(chunk => chunk.total_byte_size);
            const allEnds = fileData.column_chunks.map((chunk, i) => allOffsets[i] + allSizes[i]);

            rowGroupsStart = Math.min(...allOffsets);
            rowGroupsEnd = Math.max(...allEnds);
        }

        // Create segments using ParquetSegment domain objects
        segments.push(
            new ParquetSegment({
                id: 'header_magic',
                name: 'MAGIC',
                start: 0,
                end: constants.MAGIC_SIZE,
            })
        );

        segments.push(
            new ParquetSegment({
                id: 'rowgroups',
                name: 'DATA PAGES',
                start: rowGroupsStart,
                end: rowGroupsEnd,
            })
        );

        segments.push(
            new ParquetSegment({
                id: 'metadata',
                name: 'METADATA',
                start: rowGroupsEnd,
                end: fileData.filesize - constants.FOOTER_SIZE,
            })
        );

        segments.push(
            new ParquetSegment({
                id: 'footer',
                name: 'FOOTER',
                start: fileData.filesize - constants.FOOTER_SIZE,
                end: fileData.filesize - constants.MAGIC_SIZE,
            })
        );

        segments.push(
            new ParquetSegment({
                id: 'footer_magic',
                name: 'MAGIC',
                start: fileData.filesize - constants.MAGIC_SIZE,
                end: fileData.filesize,
            })
        );

        return segments;
    }

    /**
     * Build row group segments
     * @param {object} fileData - Raw parquet file data
     * @returns {ParquetSegment[]} Array of row group segments
     */
    static buildRowGroupSegments(fileData) {
        const metadata = fileData.metadata;
        if (!metadata?.row_groups) {
            return [];
        }

        const segments = [];

        // Calculate actual row group boundaries based on column chunk offsets
        metadata.row_groups.forEach((rowGroup, index) => {
            const rowGroupChunks = this._getChunksForRowGroup(fileData, index);

            if (rowGroupChunks.length === 0) {
                return;
            }

            // Find the actual range of this row group's data
            const chunkOffsets = rowGroupChunks.map(chunk => chunk.start_offset);
            const chunkSizes = rowGroupChunks.map(chunk => chunk.total_byte_size);
            const chunkEnds = rowGroupChunks.map((chunk, i) => chunkOffsets[i] + chunkSizes[i]);

            const start = Math.min(...chunkOffsets);
            const end = Math.max(...chunkEnds);

            segments.push(
                new ParquetSegment({
                    id: `rowgroup_${index}`,
                    name: `RG${index}`,
                    start: start,
                    end: end,
                    rowGroupIndex: index,
                    metadata: rowGroup,
                })
            );
        });

        return segments;
    }

    /**
     * Build column chunk segments within a row group
     * @param {object} fileData - Raw parquet file data
     * @param {number} rowGroupIndex - Index of the row group
     * @returns {ParquetSegment[]} Array of column chunk segments
     */
    static buildColumnChunkSegments(fileData, rowGroupIndex) {
        const segments = [];
        const rowGroupChunks = this._getChunksForRowGroup(fileData, rowGroupIndex);

        rowGroupChunks.forEach((chunk, chunkIndex) => {
            const columnPath = chunk.path_in_schema;
            const chunkSize = chunk.total_byte_size || 0;
            const chunkStart = chunk.start_offset || 0;

            // Find the corresponding metadata for this column chunk
            const columnMetadata = this._findColumnMetadata(fileData, rowGroupIndex, columnPath);

            segments.push(
                new ParquetSegment({
                    id: `chunk_${rowGroupIndex}_${chunkIndex}`,
                    name: columnPath,
                    start: chunkStart,
                    end: chunkStart + chunkSize,
                    rowGroupIndex: rowGroupIndex,
                    chunkIndex: chunkIndex,
                    columnPath: columnPath,
                    physicalMetadata: chunk,
                    logicalMetadata: columnMetadata,
                })
            );
        });

        // Sort by actual start offset to show proper file order
        segments.sort((a, b) => a.start - b.start);
        return segments;
    }

    /**
     * Build page segments within a column chunk
     * @param {object} fileData - Raw parquet file data
     * @param {number} rowGroupIndex - Index of the row group
     * @param {number} chunkIndex - Index of the chunk within the row group
     * @returns {ParquetSegment[]} Array of page segments
     */
    static buildPageSegments(fileData, rowGroupIndex, chunkIndex) {
        const segments = [];
        const chunk = this._getChunkData(fileData, rowGroupIndex, chunkIndex);

        if (!chunk) {
            return segments;
        }

        // Dictionary page
        if (chunk.dictionary_page) {
            const dictPage = chunk.dictionary_page;
            // Total page size includes header + compressed data
            const totalPageSize =
                (dictPage.header_size || 0) + (dictPage.compressed_page_size || 0);
            segments.push(
                new ParquetSegment({
                    id: `page_dict_${rowGroupIndex}_${chunkIndex}`,
                    name: 'DICT',
                    start: dictPage.start_offset,
                    end: dictPage.start_offset + totalPageSize,
                    rowGroupIndex: rowGroupIndex,
                    chunkIndex: chunkIndex,
                    pageIndex: 0,
                    metadata: dictPage,
                })
            );
        }

        // Data pages
        if (chunk.data_pages) {
            chunk.data_pages.forEach((page, pageIndex) => {
                // Total page size includes header + compressed data
                const totalPageSize = (page.header_size || 0) + (page.compressed_page_size || 0);
                segments.push(
                    new ParquetSegment({
                        id: `page_data_${rowGroupIndex}_${chunkIndex}_${pageIndex}`,
                        name: `DATA${pageIndex}`,
                        start: page.start_offset,
                        end: page.start_offset + totalPageSize,
                        rowGroupIndex: rowGroupIndex,
                        chunkIndex: chunkIndex,
                        pageIndex: pageIndex,
                        metadata: page,
                    })
                );
            });
        }

        // Index pages
        if (chunk.index_pages) {
            chunk.index_pages.forEach((page, pageIndex) => {
                // Total page size includes header + compressed data
                const totalPageSize = (page.header_size || 0) + (page.compressed_page_size || 0);
                segments.push(
                    new ParquetSegment({
                        id: `page_index_${rowGroupIndex}_${chunkIndex}_${pageIndex}`,
                        name: `IDX${pageIndex}`,
                        start: page.start_offset,
                        end: page.start_offset + totalPageSize,
                        rowGroupIndex: rowGroupIndex,
                        chunkIndex: chunkIndex,
                        pageIndex: pageIndex,
                        metadata: page,
                    })
                );
            });
        }

        return segments;
    }

    /**
     * Build metadata structure segments (schema, row group metadata, indices)
     * @param {object} fileData - Raw parquet file data
     * @returns {ParquetSegment[]} Array of metadata structure segments
     */
    static buildMetadataStructureSegments(fileData) {
        const segments = [];
        const metadata = fileData.metadata;

        if (!metadata) {
            return segments;
        }

        let currentOffset = metadata?.start_offset || 0;

        // Schema segment (using physical metadata offsets if available)
        if (metadata.schema_root) {
            const schemaStart = metadata.schema_root.start_offset || currentOffset;
            // Calculate the actual schema length by summing all child elements recursively
            const schemaLength = this._calculateSchemaLength(metadata.schema_root);

            segments.push(
                new ParquetSegment({
                    id: 'schema_root',
                    name: 'SCHEMA',
                    start: schemaStart,
                    end: schemaStart + schemaLength,
                    metadata: metadata.schema_root,
                })
            );

            currentOffset = schemaStart + schemaLength;
        }

        // Row Groups Metadata segment
        if (metadata.row_groups && metadata.row_groups.length > 0) {
            // Calculate size based on row group metadata
            let totalRowGroupMetadataSize = 0;
            metadata.row_groups.forEach(rg => {
                totalRowGroupMetadataSize += rg.byte_length || 200; // estimate per row group
            });

            segments.push(
                new ParquetSegment({
                    id: 'row_groups_metadata',
                    name: 'ROW GROUP METADATA',
                    start: currentOffset,
                    end: currentOffset + totalRowGroupMetadataSize,
                    metadata: {
                        name: 'Row Group Metadata',
                        row_groups: metadata.row_groups,
                        num_row_groups: metadata.row_groups.length,
                    },
                })
            );

            currentOffset += totalRowGroupMetadataSize;
        }

        // Column Indices (if any columns have column index info)
        const columnIndicesInfo = this._extractColumnIndicesInfo(fileData);
        if (columnIndicesInfo.length > 0) {
            const totalIndicesSize = columnIndicesInfo.reduce((sum, info) => sum + info.size, 0);

            segments.push(
                new ParquetSegment({
                    id: 'column_indices',
                    name: 'INDICES',
                    start: currentOffset,
                    end: currentOffset + totalIndicesSize,
                    metadata: {
                        name: 'column_indices',
                        indices_count: columnIndicesInfo.length,
                        total_size: totalIndicesSize,
                    },
                })
            );

            currentOffset += totalIndicesSize;
        }

        // Key-Value Metadata (if present)
        if (
            metadata.key_value_metadata &&
            Array.isArray(metadata.key_value_metadata) &&
            metadata.key_value_metadata.length > 0
        ) {
            // Calculate total size from individual entries
            const totalKvSize = metadata.key_value_metadata.reduce(
                (sum, entry) => sum + (entry.byte_length || 0),
                0
            );

            segments.push(
                new ParquetSegment({
                    id: 'key_value_metadata',
                    name: 'KEY-VALUE METADATA',
                    start: currentOffset,
                    end: currentOffset + totalKvSize,
                    metadata: {
                        name: 'Key-Value Metadata',
                        entries: metadata.key_value_metadata,
                        num_entries: metadata.key_value_metadata.length,
                        structured_format: true,
                    },
                })
            );

            currentOffset += totalKvSize;
        }

        return segments.sort((a, b) => a.start - b.start);
    }

    /**
     * Build individual row group metadata segments
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Map of parent IDs to row group metadata arrays
     */
    static buildRowGroupMetadataSegments(fileData) {
        const cache = {};
        const metadata = fileData.metadata;
        if (!metadata?.row_groups) {
            return cache;
        }

        const segments = [];
        let currentOffset = (fileData.metadata?.start_offset || 0) + 600; // After schema

        metadata.row_groups.forEach((rowGroup, index) => {
            const rgSize = rowGroup.byte_length || 200;

            segments.push(
                new ParquetSegment({
                    id: `rowgroup_meta_${index}`,
                    name: `ROW GROUP ${index}`,
                    start: currentOffset,
                    end: currentOffset + rgSize,
                    metadata: rowGroup,
                    rowGroupIndex: index,
                })
            );

            currentOffset += rgSize;
        });

        cache['row_groups_metadata'] = segments;
        return cache;
    }

    /**
     * Build column index segments
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Map of parent IDs to index segment arrays
     */
    static buildColumnIndexSegments(fileData) {
        const cache = {};
        const indicesInfo = this._extractColumnIndicesInfo(fileData);

        if (indicesInfo.length === 0) {
            return cache;
        }

        const segments = [];

        indicesInfo.forEach(info => {
            const segmentName = `${info.path} (RG${info.row_group}) ${info.type.toUpperCase()}`;

            segments.push(
                new ParquetSegment({
                    id: `${info.type}_${info.row_group}_${info.path}`,
                    name: segmentName,
                    start: info.offset,
                    end: info.offset + info.size,
                    metadata: {
                        column_path: info.path,
                        row_group_index: info.row_group,
                        index_type: info.type,
                    },
                })
            );
        });

        cache['column_indices'] = segments.sort((a, b) => a.start - b.start);
        return cache;
    }

    /**
     * Build key-value metadata segments
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Map of parent IDs to key-value metadata segment arrays
     */
    static buildKeyValueMetadataSegments(fileData) {
        const cache = {};
        const metadata = fileData.metadata?.metadata;

        if (!metadata?.key_value_metadata || !Array.isArray(metadata.key_value_metadata)) {
            return cache;
        }

        const segments = [];

        metadata.key_value_metadata.forEach((entry, index) => {
            // Use actual byte ranges from the schema
            const segmentName = `${entry.key}`;
            const segmentId = `kv_${index}_${entry.key.replace(/[^a-zA-Z0-9]/g, '_')}`;

            segments.push(
                new ParquetSegment({
                    id: segmentId,
                    name: segmentName,
                    start: entry.start_offset || 0,
                    end: (entry.start_offset || 0) + (entry.byte_length || 0),
                    metadata: {
                        key: entry.key,
                        value: entry.value,
                        byte_length: entry.byte_length,
                        start_offset: entry.start_offset,
                        entry_index: index,
                    },
                })
            );
        });

        cache['key_value_metadata'] = segments.sort((a, b) => a.start - b.start);
        return cache;
    }

    /**
     * Build column chunk metadata segments for row group metadata
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Map of parent IDs to column chunk metadata segment arrays
     */
    static buildColumnChunkMetadataSegments(fileData) {
        const cache = {};
        const metadata = fileData.metadata;

        if (!metadata?.row_groups) {
            return cache;
        }

        metadata.row_groups.forEach((rowGroup, rgIndex) => {
            const parentId = `rowgroup_meta_${rgIndex}`;
            const segments = [];

            if (rowGroup.column_chunks) {
                let currentOffset = 0; // Relative offset within this row group metadata

                Object.entries(rowGroup.column_chunks).forEach(
                    ([columnPath, columnMeta], chunkIndex) => {
                        const chunkSize =
                            columnMeta.total_compressed_size ||
                            columnMeta.total_uncompressed_size ||
                            100;

                        segments.push(
                            new ParquetSegment({
                                id: `column_meta_${rgIndex}_${chunkIndex}`,
                                name: columnPath,
                                start: currentOffset,
                                end: currentOffset + chunkSize,
                                metadata: columnMeta,
                                rowGroupIndex: rgIndex,
                                chunkIndex: chunkIndex,
                                columnPath: columnPath,
                            })
                        );

                        currentOffset += chunkSize;
                    }
                );
            }

            cache[parentId] = segments;
        });

        return cache;
    }

    /**
     * Build all schema element segments for all schema groups recursively
     * @param {object} fileData - Raw parquet file data
     * @returns {object} Map of parent IDs to schema element arrays
     */
    static buildAllSchemaElementSegments(fileData) {
        const cache = {};
        const metadata = fileData.metadata;
        if (!metadata?.schema_root) {
            return cache;
        }

        // Build elements for the root schema
        cache['schema_root'] = this.buildSchemaElementSegments(metadata.schema_root, 'schema_root');

        // Recursively build all nested schema groups
        this._buildAllNestedSchemaElements(metadata.schema_root, 'schema_root', cache);

        return cache;
    }

    /**
     * Recursively build schema elements for all nested groups
     * @private
     */
    static _buildAllNestedSchemaElements(schemaElement, parentId, cache) {
        if (!schemaElement?.children) {
            return;
        }

        Object.entries(schemaElement.children).forEach(([name, child]) => {
            const childId = `${parentId}_${name}`;

            // If this child is a group and has children, build its elements
            if (child.element_type === 'group' && child.children) {
                cache[childId] = this.buildSchemaElementSegments(child, childId);

                // Recursively process this group's children
                this._buildAllNestedSchemaElements(child, childId, cache);
            }
        });
    }

    /**
     * Build schema element segments for a schema group
     * @param {object} schemaGroup - Schema group object
     * @param {string} parentId - Parent segment ID
     * @returns {ParquetSegment[]} Array of schema element segments
     */
    static buildSchemaElementSegments(schemaGroup, parentId) {
        const segments = [];

        if (!schemaGroup?.children) {
            return segments;
        }

        Object.entries(schemaGroup.children).forEach(([name, child], _index) => {
            const childStart = child.start_offset || 0;
            // For groups, calculate the cumulative length including all children
            // For leaf elements, use their own byte_length
            const childLength =
                child.element_type === 'group'
                    ? this._calculateSchemaLength(child)
                    : child.byte_length || 50; // smaller fallback for leaf elements

            const segmentId = `${parentId}_${name}`;

            segments.push(
                new ParquetSegment({
                    id: segmentId,
                    name: name,
                    start: childStart,
                    end: childStart + childLength,
                    metadata: child,
                })
            );
        });

        return segments.sort((a, b) => a.start - b.start);
    }

    /**
     * Get segments for a specific level and parent
     * @param {object} hierarchyCache - Pre-built hierarchy cache
     * @param {string} level - Level name (overview, metadatastructure, schemaelements, rowgroups, columnchunks, pages)
     * @param {string} [parentId] - Parent segment ID for child levels
     * @returns {ParquetSegment[]} Array of segments for the level
     */
    static getSegmentsForLevel(hierarchyCache, level, parentId = null) {
        switch (level) {
            case 'overview':
                return hierarchyCache.overview || [];
            case 'metadatastructure':
                return hierarchyCache.metadatastructure || [];
            case 'schemaelements':
                if (parentId && hierarchyCache.schemaelements[parentId]) {
                    return hierarchyCache.schemaelements[parentId];
                }
                return [];
            case 'rowgroupelements':
                if (parentId && hierarchyCache.rowgroupelements[parentId]) {
                    return hierarchyCache.rowgroupelements[parentId];
                }
                return [];
            case 'indexelements':
                if (parentId && hierarchyCache.indexelements[parentId]) {
                    return hierarchyCache.indexelements[parentId];
                }
                return [];
            case 'keyvaluemetadata':
                if (parentId && hierarchyCache.keyvaluemetadata[parentId]) {
                    return hierarchyCache.keyvaluemetadata[parentId];
                }
                return [];
            case 'rowgroups':
                return hierarchyCache.rowgroups || [];
            case 'columnchunks':
                if (parentId && parentId.startsWith('rowgroup_') && !parentId.includes('meta')) {
                    // Handle physical row groups: rowgroup_0 -> index 0
                    const rowGroupIndex = parseInt(parentId.split('_')[1]);
                    return hierarchyCache.columnchunks[rowGroupIndex] || [];
                }
                return [];
            case 'columnchunkmetadata':
                if (parentId && parentId.startsWith('rowgroup_meta_')) {
                    // Handle metadata row groups: rowgroup_meta_0 -> column chunk metadata
                    return hierarchyCache.columnchunkmetadata[parentId] || [];
                }
                return [];
            case 'pages':
                if (parentId && parentId.includes('chunk_')) {
                    const parts = parentId.split('_');
                    const rowGroupIndex = parseInt(parts[1]);
                    const chunkIndex = parseInt(parts[2]);
                    const key = `${rowGroupIndex}_${chunkIndex}`;
                    return hierarchyCache.pages[key] || [];
                }
                return [];
            default:
                return [];
        }
    }

    /**
     * Find a segment by ID across all cached levels
     * @param {object} hierarchyCache - Pre-built hierarchy cache
     * @param {string} segmentId - Segment ID to find
     * @returns {ParquetSegment|null} Found segment or null
     */
    static findSegment(hierarchyCache, segmentId) {
        // Search overview
        const overviewFound = hierarchyCache.overview.find(seg => seg.id === segmentId);
        if (overviewFound) {
            return overviewFound;
        }

        // Search metadata structure
        const metadataFound = hierarchyCache.metadatastructure.find(seg => seg.id === segmentId);
        if (metadataFound) {
            return metadataFound;
        }

        // Search schema elements
        for (const elements of Object.values(hierarchyCache.schemaelements)) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        // Search row group metadata elements
        for (const elements of Object.values(hierarchyCache.rowgroupelements || {})) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        // Search index elements
        for (const elements of Object.values(hierarchyCache.indexelements || {})) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        // Search key-value metadata elements
        for (const elements of Object.values(hierarchyCache.keyvaluemetadata || {})) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        // Search row groups
        const rowgroupFound = hierarchyCache.rowgroups.find(seg => seg.id === segmentId);
        if (rowgroupFound) {
            return rowgroupFound;
        }

        // Search column chunks
        for (const chunks of Object.values(hierarchyCache.columnchunks)) {
            const chunkFound = chunks.find(seg => seg.id === segmentId);
            if (chunkFound) {
                return chunkFound;
            }
        }

        // Search pages
        for (const pages of Object.values(hierarchyCache.pages)) {
            const pageFound = pages.find(seg => seg.id === segmentId);
            if (pageFound) {
                return pageFound;
            }
        }

        return null;
    }

    // Private helper methods

    /**
     * Calculate the total length of a schema element (itself + all children recursively)
     * @private
     */
    static _calculateSchemaLength(schemaElement) {
        if (!schemaElement) {
            return 0;
        }

        // Start with this element's own byte_length
        let totalLength = schemaElement.byte_length || 0;

        // If this element has children, recursively add their lengths
        if (schemaElement.children) {
            Object.values(schemaElement.children).forEach(child => {
                totalLength += this._calculateSchemaLength(child);
            });
        }

        return totalLength;
    }

    /**
     * Extract column indices information from file data
     * @private
     */
    static _extractColumnIndicesInfo(fileData) {
        const indicesInfo = [];
        const metadata = fileData.metadata;

        if (!metadata?.row_groups) {
            return indicesInfo;
        }

        metadata.row_groups.forEach((rowGroup, rgIndex) => {
            if (rowGroup.column_chunks) {
                Object.entries(rowGroup.column_chunks).forEach(([columnPath, columnMeta]) => {
                    // Check for column index
                    if (columnMeta.column_index_offset && columnMeta.column_index_length) {
                        indicesInfo.push({
                            path: columnPath,
                            row_group: rgIndex,
                            type: 'column_index',
                            offset: columnMeta.column_index_offset,
                            size: columnMeta.column_index_length,
                        });
                    }

                    // Check for offset index (usually present when column index is present)
                    if (columnMeta.column_index_offset && columnMeta.column_index_length) {
                        // Offset index typically follows column index
                        const estimatedOffsetIndexSize = Math.max(
                            50,
                            Math.floor(columnMeta.column_index_length * 0.3)
                        );
                        indicesInfo.push({
                            path: columnPath,
                            row_group: rgIndex,
                            type: 'offset_index',
                            offset: columnMeta.column_index_offset + columnMeta.column_index_length,
                            size: estimatedOffsetIndexSize,
                        });
                    }
                });
            }
        });

        return indicesInfo;
    }

    /**
     * Get column chunks for a specific row group
     * @private
     */
    static _getChunksForRowGroup(fileData, rowGroupIndex) {
        const metadata = fileData.metadata;
        const numRowGroups = metadata?.row_groups?.length || 1;
        const chunksPerRowGroup = Math.ceil((fileData.column_chunks?.length || 0) / numRowGroups);
        const startIndex = rowGroupIndex * chunksPerRowGroup;
        const endIndex = Math.min(
            startIndex + chunksPerRowGroup,
            fileData.column_chunks?.length || 0
        );

        return (fileData.column_chunks || []).slice(startIndex, endIndex);
    }

    /**
     * Get chunk data for a specific row group and chunk index
     * @private
     */
    static _getChunkData(fileData, rowGroupIndex, chunkIndex) {
        const rowGroupChunks = this._getChunksForRowGroup(fileData, rowGroupIndex);
        return rowGroupChunks[chunkIndex];
    }

    /**
     * Find column metadata for a specific column in a row group
     * @private
     */
    static _findColumnMetadata(fileData, rowGroupIndex, columnPath) {
        const metadata = fileData.metadata;
        if (!metadata?.row_groups?.[rowGroupIndex]) {
            return null;
        }

        const rowGroup = metadata.row_groups[rowGroupIndex];
        return rowGroup.column_chunks?.[columnPath] || null;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SegmentHierarchyBuilder;
}
