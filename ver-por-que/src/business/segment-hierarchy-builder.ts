/**
 * Segment Hierarchy Builder
 * Pure functions for building parquet file segment hierarchies.
 */
import { ParquetConstants } from '../domain/parquet-constants';
import { ParquetSegment } from '../domain/parquet-segment';
import type { FileColumnChunk, FileData, PhysicalColumnChunk, SchemaNode } from '../types';

export interface HierarchyCache {
    overview: ParquetSegment[];
    rowgroups: ParquetSegment[];
    columnchunks: Record<number, ParquetSegment[]>;
    pages: Record<string, ParquetSegment[]>;
    metadatastructure: ParquetSegment[];
    schemaelements: Record<string, ParquetSegment[]>;
    rowgroupelements: Record<string, ParquetSegment[]>;
    indexelements: Record<string, ParquetSegment[]>;
    keyvaluemetadata: Record<string, ParquetSegment[]>;
    columnchunkmetadata: Record<string, ParquetSegment[]>;
}

interface ColumnIndexInfo {
    path: string;
    row_group: number;
    type: string;
    offset: number;
    size: number;
}

export class SegmentHierarchyBuilder {
    /** Build complete hierarchy for a parquet file. */
    static buildAll(fileData: FileData): HierarchyCache {
        const cache: HierarchyCache = {
            overview: [],
            rowgroups: [],
            columnchunks: {},
            pages: {},
            metadatastructure: [],
            schemaelements: {},
            rowgroupelements: {},
            indexelements: {},
            keyvaluemetadata: {},
            columnchunkmetadata: {},
        };

        cache.overview = this.buildOverviewSegments(fileData);
        cache.rowgroups = this.buildRowGroupSegments(fileData);
        cache.metadatastructure = this.buildMetadataStructureSegments(fileData);
        cache.schemaelements = this.buildAllSchemaElementSegments(fileData);
        cache.rowgroupelements = this.buildRowGroupMetadataSegments(fileData);
        cache.indexelements = this.buildColumnIndexSegments(fileData);
        cache.keyvaluemetadata = this.buildKeyValueMetadataSegments(fileData);
        cache.columnchunkmetadata = this.buildColumnChunkMetadataSegments(fileData);

        const metadata = fileData.metadata;
        if (metadata?.row_groups) {
            metadata.row_groups.forEach((_, index) => {
                cache.columnchunks[index] = this.buildColumnChunkSegments(fileData, index);
            });
        }

        for (const [rowGroupIndex, chunks] of Object.entries(cache.columnchunks)) {
            chunks.forEach((_chunk, chunkIndex) => {
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

    /** Build overview segments (MAGIC | DATA PAGES | METADATA | FOOTER | MAGIC). */
    static buildOverviewSegments(fileData: FileData): ParquetSegment[] {
        const segments: ParquetSegment[] = [];
        const constants = ParquetConstants.FILE_STRUCTURE;

        const metadataSize =
            fileData.metadata?.total_byte_size ||
            (fileData.metadata?.metadata
                ? JSON.stringify(fileData.metadata.metadata).length
                : 1024);

        let rowGroupsStart = constants.MAGIC_SIZE;
        let rowGroupsEnd = fileData.filesize - metadataSize - constants.FOOTER_SIZE;

        if (fileData.column_chunks && fileData.column_chunks.length > 0) {
            const allOffsets = fileData.column_chunks.map(chunk => chunk.start_offset);
            const allSizes = fileData.column_chunks.map(chunk => chunk.total_byte_size);
            const allEnds = fileData.column_chunks.map(
                (_chunk, i) => allOffsets[i]! + allSizes[i]!
            );

            rowGroupsStart = Math.min(...allOffsets);
            rowGroupsEnd = Math.max(...allEnds);
        }

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

    /** Build row group segments. */
    static buildRowGroupSegments(fileData: FileData): ParquetSegment[] {
        const metadata = fileData.metadata;
        if (!metadata?.row_groups) {
            return [];
        }

        const segments: ParquetSegment[] = [];

        metadata.row_groups.forEach((rowGroup, index) => {
            const rowGroupChunks = this._getChunksForRowGroup(fileData, index);
            if (rowGroupChunks.length === 0) {
                return;
            }

            const chunkOffsets = rowGroupChunks.map(chunk => chunk.start_offset);
            const chunkSizes = rowGroupChunks.map(chunk => chunk.total_byte_size);
            const chunkEnds = rowGroupChunks.map((_chunk, i) => chunkOffsets[i]! + chunkSizes[i]!);

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

    /** Build column chunk segments within a row group. */
    static buildColumnChunkSegments(fileData: FileData, rowGroupIndex: number): ParquetSegment[] {
        const segments: ParquetSegment[] = [];
        const rowGroupChunks = this._getChunksForRowGroup(fileData, rowGroupIndex);

        rowGroupChunks.forEach((chunk, chunkIndex) => {
            const columnPath = chunk.path_in_schema;
            const chunkSize = chunk.total_byte_size || 0;
            const chunkStart = chunk.start_offset || 0;

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

        segments.sort((a, b) => a.start - b.start);
        return segments;
    }

    /** Build page segments within a column chunk. */
    static buildPageSegments(
        fileData: FileData,
        rowGroupIndex: number,
        chunkIndex: number
    ): ParquetSegment[] {
        const segments: ParquetSegment[] = [];
        const chunk = this._getChunkData(fileData, rowGroupIndex, chunkIndex);

        if (!chunk) {
            return segments;
        }

        if (chunk.dictionary_page) {
            const dictPage = chunk.dictionary_page;
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

        if (chunk.data_pages) {
            chunk.data_pages.forEach((page, pageIndex) => {
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

        if (chunk.index_pages) {
            chunk.index_pages.forEach((page, pageIndex) => {
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

    /** Build metadata structure segments (schema, row group metadata, indices). */
    static buildMetadataStructureSegments(fileData: FileData): ParquetSegment[] {
        const segments: ParquetSegment[] = [];
        const metadata = fileData.metadata;

        if (!metadata) {
            return segments;
        }

        let currentOffset = metadata?.start_offset || 0;

        if (metadata.schema_root) {
            const schemaStart = metadata.schema_root.start_offset || currentOffset;
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

        if (metadata.row_groups && metadata.row_groups.length > 0) {
            let totalRowGroupMetadataSize = 0;
            metadata.row_groups.forEach(rg => {
                totalRowGroupMetadataSize += rg.byte_length || 200;
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

        if (
            metadata.key_value_metadata &&
            Array.isArray(metadata.key_value_metadata) &&
            metadata.key_value_metadata.length > 0
        ) {
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

    /** Build individual row group metadata segments. */
    static buildRowGroupMetadataSegments(fileData: FileData): Record<string, ParquetSegment[]> {
        const cache: Record<string, ParquetSegment[]> = {};
        const metadata = fileData.metadata;
        if (!metadata?.row_groups) {
            return cache;
        }

        const segments: ParquetSegment[] = [];
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

    /** Build column index segments. */
    static buildColumnIndexSegments(fileData: FileData): Record<string, ParquetSegment[]> {
        const cache: Record<string, ParquetSegment[]> = {};
        const indicesInfo = this._extractColumnIndicesInfo(fileData);

        if (indicesInfo.length === 0) {
            return cache;
        }

        const segments: ParquetSegment[] = [];

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

    /** Build key-value metadata segments. */
    static buildKeyValueMetadataSegments(fileData: FileData): Record<string, ParquetSegment[]> {
        const cache: Record<string, ParquetSegment[]> = {};
        const metadata = fileData.metadata?.metadata;

        if (!metadata?.key_value_metadata || !Array.isArray(metadata.key_value_metadata)) {
            return cache;
        }

        const segments: ParquetSegment[] = [];

        metadata.key_value_metadata.forEach((entry, index) => {
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

    /** Build column chunk metadata segments for row group metadata. */
    static buildColumnChunkMetadataSegments(fileData: FileData): Record<string, ParquetSegment[]> {
        const cache: Record<string, ParquetSegment[]> = {};
        const metadata = fileData.metadata;

        if (!metadata?.row_groups) {
            return cache;
        }

        metadata.row_groups.forEach((rowGroup, rgIndex) => {
            const parentId = `rowgroup_meta_${rgIndex}`;
            const segments: ParquetSegment[] = [];

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

    /** Build all schema element segments for all schema groups recursively. */
    static buildAllSchemaElementSegments(fileData: FileData): Record<string, ParquetSegment[]> {
        const cache: Record<string, ParquetSegment[]> = {};
        const metadata = fileData.metadata;
        if (!metadata?.schema_root) {
            return cache;
        }

        cache['schema_root'] = this.buildSchemaElementSegments(metadata.schema_root, 'schema_root');
        this._buildAllNestedSchemaElements(metadata.schema_root, 'schema_root', cache);

        return cache;
    }

    private static _buildAllNestedSchemaElements(
        schemaElement: SchemaNode,
        parentId: string,
        cache: Record<string, ParquetSegment[]>
    ): void {
        if (!schemaElement?.children) {
            return;
        }

        Object.entries(schemaElement.children).forEach(([name, child]) => {
            const childId = `${parentId}_${name}`;

            if (child.element_type === 'group' && child.children) {
                cache[childId] = this.buildSchemaElementSegments(child, childId);
                this._buildAllNestedSchemaElements(child, childId, cache);
            }
        });
    }

    /** Build schema element segments for a schema group. */
    static buildSchemaElementSegments(schemaGroup: SchemaNode, parentId: string): ParquetSegment[] {
        const segments: ParquetSegment[] = [];

        if (!schemaGroup?.children) {
            return segments;
        }

        Object.entries(schemaGroup.children).forEach(([name, child]) => {
            const childStart = child.start_offset || 0;
            const childLength =
                child.element_type === 'group'
                    ? this._calculateSchemaLength(child)
                    : child.byte_length || 50;

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

    /** Get segments for a specific level and parent. */
    static getSegmentsForLevel(
        hierarchyCache: HierarchyCache,
        level: string,
        parentId: string | null = null
    ): ParquetSegment[] {
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
                    const rowGroupIndex = parseInt(parentId.split('_')[1] ?? '');
                    return hierarchyCache.columnchunks[rowGroupIndex] || [];
                }
                return [];
            case 'columnchunkmetadata':
                if (parentId && parentId.startsWith('rowgroup_meta_')) {
                    return hierarchyCache.columnchunkmetadata[parentId] || [];
                }
                return [];
            case 'pages':
                if (parentId && parentId.includes('chunk_')) {
                    const parts = parentId.split('_');
                    const rowGroupIndex = parseInt(parts[1] ?? '');
                    const chunkIndex = parseInt(parts[2] ?? '');
                    const key = `${rowGroupIndex}_${chunkIndex}`;
                    return hierarchyCache.pages[key] || [];
                }
                return [];
            default:
                return [];
        }
    }

    /** Find a segment by ID across all cached levels. */
    static findSegment(hierarchyCache: HierarchyCache, segmentId: string): ParquetSegment | null {
        const overviewFound = hierarchyCache.overview.find(seg => seg.id === segmentId);
        if (overviewFound) {
            return overviewFound;
        }

        const metadataFound = hierarchyCache.metadatastructure.find(seg => seg.id === segmentId);
        if (metadataFound) {
            return metadataFound;
        }

        for (const elements of Object.values(hierarchyCache.schemaelements)) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        for (const elements of Object.values(hierarchyCache.rowgroupelements)) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        for (const elements of Object.values(hierarchyCache.indexelements)) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        for (const elements of Object.values(hierarchyCache.keyvaluemetadata)) {
            const elementFound = elements.find(seg => seg.id === segmentId);
            if (elementFound) {
                return elementFound;
            }
        }

        const rowgroupFound = hierarchyCache.rowgroups.find(seg => seg.id === segmentId);
        if (rowgroupFound) {
            return rowgroupFound;
        }

        for (const chunks of Object.values(hierarchyCache.columnchunks)) {
            const chunkFound = chunks.find(seg => seg.id === segmentId);
            if (chunkFound) {
                return chunkFound;
            }
        }

        for (const pages of Object.values(hierarchyCache.pages)) {
            const pageFound = pages.find(seg => seg.id === segmentId);
            if (pageFound) {
                return pageFound;
            }
        }

        return null;
    }

    // Private helpers

    private static _calculateSchemaLength(schemaElement: SchemaNode | undefined): number {
        if (!schemaElement) {
            return 0;
        }

        let totalLength = schemaElement.byte_length || 0;

        if (schemaElement.children) {
            Object.values(schemaElement.children).forEach(child => {
                totalLength += this._calculateSchemaLength(child);
            });
        }

        return totalLength;
    }

    private static _extractColumnIndicesInfo(fileData: FileData): ColumnIndexInfo[] {
        const indicesInfo: ColumnIndexInfo[] = [];
        const metadata = fileData.metadata;

        if (!metadata?.row_groups) {
            return indicesInfo;
        }

        metadata.row_groups.forEach((rowGroup, rgIndex) => {
            if (rowGroup.column_chunks) {
                Object.entries(rowGroup.column_chunks).forEach(([columnPath, columnMeta]) => {
                    if (columnMeta.column_index_offset && columnMeta.column_index_length) {
                        indicesInfo.push({
                            path: columnPath,
                            row_group: rgIndex,
                            type: 'column_index',
                            offset: columnMeta.column_index_offset,
                            size: columnMeta.column_index_length,
                        });
                    }

                    if (columnMeta.column_index_offset && columnMeta.column_index_length) {
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

    private static _getChunksForRowGroup(
        fileData: FileData,
        rowGroupIndex: number
    ): PhysicalColumnChunk[] {
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

    private static _getChunkData(
        fileData: FileData,
        rowGroupIndex: number,
        chunkIndex: number
    ): PhysicalColumnChunk | undefined {
        const rowGroupChunks = this._getChunksForRowGroup(fileData, rowGroupIndex);
        return rowGroupChunks[chunkIndex];
    }

    private static _findColumnMetadata(
        fileData: FileData,
        rowGroupIndex: number,
        columnPath: string
    ): FileColumnChunk | null {
        const rowGroup = fileData.metadata?.row_groups?.[rowGroupIndex];
        if (!rowGroup) {
            return null;
        }
        return rowGroup.column_chunks?.[columnPath] ?? null;
    }
}
