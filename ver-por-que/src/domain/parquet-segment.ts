/**
 * Parquet Segment
 * Domain object representing a segment in the parquet file structure.
 */
import { formatBytes } from '../format';
import { ParquetTypeResolver, type ColumnTypeInfo } from './parquet-type-resolver';
import type { FileColumnChunk, PhysicalColumnChunk, SegmentMetadata } from '../types';

export interface SegmentConfig {
    id: string;
    name: string;
    start: number;
    end: number;
    metadata?: SegmentMetadata | null;
    description?: string;
    rowGroupIndex?: number;
    chunkIndex?: number;
    pageIndex?: number;
    columnPath?: string;
    physicalMetadata?: PhysicalColumnChunk | null;
    logicalMetadata?: FileColumnChunk | null;
}

export interface CompressionInfo {
    algorithm: string;
    ratio: string | null;
    compressedSize: number | null;
    uncompressedSize: number | null;
}

export interface StatisticsInfo {
    minValue: unknown;
    maxValue: unknown;
    nullCount: number | null | undefined;
    distinctCount: number | null | undefined;
    nullPercentage: string;
    distinctPercentage: string;
}

export class ParquetSegment {
    id: string;
    name: string;
    start: number;
    end: number;
    metadata: SegmentMetadata | null;
    rowGroupIndex?: number;
    chunkIndex?: number;
    pageIndex?: number;
    columnPath?: string;
    description: string;
    physicalMetadata: PhysicalColumnChunk | null;
    logicalMetadata: FileColumnChunk | null;
    /** Optional page-kind tag; never populated today, read defensively. */
    type?: string;

    constructor(config: SegmentConfig) {
        this._validateRequired(config, ['id', 'name', 'start', 'end']);

        this.id = config.id;
        this.name = config.name;
        this.start = config.start;
        this.end = config.end;

        this.metadata = config.metadata ?? null;
        this.rowGroupIndex = config.rowGroupIndex;
        this.chunkIndex = config.chunkIndex;
        this.pageIndex = config.pageIndex;
        this.columnPath = config.columnPath;
        this.description = config.description || this._generateDescription();
        this.physicalMetadata = config.physicalMetadata ?? null;
        this.logicalMetadata = config.logicalMetadata ?? null;

        this._validate();
    }

    /** Size of this segment in bytes. */
    get size(): number {
        return Math.max(0, this.end - this.start);
    }

    get formattedSize(): string {
        return formatBytes(this.size);
    }

    get formattedStart(): string {
        return formatBytes(this.start);
    }

    get formattedEnd(): string {
        return formatBytes(this.end);
    }

    /** True if this segment can be expanded into child segments. */
    get canHaveChildren(): boolean {
        if (this.metadata && typeof this.metadata === 'object') {
            if (this.metadata.children && Object.keys(this.metadata.children).length > 0) {
                return true;
            }
            if (this.metadata.row_groups && Array.isArray(this.metadata.row_groups)) {
                return true;
            }
            if (this.metadata.indices_count && this.metadata.indices_count > 0) {
                return true;
            }
            if (
                this.metadata.entries &&
                Array.isArray(this.metadata.entries) &&
                this.metadata.entries.length > 0
            ) {
                return true;
            }
        }
        if (this.id === 'metadata' || this.id === 'rowgroups') {
            return true;
        }
        if (this.rowGroupIndex !== undefined || this.chunkIndex !== undefined) {
            return true;
        }
        return false;
    }

    /** Expected child level name for drill-down, or null. */
    get childLevelName(): string | null {
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

        if (
            this.rowGroupIndex !== undefined &&
            this.chunkIndex === undefined &&
            !this.id.includes('meta')
        ) {
            return 'columnchunks';
        }
        if (this.id.startsWith('rowgroup_meta_')) {
            return 'columnchunkmetadata';
        }
        if (this.chunkIndex !== undefined && this.pageIndex === undefined) {
            return 'pages';
        }
        if (this.metadata && this.metadata.children && this.metadata.element_type === 'group') {
            return 'schemaelements';
        }

        return null;
    }

    /** Index for color cycling based on segment position. */
    get colorIndex(): number {
        return this.rowGroupIndex ?? this.chunkIndex ?? this.pageIndex ?? 0;
    }

    /** Comprehensive type information (for column segments). */
    get typeInfo(): ColumnTypeInfo | null {
        if (!this.logicalMetadata?.metadata) {
            return null;
        }
        return ParquetTypeResolver.getColumnTypeInfo(this.logicalMetadata.metadata);
    }

    /** Compression information (for column segments). */
    get compressionInfo(): CompressionInfo | null {
        if (!this.physicalMetadata && !this.logicalMetadata) {
            return null;
        }

        const info: CompressionInfo = {
            algorithm: 'Unknown',
            ratio: null,
            compressedSize: null,
            uncompressedSize: null,
        };

        if (this.physicalMetadata?.codec !== undefined) {
            info.algorithm = ParquetTypeResolver.getCompressionName(this.physicalMetadata.codec);
        }

        const logicalMeta = this.logicalMetadata?.metadata;
        if (logicalMeta?.total_compressed_size && logicalMeta?.total_uncompressed_size) {
            info.compressedSize = logicalMeta.total_compressed_size;
            info.uncompressedSize = logicalMeta.total_uncompressed_size;
            info.ratio = ((info.compressedSize / info.uncompressedSize) * 100).toFixed(1) + '%';
        }

        return info;
    }

    /** Encoding information (for column segments). */
    get encodingInfo(): string | null {
        if (!this.logicalMetadata?.metadata?.encodings) {
            return null;
        }
        return ParquetTypeResolver.getEncodingNames(this.logicalMetadata.metadata.encodings);
    }

    /** Statistics information (for column segments). */
    get statisticsInfo(): StatisticsInfo | null {
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

    /** Create a copy of this segment with updated properties. */
    clone(updates: Partial<SegmentConfig> = {}): ParquetSegment {
        return new ParquetSegment({
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
        });
    }

    private _validateRequired(config: SegmentConfig, required: (keyof SegmentConfig)[]): void {
        for (const prop of required) {
            if (config[prop] === undefined || config[prop] === null) {
                throw new Error(`ParquetSegment: Required property '${prop}' is missing`);
            }
        }
    }

    private _validate(): void {
        if (this.start < 0) {
            throw new Error(`ParquetSegment: Start offset cannot be negative (${this.start})`);
        }
        if (this.end < this.start) {
            throw new Error(
                `ParquetSegment: End offset (${this.end}) cannot be less than start offset (${this.start})`
            );
        }

        const indices: [string, number | undefined][] = [
            ['rowGroupIndex', this.rowGroupIndex],
            ['chunkIndex', this.chunkIndex],
            ['pageIndex', this.pageIndex],
        ];
        for (const [name, value] of indices) {
            if (value !== undefined && value < 0) {
                throw new Error(`ParquetSegment: ${name} cannot be negative (${value})`);
            }
        }
    }

    private _generateDescription(): string {
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

        if (this.rowGroupIndex !== undefined && this.chunkIndex === undefined) {
            return `Row Group <code>${this.rowGroupIndex}</code>`;
        }

        if (this.columnPath) {
            return `Column Chunk <code>${this.columnPath}</code>`;
        }

        if (this.pageIndex !== undefined) {
            if (this.name.includes('DICT')) {
                return 'Dictionary page';
            } else if (this.name.includes('DATA')) {
                return `Data Page <code>${this.name}</code>`;
            } else if (this.name.includes('IDX')) {
                return `Index page <code>${this.pageIndex}</code>`;
            }
        }

        if (this.metadata && this.metadata.element_type === 'group') {
            return `Schema Group <code>${this.name}</code>`;
        } else if (this.metadata && this.metadata.element_type === 'column') {
            return `Schema Element <code>${this.name}</code>`;
        }

        return `${this.name} (${this.formattedSize})`;
    }
}
