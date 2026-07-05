/**
 * Parquet Type Resolver
 * Pure functions for resolving Parquet type codes to human-readable names.
 */
import { ParquetConstants } from './parquet-constants';
import type { ColumnMetadata, LogicalType } from '../types';

export interface ColumnTypeInfo {
    physical: string;
    logical: string | null;
    converted: string | null;
    display: string | null;
}

export class ParquetTypeResolver {
    /** Resolve physical type code to name. */
    static getPhysicalTypeName(typeCode: number | null | undefined): string {
        if (typeCode === null || typeCode === undefined) {
            return 'Unknown';
        }
        return ParquetConstants.PHYSICAL_TYPES[typeCode] ?? `TYPE_${typeCode}`;
    }

    /** Resolve logical type structure to name, or null if not present. */
    static getLogicalTypeName(logicalType: LogicalType | null | undefined): string | null {
        if (!logicalType || logicalType.logical_type === undefined) {
            return null;
        }

        const typeName = ParquetConstants.LOGICAL_TYPES[logicalType.logical_type];
        if (!typeName) {
            return `LOGICAL_${logicalType.logical_type}`;
        }

        // DECIMAL type with precision and scale
        if (logicalType.logical_type === 5 && logicalType.precision && logicalType.scale) {
            return `${typeName}(${logicalType.precision}, ${logicalType.scale})`;
        }

        // INT type with bit width and signedness
        if (
            logicalType.logical_type === 10 &&
            logicalType.bit_width &&
            logicalType.is_signed !== undefined
        ) {
            const signedness = logicalType.is_signed ? 'signed' : 'unsigned';
            return `${typeName}(${logicalType.bit_width}, ${signedness})`;
        }

        return typeName;
    }

    /** Resolve converted type code to name (legacy logical types). */
    static getConvertedTypeName(convertedType: number | null | undefined): string | null {
        if (convertedType === null || convertedType === undefined) {
            return null;
        }
        return ParquetConstants.CONVERTED_TYPES[convertedType] ?? `CONVERTED_${convertedType}`;
    }

    /** Resolve compression codec code to name. */
    static getCompressionName(codecCode: number | null | undefined): string {
        if (codecCode === null || codecCode === undefined) {
            return 'Unknown';
        }
        return ParquetConstants.COMPRESSION_CODECS[codecCode] ?? `CODEC_${codecCode}`;
    }

    /** Resolve encoding code to name. */
    static getEncodingName(encodingCode: number | null | undefined): string {
        if (encodingCode === null || encodingCode === undefined) {
            return 'Unknown';
        }
        return ParquetConstants.ENCODINGS[encodingCode] ?? `ENC_${encodingCode}`;
    }

    /** Resolve multiple encoding codes to comma-separated names. */
    static getEncodingNames(encodingCodes: number[] | null | undefined): string {
        if (!Array.isArray(encodingCodes) || encodingCodes.length === 0) {
            return 'None';
        }
        return encodingCodes.map(code => this.getEncodingName(code)).join(', ');
    }

    /** Resolve repetition type code to name. */
    static getRepetitionTypeName(repetitionType: number | null | undefined): string {
        if (repetitionType === null || repetitionType === undefined) {
            return 'Unknown';
        }
        return ParquetConstants.REPETITION_TYPES[repetitionType] ?? `REP_${repetitionType}`;
    }

    /** Combine physical, logical, and converted type info for a column. */
    static getColumnTypeInfo(columnMetadata: ColumnMetadata): ColumnTypeInfo {
        const typeInfo: ColumnTypeInfo = {
            physical: this.getPhysicalTypeName(columnMetadata.type),
            logical: null,
            converted: null,
            display: null,
        };

        if (columnMetadata.logical_type) {
            typeInfo.logical = this.getLogicalTypeName(columnMetadata.logical_type);
        }

        if (columnMetadata.converted_type !== undefined) {
            typeInfo.converted = this.getConvertedTypeName(columnMetadata.converted_type);
        }

        typeInfo.display = typeInfo.logical || typeInfo.converted || typeInfo.physical;
        return typeInfo;
    }
}
