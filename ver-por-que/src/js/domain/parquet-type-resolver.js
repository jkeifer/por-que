/**
 * Parquet Type Resolver
 * Pure functions for resolving Parquet type codes to human-readable names
 * Eliminates code duplication and provides consistent type resolution across components
 */
class ParquetTypeResolver {
    /**
     * Resolve physical type code to name
     * @param {number} typeCode - Physical type code from Parquet specification
     * @returns {string} Human-readable type name
     */
    static getPhysicalTypeName(typeCode) {
        if (typeCode === null || typeCode === undefined) {
            return 'Unknown';
        }

        return ParquetConstants.PHYSICAL_TYPES[typeCode] || `TYPE_${typeCode}`;
    }

    /**
     * Resolve logical type structure to name
     * @param {object} logicalType - Logical type object from schema
     * @returns {string|null} Human-readable logical type name or null if not present
     */
    static getLogicalTypeName(logicalType) {
        if (!logicalType || logicalType.logical_type === undefined) {
            return null;
        }

        const typeName = ParquetConstants.LOGICAL_TYPES[logicalType.logical_type];
        if (!typeName) {
            return `LOGICAL_${logicalType.logical_type}`;
        }

        // Add additional type parameters for complex types
        if (logicalType.logical_type === 5 && logicalType.precision && logicalType.scale) {
            // DECIMAL type with precision and scale
            return `${typeName}(${logicalType.precision}, ${logicalType.scale})`;
        }

        if (
            logicalType.logical_type === 10 &&
            logicalType.bit_width &&
            logicalType.is_signed !== undefined
        ) {
            // INT type with bit width and signedness
            const signedness = logicalType.is_signed ? 'signed' : 'unsigned';
            return `${typeName}(${logicalType.bit_width}, ${signedness})`;
        }

        return typeName;
    }

    /**
     * Resolve converted type code to name (legacy logical types)
     * @param {number} convertedType - Converted type code
     * @returns {string|null} Human-readable converted type name or null if not present
     */
    static getConvertedTypeName(convertedType) {
        if (convertedType === null || convertedType === undefined) {
            return null;
        }

        return ParquetConstants.CONVERTED_TYPES[convertedType] || `CONVERTED_${convertedType}`;
    }

    /**
     * Resolve compression codec code to name
     * @param {number} codecCode - Compression codec code
     * @returns {string} Human-readable compression name
     */
    static getCompressionName(codecCode) {
        if (codecCode === null || codecCode === undefined) {
            return 'Unknown';
        }

        return ParquetConstants.COMPRESSION_CODECS[codecCode] || `CODEC_${codecCode}`;
    }

    /**
     * Resolve encoding code to name
     * @param {number} encodingCode - Encoding code
     * @returns {string} Human-readable encoding name
     */
    static getEncodingName(encodingCode) {
        if (encodingCode === null || encodingCode === undefined) {
            return 'Unknown';
        }

        return ParquetConstants.ENCODINGS[encodingCode] || `ENC_${encodingCode}`;
    }

    /**
     * Resolve multiple encoding codes to comma-separated names
     * @param {number[]} encodingCodes - Array of encoding codes
     * @returns {string} Comma-separated encoding names
     */
    static getEncodingNames(encodingCodes) {
        if (!Array.isArray(encodingCodes) || encodingCodes.length === 0) {
            return 'None';
        }

        return encodingCodes.map(code => this.getEncodingName(code)).join(', ');
    }

    /**
     * Resolve page type code to name
     * @param {number} pageType - Page type code
     * @returns {string} Human-readable page type name
     */
    static getPageTypeName(pageType) {
        if (pageType === null || pageType === undefined) {
            return 'Unknown';
        }

        return ParquetConstants.PAGE_TYPES[pageType] || `PAGE_${pageType}`;
    }

    /**
     * Resolve repetition type code to name
     * @param {number} repetitionType - Repetition type code
     * @returns {string} Human-readable repetition type name
     */
    static getRepetitionTypeName(repetitionType) {
        if (repetitionType === null || repetitionType === undefined) {
            return 'Unknown';
        }

        return ParquetConstants.REPETITION_TYPES[repetitionType] || `REP_${repetitionType}`;
    }

    /**
     * Get comprehensive type information for a column
     * Combines physical type, logical type, and converted type information
     * @param {object} columnMetadata - Column metadata object
     * @returns {object} Comprehensive type information
     */
    static getColumnTypeInfo(columnMetadata) {
        const typeInfo = {
            physical: this.getPhysicalTypeName(columnMetadata.type),
            logical: null,
            converted: null,
            display: null,
        };

        // Get logical type info
        if (columnMetadata.logical_type) {
            typeInfo.logical = this.getLogicalTypeName(columnMetadata.logical_type);
        }

        // Get converted type info (legacy)
        if (columnMetadata.converted_type !== undefined) {
            typeInfo.converted = this.getConvertedTypeName(columnMetadata.converted_type);
        }

        // Determine best display name (prefer logical over converted over physical)
        typeInfo.display = typeInfo.logical || typeInfo.converted || typeInfo.physical;

        return typeInfo;
    }

    /**
     * Format type information as a human-readable string
     * @param {object} columnMetadata - Column metadata object
     * @returns {string} Formatted type string
     */
    static formatTypeString(columnMetadata) {
        const typeInfo = this.getColumnTypeInfo(columnMetadata);

        if (typeInfo.logical && typeInfo.logical !== typeInfo.physical) {
            return `${typeInfo.logical} (${typeInfo.physical})`;
        }

        if (typeInfo.converted && typeInfo.converted !== typeInfo.physical) {
            return `${typeInfo.converted} (${typeInfo.physical})`;
        }

        return typeInfo.physical;
    }

    /**
     * Check if a type supports statistics
     * @param {number} physicalType - Physical type code
     * @returns {boolean} True if type supports statistics
     */
    static supportsStatistics(physicalType) {
        // All types except INT96 typically support statistics
        return physicalType !== 3; // INT96 is often not supported for statistics
    }

    /**
     * Get default statistics display precision for numeric types
     * @param {number} physicalType - Physical type code
     * @returns {number} Decimal places to show for statistics
     */
    static getStatisticsPrecision(physicalType) {
        const precisionMap = {
            [0]: 0, // BOOLEAN - no decimals
            [1]: 0, // INT32 - no decimals
            [2]: 0, // INT64 - no decimals
            [3]: 0, // INT96 - no decimals
            [4]: 6, // FLOAT - 6 decimal places
            [5]: 10, // DOUBLE - 10 decimal places
            [6]: 0, // BYTE_ARRAY - no decimals (string)
            [7]: 0, // FIXED_LEN_BYTE_ARRAY - no decimals
        };

        return precisionMap[physicalType] || 2;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ParquetTypeResolver;
}
