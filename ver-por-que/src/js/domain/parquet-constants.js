/**
 * Parquet Constants
 * Centralized repository for all Parquet format constants and type mappings
 * Single source of truth to eliminate duplication across components
 */
class ParquetConstants {
    /**
     * Physical data types (from Parquet specification)
     * Used in schema definitions and column metadata
     */
    static PHYSICAL_TYPES = {
        0: 'BOOLEAN',
        1: 'INT32',
        2: 'INT64',
        3: 'INT96',
        4: 'FLOAT',
        5: 'DOUBLE',
        6: 'BYTE_ARRAY',
        7: 'FIXED_LEN_BYTE_ARRAY',
    };

    /**
     * Logical types (from Parquet specification)
     * Higher-level semantic types that map to physical types
     */
    static LOGICAL_TYPES = {
        1: 'STRING',
        2: 'MAP',
        3: 'LIST',
        4: 'ENUM',
        5: 'DECIMAL',
        6: 'DATE',
        7: 'TIME',
        8: 'TIMESTAMP',
        10: 'INT',
        11: 'UNKNOWN',
        12: 'JSON',
        13: 'BSON',
        14: 'UUID',
        15: 'FLOAT16',
        16: 'VARIANT',
        17: 'GEOMETRY',
        18: 'GEOGRAPHY',
    };

    /**
     * Converted types (legacy logical types from older Parquet versions)
     * Maintained for backward compatibility
     */
    static CONVERTED_TYPES = {
        0: 'UTF8',
        1: 'MAP',
        2: 'MAP_KEY_VALUE',
        3: 'LIST',
        4: 'ENUM',
        5: 'DECIMAL',
        6: 'DATE',
        7: 'TIME_MILLIS',
        8: 'TIME_MICROS',
        9: 'TIMESTAMP_MILLIS',
        10: 'TIMESTAMP_MICROS',
        11: 'UINT_8',
        12: 'UINT_16',
        13: 'UINT_32',
        14: 'UINT_64',
        15: 'INT_8',
        16: 'INT_16',
        17: 'INT_32',
        18: 'INT_64',
        19: 'JSON',
        20: 'BSON',
        21: 'INTERVAL',
    };

    /**
     * Compression algorithms (from Parquet specification)
     * Used in column chunk metadata and page headers
     */
    static COMPRESSION_CODECS = {
        0: 'UNCOMPRESSED',
        1: 'SNAPPY',
        2: 'GZIP',
        3: 'LZO',
        4: 'BROTLI',
        5: 'LZ4',
        6: 'ZSTD',
    };

    /**
     * Encoding algorithms (from Parquet specification)
     * Used in page headers and column metadata
     */
    static ENCODINGS = {
        0: 'PLAIN',
        2: 'DICTIONARY',
        3: 'RLE',
        4: 'BIT_PACKED',
        5: 'DELTA_BINARY_PACKED',
        6: 'DELTA_LENGTH_BYTE_ARRAY',
        7: 'DELTA_BYTE_ARRAY',
        8: 'RLE_DICTIONARY',
        9: 'BYTE_STREAM_SPLIT',
    };

    /**
     * Page types (from Parquet specification)
     * Used in page header definitions
     */
    static PAGE_TYPES = {
        0: 'DATA_V1',
        1: 'INDEX',
        2: 'DICTIONARY',
        3: 'DATA_V2',
    };

    /**
     * Repetition types for schema elements
     * Defines nullability and cardinality of schema fields
     */
    static REPETITION_TYPES = {
        0: 'REQUIRED',
        1: 'OPTIONAL',
        2: 'REPEATED',
    };

    /**
     * File structure constants
     * Physical layout parameters of Parquet files
     */
    static FILE_STRUCTURE = {
        MAGIC_SIZE: 4, // PAR1 magic number size
        FOOTER_SIZE: 8, // Footer length (4 bytes) + PAR1 (4 bytes)
        MAGIC_BYTES: 'PAR1', // File format identifier
    };

    /**
     * Element types for schema tree
     * Distinguishes between groups and leaf columns
     */
    static ELEMENT_TYPES = {
        GROUP: 'group',
        COLUMN: 'column',
    };

    /**
     * Get all available physical type codes
     */
    static getPhysicalTypeCodes() {
        return Object.keys(this.PHYSICAL_TYPES).map(Number);
    }

    /**
     * Get all available logical type codes
     */
    static getLogicalTypeCodes() {
        return Object.keys(this.LOGICAL_TYPES).map(Number);
    }

    /**
     * Get all available compression codec codes
     */
    static getCompressionCodecCodes() {
        return Object.keys(this.COMPRESSION_CODECS).map(Number);
    }

    /**
     * Get all available encoding codes
     */
    static getEncodingCodes() {
        return Object.keys(this.ENCODINGS).map(Number);
    }

    /**
     * Validate if a type code exists in the given type system
     */
    static isValidPhysicalType(typeCode) {
        return typeCode in this.PHYSICAL_TYPES;
    }

    static isValidLogicalType(typeCode) {
        return typeCode in this.LOGICAL_TYPES;
    }

    static isValidCompressionCodec(codecCode) {
        return codecCode in this.COMPRESSION_CODECS;
    }

    static isValidEncoding(encodingCode) {
        return encodingCode in this.ENCODINGS;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ParquetConstants;
}
