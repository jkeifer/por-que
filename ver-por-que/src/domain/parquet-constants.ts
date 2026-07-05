/**
 * Parquet Constants
 * Centralized repository for all Parquet format constants and type mappings.
 */
export class ParquetConstants {
    /** Physical data types (from Parquet specification). */
    static PHYSICAL_TYPES: Record<number, string> = {
        0: 'BOOLEAN',
        1: 'INT32',
        2: 'INT64',
        3: 'INT96',
        4: 'FLOAT',
        5: 'DOUBLE',
        6: 'BYTE_ARRAY',
        7: 'FIXED_LEN_BYTE_ARRAY',
    };

    /** Logical types (from Parquet specification). */
    static LOGICAL_TYPES: Record<number, string> = {
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

    /** Converted types (legacy logical types from older Parquet versions). */
    static CONVERTED_TYPES: Record<number, string> = {
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

    /** Compression algorithms (from Parquet specification). */
    static COMPRESSION_CODECS: Record<number, string> = {
        0: 'UNCOMPRESSED',
        1: 'SNAPPY',
        2: 'GZIP',
        3: 'LZO',
        4: 'BROTLI',
        5: 'LZ4',
        6: 'ZSTD',
    };

    /** Encoding algorithms (from Parquet specification). */
    static ENCODINGS: Record<number, string> = {
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

    /** Page types (from Parquet specification). */
    static PAGE_TYPES: Record<number, string> = {
        0: 'DATA_V1',
        1: 'INDEX',
        2: 'DICTIONARY',
        3: 'DATA_V2',
    };

    /** Repetition types for schema elements. */
    static REPETITION_TYPES: Record<number, string> = {
        0: 'REQUIRED',
        1: 'OPTIONAL',
        2: 'REPEATED',
    };

    /** Physical layout parameters of Parquet files. */
    static FILE_STRUCTURE = {
        MAGIC_SIZE: 4, // PAR1 magic number size
        FOOTER_SIZE: 8, // Footer length (4 bytes) + PAR1 (4 bytes)
        MAGIC_BYTES: 'PAR1', // File format identifier
    };
}
