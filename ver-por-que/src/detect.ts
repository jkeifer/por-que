/** Parquet files start (and end) with the 4-byte magic number "PAR1". */
const PARQUET_MAGIC = [0x50, 0x41, 0x52, 0x31]; // 'P','A','R','1'

/**
 * Detects parquet by magic bytes, falling back to the .parquet extension when
 * the header is inconclusive (e.g. a truncated read). Magic wins over the name.
 */
export function isParquet(head: Uint8Array, name?: string): boolean {
    if (head.length >= 4 && PARQUET_MAGIC.every((b, i) => head[i] === b)) {
        return true;
    }
    return name !== undefined && name.toLowerCase().endsWith('.parquet');
}
