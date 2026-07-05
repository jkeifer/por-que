/**
 * Shared number/byte formatting helpers.
 */

/** Format a byte count as a human-readable size string. */
export function formatBytes(bytes: number, decimals = 2): string {
    if (bytes === 0) {
        return '0 Bytes';
    }
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + (sizes[i] ?? 'Bytes');
}

/** Format a number with locale grouping, or 'N/A' when nullish. */
export function formatNumber(num: number | null | undefined): string {
    if (num === null || num === undefined) {
        return 'N/A';
    }
    return num.toLocaleString();
}

/** Format a byte offset with underscore thousands grouping (e.g. 1_234_567). */
export function formatOffset(n: number): string {
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_');
}
