/**
 * Utility functions for formatting data in the Parquet Explorer
 */

/**
 * Format bytes into human readable format
 * @param {number} bytes - Number of bytes
 * @param {number} decimals - Number of decimal places
 * @returns {string} Formatted byte string
 */
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Format numbers with thousand separators
 * @param {number} num - Number to format
 * @returns {string} Formatted number string
 */
function formatNumber(num) {
    return num.toLocaleString();
}

/**
 * Format compression ratio as percentage
 * @param {number} uncompressed - Uncompressed size
 * @param {number} compressed - Compressed size
 * @returns {string} Formatted compression ratio
 */
function formatCompressionRatio(uncompressed, compressed) {
    if (uncompressed === 0) return '0%';
    const ratio = ((uncompressed - compressed) / uncompressed) * 100;
    return ratio.toFixed(1) + '%';
}

/**
 * Format Parquet type with appropriate styling
 * @param {string} type - Parquet type name
 * @returns {string} HTML string with styled type
 */
function formatParquetType(type) {
    const typeColors = {
        'INT32': '#2196F3',
        'INT64': '#2196F3',
        'FLOAT': '#FF9800',
        'DOUBLE': '#FF9800',
        'BOOLEAN': '#9C27B0',
        'BYTE_ARRAY': '#4CAF50',
        'FIXED_LEN_BYTE_ARRAY': '#4CAF50',
        'group': '#795548'
    };

    const color = typeColors[type] || '#666';
    return `<span class="parquet-type" style="color: ${color}; font-weight: 500;">${type}</span>`;
}

/**
 * Format logical type information
 * @param {string} logicalType - Logical type name
 * @returns {string} Formatted logical type
 */
function formatLogicalType(logicalType) {
    if (!logicalType) return '';

    const logicalTypeColors = {
        'STRING': '#4CAF50',
        'INT_8': '#2196F3',
        'INT_16': '#2196F3',
        'INT_32': '#2196F3',
        'INT_64': '#2196F3',
        'UINT_8': '#2196F3',
        'UINT_16': '#2196F3',
        'UINT_32': '#2196F3',
        'UINT_64': '#2196F3',
        'DECIMAL': '#FF9800',
        'DATE': '#9C27B0',
        'TIME_MILLIS': '#9C27B0',
        'TIME_MICROS': '#9C27B0',
        'TIMESTAMP_MILLIS': '#9C27B0',
        'TIMESTAMP_MICROS': '#9C27B0',
        'JSON': '#FF5722',
        'BSON': '#FF5722',
        'UUID': '#607D8B'
    };

    const color = logicalTypeColors[logicalType] || '#666';
    return `<span class="logical-type" style="background: ${color}20; color: ${color}; padding: 2px 6px; border-radius: 3px; font-size: 0.8em;">${logicalType}</span>`;
}

/**
 * Format encoding types
 * @param {string[]} encodings - Array of encoding names
 * @returns {string} Formatted encoding list
 */
function formatEncodings(encodings) {
    if (!encodings || encodings.length === 0) return 'None';

    return encodings.map(encoding =>
        `<span class="encoding-badge" style="background: #e3f2fd; color: #1976d2; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; margin-right: 4px;">${encoding}</span>`
    ).join('');
}

/**
 * Format file offset as hex and decimal
 * @param {number} offset - Byte offset
 * @returns {string} Formatted offset
 */
function formatOffset(offset) {
    if (offset === null || offset === undefined) {
        return '<span class="offset-value">N/A</span>';
    }

    const hex = offset.toString(16).toUpperCase().padStart(8, '0');
    const dec = formatNumber(offset);
    return `<span class="offset-value">
        <span class="hex-offset">0x${hex}</span>
        <span class="dec-offset">(${dec})</span>
    </span>`;
}

/**
 * Format duration in milliseconds to human readable format
 * @param {number} ms - Duration in milliseconds
 * @returns {string} Formatted duration
 */
function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
    return `${(ms / 3600000).toFixed(1)}h`;
}

/**
 * Format percentage with appropriate precision
 * @param {number} value - Value as decimal (0.25 = 25%)
 * @param {number} decimals - Number of decimal places
 * @returns {string} Formatted percentage
 */
function formatPercentage(value, decimals = 1) {
    return (value * 100).toFixed(decimals) + '%';
}

/**
 * Truncate long strings with ellipsis
 * @param {string} str - String to truncate
 * @param {number} maxLength - Maximum length
 * @returns {string} Truncated string
 */
function truncateString(str, maxLength = 50) {
    if (!str || str.length <= maxLength) return str;
    return str.substring(0, maxLength - 3) + '...';
}

/**
 * Format JSON with syntax highlighting
 * @param {object} obj - Object to format as JSON
 * @returns {string} Formatted JSON with basic syntax highlighting
 */
function formatJSON(obj) {
    const json = JSON.stringify(obj, null, 2);

    return json
        .replace(/(".*?"):/g, '<span style="color: #0066cc; font-weight: 500;">$1</span>:')
        .replace(/: (".*?")/g, ': <span style="color: #00aa00;">$1</span>')
        .replace(/: (true|false)/g, ': <span style="color: #aa6600;">$1</span>')
        .replace(/: (null)/g, ': <span style="color: #999;">$1</span>')
        .replace(/: (\d+)/g, ': <span style="color: #0000aa;">$1</span>');
}

/**
 * Create a progress bar HTML element
 * @param {number} percentage - Percentage (0-100)
 * @param {string} color - Color for the bar
 * @returns {string} HTML string for progress bar
 */
function createProgressBar(percentage, color = '#4CAF50') {
    const clampedPercentage = Math.max(0, Math.min(100, percentage));

    return `
        <div class="progress-bar-container" style="background: #f0f0f0; border-radius: 4px; height: 8px; overflow: hidden;">
            <div class="progress-bar" style="background: ${color}; height: 100%; width: ${clampedPercentage}%; transition: width 0.3s ease;"></div>
        </div>
        <span class="progress-text" style="font-size: 0.9em; color: #666;">${clampedPercentage.toFixed(1)}%</span>
    `;
}

/**
 * Create a badge for repetition levels
 * @param {string} repetition - Repetition level (required, optional, repeated)
 * @returns {string} HTML string for repetition badge
 */
function formatRepetition(repetition) {
    const colors = {
        'required': '#f44336',
        'optional': '#ff9800',
        'repeated': '#2196f3'
    };

    const color = colors[repetition] || '#666';

    return `<span class="repetition-badge" style="background: ${color}20; color: ${color}; padding: 1px 4px; border-radius: 2px; font-size: 0.7em; text-transform: uppercase; font-weight: 500;">${repetition}</span>`;
}

// Export functions for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        formatBytes,
        formatNumber,
        formatCompressionRatio,
        formatParquetType,
        formatLogicalType,
        formatEncodings,
        formatOffset,
        formatDuration,
        formatPercentage,
        truncateString,
        formatJSON,
        createProgressBar,
        formatRepetition
    };
}
