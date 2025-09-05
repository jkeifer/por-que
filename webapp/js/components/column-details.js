/**
 * Column Details Component
 * Displays detailed information about individual columns
 */
class ColumnDetails {
    constructor(container) {
        this.container = container;
        this.columns = [];
        this.selectedColumn = null;
    }

    /**
     * Initialize with column data
     * @param {Array} columns - Array of column information
     */
    init(columns) {
        this.columns = columns;
        this.renderColumnSelector();
    }

    /**
     * Render column selector dropdown
     */
    renderColumnSelector() {
        const selector = this.container.querySelector('#column-select');
        if (!selector) return;

        selector.innerHTML = '<option value="">Select a column...</option>';

        this.columns.forEach((column, index) => {
            const option = document.createElement('option');
            option.value = index;
            option.textContent = column.path || column.name || `Column ${index}`;
            selector.appendChild(option);
        });

        selector.addEventListener('change', (e) => {
            if (e.target.value) {
                this.selectColumn(parseInt(e.target.value));
            } else {
                this.clearSelection();
            }
        });
    }

    /**
     * Select and display a column
     * @param {number} index - Column index
     */
    selectColumn(index) {
        if (index < 0 || index >= this.columns.length) return;

        this.selectedColumn = this.columns[index];
        this.renderColumnDetails(this.selectedColumn);
    }

    /**
     * Clear column selection
     */
    clearSelection() {
        this.selectedColumn = null;
        const detailsContainer = this.container.querySelector('#column-details');
        if (detailsContainer) {
            detailsContainer.innerHTML = '<p class="no-selection">Select a column to view details</p>';
        }
    }

    /**
     * Render detailed column information
     * @param {object} column - Column data
     */
    renderColumnDetails(column) {
        const detailsContainer = this.container.querySelector('#column-details');
        if (!detailsContainer) return;

        const details = this.analyzeColumn(column);

        detailsContainer.innerHTML = `
            <div class="column-details-content">
                <div class="column-header">
                    <h4>${column.path || column.name || 'Unnamed Column'}</h4>
                    <div class="column-badges">
                        ${formatParquetType(column.type || 'UNKNOWN')}
                        ${column.logicalType ? formatLogicalType(column.logicalType) : ''}
                        ${column.repetition ? formatRepetition(column.repetition) : ''}
                    </div>
                </div>

                <div class="column-details-grid">
                    <div class="detail-section">
                        <h5>Basic Information</h5>
                        <div class="detail-grid">
                            ${this.renderBasicInfo(details.basic)}
                        </div>
                    </div>

                    <div class="detail-section">
                        <h5>Storage Details</h5>
                        <div class="detail-grid">
                            ${this.renderStorageInfo(details.storage)}
                        </div>
                    </div>

                    <div class="detail-section">
                        <h5>Statistics</h5>
                        <div class="detail-grid">
                            ${this.renderStatistics(details.statistics)}
                        </div>
                    </div>

                    <div class="detail-section">
                        <h5>Encoding & Compression</h5>
                        <div class="detail-grid">
                            ${this.renderEncodingInfo(details.encoding)}
                        </div>
                    </div>
                </div>

                ${details.pages.length > 0 ? this.renderPageInfo(details.pages) : ''}
            </div>
        `;
    }

    /**
     * Analyze column data and extract key information
     * @param {object} column - Column data
     * @returns {object} Analyzed column information
     */
    analyzeColumn(column) {
        return {
            basic: {
                name: column.name || column.path || 'Unnamed',
                path: column.path || 'N/A',
                type: column.type || 'UNKNOWN',
                logicalType: column.logicalType || null,
                repetition: column.repetition || 'required'
            },
            storage: {
                fileOffset: column.fileOffset || 0,
                totalSize: column.totalSize || 0,
                compressedSize: column.compressedSize || 0,
                uncompressedSize: column.uncompressedSize || 0,
                compressionRatio: this.calculateCompressionRatio(
                    column.uncompressedSize,
                    column.compressedSize
                )
            },
            statistics: {
                numValues: column.numValues || 0,
                nullCount: column.nullCount || 0,
                distinctCount: column.distinctCount || null,
                minValue: column.minValue || null,
                maxValue: column.maxValue || null
            },
            encoding: {
                encodings: column.encodings || [],
                compression: column.compression || 'UNCOMPRESSED',
                dictionaryPageOffset: column.dictionaryPageOffset || null,
                dataPageOffset: column.dataPageOffset || null
            },
            pages: column.pages || []
        };
    }

    /**
     * Calculate compression ratio
     * @param {number} uncompressed - Uncompressed size
     * @param {number} compressed - Compressed size
     * @returns {number} Compression ratio (0-1)
     */
    calculateCompressionRatio(uncompressed, compressed) {
        if (!uncompressed || uncompressed === 0) return 0;
        return Math.max(0, (uncompressed - compressed) / uncompressed);
    }

    /**
     * Render basic information section
     * @param {object} basic - Basic column info
     * @returns {string} HTML string
     */
    renderBasicInfo(basic) {
        return `
            <div class="detail-item">
                <label>Name:</label>
                <span>${basic.name}</span>
            </div>
            <div class="detail-item">
                <label>Path:</label>
                <span class="path-value">${basic.path}</span>
            </div>
            <div class="detail-item">
                <label>Physical Type:</label>
                <span>${formatParquetType(basic.type)}</span>
            </div>
            ${basic.logicalType ? `
            <div class="detail-item">
                <label>Logical Type:</label>
                <span>${formatLogicalType(basic.logicalType)}</span>
            </div>
            ` : ''}
            <div class="detail-item">
                <label>Repetition:</label>
                <span>${formatRepetition(basic.repetition)}</span>
            </div>
        `;
    }

    /**
     * Render storage information section
     * @param {object} storage - Storage info
     * @returns {string} HTML string
     */
    renderStorageInfo(storage) {
        return `
            <div class="detail-item">
                <label>File Offset:</label>
                <span>${formatOffset(storage.fileOffset)}</span>
            </div>
            <div class="detail-item">
                <label>Total Size:</label>
                <span>${formatBytes(storage.totalSize)}</span>
            </div>
            <div class="detail-item">
                <label>Compressed Size:</label>
                <span>${formatBytes(storage.compressedSize)}</span>
            </div>
            <div class="detail-item">
                <label>Uncompressed Size:</label>
                <span>${formatBytes(storage.uncompressedSize)}</span>
            </div>
            <div class="detail-item">
                <label>Compression Ratio:</label>
                <span>${formatPercentage(storage.compressionRatio)} saved</span>
            </div>
        `;
    }

    /**
     * Render statistics section
     * @param {object} stats - Statistics info
     * @returns {string} HTML string
     */
    renderStatistics(stats) {
        const nullPercentage = stats.numValues > 0
            ? (stats.nullCount / stats.numValues) * 100
            : 0;

        return `
            <div class="detail-item">
                <label>Total Values:</label>
                <span>${formatNumber(stats.numValues)}</span>
            </div>
            <div class="detail-item">
                <label>Null Count:</label>
                <span>${formatNumber(stats.nullCount)} (${nullPercentage.toFixed(1)}%)</span>
            </div>
            ${stats.distinctCount !== null ? `
            <div class="detail-item">
                <label>Distinct Count:</label>
                <span>${formatNumber(stats.distinctCount)}</span>
            </div>
            ` : ''}
            ${stats.minValue !== null ? `
            <div class="detail-item">
                <label>Min Value:</label>
                <span class="value-preview">${truncateString(String(stats.minValue), 30)}</span>
            </div>
            ` : ''}
            ${stats.maxValue !== null ? `
            <div class="detail-item">
                <label>Max Value:</label>
                <span class="value-preview">${truncateString(String(stats.maxValue), 30)}</span>
            </div>
            ` : ''}
        `;
    }

    /**
     * Render encoding information section
     * @param {object} encoding - Encoding info
     * @returns {string} HTML string
     */
    renderEncodingInfo(encoding) {
        return `
            <div class="detail-item">
                <label>Encodings:</label>
                <span>${formatEncodings(encoding.encodings)}</span>
            </div>
            <div class="detail-item">
                <label>Compression:</label>
                <span class="compression-value">${encoding.compression}</span>
            </div>
            ${encoding.dictionaryPageOffset ? `
            <div class="detail-item">
                <label>Dictionary Page:</label>
                <span>${formatOffset(encoding.dictionaryPageOffset)}</span>
            </div>
            ` : ''}
            <div class="detail-item">
                <label>Data Page Offset:</label>
                <span>${formatOffset(encoding.dataPageOffset)}</span>
            </div>
        `;
    }

    /**
     * Render page information section
     * @param {Array} pages - Page data
     * @returns {string} HTML string
     */
    renderPageInfo(pages) {
        if (!pages || pages.length === 0) {
            return '';
        }

        let html = `
            <div class="detail-section pages-section">
                <h5>Pages (${pages.length})</h5>
                <div class="pages-table-container">
                    <table class="pages-table">
                        <thead>
                            <tr>
                                <th>Type</th>
                                <th>Offset</th>
                                <th>Size</th>
                                <th>Rows</th>
                                <th>Encoding</th>
                            </tr>
                        </thead>
                        <tbody>
        `;

        pages.forEach((page, index) => {
            html += `
                <tr class="page-row">
                    <td><span class="page-type">${page.type || 'DATA'}</span></td>
                    <td>${formatOffset(page.offset || 0)}</td>
                    <td>${formatBytes(page.size || 0)}</td>
                    <td>${formatNumber(page.numRows || 0)}</td>
                    <td>${page.encoding || 'N/A'}</td>
                </tr>
            `;
        });

        html += `
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        return html;
    }

    /**
     * Update with new column data
     * @param {Array} columns - New column data
     */
    update(columns) {
        this.columns = columns;
        this.renderColumnSelector();
        this.clearSelection();
    }
}

// Add styles for column details
const columnDetailsStyles = `
.no-selection {
    text-align: center;
    color: #999;
    padding: 2rem;
    font-style: italic;
}

.column-details-content {
    max-height: 600px;
    overflow-y: auto;
}

.column-header {
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 2px solid #e0e0e0;
}

.column-header h4 {
    margin: 0 0 0.5rem 0;
    color: #333;
    font-size: 1.3rem;
}

.column-badges {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
}

.column-details-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1.5rem;
}

.detail-section {
    background: #f8f9fa;
    border-radius: 8px;
    padding: 1rem;
    border: 1px solid #e9ecef;
}

.detail-section h5 {
    margin: 0 0 1rem 0;
    color: #495057;
    font-size: 1rem;
    font-weight: 600;
}

.detail-grid {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
}

.detail-item {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 0.5rem;
    padding: 0.25rem 0;
    border-bottom: 1px solid #e9ecef;
}

.detail-item:last-child {
    border-bottom: none;
}

.detail-item label {
    font-weight: 500;
    color: #6c757d;
    flex-shrink: 0;
    min-width: 100px;
}

.detail-item span {
    text-align: right;
    word-break: break-word;
}

.path-value {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.9em;
    background: #f0f0f0;
    padding: 2px 4px;
    border-radius: 3px;
}

.value-preview {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.9em;
    background: #f0f0f0;
    padding: 2px 4px;
    border-radius: 3px;
    max-width: 150px;
}

.compression-value {
    background: #fff3cd;
    color: #856404;
    padding: 2px 6px;
    border-radius: 3px;
    font-weight: 500;
    font-size: 0.9em;
}

.pages-section {
    grid-column: 1 / -1;
    margin-top: 1rem;
}

.pages-table-container {
    overflow-x: auto;
}

.pages-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

.pages-table th,
.pages-table td {
    padding: 0.5rem;
    text-align: left;
    border-bottom: 1px solid #e0e0e0;
}

.pages-table th {
    background: #f5f5f5;
    font-weight: 500;
    color: #333;
    position: sticky;
    top: 0;
    z-index: 1;
}

.page-type {
    background: #e3f2fd;
    color: #1976d2;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 0.8em;
    font-weight: 500;
}

.page-row:hover {
    background-color: #f8f9fa;
}

@media (max-width: 768px) {
    .column-details-grid {
        grid-template-columns: 1fr;
    }

    .detail-item {
        flex-direction: column;
        align-items: flex-start;
    }

    .detail-item span {
        text-align: left;
        margin-top: 0.25rem;
    }
}

@media (prefers-color-scheme: dark) {
    .column-header {
        border-bottom-color: #555;
    }

    .column-header h4 {
        color: #e0e0e0;
    }

    .detail-section {
        background: #2d2d2d;
        border-color: #555;
    }

    .detail-section h5 {
        color: #e0e0e0;
    }

    .detail-item {
        border-bottom-color: #555;
    }

    .detail-item label {
        color: #ccc;
    }

    .path-value,
    .value-preview {
        background: #3a3a3a;
        color: #e0e0e0;
    }

    .compression-value {
        background: #3d3d00;
        color: #ffeb3b;
    }

    .pages-table th {
        background: #3a3a3a;
        color: #e0e0e0;
    }

    .pages-table th,
    .pages-table td {
        border-bottom-color: #555;
    }

    .page-row:hover {
        background-color: #2a2a2a;
    }

    .no-selection {
        color: #666;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#column-details-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'column-details-styles';
    styleElement.textContent = columnDetailsStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ColumnDetails;
}
