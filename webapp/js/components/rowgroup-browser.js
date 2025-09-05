/**
 * Row Group Browser Component
 * Hierarchical navigation: Row Groups → Column Chunks → Pages → Page Details
 */
class RowGroupBrowser {
    constructor(container) {
        this.container = container;
        this.data = null;
        this.app = null;
        this.currentView = 'rowgroups'; // rowgroups, chunks, pages, page-detail
        this.selectedRowGroup = null;
        this.selectedColumn = null;
        this.selectedPage = null;
        this.breadcrumb = [];
    }

    /**
     * Initialize with data
     */
    init(data, app) {
        this.data = data;
        this.app = app;
        this.showRowGroupsView();
    }

    /**
     * Show row groups overview
     */
    showRowGroupsView() {
        this.currentView = 'rowgroups';
        this.breadcrumb = ['Row Groups'];

        const rowGroups = this.data.metadata.metadata.row_groups || [];

        this.container.innerHTML = `
            <div class="rowgroup-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="rowgroups-overview">
                    <div class="view-header">
                        <h3>Row Groups (${rowGroups.length})</h3>
                        <p>Select a row group to view its column chunks</p>
                    </div>

                    <div class="rowgroups-grid">
                        ${rowGroups.map((rowGroup, index) => {
                            const columnChunks = Object.keys(rowGroup.column_chunks || {});
                            const totalPages = columnChunks.reduce((sum, columnPath) => {
                                const chunk = rowGroup.column_chunks[columnPath];
                                if (!chunk.metadata) return sum;
                                // Find the physical chunk data
                                const physicalChunk = this.data.column_chunks.find(c =>
                                    c.path_in_schema === columnPath
                                );
                                if (physicalChunk) {
                                    let pageCount = 0;
                                    if (physicalChunk.data_pages) pageCount += physicalChunk.data_pages.length;
                                    if (physicalChunk.dictionary_page) pageCount += 1;
                                    if (physicalChunk.index_pages) pageCount += physicalChunk.index_pages.length;
                                    return sum + pageCount;
                                }
                                return sum;
                            }, 0);

                            const compressionRatio = rowGroup.compression_stats
                                ? (rowGroup.compression_stats.ratio * 100).toFixed(1) + '%'
                                : 'N/A';

                            return `
                                <div class="rowgroup-card" data-rowgroup="${index}">
                                    <div class="rowgroup-header">
                                        <h4>Row Group ${index}</h4>
                                        <span class="compression-badge">${compressionRatio}</span>
                                    </div>
                                    <div class="rowgroup-stats">
                                        <div class="stat">
                                            <label>Rows:</label>
                                            <span>${formatNumber(rowGroup.row_count)}</span>
                                        </div>
                                        <div class="stat">
                                            <label>Columns:</label>
                                            <span>${columnChunks.length}</span>
                                        </div>
                                        <div class="stat">
                                            <label>Pages:</label>
                                            <span>${totalPages}</span>
                                        </div>
                                        <div class="stat">
                                            <label>Size:</label>
                                            <span>${formatBytes(rowGroup.total_byte_size)}</span>
                                        </div>
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;

        this.attachRowGroupClickHandlers();
    }

    /**
     * Show column chunks for selected row group
     */
    showChunksView(rowGroupIndex) {
        this.currentView = 'chunks';
        this.selectedRowGroup = rowGroupIndex;
        this.breadcrumb = ['Row Groups', `Row Group ${rowGroupIndex}`];

        const rowGroup = this.data.metadata.metadata.row_groups[rowGroupIndex];
        const columnChunks = Object.entries(rowGroup.column_chunks || {});

        this.container.innerHTML = `
            <div class="rowgroup-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="chunks-view">
                    <div class="view-header">
                        <h3>Column Chunks in Row Group ${rowGroupIndex} (${columnChunks.length})</h3>
                        <p>Select a column to view its pages</p>
                    </div>

                    <div class="chunks-table-container">
                        <table class="chunks-table">
                            <thead>
                                <tr>
                                    <th>Column</th>
                                    <th>Type</th>
                                    <th>Offset</th>
                                    <th>Size</th>
                                    <th>Values</th>
                                    <th>Pages</th>
                                    <th>Compression</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${columnChunks.map(([columnPath, chunkMeta]) => {
                                    // Find the physical chunk data
                                    const physicalChunk = this.data.column_chunks.find(c =>
                                        c.path_in_schema === columnPath
                                    );

                                    let pageCount = 0;
                                    let compressionName = 'UNKNOWN';
                                    let totalSize = chunkMeta.metadata?.total_compressed_size || 0;

                                    if (physicalChunk) {
                                        if (physicalChunk.data_pages) pageCount += physicalChunk.data_pages.length;
                                        if (physicalChunk.dictionary_page) pageCount += 1;
                                        if (physicalChunk.index_pages) pageCount += physicalChunk.index_pages.length;
                                        compressionName = this.app.getCompressionName(physicalChunk.codec);
                                        totalSize = physicalChunk.total_byte_size;
                                    }

                                    // Get column type from schema
                                    let columnType = 'UNKNOWN';
                                    const schemaColumn = this.app.findColumnInSchema(this.data.metadata.metadata.schema, columnPath);
                                    if (schemaColumn) {
                                        columnType = this.app.getTypeDisplay(schemaColumn);
                                    }

                                    return `
                                        <tr class="chunk-row" data-column="${columnPath}">
                                            <td class="column-name">${columnPath}</td>
                                            <td><span class="column-type">${columnType}</span></td>
                                            <td>${formatOffset(chunkMeta.file_offset || 0)}</td>
                                            <td>${formatBytes(totalSize)}</td>
                                            <td>${formatNumber(chunkMeta.metadata?.num_values || 0)}</td>
                                            <td>${pageCount}</td>
                                            <td>${compressionName}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;

        this.attachChunkClickHandlers();
    }

    /**
     * Show pages for selected column in selected row group
     */
    showPagesView(columnPath) {
        this.currentView = 'pages';
        this.selectedColumn = columnPath;
        this.breadcrumb = ['Row Groups', `Row Group ${this.selectedRowGroup}`, columnPath];

        // Find the physical chunk data for this column
        const physicalChunk = this.data.column_chunks.find(c =>
            c.path_in_schema === columnPath
        );

        if (!physicalChunk) {
            this.container.innerHTML = '<p class="no-data">No page data found for this column</p>';
            return;
        }

        const pages = this.app.mapPageData(physicalChunk);

        this.container.innerHTML = `
            <div class="rowgroup-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="pages-view">
                    <div class="view-header">
                        <h3>Pages in "${columnPath}" (${pages.length})</h3>
                        <p>Select a page to view its details</p>
                    </div>

                    <div class="pages-table-container">
                        <table class="pages-table">
                            <thead>
                                <tr>
                                    <th>Page</th>
                                    <th>Type</th>
                                    <th>Offset</th>
                                    <th>Compressed Size</th>
                                    <th>Uncompressed Size</th>
                                    <th>Rows/Values</th>
                                    <th>Encoding</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${pages.map((page, index) => `
                                    <tr class="page-row" data-page-index="${index}">
                                        <td>Page ${index}</td>
                                        <td><span class="page-type">${page.type}</span></td>
                                        <td>${formatOffset(page.offset || 0)}</td>
                                        <td>${formatBytes(page.size || 0)}</td>
                                        <td>${page.uncompressedSize ? formatBytes(page.uncompressedSize) : 'N/A'}</td>
                                        <td>${formatNumber(page.numRows || 0)}</td>
                                        <td>${page.encoding || 'N/A'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;

        this.attachPageClickHandlers();
    }

    /**
     * Show detailed page information
     */
    showPageDetailView(pageIndex) {
        this.currentView = 'page-detail';
        this.selectedPage = pageIndex;
        this.breadcrumb = ['Row Groups', `Row Group ${this.selectedRowGroup}`, this.selectedColumn, `Page ${pageIndex}`];

        // Find the physical chunk data
        const physicalChunk = this.data.column_chunks.find(c =>
            c.path_in_schema === this.selectedColumn
        );

        if (!physicalChunk) {
            this.container.innerHTML = '<p class="no-data">No page data found</p>';
            return;
        }

        const pages = this.app.mapPageData(physicalChunk);
        const page = pages[pageIndex];

        // Get raw page data from chunk
        let rawPageData = null;
        if (page.type === 'DICTIONARY' && physicalChunk.dictionary_page) {
            rawPageData = physicalChunk.dictionary_page;
        } else if (physicalChunk.data_pages && physicalChunk.data_pages[pageIndex]) {
            rawPageData = physicalChunk.data_pages[pageIndex];
        } else if (physicalChunk.index_pages && physicalChunk.index_pages[pageIndex]) {
            rawPageData = physicalChunk.index_pages[pageIndex];
        }

        this.container.innerHTML = `
            <div class="rowgroup-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="page-detail-view">
                    <div class="view-header">
                        <h3>Page ${pageIndex} Details</h3>
                        <span class="page-type-badge">${page.type}</span>
                    </div>

                    <div class="page-detail-content">
                        <div class="detail-sections">
                            <div class="detail-section">
                                <h4>Context Information</h4>
                                <div class="detail-grid">
                                    <div class="detail-item">
                                        <label>Row Group:</label>
                                        <span>${this.selectedRowGroup}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Column:</label>
                                        <span>${this.selectedColumn}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Page Type:</label>
                                        <span>${page.type}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Page Index:</label>
                                        <span>${pageIndex}</span>
                                    </div>
                                </div>
                            </div>

                            <div class="detail-section">
                                <h4>Page Information</h4>
                                <div class="detail-grid">
                                    <div class="detail-item">
                                        <label>Offset:</label>
                                        <span>${formatOffset(page.offset || 0)}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Compressed Size:</label>
                                        <span>${formatBytes(page.size || 0)}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Uncompressed Size:</label>
                                        <span>${page.uncompressedSize ? formatBytes(page.uncompressedSize) : 'N/A'}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Values/Rows:</label>
                                        <span>${formatNumber(page.numRows || 0)}</span>
                                    </div>
                                    <div class="detail-item">
                                        <label>Encoding:</label>
                                        <span>${page.encoding || 'N/A'}</span>
                                    </div>
                                    ${page.size && page.uncompressedSize && page.uncompressedSize > 0 ? `
                                    <div class="detail-item">
                                        <label>Compression Ratio:</label>
                                        <span>${((page.size / page.uncompressedSize) * 100).toFixed(1)}%</span>
                                    </div>
                                    ` : ''}
                                </div>
                            </div>

                            ${rawPageData && rawPageData.statistics ? `
                            <div class="detail-section">
                                <h4>Statistics</h4>
                                <div class="detail-grid">
                                    ${rawPageData.statistics.min_value !== undefined ? `
                                    <div class="detail-item">
                                        <label>Min Value:</label>
                                        <span class="value-display">${this.formatValue(rawPageData.statistics.min_value)}</span>
                                    </div>
                                    ` : ''}
                                    ${rawPageData.statistics.max_value !== undefined ? `
                                    <div class="detail-item">
                                        <label>Max Value:</label>
                                        <span class="value-display">${this.formatValue(rawPageData.statistics.max_value)}</span>
                                    </div>
                                    ` : ''}
                                    ${rawPageData.statistics.null_count !== undefined ? `
                                    <div class="detail-item">
                                        <label>Null Count:</label>
                                        <span>${formatNumber(rawPageData.statistics.null_count)}</span>
                                    </div>
                                    ` : ''}
                                    ${rawPageData.statistics.distinct_count !== undefined ? `
                                    <div class="detail-item">
                                        <label>Distinct Count:</label>
                                        <span>${formatNumber(rawPageData.statistics.distinct_count)}</span>
                                    </div>
                                    ` : ''}
                                </div>
                            </div>
                            ` : ''}

                            <div class="detail-section">
                                <h4>Raw Page Data</h4>
                                <div class="raw-data">
                                    <pre><code>${JSON.stringify(rawPageData, null, 2)}</code></pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        this.attachBreadcrumbHandlers();
    }

    /**
     * Render breadcrumb navigation
     */
    renderBreadcrumb() {
        return this.breadcrumb.map((item, index) => {
            const isLast = index === this.breadcrumb.length - 1;
            const isClickable = !isLast;

            return `
                <span class="breadcrumb-item ${isClickable ? 'clickable' : ''}" data-level="${index}">
                    ${item}
                </span>
                ${!isLast ? '<span class="breadcrumb-separator">→</span>' : ''}
            `;
        }).join('');
    }

    /**
     * Format value for display
     */
    formatValue(value) {
        if (value === null || value === undefined) return 'null';
        if (typeof value === 'string' && value.length > 50) {
            return value.substring(0, 50) + '...';
        }
        return String(value);
    }

    /**
     * Event handlers
     */
    attachRowGroupClickHandlers() {
        this.container.querySelectorAll('.rowgroup-card').forEach(card => {
            card.addEventListener('click', (e) => {
                const rowGroupIndex = parseInt(e.currentTarget.dataset.rowgroup);
                this.showChunksView(rowGroupIndex);
            });
        });

        this.attachBreadcrumbHandlers();
    }

    attachChunkClickHandlers() {
        this.container.querySelectorAll('.chunk-row').forEach(row => {
            row.addEventListener('click', (e) => {
                const columnPath = e.currentTarget.dataset.column;
                this.showPagesView(columnPath);
            });
        });

        this.attachBreadcrumbHandlers();
    }

    attachPageClickHandlers() {
        this.container.querySelectorAll('.page-row').forEach(row => {
            row.addEventListener('click', (e) => {
                const pageIndex = parseInt(e.currentTarget.dataset.pageIndex);
                this.showPageDetailView(pageIndex);
            });
        });

        this.attachBreadcrumbHandlers();
    }

    attachBreadcrumbHandlers() {
        this.container.querySelectorAll('.breadcrumb-item.clickable').forEach(item => {
            item.addEventListener('click', (e) => {
                const level = parseInt(e.currentTarget.dataset.level);
                this.navigateToLevel(level);
            });
        });
    }

    /**
     * Navigate to specific breadcrumb level
     */
    navigateToLevel(level) {
        switch (level) {
            case 0: // Row Groups
                this.showRowGroupsView();
                break;
            case 1: // Specific row group chunks
                if (this.selectedRowGroup !== null) {
                    this.showChunksView(this.selectedRowGroup);
                }
                break;
            case 2: // Specific column pages
                if (this.selectedRowGroup !== null && this.selectedColumn) {
                    this.showPagesView(this.selectedColumn);
                }
                break;
        }
    }
}

// Add CSS styles for the row group browser
const rowGroupBrowserStyles = `
:root {
    --primary-color: #667eea;
    --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    --secondary-color: #f093fb;
    --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    --success-color: #4ecdc4;
    --success-gradient: linear-gradient(135deg, #4ecdc4 0%, #44a08d 100%);
    --warning-color: #ffd93d;
    --warning-gradient: linear-gradient(135deg, #ffd93d 0%, #ff6b6b 100%);
    --surface: rgba(255, 255, 255, 0.9);
    --surface-hover: rgba(255, 255, 255, 0.95);
    --text-primary: #2d3748;
    --text-secondary: #4a5568;
    --border: rgba(255, 255, 255, 0.2);
    --shadow: 0 8px 32px rgba(31, 38, 135, 0.37);
    --shadow-hover: 0 16px 40px rgba(31, 38, 135, 0.5);
    --glass-bg: rgba(255, 255, 255, 0.1);
    --glass-border: 1px solid rgba(255, 255, 255, 0.18);
}

.rowgroup-browser {
    padding: 2rem;
    background: var(--primary-gradient);
    min-height: 100vh;
    position: relative;
}

.rowgroup-browser::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grain" width="100" height="100" patternUnits="userSpaceOnUse"><circle cx="25" cy="25" r="1" fill="%23ffffff" opacity="0.1"/><circle cx="75" cy="25" r="1.5" fill="%23ffffff" opacity="0.08"/><circle cx="50" cy="75" r="0.8" fill="%23ffffff" opacity="0.12"/></pattern></defs><rect width="100" height="100" fill="url(%23grain)"/></svg>');
    pointer-events: none;
    z-index: 0;
}

.rowgroup-browser > * {
    position: relative;
    z-index: 1;
}

.browser-header {
    margin-bottom: 2rem;
}

.breadcrumb {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.8rem 1.5rem;
    background: var(--glass-bg);
    backdrop-filter: blur(10px);
    border: var(--glass-border);
    border-radius: 25px;
    box-shadow: var(--shadow);
}

.breadcrumb-item {
    color: white;
    font-weight: 500;
    padding: 0.3rem 0.8rem;
    border-radius: 15px;
    transition: all 0.3s ease;
}

.breadcrumb-item.clickable {
    cursor: pointer;
    background: rgba(255, 255, 255, 0.1);
}

.breadcrumb-item.clickable:hover {
    background: rgba(255, 255, 255, 0.2);
    transform: translateY(-1px);
}

.breadcrumb-separator {
    color: rgba(255, 255, 255, 0.6);
    font-weight: bold;
}

.view-header {
    text-align: center;
    margin-bottom: 2rem;
    color: white;
}

.view-header h3 {
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(135deg, #ffffff 0%, #f0f8ff 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0 0 0.5rem 0;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.view-header p {
    font-size: 1.1rem;
    opacity: 0.9;
    margin: 0;
}

.rowgroups-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 2rem;
    margin-top: 2rem;
}

.rowgroup-card {
    background: var(--surface);
    backdrop-filter: blur(15px);
    border: var(--glass-border);
    border-radius: 20px;
    padding: 2rem;
    cursor: pointer;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
    box-shadow: var(--shadow);
}

.rowgroup-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: var(--success-gradient);
    opacity: 0;
    transition: all 0.4s ease;
}

.rowgroup-card::after {
    content: '';
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle, rgba(255, 255, 255, 0.1) 0%, transparent 70%);
    opacity: 0;
    transition: all 0.6s ease;
    pointer-events: none;
}

.rowgroup-card:hover {
    background: var(--surface-hover);
    transform: translateY(-8px) scale(1.02);
    box-shadow: var(--shadow-hover);
    border-color: rgba(255, 255, 255, 0.3);
}

.rowgroup-card:hover::before {
    opacity: 1;
    height: 6px;
}

.rowgroup-card:hover::after {
    opacity: 1;
    animation: shimmer 2s ease-in-out infinite;
}

@keyframes shimmer {
    0%, 100% { transform: translate(-50%, -50%) rotate(0deg); }
    50% { transform: translate(-50%, -50%) rotate(180deg); }
}

.rowgroup-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1.5rem;
}

.rowgroup-header h4 {
    margin: 0;
    font-size: 1.4rem;
    background: var(--primary-gradient);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: 700;
    letter-spacing: -0.02em;
}

.compression-badge {
    background: var(--success-gradient);
    color: white;
    padding: 0.4rem 0.8rem;
    border-radius: 15px;
    font-size: 0.8rem;
    font-weight: 600;
    white-space: nowrap;
    box-shadow: 0 4px 12px rgba(78, 205, 196, 0.3);
    animation: pulse-subtle 3s ease-in-out infinite;
}

@keyframes pulse-subtle {
    0%, 100% { transform: scale(1); box-shadow: 0 4px 12px rgba(78, 205, 196, 0.3); }
    50% { transform: scale(1.05); box-shadow: 0 6px 16px rgba(78, 205, 196, 0.4); }
}

.rowgroup-stats {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
}

.rowgroup-stats .stat {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 1rem;
    padding: 0.8rem 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    transition: all 0.3s ease;
}

.rowgroup-stats .stat:last-child {
    border-bottom: none;
}

.rowgroup-stats .stat:hover {
    background: rgba(255, 255, 255, 0.05);
    border-radius: 8px;
    padding: 0.8rem;
    margin: 0 -0.5rem;
}

.rowgroup-stats .stat label {
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.9rem;
    opacity: 0.8;
}

.rowgroup-stats .stat span {
    color: var(--text-primary);
    font-weight: 700;
    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.chunks-table-container, .pages-table-container {
    background: var(--surface);
    backdrop-filter: blur(15px);
    border: var(--glass-border);
    border-radius: 20px;
    overflow: hidden;
    box-shadow: var(--shadow);
    margin-top: 2rem;
}

.chunks-table, .pages-table {
    width: 100%;
    border-collapse: collapse;
}

.chunks-table th, .pages-table th {
    background: var(--primary-gradient);
    color: white;
    padding: 1.2rem;
    font-weight: 600;
    text-align: left;
    font-size: 0.95rem;
    letter-spacing: 0.5px;
}

.chunks-table td, .pages-table td {
    padding: 1rem 1.2rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    transition: all 0.3s ease;
}

.chunk-row, .page-row {
    cursor: pointer;
    transition: all 0.3s ease;
}

.chunk-row:hover, .page-row:hover {
    background: rgba(255, 255, 255, 0.1);
    transform: scale(1.005);
}

.column-name {
    font-family: 'Monaco', 'Consolas', 'SF Mono', monospace;
    font-size: 0.9em;
    background: rgba(102, 126, 234, 0.1);
    color: var(--primary-color);
    padding: 0.3rem 0.8rem;
    border-radius: 8px;
    font-weight: 600;
}

.column-type, .page-type {
    background: var(--secondary-gradient);
    color: white;
    padding: 0.3rem 0.8rem;
    border-radius: 12px;
    font-size: 0.8em;
    font-weight: 600;
    box-shadow: 0 2px 8px rgba(240, 147, 251, 0.3);
}

.page-detail-view {
    background: var(--surface);
    backdrop-filter: blur(15px);
    border: var(--glass-border);
    border-radius: 20px;
    padding: 2rem;
    box-shadow: var(--shadow);
    margin-top: 2rem;
}

.page-type-badge {
    background: var(--warning-gradient);
    color: white;
    padding: 0.5rem 1rem;
    border-radius: 15px;
    font-weight: 600;
    box-shadow: 0 4px 12px rgba(255, 217, 61, 0.3);
}

.detail-sections {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 2rem;
    margin-top: 2rem;
}

.detail-section {
    background: rgba(255, 255, 255, 0.05);
    border: var(--glass-border);
    border-radius: 16px;
    padding: 1.5rem;
    backdrop-filter: blur(10px);
}

.detail-section h4 {
    margin: 0 0 1rem 0;
    color: var(--text-primary);
    font-weight: 600;
    font-size: 1.2rem;
}

.detail-grid {
    display: flex;
    flex-direction: column;
    gap: 0.8rem;
}

.detail-item {
    display: flex;
    justify-content: space-between;
    padding: 0.5rem 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.detail-item:last-child {
    border-bottom: none;
}

.detail-item label {
    font-weight: 600;
    color: var(--text-secondary);
    opacity: 0.9;
}

.detail-item span {
    font-weight: 600;
    color: var(--text-primary);
}

.value-display {
    font-family: 'Monaco', 'Consolas', 'SF Mono', monospace;
    background: rgba(255, 255, 255, 0.1);
    padding: 0.2rem 0.5rem;
    border-radius: 6px;
    font-size: 0.9em;
}

.raw-data {
    background: rgba(0, 0, 0, 0.2);
    border-radius: 12px;
    padding: 1rem;
    margin-top: 1rem;
    font-family: 'Monaco', 'Consolas', 'SF Mono', monospace;
    font-size: 0.85rem;
    color: #e2e8f0;
    overflow: auto;
    max-height: 300px;
}

@media (max-width: 768px) {
    .rowgroup-browser {
        padding: 1rem;
    }

    .rowgroups-grid {
        grid-template-columns: 1fr;
        gap: 1rem;
    }

    .rowgroup-stats {
        grid-template-columns: 1fr;
    }

    .view-header h3 {
        font-size: 1.8rem;
    }

    .detail-sections {
        grid-template-columns: 1fr;
        gap: 1rem;
    }

    .breadcrumb {
        flex-wrap: wrap;
        padding: 0.6rem 1rem;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#rowgroup-browser-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'rowgroup-browser-styles';
    styleElement.textContent = rowGroupBrowserStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = RowGroupBrowser;
}
