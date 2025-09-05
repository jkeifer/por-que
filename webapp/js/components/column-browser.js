/**
 * Column Browser Component
 * Hierarchical navigation: Columns → Column Chunks → Pages → Page Details
 */
class ColumnBrowser {
    constructor(container) {
        this.container = container;
        this.data = null;
        this.app = null;
        this.currentView = 'columns'; // columns, chunks, pages, page-detail
        this.selectedColumn = null;
        this.selectedChunk = null;
        this.selectedPage = null;
        this.breadcrumb = [];
    }

    /**
     * Initialize with data
     */
    init(data, app) {
        this.data = data;
        this.app = app;
        this.showColumnsView();
    }

    /**
     * Show columns overview
     */
    showColumnsView() {
        this.currentView = 'columns';
        this.breadcrumb = ['Columns'];

        // Group column chunks by column path
        const columnGroups = {};
        this.data.column_chunks.forEach(chunk => {
            const columnPath = chunk.path_in_schema;
            if (!columnGroups[columnPath]) {
                columnGroups[columnPath] = [];
            }
            columnGroups[columnPath].push(chunk);
        });

        const columns = Object.keys(columnGroups);

        this.container.innerHTML = `
            <div class="column-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="columns-overview">
                    <div class="view-header">
                        <h3>Columns (${columns.length})</h3>
                        <p>Select a column to view its chunks</p>
                    </div>

                    <div class="columns-grid">
                        ${columns.map(columnPath => {
                            const chunks = columnGroups[columnPath];
                            const totalSize = chunks.reduce((sum, chunk) => sum + chunk.total_byte_size, 0);
                            const totalPages = chunks.reduce((sum, chunk) => {
                                let pageCount = 0;
                                if (chunk.data_pages) pageCount += chunk.data_pages.length;
                                if (chunk.dictionary_page) pageCount += 1;
                                if (chunk.index_pages) pageCount += chunk.index_pages.length;
                                return sum + pageCount;
                            }, 0);

                            // Get column type from schema
                            let columnType = 'UNKNOWN';
                            const schemaColumn = this.app.findColumnInSchema(this.data.metadata.metadata.schema, columnPath);
                            if (schemaColumn) {
                                columnType = this.app.getTypeDisplay(schemaColumn);
                            }

                            return `
                                <div class="column-card" data-column="${columnPath}">
                                    <div class="column-header">
                                        <h4>${columnPath}</h4>
                                        <span class="column-type">${columnType}</span>
                                    </div>
                                    <div class="column-stats">
                                        <div class="stat">
                                            <label>Chunks:</label>
                                            <span>${chunks.length}</span>
                                        </div>
                                        <div class="stat">
                                            <label>Pages:</label>
                                            <span>${totalPages}</span>
                                        </div>
                                        <div class="stat">
                                            <label>Size:</label>
                                            <span>${formatBytes(totalSize)}</span>
                                        </div>
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;

        this.attachColumnClickHandlers();
    }

    /**
     * Show column chunks for selected column
     */
    showChunksView(columnPath) {
        this.currentView = 'chunks';
        this.selectedColumn = columnPath;
        this.breadcrumb = ['Columns', columnPath];

        const chunks = this.data.column_chunks.filter(chunk => chunk.path_in_schema === columnPath);

        this.container.innerHTML = `
            <div class="column-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="chunks-view">
                    <div class="view-header">
                        <h3>Column Chunks for "${columnPath}" (${chunks.length})</h3>
                        <p>Select a chunk to view its pages</p>
                    </div>

                    <div class="chunks-table-container">
                        <table class="chunks-table">
                            <thead>
                                <tr>
                                    <th>Chunk</th>
                                    <th>Offset</th>
                                    <th>Size</th>
                                    <th>Values</th>
                                    <th>Pages</th>
                                    <th>Compression</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${chunks.map((chunk, index) => {
                                    let pageCount = 0;
                                    if (chunk.data_pages) pageCount += chunk.data_pages.length;
                                    if (chunk.dictionary_page) pageCount += 1;
                                    if (chunk.index_pages) pageCount += chunk.index_pages.length;

                                    return `
                                        <tr class="chunk-row" data-chunk-index="${index}">
                                            <td>Chunk ${index}</td>
                                            <td>${formatOffset(chunk.start_offset)}</td>
                                            <td>${formatBytes(chunk.total_byte_size)}</td>
                                            <td>${formatNumber(chunk.num_values)}</td>
                                            <td>${pageCount}</td>
                                            <td>${this.app.getCompressionName(chunk.codec)}</td>
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
     * Show pages for selected chunk
     */
    showPagesView(chunkIndex) {
        this.currentView = 'pages';
        this.selectedChunk = chunkIndex;
        this.breadcrumb = ['Columns', this.selectedColumn, `Chunk ${chunkIndex}`];

        const chunks = this.data.column_chunks.filter(chunk => chunk.path_in_schema === this.selectedColumn);
        const chunk = chunks[chunkIndex];
        const pages = this.app.mapPageData(chunk);

        this.container.innerHTML = `
            <div class="column-browser">
                <div class="browser-header">
                    <div class="breadcrumb">
                        ${this.renderBreadcrumb()}
                    </div>
                </div>

                <div class="pages-view">
                    <div class="view-header">
                        <h3>Pages in Chunk ${chunkIndex} (${pages.length})</h3>
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
        this.breadcrumb = ['Columns', this.selectedColumn, `Chunk ${this.selectedChunk}`, `Page ${pageIndex}`];

        const chunks = this.data.column_chunks.filter(chunk => chunk.path_in_schema === this.selectedColumn);
        const chunk = chunks[this.selectedChunk];
        const pages = this.app.mapPageData(chunk);
        const page = pages[pageIndex];

        // Get raw page data from chunk
        let rawPageData = null;
        if (page.type === 'DICTIONARY' && chunk.dictionary_page) {
            rawPageData = chunk.dictionary_page;
        } else if (chunk.data_pages && chunk.data_pages[pageIndex]) {
            rawPageData = chunk.data_pages[pageIndex];
        } else if (chunk.index_pages && chunk.index_pages[pageIndex]) {
            rawPageData = chunk.index_pages[pageIndex];
        }

        this.container.innerHTML = `
            <div class="column-browser">
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
                                <h4>Basic Information</h4>
                                <div class="detail-grid">
                                    <div class="detail-item">
                                        <label>Page Type:</label>
                                        <span>${page.type}</span>
                                    </div>
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
    attachColumnClickHandlers() {
        this.container.querySelectorAll('.column-card').forEach(card => {
            card.addEventListener('click', (e) => {
                const columnPath = e.currentTarget.dataset.column;
                this.showChunksView(columnPath);
            });
        });

        this.attachBreadcrumbHandlers();
    }

    attachChunkClickHandlers() {
        this.container.querySelectorAll('.chunk-row').forEach(row => {
            row.addEventListener('click', (e) => {
                const chunkIndex = parseInt(e.currentTarget.dataset.chunkIndex);
                this.showPagesView(chunkIndex);
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
            case 0: // Columns
                this.showColumnsView();
                break;
            case 1: // Specific column chunks
                if (this.selectedColumn) {
                    this.showChunksView(this.selectedColumn);
                }
                break;
            case 2: // Specific chunk pages
                if (this.selectedColumn && this.selectedChunk !== null) {
                    this.showPagesView(this.selectedChunk);
                }
                break;
        }
    }
}

// Add CSS styles for the column browser and page analytics
const columnBrowserStyles = `
/* Global Design System */
:root {
    --primary-color: #667eea;
    --primary-light: #764ba2;
    --secondary-color: #f093fb;
    --accent-color: #4facfe;
    --success-color: #00d4aa;
    --warning-color: #ffd93d;
    --danger-color: #ff6b6b;
    --dark-color: #2c3e50;
    --light-gray: #f8f9fa;
    --medium-gray: #6c757d;
    --border-color: #e9ecef;
    --shadow-light: 0 2px 4px rgba(0,0,0,0.1);
    --shadow-medium: 0 4px 6px rgba(0,0,0,0.07);
    --shadow-heavy: 0 10px 15px rgba(0,0,0,0.1);
    --border-radius: 12px;
    --border-radius-sm: 8px;
    --transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Page Analytics Styles */
.pages-analytics {
    padding: 2rem;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    min-height: 100vh;
    color: white;
}

.analytics-header {
    text-align: center;
    margin-bottom: 3rem;
    animation: fadeInUp 0.8s ease;
}

.analytics-header h2 {
    margin: 0 0 1rem 0;
    font-size: 2.5rem;
    font-weight: 700;
    background: linear-gradient(45deg, #fff, #f093fb);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.analytics-header p {
    font-size: 1.1rem;
    opacity: 0.9;
    margin: 0;
    max-width: 600px;
    margin: 0 auto;
}

.analytics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 1.5rem;
    margin-bottom: 3rem;
    animation: fadeInUp 0.8s ease 0.2s both;
}

.stat-card {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.2);
    border-radius: var(--border-radius);
    padding: 2rem;
    text-align: center;
    transition: var(--transition);
    color: var(--dark-color);
    position: relative;
    overflow: hidden;
}

.stat-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
    opacity: 0;
    transition: opacity 0.3s;
}

.stat-card:hover {
    transform: translateY(-8px);
    box-shadow: var(--shadow-heavy);
    background: white;
}

.stat-card:hover::before {
    opacity: 1;
}

.stat-card h4 {
    margin: 0 0 0.5rem 0;
    color: var(--medium-gray);
    font-size: 0.85rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.stat-value {
    font-size: 2.5rem;
    font-weight: 800;
    background: linear-gradient(45deg, var(--primary-color), var(--accent-color));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.5rem;
    line-height: 1;
}

.stat-detail {
    font-size: 0.9rem;
    color: var(--medium-gray);
    opacity: 0.8;
}

.analytics-sections {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 2rem;
    margin-bottom: 2rem;
    animation: fadeInUp 0.8s ease 0.4s both;
}

.analytics-section {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.2);
    border-radius: var(--border-radius);
    padding: 2rem;
    color: var(--dark-color);
    box-shadow: var(--shadow-medium);
}

.analytics-section h3 {
    margin: 0 0 1.5rem 0;
    font-size: 1.3rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--primary-color), var(--primary-light));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.distribution-chart {
    display: flex;
    flex-direction: column;
    gap: 1.2rem;
}

.distribution-item {
    display: flex;
    flex-direction: column;
    gap: 0.6rem;
}

.distribution-bar {
    height: 12px;
    background: rgba(102, 126, 234, 0.1);
    border-radius: 6px;
    overflow: hidden;
    position: relative;
}

.distribution-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--primary-color), var(--accent-color));
    border-radius: 6px;
    transition: width 1s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}

.distribution-fill::after {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
    animation: shimmer 2s infinite;
}

@keyframes shimmer {
    0% { left: -100%; }
    100% { left: 100%; }
}

.distribution-label {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.95rem;
}

.distribution-type {
    font-weight: 600;
    color: var(--dark-color);
}

.distribution-count {
    color: var(--medium-gray);
    font-weight: 500;
    font-size: 0.85rem;
}

.columns-ranking {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}

.column-rank-item {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1rem;
    background: rgba(102, 126, 234, 0.05);
    border-radius: var(--border-radius-sm);
    border: 1px solid rgba(102, 126, 234, 0.1);
    transition: var(--transition);
}

.column-rank-item:hover {
    background: rgba(102, 126, 234, 0.1);
    transform: translateX(4px);
}

.rank-number {
    flex-shrink: 0;
    width: 36px;
    height: 36px;
    background: linear-gradient(45deg, var(--primary-color), var(--accent-color));
    color: white;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 0.9rem;
    box-shadow: var(--shadow-light);
}

.column-info {
    flex: 1;
    min-width: 0;
}

.column-path {
    font-weight: 600;
    color: var(--dark-color);
    word-break: break-word;
    margin-bottom: 0.3rem;
    font-size: 0.95rem;
}

.column-stats {
    font-size: 0.8rem;
    color: var(--medium-gray);
    font-weight: 500;
}

.navigation-hint {
    background: linear-gradient(135deg, rgba(255,255,255,0.95), rgba(240,147,251,0.1));
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.3);
    border-radius: var(--border-radius);
    padding: 2rem;
    text-align: center;
    animation: fadeInUp 0.8s ease 0.6s both;
    color: var(--dark-color);
}

.navigation-hint h3 {
    margin: 0 0 1rem 0;
    font-size: 1.2rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--primary-light), var(--secondary-color));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.navigation-hint p {
    margin: 0;
    font-size: 1rem;
    line-height: 1.6;
    opacity: 0.9;
}

@keyframes fadeInUp {
    from {
        opacity: 0;
        transform: translateY(30px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

@media (max-width: 1024px) {
    .analytics-sections {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 768px) {
    .pages-analytics {
        padding: 1.5rem;
    }

    .analytics-header h2 {
        font-size: 2rem;
    }

    .analytics-grid {
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
    }

    .stat-value {
        font-size: 2rem;
    }

    .analytics-sections {
        gap: 1rem;
    }

    .analytics-section,
    .stat-card,
    .navigation-hint {
        padding: 1.5rem;
    }
}

/* Original Column Browser Styles */
/* Page Analytics Styles */
.pages-analytics {
    padding: 1.5rem;
}

.analytics-header {
    text-align: center;
    margin-bottom: 2rem;
}

.analytics-header h2 {
    margin: 0 0 0.5rem 0;
    color: #333;
}

.analytics-header p {
    color: #666;
    margin: 0;
}

.analytics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}

.stat-card {
    background: #f8f9fa;
    border: 1px solid #e9ecef;
    border-radius: 8px;
    padding: 1.5rem;
    text-align: center;
    transition: transform 0.2s, box-shadow 0.2s;
}

.stat-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

.stat-card h4 {
    margin: 0 0 0.5rem 0;
    color: #666;
    font-size: 0.9rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.stat-value {
    font-size: 2rem;
    font-weight: 700;
    color: #2196f3;
    margin-bottom: 0.25rem;
}

.stat-detail {
    font-size: 0.8rem;
    color: #999;
}

.analytics-sections {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 2rem;
    margin-bottom: 2rem;
}

.analytics-section {
    background: #fff;
    border: 1px solid #e9ecef;
    border-radius: 8px;
    padding: 1.5rem;
}

.analytics-section h3 {
    margin: 0 0 1.5rem 0;
    color: #333;
    font-size: 1.1rem;
}

.distribution-chart {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}

.distribution-item {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.distribution-bar {
    height: 8px;
    background: #e9ecef;
    border-radius: 4px;
    overflow: hidden;
}

.distribution-fill {
    height: 100%;
    background: linear-gradient(90deg, #2196f3, #64b5f6);
    transition: width 0.8s ease;
}

.distribution-label {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.9rem;
}

.distribution-type {
    font-weight: 500;
    color: #333;
}

.distribution-count {
    color: #666;
    font-size: 0.8rem;
}

.columns-ranking {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}

.column-rank-item {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 0.75rem;
    background: #f8f9fa;
    border-radius: 6px;
    border: 1px solid #e9ecef;
}

.rank-number {
    flex-shrink: 0;
    width: 32px;
    height: 32px;
    background: #2196f3;
    color: white;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 600;
    font-size: 0.9rem;
}

.column-info {
    flex: 1;
    min-width: 0;
}

.column-path {
    font-weight: 500;
    color: #333;
    word-break: break-word;
    margin-bottom: 0.25rem;
}

.column-stats {
    font-size: 0.8rem;
    color: #666;
}

.navigation-hint {
    background: linear-gradient(135deg, #f3e5f5, #e1f5fe);
    border: 1px solid #e1bee7;
    border-radius: 8px;
    padding: 1.5rem;
    text-align: center;
}

.navigation-hint h3 {
    margin: 0 0 0.5rem 0;
    color: #7b1fa2;
}

.navigation-hint p {
    margin: 0;
    color: #5e35b1;
}

@media (max-width: 1024px) {
    .analytics-sections {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 768px) {
    .analytics-grid {
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    }

    .stat-value {
        font-size: 1.5rem;
    }

    .pages-analytics {
        padding: 1rem;
    }

    .analytics-sections {
        gap: 1rem;
    }
}

/* Original Column Browser Styles */
.column-browser {
    padding: 2rem;
    background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
    min-height: 100vh;
}

.browser-header {
    margin-bottom: 2rem;
    animation: fadeInDown 0.6s ease;
}

.breadcrumb {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.95rem;
    background: rgba(255, 255, 255, 0.9);
    padding: 1rem 1.5rem;
    border-radius: var(--border-radius);
    backdrop-filter: blur(10px);
    box-shadow: var(--shadow-medium);
    border: 1px solid rgba(255, 255, 255, 0.3);
}

.breadcrumb-item {
    padding: 0.5rem 1rem;
    border-radius: var(--border-radius-sm);
    font-weight: 600;
    color: var(--dark-color);
    transition: var(--transition);
}

.breadcrumb-item.clickable {
    background: rgba(102, 126, 234, 0.1);
    cursor: pointer;
    border: 1px solid rgba(102, 126, 234, 0.2);
}

.breadcrumb-item.clickable:hover {
    background: rgba(102, 126, 234, 0.2);
    transform: translateY(-2px);
    box-shadow: var(--shadow-light);
}

.breadcrumb-separator {
    color: var(--medium-gray);
    font-weight: 600;
    font-size: 1.2rem;
    margin: 0 0.5rem;
}

.view-header {
    text-align: center;
    margin-bottom: 2rem;
    animation: fadeInUp 0.6s ease 0.2s both;
}

.view-header h3 {
    margin: 0 0 0.5rem 0;
    font-size: 2rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--primary-color), var(--primary-light));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.view-header p {
    color: var(--medium-gray);
    margin: 0;
    font-size: 1.1rem;
    opacity: 0.8;
}

.columns-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 1.5rem;
    margin-top: 2rem;
    animation: fadeInUp 0.6s ease 0.4s both;
}

.column-card {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.3);
    border-radius: var(--border-radius);
    padding: 1.5rem;
    cursor: pointer;
    transition: var(--transition);
    position: relative;
    overflow: hidden;
    box-shadow: var(--shadow-medium);
}

.column-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: linear-gradient(90deg, var(--primary-color), var(--accent-color));
    opacity: 0;
    transition: opacity 0.3s;
}

.column-card:hover {
    transform: translateY(-8px);
    box-shadow: var(--shadow-heavy);
    background: white;
}

.column-card:hover::before {
    opacity: 1;
}

.column-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1rem;
}

.column-header h4 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--dark-color);
    word-break: break-word;
    line-height: 1.3;
}

.column-type {
    background: linear-gradient(45deg, var(--primary-color), var(--accent-color));
    color: white;
    padding: 4px 12px;
    border-radius: 16px;
    font-size: 0.8em;
    font-weight: 600;
    white-space: nowrap;
    box-shadow: var(--shadow-light);
}

.column-stats {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
}

.column-stats .stat {
    display: flex;
    justify-content: space-between;
    font-size: 0.9rem;
    padding: 0.5rem;
    background: rgba(102, 126, 234, 0.05);
    border-radius: var(--border-radius-sm);
}

.column-stats .stat label {
    color: var(--medium-gray);
    font-weight: 600;
}

.column-stats .stat span {
    color: var(--dark-color);
    font-weight: 700;
}

.chunks-table-container,
.pages-table-container {
    margin-top: 2rem;
    overflow-x: auto;
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(10px);
    border-radius: var(--border-radius);
    border: 1px solid rgba(255, 255, 255, 0.3);
    box-shadow: var(--shadow-medium);
    animation: fadeInUp 0.6s ease 0.4s both;
}

.chunks-table,
.pages-table {
    width: 100%;
    border-collapse: collapse;
}

.chunks-table th,
.chunks-table td,
.pages-table th,
.pages-table td {
    padding: 1rem;
    text-align: left;
    border-bottom: 1px solid rgba(102, 126, 234, 0.1);
}

.chunks-table th,
.pages-table th {
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.1), rgba(79, 172, 254, 0.1));
    font-weight: 700;
    color: var(--dark-color);
    position: sticky;
    top: 0;
    z-index: 1;
    text-transform: uppercase;
    font-size: 0.85rem;
    letter-spacing: 0.5px;
}

.chunk-row,
.page-row {
    cursor: pointer;
    transition: var(--transition);
}

.chunk-row:hover,
.page-row:hover {
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.05), rgba(79, 172, 254, 0.05));
    transform: translateX(4px);
}

.page-detail-view {
    animation: fadeIn 0.6s ease;
}

.page-detail-view .view-header {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 1rem;
    margin-bottom: 2rem;
    text-align: center;
}

.page-detail-view .view-header h3 {
    font-size: 1.8rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--primary-color), var(--primary-light));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0;
}

.page-type-badge {
    background: linear-gradient(45deg, var(--success-color), var(--accent-color));
    color: white;
    padding: 8px 20px;
    border-radius: 24px;
    font-size: 0.9rem;
    font-weight: 700;
    box-shadow: var(--shadow-medium);
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0%, 100% { transform: scale(1); }
    50% { transform: scale(1.05); }
}

.detail-sections {
    display: flex;
    flex-direction: column;
    gap: 2rem;
}

.detail-section {
    background: rgba(255, 255, 255, 0.95);
    backdrop-filter: blur(10px);
    border-radius: var(--border-radius);
    padding: 2rem;
    border: 1px solid rgba(255, 255, 255, 0.3);
    box-shadow: var(--shadow-medium);
    animation: fadeInUp 0.6s ease both;
}

.detail-section h4 {
    margin: 0 0 1.5rem 0;
    font-size: 1.2rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--primary-color), var(--primary-light));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    position: relative;
    padding-bottom: 0.5rem;
}

.detail-section h4::after {
    content: '';
    position: absolute;
    bottom: 0;
    left: 0;
    width: 60px;
    height: 3px;
    background: linear-gradient(90deg, var(--primary-color), var(--accent-color));
    border-radius: 2px;
}

.detail-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 1rem;
}

.detail-item {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    padding: 1rem;
    background: rgba(102, 126, 234, 0.03);
    border-radius: var(--border-radius-sm);
    border: 1px solid rgba(102, 126, 234, 0.1);
    transition: var(--transition);
}

.detail-item:hover {
    background: rgba(102, 126, 234, 0.08);
    transform: translateY(-2px);
}

.detail-item label {
    font-weight: 700;
    color: var(--medium-gray);
    flex-shrink: 0;
    min-width: 140px;
    font-size: 0.9rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.detail-item span {
    text-align: right;
    word-break: break-word;
    font-weight: 600;
    color: var(--dark-color);
}

.value-display {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.9em;
    background: rgba(102, 126, 234, 0.1);
    color: var(--primary-color);
    padding: 6px 12px;
    border-radius: var(--border-radius-sm);
    font-weight: 600;
}

.raw-data {
    max-height: 500px;
    overflow: auto;
    background: linear-gradient(135deg, #2d3748, #4a5568);
    color: #e2e8f0;
    border-radius: var(--border-radius);
    padding: 1.5rem;
    box-shadow: inset 0 2px 10px rgba(0,0,0,0.3);
    border: 1px solid rgba(255, 255, 255, 0.1);
}

.raw-data pre {
    margin: 0;
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 0.85rem;
    line-height: 1.6;
}

@keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
}

@keyframes fadeInDown {
    from {
        opacity: 0;
        transform: translateY(-20px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

@media (max-width: 1200px) {
    .columns-grid {
        grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    }
}

@media (max-width: 768px) {
    .column-browser {
        padding: 1.5rem;
    }

    .columns-grid {
        grid-template-columns: 1fr;
        gap: 1rem;
    }

    .column-stats {
        grid-template-columns: 1fr;
    }

    .detail-grid {
        grid-template-columns: 1fr;
    }

    .detail-item {
        flex-direction: column;
        align-items: flex-start;
        gap: 0.5rem;
    }

    .detail-item span {
        text-align: left;
    }

    .view-header h3 {
        font-size: 1.5rem;
    }

    .breadcrumb {
        padding: 0.75rem 1rem;
        flex-wrap: wrap;
    }

    .breadcrumb-item {
        padding: 0.3rem 0.8rem;
        font-size: 0.85rem;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#column-browser-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'column-browser-styles';
    styleElement.textContent = columnBrowserStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ColumnBrowser;
}
