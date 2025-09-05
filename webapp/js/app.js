/**
 * Main application logic for the Parquet Explorer (JSON Mode)
 */
class ParquetExplorer {
    constructor() {
        this.currentFile = null;
        this.parquetData = null;
        this.activeTab = 'overview';

        // Bind methods
        this.handleFileSelect = this.handleFileSelect.bind(this);
        this.handleURLLoad = this.handleURLLoad.bind(this);
        this.handleTabSwitch = this.handleTabSwitch.bind(this);
        this.handleReset = this.handleReset.bind(this);
    }

    /**
     * Initialize the application
     */
    async init() {
        try {
            this.setupEventListeners();
            this.hideLoadingScreen();
        } catch (error) {
            this.showError(`Initialization failed: ${error.message}`);
        }
    }

    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // File input
        const dropZone = document.getElementById('drop-zone');
        const fileInput = document.getElementById('file-input');

        // Drag and drop
        dropZone.addEventListener('click', () => fileInput.click());
        dropZone.addEventListener('dragover', this.handleDragOver);
        dropZone.addEventListener('dragleave', this.handleDragLeave);
        dropZone.addEventListener('drop', this.handleDrop.bind(this));

        // File input change
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                this.handleFileSelect(e.target.files[0]);
            }
        });

        // URL input
        const urlInput = document.getElementById('url-input');
        const loadUrlBtn = document.getElementById('load-url-btn');

        loadUrlBtn.addEventListener('click', () => {
            const url = urlInput.value.trim();
            if (url) {
                this.handleURLLoad(url);
            }
        });

        urlInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                const url = urlInput.value.trim();
                if (url) {
                    this.handleURLLoad(url);
                }
            }
        });

        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', this.handleTabSwitch);
        });

        // Reset button
        document.getElementById('reset-btn').addEventListener('click', this.handleReset);

        // Export JSON button
        document.getElementById('export-json').addEventListener('click', () => {
            if (this.parquetData) {
                this.exportJSON();
            }
        });
    }

    /**
     * Handle drag over event
     */
    handleDragOver(e) {
        e.preventDefault();
        e.currentTarget.classList.add('drag-over');
    }

    /**
     * Handle drag leave event
     */
    handleDragLeave(e) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');
    }

    /**
     * Handle drop event
     */
    handleDrop(e) {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');

        const files = e.dataTransfer.files;
        if (files.length > 0) {
            this.handleFileSelect(files[0]);
        }
    }

    /**
     * Handle file selection
     */
    async handleFileSelect(file) {
        if (!file.name.toLowerCase().endsWith('.json')) {
            this.showError('Please select a .json file');
            return;
        }

        this.showLoadingScreen();
        this.updateLoadingStatus('Reading JSON file...');

        try {
            const text = await file.text();
            this.updateLoadingStatus('Parsing JSON data...');
            await this.parseJSON(text, file.name);
        } catch (error) {
            this.showError(`Failed to parse file: ${error.message}`);
        }
    }

    /**
     * Handle URL loading
     */
    async handleURLLoad(url) {
        this.showLoadingScreen();
        this.updateLoadingStatus('Fetching remote JSON...');

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            this.updateLoadingStatus('Parsing JSON data...');
            const text = await response.text();
            await this.parseJSON(text, url);
        } catch (error) {
            this.showError(`Failed to load URL: ${error.message}`);
        }
    }

    /**
     * Parse the JSON data
     */
    async parseJSON(jsonText, source) {
        try {
            const data = JSON.parse(jsonText);

            // Validate the JSON structure matches our schema
            this.validateParquetJSON(data);

            this.parquetData = data;

            // Show the explorer interface
            this.showExplorer();

            // Populate the UI with data
            this.populateUI();

            this.hideLoadingScreen();
        } catch (error) {
            throw new Error(`JSON parsing failed: ${error.message}`);
        }
    }

    /**
     * Validate that the JSON matches our expected parquet schema
     */
    validateParquetJSON(data) {
        // Check for required top-level fields
        const requiredFields = ['source', 'filesize', 'column_chunks', 'metadata'];
        for (const field of requiredFields) {
            if (!(field in data)) {
                throw new Error(`Missing required field: ${field}`);
            }
        }

        // Check metadata structure
        if (!data.metadata || !data.metadata.metadata) {
            throw new Error('Invalid metadata structure');
        }

        const metadata = data.metadata.metadata;
        if (!metadata.schema) {
            throw new Error('Missing schema in metadata');
        }

        console.log('JSON validation passed');
    }

    /**
     * Populate the UI with parsed data
     */
    populateUI() {
        if (!this.parquetData) return;

        try {
            console.log('Parsed data:', this.parquetData);

            // Update overview
            this.updateOverview(this.parquetData);

            // Update schema
            this.updateSchema(this.parquetData.metadata.metadata.schema);

            // Initialize file structure visualization
            this.initializeFileStructureViz(this.parquetData);

            // Initialize additional components
            this.initializeComponents(this.parquetData);

        } catch (error) {
            console.error('Error populating UI:', error);
            this.showError(`Failed to populate UI: ${error.message}`);
        }
    }

    /**
     * Initialize additional UI components
     */
    initializeComponents(data) {
        // Initialize hierarchical row group browser
        if (data.metadata && data.metadata.metadata && data.metadata.metadata.row_groups) {
            const rowGroupContainer = document.getElementById('rowgroups-tab');
            if (rowGroupContainer && typeof RowGroupBrowser !== 'undefined') {
                const rowGroupBrowser = new RowGroupBrowser(rowGroupContainer);
                rowGroupBrowser.init(data, this);
            } else {
                // Fallback to old visualization if RowGroupBrowser not available
                this.initializeFallbackRowGroupsTab(data, rowGroupContainer);
            }
        }

        // Initialize hierarchical column browser
        if (data.column_chunks && data.column_chunks.length > 0) {
            const columnContainer = document.getElementById('columns-tab');
            if (columnContainer && typeof ColumnBrowser !== 'undefined') {
                const columnBrowser = new ColumnBrowser(columnContainer);
                columnBrowser.init(data, this);
            } else {
                // Fallback to old column details if ColumnBrowser not available
                this.initializeFallbackColumnsTab(data, columnContainer);
            }
        }

        // Initialize pages tab
        const pagesContainer = document.getElementById('pages-content');
        if (pagesContainer) {
            this.initializePagesTab(data, pagesContainer);
        }

        // Initialize metadata tab
        const metadataContainer = document.getElementById('metadata-content');
        if (metadataContainer) {
            metadataContainer.innerHTML = `<pre><code>${JSON.stringify(data, null, 2)}</code></pre>`;
        }
    }

    /**
     * Update the overview tab
     */
    updateOverview(data) {
        const fileStats = document.getElementById('file-stats');
        const schemaStats = document.getElementById('schema-stats');
        const compressionStats = document.getElementById('compression-stats');

        const metadata = data.metadata.metadata;

        fileStats.innerHTML = `
            <div class="stat">
                <label>Source:</label>
                <span>${data.source}</span>
            </div>
            <div class="stat">
                <label>Size:</label>
                <span>${formatBytes(data.filesize)}</span>
            </div>
            <div class="stat">
                <label>Parquet Version:</label>
                <span>${metadata.version}</span>
            </div>
            <div class="stat">
                <label>Created By:</label>
                <span>${metadata.created_by || 'Unknown'}</span>
            </div>
        `;

        const schema = metadata.schema;
        schemaStats.innerHTML = `
            <div class="stat">
                <label>Root Name:</label>
                <span>${schema.name}</span>
            </div>
            <div class="stat">
                <label>Columns:</label>
                <span>${metadata.column_count || 'N/A'}</span>
            </div>
            <div class="stat">
                <label>Rows:</label>
                <span>${metadata.row_count ? metadata.row_count.toLocaleString() : 'N/A'}</span>
            </div>
            <div class="stat">
                <label>Row Groups:</label>
                <span>${metadata.row_group_count || 'N/A'}</span>
            </div>
        `;

        const compressionInfo = metadata.compression_stats;
        if (compressionInfo) {
            compressionStats.innerHTML = `
                <div class="stat">
                    <label>Compressed Size:</label>
                    <span>${formatBytes(compressionInfo.total_compressed || 0)}</span>
                </div>
                <div class="stat">
                    <label>Uncompressed Size:</label>
                    <span>${formatBytes(compressionInfo.total_uncompressed || 0)}</span>
                </div>
                <div class="stat">
                    <label>Compression Ratio:</label>
                    <span>${compressionInfo.ratio ? (compressionInfo.ratio * 100).toFixed(1) + '%' : 'N/A'}</span>
                </div>
                <div class="stat">
                    <label>Space Saved:</label>
                    <span>${compressionInfo.space_saved_percent ? compressionInfo.space_saved_percent.toFixed(1) + '%' : 'N/A'}</span>
                </div>
            `;
        } else {
            compressionStats.innerHTML = `
                <div class="stat">
                    <label>Compression Info:</label>
                    <span>Not Available</span>
                </div>
            `;
        }
    }

    /**
     * Update the schema tab
     */
    updateSchema(schema) {
        const schemaTree = document.getElementById('schema-tree');
        if (typeof SchemaTree !== 'undefined') {
            const tree = new SchemaTree(schemaTree);
            tree.render(schema);
        } else {
            schemaTree.innerHTML = this.renderSchemaNode(schema);
        }
    }

    /**
     * Render a schema node as HTML (fallback if SchemaTree component not available)
     */
    renderSchemaNode(node, level = 0) {
        let html = `<div class="schema-node" style="margin-left: ${level * 20}px">`;

        html += `<span class="schema-name">${node.name}</span>`;

        if (node.element_type === 'column') {
            // This is a leaf node (column)
            html += `<span class="schema-type">${this.getTypeDisplay(node)}</span>`;
            if (node.logical_type) {
                html += `<span class="schema-logical-type">${this.getLogicalTypeDisplay(node.logical_type)}</span>`;
            }
        } else {
            // This is a group node
            html += `<span class="schema-type">group</span>`;
        }

        if (node.children && Object.keys(node.children).length > 0) {
            html += '<div class="schema-children">';
            for (const [childName, child] of Object.entries(node.children)) {
                html += this.renderSchemaNode(child, level + 1);
            }
            html += '</div>';
        }

        html += '</div>';
        return html;
    }

    /**
     * Get display string for physical type
     */
    getTypeDisplay(node) {
        const typeMap = {
            0: 'BOOLEAN',
            1: 'INT32',
            2: 'INT64',
            3: 'INT96',
            4: 'FLOAT',
            5: 'DOUBLE',
            6: 'BYTE_ARRAY',
            7: 'FIXED_LEN_BYTE_ARRAY'
        };
        return typeMap[node.type] || `TYPE_${node.type}`;
    }

    /**
     * Get display string for logical type
     */
    getLogicalTypeDisplay(logicalType) {
        if (!logicalType) return '';

        const typeMap = {
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
            18: 'GEOGRAPHY'
        };

        return typeMap[logicalType.logical_type] || `LOGICAL_${logicalType.logical_type}`;
    }

    /**
     * Get compression algorithm name from code
     */
    getCompressionName(code) {
        const compressionMap = {
            0: 'UNCOMPRESSED',
            1: 'SNAPPY',
            2: 'GZIP',
            3: 'LZO',
            4: 'BROTLI',
            5: 'LZ4',
            6: 'ZSTD'
        };

        return compressionMap[code] || `COMPRESSION_${code}`;
    }

    /**
     * Get encoding name from code
     */
    getEncodingName(code) {
        const encodingMap = {
            0: 'PLAIN',
            2: 'DICTIONARY',
            3: 'RLE',
            4: 'BIT_PACKED',
            5: 'DELTA_BINARY_PACKED',
            6: 'DELTA_LENGTH_BYTE_ARRAY',
            7: 'DELTA_BYTE_ARRAY',
            8: 'RLE_DICTIONARY',
            9: 'BYTE_STREAM_SPLIT'
        };

        return encodingMap[code] || `ENCODING_${code}`;
    }

    /**
     * Find a column in the schema tree by path
     */
    findColumnInSchema(schema, path) {
        if (!schema || !path) return null;

        const pathParts = path.split('.');
        let currentNode = schema;

        for (const part of pathParts) {
            if (currentNode.children && currentNode.children[part]) {
                currentNode = currentNode.children[part];
            } else {
                return null;
            }
        }

        return currentNode.element_type === 'column' ? currentNode : null;
    }

    /**
     * Fallback column initialization (old style)
     */
    initializeFallbackColumnsTab(data, columnContainer) {
        if (!columnContainer) return;

        // Create a simple column list as fallback
        const columns = data.column_chunks.map(chunk => chunk.path_in_schema);
        columnContainer.innerHTML = `
            <div class="columns-fallback">
                <h3>Columns (${columns.length})</h3>
                <div class="column-list">
                    ${columns.map(col => `<div class="column-item">${col}</div>`).join('')}
                </div>
            </div>
        `;
    }

    /**
     * Fallback row group initialization (old style)
     */
    initializeFallbackRowGroupsTab(data, rowGroupContainer) {
        if (!rowGroupContainer) return;

        const rowGroups = data.metadata.metadata.row_groups.map((rg, index) => {
            // Calculate compression ratio from row group data
            let compressionRatio = 0;
            if (rg.compression_stats && rg.compression_stats.ratio) {
                compressionRatio = rg.compression_stats.ratio;
            } else if (rg.total_compressed_size && rg.total_uncompressed_size) {
                // Calculate ratio if not directly available
                compressionRatio = rg.total_compressed_size / rg.total_uncompressed_size;
            } else {
                // Try to calculate from column chunks
                let totalCompressed = 0;
                let totalUncompressed = 0;
                if (rg.column_chunks) {
                    Object.values(rg.column_chunks).forEach(chunk => {
                        if (chunk.metadata) {
                            totalCompressed += chunk.metadata.total_compressed_size || 0;
                            totalUncompressed += chunk.metadata.total_uncompressed_size || 0;
                        }
                    });
                    if (totalUncompressed > 0) {
                        compressionRatio = totalCompressed / totalUncompressed;
                    }
                }
            }

            return {
                id: index,
                rows: rg.row_count,
                size: rg.total_byte_size,
                compressionRatio: compressionRatio
            };
        });

        // Simple fallback display
        rowGroupContainer.innerHTML = `
            <div class="rowgroups-fallback">
                <h3>Row Groups (${rowGroups.length})</h3>
                <div class="rowgroup-list">
                    ${rowGroups.map(rg => `
                        <div class="rowgroup-item">
                            <strong>Row Group ${rg.id}</strong><br>
                            Rows: ${formatNumber(rg.rows)}<br>
                            Size: ${formatBytes(rg.size)}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    /**
     * Initialize the Pages tab with useful page analytics
     */
    initializePagesTab(data, container) {
        if (!data.column_chunks || data.column_chunks.length === 0) {
            container.innerHTML = '<p class="no-data">No page data available</p>';
            return;
        }

        // Analyze pages across all chunks
        let totalPages = 0;
        let totalSize = 0;
        let totalUncompressedSize = 0;
        const pageTypes = {};
        const encodings = {};
        const compressionRatios = [];
        const pageSizes = [];
        const columnsWithPages = {};

        data.column_chunks.forEach(chunk => {
            const pages = this.mapPageData(chunk);
            const columnPath = chunk.path_in_schema;

            if (!columnsWithPages[columnPath]) {
                columnsWithPages[columnPath] = {
                    pages: 0,
                    size: 0,
                    types: new Set()
                };
            }

            pages.forEach(page => {
                totalPages++;
                const pageSize = page.size || 0;
                totalSize += pageSize;
                pageSizes.push(pageSize);

                // Track uncompressed size if available
                if (page.uncompressedSize) {
                    totalUncompressedSize += page.uncompressedSize;
                    if (pageSize > 0) {
                        compressionRatios.push(pageSize / page.uncompressedSize);
                    }
                }

                // Count page types
                pageTypes[page.type] = (pageTypes[page.type] || 0) + 1;

                // Count encodings
                if (page.encoding && page.encoding !== 'N/A') {
                    encodings[page.encoding] = (encodings[page.encoding] || 0) + 1;
                }

                // Track per-column stats
                columnsWithPages[columnPath].pages++;
                columnsWithPages[columnPath].size += pageSize;
                columnsWithPages[columnPath].types.add(page.type);
            });
        });

        // Calculate statistics
        const avgPageSize = totalPages > 0 ? totalSize / totalPages : 0;
        const avgCompressionRatio = compressionRatios.length > 0
            ? compressionRatios.reduce((a, b) => a + b, 0) / compressionRatios.length
            : 0;

        // Sort columns by page count for display
        const sortedColumns = Object.entries(columnsWithPages)
            .sort(([,a], [,b]) => b.pages - a.pages)
            .slice(0, 10); // Top 10 columns by page count

        container.innerHTML = `
            <div class="pages-analytics">
                <div class="analytics-header">
                    <h2>Page Analytics</h2>
                    <p>Analysis of page distribution and characteristics across the parquet file</p>
                </div>

                <div class="analytics-grid">
                    <div class="stat-card">
                        <h4>Total Pages</h4>
                        <div class="stat-value">${totalPages.toLocaleString()}</div>
                        <div class="stat-detail">Across ${Object.keys(columnsWithPages).length} columns</div>
                    </div>

                    <div class="stat-card">
                        <h4>Total Size</h4>
                        <div class="stat-value">${formatBytes(totalSize)}</div>
                        ${totalUncompressedSize > 0 ?
                            `<div class="stat-detail">${formatBytes(totalUncompressedSize)} uncompressed</div>` : ''}
                    </div>

                    <div class="stat-card">
                        <h4>Average Page Size</h4>
                        <div class="stat-value">${formatBytes(avgPageSize)}</div>
                        <div class="stat-detail">Per page</div>
                    </div>

                    ${avgCompressionRatio > 0 ? `
                    <div class="stat-card">
                        <h4>Avg Compression</h4>
                        <div class="stat-value">${(avgCompressionRatio * 100).toFixed(1)}%</div>
                        <div class="stat-detail">Compression ratio</div>
                    </div>
                    ` : ''}
                </div>

                <div class="analytics-sections">
                    <div class="analytics-section">
                        <h3>Page Type Distribution</h3>
                        <div class="distribution-chart">
                            ${Object.entries(pageTypes).map(([type, count]) => {
                                const percentage = (count / totalPages * 100).toFixed(1);
                                return `
                                    <div class="distribution-item">
                                        <div class="distribution-bar">
                                            <div class="distribution-fill" style="width: ${percentage}%"></div>
                                        </div>
                                        <div class="distribution-label">
                                            <span class="distribution-type">${type}</span>
                                            <span class="distribution-count">${count} (${percentage}%)</span>
                                        </div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>

                    <div class="analytics-section">
                        <h3>Encoding Distribution</h3>
                        <div class="distribution-chart">
                            ${Object.entries(encodings).map(([encoding, count]) => {
                                const percentage = (count / totalPages * 100).toFixed(1);
                                return `
                                    <div class="distribution-item">
                                        <div class="distribution-bar">
                                            <div class="distribution-fill" style="width: ${percentage}%"></div>
                                        </div>
                                        <div class="distribution-label">
                                            <span class="distribution-type">${encoding}</span>
                                            <span class="distribution-count">${count} (${percentage}%)</span>
                                        </div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>

                    <div class="analytics-section">
                        <h3>Top Columns by Page Count</h3>
                        <div class="columns-ranking">
                            ${sortedColumns.map(([columnPath, stats], index) => `
                                <div class="column-rank-item">
                                    <div class="rank-number">${index + 1}</div>
                                    <div class="column-info">
                                        <div class="column-path">${columnPath}</div>
                                        <div class="column-stats">
                                            ${stats.pages} pages • ${formatBytes(stats.size)} •
                                            Types: ${Array.from(stats.types).join(', ')}
                                        </div>
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                </div>

                <div class="navigation-hint">
                    <h3>💡 Navigation Tip</h3>
                    <p>For detailed page exploration, use the <strong>Columns</strong> tab to drill down:
                    Column → Chunks → Pages → Page Details</p>
                </div>
            </div>
        `;
    }

    /**
     * Map page data from chunk to expected format
     */
    mapPageData(chunk) {
        const pages = [];

        // Add dictionary page if it exists
        if (chunk.dictionary_page) {
            pages.push({
                type: 'DICTIONARY',
                offset: chunk.dictionary_page.start_offset,
                size: chunk.dictionary_page.compressed_page_size,
                numRows: 0, // Dictionary pages don't have rows
                encoding: this.getEncodingName(chunk.dictionary_page.encoding)
            });
        }

        // Add data pages
        if (chunk.data_pages && chunk.data_pages.length > 0) {
            chunk.data_pages.forEach((page, index) => {
                let pageType = 'DATA';
                let numRows = 0;

                // Determine page type and row count based on page_type field
                if (page.page_type !== undefined) {
                    const pageTypeMap = {
                        0: 'DATA_V1',
                        1: 'INDEX',
                        2: 'DICTIONARY',
                        3: 'DATA_V2'
                    };
                    pageType = pageTypeMap[page.page_type] || `PAGE_${page.page_type}`;
                }

                // Get number of rows/values
                if (page.num_values !== undefined) {
                    numRows = page.num_values;
                } else if (page.num_rows !== undefined) {
                    numRows = page.num_rows;
                }

                pages.push({
                    type: pageType,
                    offset: page.start_offset,
                    size: page.compressed_page_size,
                    numRows: numRows,
                    encoding: this.getEncodingName(page.encoding)
                });
            });
        }

        // Add index pages if they exist
        if (chunk.index_pages && chunk.index_pages.length > 0) {
            chunk.index_pages.forEach((page, index) => {
                pages.push({
                    type: 'INDEX',
                    offset: page.start_offset,
                    size: page.compressed_page_size,
                    numRows: 0, // Index pages don't have rows
                    encoding: 'N/A'
                });
            });
        }

        return pages;
    }

    /**
     * Initialize file structure visualization
     */
    initializeFileStructureViz(data) {
        const container = document.getElementById('rowgroup-chart');
        if (!container || !data.column_chunks) return;

        try {
            // Initialize the stacked byte range visualizer
            this.fileStructureViz = new StackedByteRangeVisualizer(container);
            this.fileStructureViz.initWithData(data);
        } catch (error) {
            console.error('Error creating file structure visualization:', error);
            container.innerHTML = '<p class="viz-error">Unable to create file structure visualization</p>';
        }
    }

    /**
     * Handle tab switching
     */
    handleTabSwitch(e) {
        const tabName = e.target.dataset.tab;

        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        e.target.classList.add('active');

        // Update active tab content
        document.querySelectorAll('.tab-pane').forEach(pane => {
            pane.classList.remove('active');
        });
        document.getElementById(`${tabName}-tab`).classList.add('active');

        this.activeTab = tabName;
    }

    /**
     * Handle reset
     */
    handleReset() {
        this.currentFile = null;
        this.parquetData = null;
        this.showFileInput();
    }

    /**
     * Export JSON
     */
    exportJSON() {
        if (!this.parquetData) return;

        const jsonStr = JSON.stringify(this.parquetData, null, 2);
        const blob = new Blob([jsonStr], { type: 'application/json' });
        const url = URL.createObjectURL(blob);

        const a = document.createElement('a');
        a.href = url;
        a.download = 'parquet-structure.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);

        URL.revokeObjectURL(url);
    }

    /**
     * UI state management
     */
    showLoadingScreen() {
        document.getElementById('loading-screen').style.display = 'flex';
        document.getElementById('app-main').style.display = 'none';
    }

    hideLoadingScreen() {
        document.getElementById('loading-screen').style.display = 'none';
        document.getElementById('app-main').style.display = 'block';
    }

    updateLoadingStatus(status) {
        document.getElementById('loading-status').textContent = status;
    }

    showFileInput() {
        document.getElementById('file-input-section').style.display = 'block';
        document.getElementById('explorer-section').style.display = 'none';
        document.getElementById('error-section').style.display = 'none';
    }

    showExplorer() {
        document.getElementById('file-input-section').style.display = 'none';
        document.getElementById('explorer-section').style.display = 'block';
        document.getElementById('error-section').style.display = 'none';
    }

    showError(message) {
        document.getElementById('error-message').textContent = message;
        document.getElementById('file-input-section').style.display = 'none';
        document.getElementById('explorer-section').style.display = 'none';
        document.getElementById('error-section').style.display = 'block';
        this.hideLoadingScreen();
    }
}

// Utility functions
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatNumber(num) {
    if (num === null || num === undefined) return 'N/A';
    return num.toLocaleString();
}

// Make utility functions globally available
window.formatBytes = formatBytes;
window.formatNumber = formatNumber;

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    const app = new ParquetExplorer();
    app.init().catch(error => {
        console.error('Failed to initialize app:', error);
        document.getElementById('loading-status').textContent = `Error: ${error.message}`;
    });
});
