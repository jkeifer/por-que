/**
 * File Overview Component
 * Displays high-level statistics and information about the Parquet file
 */
class FileOverview {
    constructor(container) {
        this.container = container;
        this.data = null;
    }

    /**
     * Render the overview with parsed data
     * @param {object} data - Parsed parquet file data
     */
    render(data) {
        this.data = data;

        const overview = this.generateOverviewData(data);

        // Update file stats
        const fileStats = this.container.querySelector('#file-stats');
        if (fileStats) {
            fileStats.innerHTML = this.renderFileStats(overview.file);
        }

        // Update schema stats
        const schemaStats = this.container.querySelector('#schema-stats');
        if (schemaStats) {
            schemaStats.innerHTML = this.renderSchemaStats(overview.schema);
        }

        // Update compression stats
        const compressionStats = this.container.querySelector('#compression-stats');
        if (compressionStats) {
            compressionStats.innerHTML = this.renderCompressionStats(overview.compression);
        }

        // Render row group chart
        const chartContainer = this.container.querySelector('#rowgroup-chart');
        if (chartContainer) {
            this.renderRowGroupChart(chartContainer, overview.rowGroups);
        }
    }

    /**
     * Generate overview data from raw parsed data
     * @param {object} data - Raw parsed data
     * @returns {object} Structured overview data
     */
    generateOverviewData(data) {
        const metadata = data.metadata || {};
        const schema = metadata.schema || {};

        // File information
        const file = {
            source: data.source || 'Unknown',
            size: data.filesize || 0,
            magicHeader: metadata.magic_header || 'N/A',
            magicFooter: metadata.magic_footer || 'N/A',
            format: 'Parquet',
            createdBy: metadata.created_by || 'Unknown'
        };

        // Schema information
        const schemaInfo = this.analyzeSchema(schema);

        // Compression information (placeholder for now)
        const compression = {
            algorithm: 'N/A',
            ratio: 'N/A',
            originalSize: data.filesize || 0,
            compressedSize: data.filesize || 0
        };

        // Row groups (placeholder for now)
        const rowGroups = this.generateMockRowGroups();

        return {
            file,
            schema: schemaInfo,
            compression,
            rowGroups
        };
    }

    /**
     * Analyze schema structure
     * @param {object} schema - Schema object
     * @returns {object} Schema analysis
     */
    analyzeSchema(schema) {
        if (!schema || !schema.fields) {
            return {
                totalFields: 0,
                maxDepth: 0,
                typeDistribution: {}
            };
        }

        let totalFields = 0;
        let maxDepth = 0;
        const typeDistribution = {};

        function traverse(node, depth = 0) {
            if (!node) return;

            totalFields++;
            maxDepth = Math.max(maxDepth, depth);

            const type = node.type || 'unknown';
            typeDistribution[type] = (typeDistribution[type] || 0) + 1;

            if (node.fields && node.fields.length > 0) {
                node.fields.forEach(child => traverse(child, depth + 1));
            }
        }

        traverse(schema);

        return {
            name: schema.name || 'root',
            totalFields,
            maxDepth,
            typeDistribution
        };
    }

    /**
     * Generate mock row group data (placeholder)
     * @returns {Array} Mock row group data
     */
    generateMockRowGroups() {
        return [
            { id: 0, rows: 10000, size: 2048000, compressionRatio: 0.65 },
            { id: 1, rows: 15000, size: 3072000, compressionRatio: 0.72 },
            { id: 2, rows: 8000, size: 1536000, compressionRatio: 0.58 }
        ];
    }

    /**
     * Render file statistics
     * @param {object} fileInfo - File information
     * @returns {string} HTML string
     */
    renderFileStats(fileInfo) {
        return `
            <div class="stat">
                <label>Source:</label>
                <span title="${fileInfo.source}">${truncateString(fileInfo.source, 30)}</span>
            </div>
            <div class="stat">
                <label>Size:</label>
                <span>${formatBytes(fileInfo.size)}</span>
            </div>
            <div class="stat">
                <label>Format:</label>
                <span>${fileInfo.format}</span>
            </div>
            <div class="stat">
                <label>Magic Header:</label>
                <span class="magic-bytes">${fileInfo.magicHeader}</span>
            </div>
            <div class="stat">
                <label>Magic Footer:</label>
                <span class="magic-bytes">${fileInfo.magicFooter}</span>
            </div>
        `;
    }

    /**
     * Render schema statistics
     * @param {object} schemaInfo - Schema information
     * @returns {string} HTML string
     */
    renderSchemaStats(schemaInfo) {
        const typeEntries = Object.entries(schemaInfo.typeDistribution);
        const typeBreakdown = typeEntries
            .sort((a, b) => b[1] - a[1])
            .slice(0, 5) // Show top 5 types
            .map(([type, count]) =>
                `<div class="type-stat">
                    ${formatParquetType(type)}: <span class="count">${count}</span>
                </div>`
            ).join('');

        return `
            <div class="stat">
                <label>Root Name:</label>
                <span>${schemaInfo.name}</span>
            </div>
            <div class="stat">
                <label>Total Fields:</label>
                <span>${formatNumber(schemaInfo.totalFields)}</span>
            </div>
            <div class="stat">
                <label>Max Depth:</label>
                <span>${schemaInfo.maxDepth}</span>
            </div>
            <div class="type-breakdown">
                <label>Type Distribution:</label>
                <div class="type-stats">
                    ${typeBreakdown}
                </div>
            </div>
        `;
    }

    /**
     * Render compression statistics
     * @param {object} compressionInfo - Compression information
     * @returns {string} HTML string
     */
    renderCompressionStats(compressionInfo) {
        const spaceSaved = compressionInfo.originalSize - compressionInfo.compressedSize;
        const savingsPercent = compressionInfo.originalSize > 0
            ? (spaceSaved / compressionInfo.originalSize) * 100
            : 0;

        return `
            <div class="stat">
                <label>Algorithm:</label>
                <span>${compressionInfo.algorithm}</span>
            </div>
            <div class="stat">
                <label>Original Size:</label>
                <span>${formatBytes(compressionInfo.originalSize)}</span>
            </div>
            <div class="stat">
                <label>Compressed Size:</label>
                <span>${formatBytes(compressionInfo.compressedSize)}</span>
            </div>
            <div class="stat">
                <label>Space Saved:</label>
                <span>${formatBytes(spaceSaved)} (${savingsPercent.toFixed(1)}%)</span>
            </div>
        `;
    }

    /**
     * Render row group distribution chart using D3.js
     * @param {HTMLElement} container - Chart container
     * @param {Array} rowGroups - Row group data
     */
    renderRowGroupChart(container, rowGroups) {
        // Clear existing chart
        container.innerHTML = '';

        if (!rowGroups || rowGroups.length === 0) {
            container.innerHTML = '<p class="no-data">No row group data available</p>';
            return;
        }

        const margin = { top: 20, right: 30, bottom: 40, left: 50 };
        const width = container.offsetWidth - margin.left - margin.right;
        const height = 200 - margin.top - margin.bottom;

        // Create SVG
        const svg = d3.select(container)
            .append('svg')
            .attr('width', width + margin.left + margin.right)
            .attr('height', height + margin.top + margin.bottom);

        const g = svg.append('g')
            .attr('transform', `translate(${margin.left},${margin.top})`);

        // Scales
        const xScale = d3.scaleBand()
            .domain(rowGroups.map(d => `RG ${d.id}`))
            .range([0, width])
            .padding(0.1);

        const yScale = d3.scaleLinear()
            .domain([0, d3.max(rowGroups, d => d.size)])
            .range([height, 0]);

        // Create bars
        g.selectAll('.bar')
            .data(rowGroups)
            .enter().append('rect')
            .attr('class', 'bar')
            .attr('x', d => xScale(`RG ${d.id}`))
            .attr('width', xScale.bandwidth())
            .attr('y', d => yScale(d.size))
            .attr('height', d => height - yScale(d.size))
            .attr('fill', '#667eea')
            .attr('opacity', 0.8)
            .on('mouseover', function(event, d) {
                // Tooltip on hover
                const tooltip = d3.select('body')
                    .append('div')
                    .attr('class', 'chart-tooltip')
                    .style('position', 'absolute')
                    .style('background', 'rgba(0,0,0,0.8)')
                    .style('color', 'white')
                    .style('padding', '8px')
                    .style('border-radius', '4px')
                    .style('font-size', '12px')
                    .style('pointer-events', 'none')
                    .style('opacity', 0);

                tooltip.html(`
                    Row Group ${d.id}<br/>
                    Rows: ${formatNumber(d.rows)}<br/>
                    Size: ${formatBytes(d.size)}<br/>
                    Compression: ${formatPercentage(d.compressionRatio)}
                `)
                .style('left', (event.pageX + 10) + 'px')
                .style('top', (event.pageY - 10) + 'px')
                .transition()
                .duration(200)
                .style('opacity', 1);
            })
            .on('mouseout', function() {
                d3.selectAll('.chart-tooltip').remove();
            });

        // X axis
        g.append('g')
            .attr('transform', `translate(0,${height})`)
            .call(d3.axisBottom(xScale));

        // Y axis
        g.append('g')
            .call(d3.axisLeft(yScale)
                .tickFormat(d => formatBytes(d, 0)));

        // Y axis label
        g.append('text')
            .attr('transform', 'rotate(-90)')
            .attr('y', 0 - margin.left)
            .attr('x', 0 - (height / 2))
            .attr('dy', '1em')
            .style('text-anchor', 'middle')
            .style('font-size', '12px')
            .style('fill', '#666')
            .text('Size');
    }

    /**
     * Update the overview with new data
     * @param {object} data - New parquet file data
     */
    update(data) {
        this.render(data);
    }

    /**
     * Get summary statistics
     * @returns {object} Summary statistics
     */
    getSummary() {
        if (!this.data) return null;

        const overview = this.generateOverviewData(this.data);

        return {
            fileSize: overview.file.size,
            totalFields: overview.schema.totalFields,
            maxDepth: overview.schema.maxDepth,
            typeCount: Object.keys(overview.schema.typeDistribution).length,
            rowGroupCount: overview.rowGroups.length
        };
    }
}

// Add styles specific to file overview
const overviewStyles = `
.magic-bytes {
    font-family: 'Monaco', 'Consolas', monospace;
    background: #f0f0f0;
    padding: 2px 4px;
    border-radius: 3px;
    font-size: 0.9em;
}

.type-breakdown {
    flex-direction: column;
    align-items: flex-start;
}

.type-stats {
    margin-top: 0.5rem;
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
}

.type-stat {
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    font-size: 0.9em;
}

.count {
    font-weight: 500;
    color: #666;
}

.no-data {
    text-align: center;
    color: #999;
    padding: 2rem;
    font-style: italic;
}

.chart-tooltip {
    z-index: 1000;
}

@media (prefers-color-scheme: dark) {
    .magic-bytes {
        background: #3a3a3a;
        color: #e0e0e0;
    }

    .count {
        color: #ccc;
    }

    .no-data {
        color: #666;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#overview-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'overview-styles';
    styleElement.textContent = overviewStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FileOverview;
}
