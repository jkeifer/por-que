/**
 * Row Group Visualization Component
 * Renders interactive visualizations of row groups and their properties
 */
class RowGroupVisualization {
    constructor(container) {
        this.container = container;
        this.data = null;
    }

    /**
     * Render row group visualizations
     * @param {Array} rowGroups - Array of row group data
     */
    render(rowGroups) {
        this.data = rowGroups;

        if (!rowGroups || rowGroups.length === 0) {
            this.container.innerHTML = '<p class="no-data">No row group data available</p>';
            return;
        }

        // Create layout
        this.container.innerHTML = `
            <div class="rowgroup-viz-container">
                <div class="rowgroup-table-container">
                    <h4>Row Groups Overview</h4>
                    <div class="rowgroup-table" id="rowgroup-table"></div>
                </div>
                <div class="rowgroup-charts-container">
                    <h4>Distribution Charts</h4>
                    <div class="chart-grid">
                        <div class="chart-item">
                            <h5>Size Distribution</h5>
                            <div id="size-chart"></div>
                        </div>
                        <div class="chart-item">
                            <h5>Row Count Distribution</h5>
                            <div id="rows-chart"></div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        this.renderTable(rowGroups);
        this.renderCharts(rowGroups);
    }

    /**
     * Render row groups table
     * @param {Array} rowGroups - Row group data
     */
    renderTable(rowGroups) {
        const tableContainer = document.getElementById('rowgroup-table');

        let tableHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Rows</th>
                        <th>Size</th>
                        <th>Compression</th>
                        <th>Avg Row Size</th>
                    </tr>
                </thead>
                <tbody>
        `;

        rowGroups.forEach(rg => {
            const avgRowSize = rg.rows > 0 ? rg.size / rg.rows : 0;
            tableHTML += `
                <tr class="rowgroup-row" data-id="${rg.id}">
                    <td>${rg.id}</td>
                    <td>${formatNumber(rg.rows)}</td>
                    <td>${formatBytes(rg.size)}</td>
                    <td>${formatPercentage(rg.compressionRatio)}</td>
                    <td>${formatBytes(avgRowSize)}</td>
                </tr>
            `;
        });

        tableHTML += '</tbody></table>';
        tableContainer.innerHTML = tableHTML;

        // Add row click handlers
        tableContainer.querySelectorAll('.rowgroup-row').forEach(row => {
            row.addEventListener('click', () => {
                const id = parseInt(row.dataset.id);
                this.selectRowGroup(id);
            });
        });
    }

    /**
     * Render distribution charts
     * @param {Array} rowGroups - Row group data
     */
    renderCharts(rowGroups) {
        this.renderSizeChart(rowGroups);
        this.renderRowsChart(rowGroups);
    }

    /**
     * Render size distribution chart
     * @param {Array} rowGroups - Row group data
     */
    renderSizeChart(rowGroups) {
        const container = document.getElementById('size-chart');
        if (!container) return;

        // Simple bar chart implementation
        const maxSize = Math.max(...rowGroups.map(rg => rg.size));

        let chartHTML = '<div class="simple-bar-chart">';
        rowGroups.forEach(rg => {
            const percentage = (rg.size / maxSize) * 100;
            chartHTML += `
                <div class="bar-item" title="Row Group ${rg.id}: ${formatBytes(rg.size)}">
                    <div class="bar-label">RG ${rg.id}</div>
                    <div class="bar-container">
                        <div class="bar" style="width: ${percentage}%"></div>
                    </div>
                    <div class="bar-value">${formatBytes(rg.size, 1)}</div>
                </div>
            `;
        });
        chartHTML += '</div>';

        container.innerHTML = chartHTML;
    }

    /**
     * Render row count distribution chart
     * @param {Array} rowGroups - Row group data
     */
    renderRowsChart(rowGroups) {
        const container = document.getElementById('rows-chart');
        if (!container) return;

        const maxRows = Math.max(...rowGroups.map(rg => rg.rows));

        let chartHTML = '<div class="simple-bar-chart">';
        rowGroups.forEach(rg => {
            const percentage = (rg.rows / maxRows) * 100;
            chartHTML += `
                <div class="bar-item" title="Row Group ${rg.id}: ${formatNumber(rg.rows)} rows">
                    <div class="bar-label">RG ${rg.id}</div>
                    <div class="bar-container">
                        <div class="bar rows-bar" style="width: ${percentage}%"></div>
                    </div>
                    <div class="bar-value">${formatNumber(rg.rows)}</div>
                </div>
            `;
        });
        chartHTML += '</div>';

        container.innerHTML = chartHTML;
    }

    /**
     * Select and highlight a row group
     * @param {number} id - Row group ID
     */
    selectRowGroup(id) {
        // Remove previous selections
        this.container.querySelectorAll('.rowgroup-row.selected').forEach(row => {
            row.classList.remove('selected');
        });

        // Add selection to new row
        const row = this.container.querySelector(`[data-id="${id}"]`);
        if (row) {
            row.classList.add('selected');
        }

        // Emit custom event
        this.container.dispatchEvent(new CustomEvent('rowGroupSelected', {
            detail: { id, data: this.data.find(rg => rg.id === id) }
        }));
    }

    /**
     * Update with new data
     * @param {Array} rowGroups - New row group data
     */
    update(rowGroups) {
        this.render(rowGroups);
    }
}

// Add styles for row group visualization
const rowGroupStyles = `
.rowgroup-viz-container {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 2rem;
}

.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}

.data-table th,
.data-table td {
    padding: 0.5rem;
    text-align: left;
    border-bottom: 1px solid #e0e0e0;
}

.data-table th {
    background: #f5f5f5;
    font-weight: 500;
    color: #333;
}

.rowgroup-row {
    cursor: pointer;
    transition: background-color 0.2s ease;
}

.rowgroup-row:hover {
    background-color: #f8f9fa;
}

.rowgroup-row.selected {
    background-color: #e3f2fd;
}

.chart-grid {
    display: flex;
    flex-direction: column;
    gap: 1.5rem;
}

.chart-item h5 {
    margin-bottom: 0.5rem;
    color: #333;
}

.simple-bar-chart {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.bar-item {
    display: grid;
    grid-template-columns: 40px 1fr 60px;
    gap: 0.5rem;
    align-items: center;
    font-size: 0.8rem;
}

.bar-label {
    font-weight: 500;
    color: #666;
}

.bar-container {
    background: #f0f0f0;
    border-radius: 2px;
    height: 16px;
    position: relative;
}

.bar {
    background: #667eea;
    height: 100%;
    border-radius: 2px;
    transition: width 0.3s ease;
}

.rows-bar {
    background: #4caf50;
}

.bar-value {
    font-weight: 500;
    color: #333;
    text-align: right;
}

@media (max-width: 768px) {
    .rowgroup-viz-container {
        grid-template-columns: 1fr;
    }

    .chart-grid {
        gap: 1rem;
    }
}

@media (prefers-color-scheme: dark) {
    .data-table th {
        background: #3a3a3a;
        color: #e0e0e0;
    }

    .data-table th,
    .data-table td {
        border-bottom-color: #555;
    }

    .rowgroup-row:hover {
        background-color: #2a2a2a;
    }

    .rowgroup-row.selected {
        background-color: #1a365d;
    }

    .chart-item h5 {
        color: #e0e0e0;
    }

    .bar-label,
    .bar-value {
        color: #ccc;
    }

    .bar-container {
        background: #3a3a3a;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#rowgroup-viz-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'rowgroup-viz-styles';
    styleElement.textContent = rowGroupStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = RowGroupVisualization;
}
