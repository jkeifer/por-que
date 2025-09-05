/**
 * Schema Tree Component
 * Renders an interactive tree view of the Parquet schema
 */
class SchemaTree {
    constructor(container) {
        this.container = container;
        this.expandedNodes = new Set();
        this.searchTerm = '';
    }

    /**
     * Render the schema tree
     * @param {object} schema - Schema object
     */
    render(schema) {
        this.currentSchema = schema;
        this.container.innerHTML = this.renderNode(schema, 0, 'root');
        this.attachEventListeners();
    }

    /**
     * Render a single schema node
     * @param {object} node - Schema node
     * @param {number} level - Nesting level
     * @param {string} path - Full path to this node
     * @returns {string} HTML string
     */
    renderNode(node, level = 0, path = '') {
        const hasChildren = node.children && Object.keys(node.children).length > 0;
        const isExpanded = this.expandedNodes.has(path);
        const indent = level * 20;

        let html = `<div class="schema-node" data-path="${path}" style="margin-left: ${indent}px;">`;

        // Node header
        html += '<div class="schema-node-header">';

        if (hasChildren) {
            const expandIcon = isExpanded ? '▼' : '▶';
            html += `<span class="expand-icon" data-path="${path}">${expandIcon}</span>`;
        } else {
            html += '<span class="expand-icon-placeholder">•</span>';
        }

        html += `<span class="schema-name">${node.name || 'root'}</span>`;

        // Handle different node types based on element_type
        if (node.element_type === 'column') {
            html += `<span class="schema-type">${this.formatParquetType(node.type)}</span>`;

            if (node.logical_type) {
                html += `<span class="logical-type">${this.formatLogicalType(node.logical_type)}</span>`;
            }

            if (node.repetition !== undefined) {
                html += `<span class="repetition">${this.formatRepetition(node.repetition)}</span>`;
            }
        } else {
            html += `<span class="schema-type">${node.element_type || 'group'}</span>`;

            if (node.repetition !== undefined) {
                html += `<span class="repetition">${this.formatRepetition(node.repetition)}</span>`;
            }
        }

        html += '</div>';

        // Children (if expanded)
        if (hasChildren && isExpanded) {
            html += '<div class="schema-children">';
            for (const [childName, child] of Object.entries(node.children)) {
                const childPath = path ? `${path}.${childName}` : childName;
                html += this.renderNode(child, level + 1, childPath);
            }
            html += '</div>';
        }

        html += '</div>';
        return html;
    }

    /**
     * Attach event listeners for interaction
     */
    attachEventListeners() {
        // Expand/collapse functionality
        this.container.querySelectorAll('.expand-icon').forEach(icon => {
            icon.addEventListener('click', (e) => {
                const path = e.target.dataset.path;
                this.toggleNode(path);
            });
        });

        // Node selection (for future functionality)
        this.container.querySelectorAll('.schema-node-header').forEach(header => {
            header.addEventListener('click', (e) => {
                if (!e.target.classList.contains('expand-icon')) {
                    const path = e.currentTarget.closest('.schema-node').dataset.path;
                    this.selectNode(path);
                }
            });
        });
    }

    /**
     * Toggle a node's expanded state
     * @param {string} path - Node path
     */
    toggleNode(path) {
        if (this.expandedNodes.has(path)) {
            this.expandedNodes.delete(path);
        } else {
            this.expandedNodes.add(path);
        }

        // Re-render to update the UI
        // In a more sophisticated implementation, we'd only update the affected node
        this.render(this.currentSchema);
    }

    /**
     * Select a node (highlight it)
     * @param {string} path - Node path
     */
    selectNode(path) {
        // Remove previous selection
        this.container.querySelectorAll('.schema-node-header.selected').forEach(header => {
            header.classList.remove('selected');
        });

        // Add selection to new node
        const node = this.container.querySelector(`[data-path="${path}"] .schema-node-header`);
        if (node) {
            node.classList.add('selected');
        }

        // Emit custom event for other components to listen to
        this.container.dispatchEvent(new CustomEvent('nodeSelected', {
            detail: { path }
        }));
    }

    /**
     * Expand all nodes
     */
    expandAll() {
        const allNodes = this.container.querySelectorAll('.schema-node');
        allNodes.forEach(node => {
            const path = node.dataset.path;
            this.expandedNodes.add(path);
        });
        this.render(this.currentSchema);
    }

    /**
     * Collapse all nodes
     */
    collapseAll() {
        this.expandedNodes.clear();
        this.render(this.currentSchema);
    }

    /**
     * Filter nodes based on search term
     * @param {string} searchTerm - Search term
     */
    filter(searchTerm) {
        this.searchTerm = searchTerm.toLowerCase();

        if (!searchTerm) {
            // Show all nodes
            this.container.querySelectorAll('.schema-node').forEach(node => {
                node.style.display = 'block';
            });
            return;
        }

        // Hide/show nodes based on search
        this.container.querySelectorAll('.schema-node').forEach(node => {
            const nameElement = node.querySelector('.schema-name');
            const name = nameElement ? nameElement.textContent.toLowerCase() : '';
            const path = node.dataset.path.toLowerCase();

            const matches = name.includes(searchTerm) || path.includes(searchTerm);
            node.style.display = matches ? 'block' : 'none';

            // If a child matches, show all parents
            if (matches) {
                let parent = node.parentElement;
                while (parent && parent.classList.contains('schema-children')) {
                    const parentNode = parent.closest('.schema-node');
                    if (parentNode) {
                        parentNode.style.display = 'block';
                        // Auto-expand parent
                        const parentPath = parentNode.dataset.path;
                        this.expandedNodes.add(parentPath);
                    }
                    parent = parentNode ? parentNode.parentElement : null;
                }
            }
        });

        // Re-render to update expand icons
        this.render(this.currentSchema);
    }

    /**
     * Format Parquet physical type for display
     * @param {number} type - Physical type number
     * @returns {string} Formatted type string
     */
    formatParquetType(type) {
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
        return typeMap[type] || `TYPE_${type}`;
    }

    /**
     * Format logical type for display
     * @param {object} logicalType - Logical type object
     * @returns {string} Formatted logical type string
     */
    formatLogicalType(logicalType) {
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
     * Format repetition for display
     * @param {number} repetition - Repetition number
     * @returns {string} Formatted repetition string
     */
    formatRepetition(repetition) {
        const repetitionMap = {
            0: 'required',
            1: 'optional',
            2: 'repeated'
        };
        return repetitionMap[repetition] || `REP_${repetition}`;
    }

    /**
     * Get statistics about the schema
     * @param {object} schema - Schema object
     * @returns {object} Schema statistics
     */
    getStats(schema) {
        let totalFields = 0;
        let maxDepth = 0;
        const typeCount = {};

        function traverse(node, depth = 0) {
            totalFields++;
            maxDepth = Math.max(maxDepth, depth);

            const type = node.type || 'unknown';
            typeCount[type] = (typeCount[type] || 0) + 1;

            if (node.children) {
                for (const child of Object.values(node.children)) {
                    traverse(child, depth + 1);
                }
            }
        }

        traverse(schema);

        return {
            totalFields,
            maxDepth,
            typeCount
        };
    }

    /**
     * Update the schema and re-render
     * @param {object} schema - New schema object
     */
    update(schema) {
        this.currentSchema = schema;
        this.render(schema);
    }
}

// Add CSS styles for schema tree (if not already in main CSS)
const schemaTreeStyles = `
.schema-tree {
    font-family: 'Monaco', 'Consolas', monospace;
    font-size: 14px;
    line-height: 1.4;
}

.schema-node {
    margin: 2px 0;
}

.schema-node-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 8px;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.2s ease;
}

.schema-node-header:hover {
    background-color: #f5f5f5;
}

.schema-node-header.selected {
    background-color: #e3f2fd;
    border: 1px solid #2196f3;
}

.expand-icon {
    cursor: pointer;
    user-select: none;
    color: #666;
    font-size: 12px;
    width: 12px;
    text-align: center;
}

.expand-icon-placeholder {
    width: 12px;
    text-align: center;
    color: #ccc;
    font-size: 8px;
}

.schema-name {
    font-weight: 500;
    color: #333;
    min-width: 100px;
}

.schema-type {
    font-size: 12px;
    font-weight: 500;
    padding: 2px 6px;
    border-radius: 3px;
    background: #e3f2fd;
    color: #1976d2;
}

.logical-type {
    font-size: 11px;
    padding: 1px 4px;
    border-radius: 2px;
    background: #f3e5f5;
    color: #7b1fa2;
}

.repetition {
    font-size: 10px;
    padding: 1px 3px;
    border-radius: 2px;
    text-transform: uppercase;
    font-weight: 500;
}

.schema-children {
    border-left: 1px solid #e0e0e0;
    margin-left: 10px;
}

@media (prefers-color-scheme: dark) {
    .schema-node-header:hover {
        background-color: #2a2a2a;
    }

    .schema-node-header.selected {
        background-color: #1a365d;
        border-color: #3182ce;
    }

    .schema-name {
        color: #e0e0e0;
    }

    .expand-icon {
        color: #999;
    }

    .schema-children {
        border-left-color: #555;
    }
}
`;

// Inject styles if not already present
if (!document.querySelector('#schema-tree-styles')) {
    const styleElement = document.createElement('style');
    styleElement.id = 'schema-tree-styles';
    styleElement.textContent = schemaTreeStyles;
    document.head.appendChild(styleElement);
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SchemaTree;
}
