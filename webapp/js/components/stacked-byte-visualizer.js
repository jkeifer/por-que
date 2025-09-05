/**
 * Stacked Byte Range Visualizer
 * Creates macOS-style stacked visualization for file structure exploration
 */
class StackedByteRangeVisualizer {
    // Constants for minimum width calculations
    static MAX_MIN_WIDTH_PX = 25; // Maximum minimum width in pixels
    static MIN_MIN_WIDTH_PX = 3;  // Absolute minimum width in pixels
    static LOG_SCALE_FACTOR = 2;  // Logarithmic scaling factor
    constructor(container) {
        this.container = container;
        this.analyzer = null;
        this.levels = []; // Track active levels
        this.selections = {}; // Track selections per level
        this.tooltip = null;
        this.infoPanel = null;

        this.init();
    }

    init() {
        // Clear container
        this.container.innerHTML = '';

        // Create main structure
        this.container.className = 'file-structure-explorer';

        // Create levels container
        this.levelsContainer = document.createElement('div');
        this.levelsContainer.className = 'levels-container';
        this.container.appendChild(this.levelsContainer);

        // Create info panel
        this.createInfoPanel();

        // Create tooltip
        this.createTooltip();
    }

    /**
     * Initialize with parquet data
     */
    initWithData(data) {
        try {
            this.data = data; // Store the original data
            this.analyzer = new FileStructureAnalyzer(data);

            // Start with overview level
            this.addLevel('overview', null, this.analyzer.getSegmentsForLevel('overview'));

            // Show file overview in info panel by default
            this.updateInfoPanel(null);

        } catch (error) {
            console.error('Error initializing file structure analyzer:', error);
            this.showError('Unable to analyze file structure');
        }
    }

    /**
     * Add a new level to the visualization
     */
    addLevel(levelName, parentId, segments) {
        const level = {
            name: levelName,
            parentId: parentId,
            segments: segments,
            element: this.createLevelElement(segments, levelName)
        };

        this.levels.push(level);
        this.levelsContainer.appendChild(level.element);

        return level;
    }

    /**
     * Remove levels from a given index
     */
    removeLevelsFrom(index) {
        while (this.levels.length > index) {
            const level = this.levels.pop();
            level.element.remove();

            // Clear selections for removed levels
            delete this.selections[level.name];
        }
    }

    /**
     * Get container width for pixel calculations
     */
    getContainerWidth() {
        // Try to get actual container width, fallback to reasonable default
        if (this.container && this.container.offsetWidth > 0) {
            return Math.max(800, this.container.offsetWidth - 40); // Account for padding
        }
        return 1000; // Default assumption
    }

    /**
     * Create a level visualization element
     */
    createLevelElement(segments, levelName) {
        const levelDiv = document.createElement('div');
        levelDiv.className = 'byte-range-level';
        levelDiv.dataset.level = levelName;

        const barDiv = document.createElement('div');
        barDiv.className = 'byte-range-bar';

        // Calculate total range for this level
        const minStart = Math.min(...segments.map(s => s.start));
        const maxEnd = Math.max(...segments.map(s => s.end));
        const totalSize = maxEnd - minStart;

        // Get container width for pixel calculations
        const containerWidth = this.getContainerWidth();

        // Calculate proportional widths with minimum width allocation
        const segmentWidths = this.calculateProportionalWidths(segments, totalSize, containerWidth);

        // Create segments with calculated widths
        let currentLeft = 0;
        segments.forEach((segment, index) => {
            const segmentDiv = this.createSegmentElement(segment, currentLeft, segmentWidths[index], levelName);
            barDiv.appendChild(segmentDiv);
            currentLeft += segmentWidths[index].finalWidthPercent;
        });

        // Add tick marks
        // Tick marks removed for cleaner visualization

        levelDiv.appendChild(barDiv);
        return levelDiv;
    }

    /**
     * Calculate proportional widths for all segments with minimum width allocation
     */
    calculateProportionalWidths(segments, totalSize, containerWidth) {
        // Calculate minimum widths - now gives all segments a consistent baseline minimum width
        const minWidths = this.calculateLogarithmicMinWidths(segments, totalSize, containerWidth);

        // Calculate natural proportional widths
        const segmentData = segments.map(segment => ({
            segment: segment,
            naturalWidthPercent: (segment.size / totalSize) * 100,
            naturalWidthPixels: ((segment.size / totalSize) * 100 / 100) * containerWidth,
            finalWidthPercent: 0,
            finalWidthPixels: 0,
            isExpanded: false
        }));

        // Identify segments that need expansion
        let totalNaturalWidthUsed = 0;
        let expandedCount = 0;

        segmentData.forEach((data, index) => {
            totalNaturalWidthUsed += data.naturalWidthPercent;
            const segmentMinWidth = minWidths[index];
            const segmentMinWidthPercent = (segmentMinWidth / containerWidth) * 100;

            if (data.naturalWidthPixels < segmentMinWidth) {
                data.isExpanded = true;
                data.finalWidthPercent = segmentMinWidthPercent;
                data.finalWidthPixels = segmentMinWidth;
                expandedCount++;
            } else {
                data.finalWidthPercent = data.naturalWidthPercent;
                data.finalWidthPixels = data.naturalWidthPixels;
            }
        });

        // Calculate space taken by expanded segments beyond their natural size
        const expandedSegments = segmentData.filter(d => d.isExpanded);
        const extraSpaceUsed = expandedSegments.reduce((sum, data) => {
            return sum + (data.finalWidthPercent - data.naturalWidthPercent);
        }, 0);

        // If we used extra space, proportionally reduce non-expanded segments
        if (extraSpaceUsed > 0) {
            const availableSpace = 100 - expandedSegments.reduce((sum, d) => sum + d.finalWidthPercent, 0);
            const naturalSpaceForNonExpanded = segmentData
                .filter(d => !d.isExpanded)
                .reduce((sum, d) => sum + d.naturalWidthPercent, 0);

            if (naturalSpaceForNonExpanded > 0) {
                const scaleFactor = availableSpace / naturalSpaceForNonExpanded;

                segmentData.forEach(data => {
                    if (!data.isExpanded) {
                        data.finalWidthPercent = data.naturalWidthPercent * scaleFactor;
                        data.finalWidthPixels = (data.finalWidthPercent / 100) * containerWidth;
                    }
                });
            }
        }


        return segmentData;
    }

    /**
     * Calculate logarithmic minimum widths for segments based on relative sizes
     */
    calculateLogarithmicMinWidths(segments, totalSize, containerWidth) {
        // Start with a reasonable baseline minimum width (5px)
        const startingBaseline = 5;

        // Adjust baseline downward if there's not enough space for all segments
        const adjustedBaseline = Math.min(startingBaseline, containerWidth / segments.length);

        // If adjusted baseline is too small, fall back to uniform distribution
        if (adjustedBaseline < StackedByteRangeVisualizer.MIN_MIN_WIDTH_PX) {
            return segments.map(() => containerWidth / segments.length);
        }

        // Get segment sizes and calculate logarithmic scaling
        const sizes = segments.map(s => Math.max(1, s.size)); // Avoid log(0)
        const minSize = Math.min(...sizes);
        const maxSize = Math.max(...sizes);

        // If all segments are the same size, use adjusted baseline for all
        if (minSize === maxSize) {
            return segments.map(() => adjustedBaseline);
        }

        // Calculate logarithmic widths independently for each segment
        const logMinSize = Math.log10(minSize);
        const logMaxSize = Math.log10(maxSize);
        const logRange = logMaxSize - logMinSize;

        const logarithmicWidths = sizes.map(size => {
            // Calculate position on logarithmic scale (0 to 1)
            const logSize = Math.log10(size);
            const logPosition = (logSize - logMinSize) / logRange;

            // Apply logarithmic scaling with configurable factor
            const scaledPosition = Math.pow(logPosition, 1 / StackedByteRangeVisualizer.LOG_SCALE_FACTOR);

            // Map to width range from adjusted baseline to MAX_MIN_WIDTH_PX
            const minWidth = adjustedBaseline +
                (StackedByteRangeVisualizer.MAX_MIN_WIDTH_PX - adjustedBaseline) * scaledPosition;

            return minWidth;
        });

        // Calculate total width of all logarithmic segments
        const totalLogarithmicWidth = logarithmicWidths.reduce((sum, width) => sum + width, 0);

        // If total exceeds container width, scale everything down proportionally
        if (totalLogarithmicWidth > containerWidth) {
            const scalingFactor = containerWidth / totalLogarithmicWidth;
            return logarithmicWidths.map(width => width * scalingFactor);
        }

        // Otherwise, return the logarithmic widths as calculated
        return logarithmicWidths;
    }

    /**
     * Create a segment element with calculated width
     */
    createSegmentElement(segment, leftPercent, segmentData, levelName) {
        const segmentDiv = document.createElement('div');
        segmentDiv.className = 'byte-segment';
        segmentDiv.dataset.segmentId = segment.id;
        segmentDiv.dataset.level = levelName;

        // Use calculated position and width
        segmentDiv.style.left = leftPercent + '%';
        segmentDiv.style.width = segmentData.finalWidthPercent + '%';

        // Add styling class based on segment type
        const colorClass = this.getColorClass(segment, levelName);
        segmentDiv.classList.add(colorClass);

        // Add label directly on segment if wide enough
        if (segmentData.finalWidthPercent > 3) { // Show label if wider than 3%
            const labelDiv = document.createElement('div');
            labelDiv.className = 'segment-label';
            labelDiv.textContent = segment.name;
            segmentDiv.appendChild(labelDiv);
        }

        // Add event listeners
        this.addSegmentEventListeners(segmentDiv, segment, levelName);

        return segmentDiv;
    }



    /**
     * Get color class for segment
     */
    getColorClass(segment, levelName) {
        const type = segment.type;

        if (type === 'header' || type === 'footer') {
            return 'segment-header';
        } else if (type === 'metadata') {
            return 'segment-metadata';
        } else if (type === 'rowgroups' || type === 'rowgroup') {
            const index = segment.rowGroupIndex || 0;
            return `segment-rowgroup-${index % 4}`;
        } else if (type === 'column') {
            const index = segment.chunkIndex || 0;
            return `segment-column-${index % 4}`;
        } else if (type === 'data') {
            return 'segment-data';
        } else if (type === 'dictionary') {
            return 'segment-dictionary';
        } else if (type === 'index') {
            return 'segment-index';
        }

        return 'segment-header'; // fallback
    }

    /**
     * Add event listeners to segment
     */
    addSegmentEventListeners(segmentDiv, segment, levelName) {
        // Click handler
        segmentDiv.addEventListener('click', (e) => {
            e.stopPropagation();
            this.handleSegmentClick(segment, levelName);
        });

        // Hover handlers
        segmentDiv.addEventListener('mouseenter', (e) => {
            this.showTooltip(e, segment);
            segmentDiv.style.opacity = '0.8';
        });

        segmentDiv.addEventListener('mouseleave', (e) => {
            this.hideTooltip();
            segmentDiv.style.opacity = '';
        });
    }

    /**
     * Handle segment click
     */
    handleSegmentClick(segment, levelName) {
        const currentSelection = this.selections[levelName];

        if (currentSelection === segment.id) {
            // Clicking selected segment - collapse children
            this.deselectSegment(levelName);
        } else {
            // Select new segment
            this.selectSegment(segment, levelName);
        }
    }

    /**
     * Select a segment
     */
    selectSegment(segment, levelName) {
        // Clear previous selection at this level
        this.clearSelection(levelName);

        // Set new selection
        this.selections[levelName] = segment.id;

        // Update visual selection
        this.updateSelectionVisuals(levelName, segment.id);

        // Remove any child levels
        const levelIndex = this.levels.findIndex(l => l.name === levelName);
        this.removeLevelsFrom(levelIndex + 1);

        // Add child level if segment has children
        if (segment.hasChildren && segment.children && segment.children.length > 0) {
            const childLevelName = this.getChildLevelName(levelName);
            this.addLevel(childLevelName, segment.id, segment.children);
        }

        // Update info panel
        this.updateInfoPanel(segment);
    }

    /**
     * Deselect segment and remove children
     */
    deselectSegment(levelName) {
        this.clearSelection(levelName);

        // Remove child levels
        const levelIndex = this.levels.findIndex(l => l.name === levelName);
        this.removeLevelsFrom(levelIndex + 1);

        // Clear info panel
        this.updateInfoPanel(null);
    }

    /**
     * Clear selection visuals for a level
     */
    clearSelection(levelName) {
        delete this.selections[levelName];

        const level = this.levels.find(l => l.name === levelName);
        if (level) {
            const segments = level.element.querySelectorAll('.byte-segment');
            segments.forEach(seg => seg.classList.remove('selected'));
        }
    }

    /**
     * Update selection visuals
     */
    updateSelectionVisuals(levelName, segmentId) {
        const level = this.levels.find(l => l.name === levelName);
        if (!level) return;

        const segments = level.element.querySelectorAll('.byte-segment');
        segments.forEach(seg => {
            if (seg.dataset.segmentId === segmentId) {
                seg.classList.add('selected');
            } else {
                seg.classList.remove('selected');
            }
        });
    }

    /**
     * Get child level name
     */
    getChildLevelName(parentLevelName) {
        const levelHierarchy = {
            'overview': 'rowgroups',
            'rowgroups': 'columnchunks',
            'columnchunks': 'pages'
        };

        return levelHierarchy[parentLevelName] || null;
    }


    /**
     * Create tooltip
     */
    createTooltip() {
        this.tooltip = document.createElement('div');
        this.tooltip.className = 'byte-tooltip';
        this.tooltip.style.cssText = `
            position: absolute;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 12px;
            pointer-events: none;
            z-index: 1000;
            visibility: hidden;
        `;
        document.body.appendChild(this.tooltip);
    }

    /**
     * Show tooltip
     */
    showTooltip(event, segment) {
        let content = `
            <strong>${segment.name}</strong><br/>
            Range: ${formatBytes(segment.start)} - ${formatBytes(segment.end)}<br/>
            Size: ${formatBytes(segment.size)}<br/>
        `;

        // Add type-specific tooltip info
        if (segment.type === 'column' && segment.logicalMetadata?.metadata) {
            const meta = segment.logicalMetadata.metadata;
            if (meta.type !== undefined) {
                const typeNames = {
                    0: 'BOOLEAN', 1: 'INT32', 2: 'INT64', 3: 'INT96',
                    4: 'FLOAT', 5: 'DOUBLE', 6: 'BYTE_ARRAY', 7: 'FIXED_LEN_BYTE_ARRAY'
                };
                content += `Type: ${typeNames[meta.type] || `TYPE_${meta.type}`}<br/>`;
            }
            if (meta.total_compressed_size && meta.total_uncompressed_size) {
                const ratio = (meta.total_compressed_size / meta.total_uncompressed_size * 100).toFixed(1);
                content += `Compression: ${ratio}%<br/>`;
            }
        } else {
            content += `${segment.description}<br/>`;
        }

        content += '<em>Click for full details</em>';

        this.tooltip.innerHTML = content;
        this.tooltip.style.left = (event.pageX + 10) + 'px';
        this.tooltip.style.top = (event.pageY - 10) + 'px';
        this.tooltip.style.visibility = 'visible';
    }

    /**
     * Hide tooltip
     */
    hideTooltip() {
        this.tooltip.style.visibility = 'hidden';
    }

    /**
     * Create info panel
     */
    createInfoPanel() {
        this.infoPanel = document.createElement('div');
        this.infoPanel.className = 'info-panel';
        this.infoPanel.style.display = 'none';
        this.container.appendChild(this.infoPanel);
    }

    /**
     * Update info panel
     */
    updateInfoPanel(segment) {
        this.infoPanel.style.display = 'block';

        let html;

        if (!segment) {
            // Show file overview when no segment is selected
            html = this.generateOverviewInfoPanel();
        } else {
            // Generate organized content based on segment type
            html = `<h4>${segment.description}</h4>`;

        if (segment.type === 'column' && segment.logicalMetadata?.metadata) {
            html += this.generateColumnInfoPanel(segment);
        } else if (segment.type === 'rowgroup' && segment.metadata) {
            html += this.generateRowGroupInfoPanel(segment);
        } else if (segment.type === 'data' && segment.metadata) {
            html += this.generatePageInfoPanel(segment, 'Data Page');
        } else if (segment.type === 'dictionary' && segment.metadata) {
            html += this.generatePageInfoPanel(segment, 'Dictionary Page');
        } else if (segment.type === 'index' && segment.metadata) {
            html += this.generatePageInfoPanel(segment, 'Index Page');
        } else if (segment.type === 'metadata') {
            html += this.generateMetadataInfoPanel(segment);
            } else {
                html += this.generateBasicInfoPanel(segment);
            }
        }

        this.infoPanel.innerHTML = html;
    }

    /**
     * Generate overview info panel when no segment is selected
     */
    generateOverviewInfoPanel() {
        let html = '<h4>File Overview</h4><div class="info-sections">';

        const data = this.data;
        const metadata = this.data?.metadata?.metadata;

        if (!data || !metadata) {
            html += this.generateInfoSection('Information', [
                ['Status', 'No file data available']
            ]);
            html += '</div>';
            return html;
        }

        // File Information
        html += this.generateInfoSection('File Information', [
            ['Source', data.source || 'Unknown'],
            ['Total Size', formatBytes(data.filesize || 0)],
            ['Parquet Version', metadata.version || 'Unknown'],
            ['Created By', metadata.created_by || 'Unknown']
        ]);

        // Schema Summary
        const schema = metadata.schema;
        if (schema) {
            html += this.generateInfoSection('Schema Summary', [
                ['Total Columns', metadata.column_count ? metadata.column_count.toLocaleString() : 'N/A'],
                ['Total Rows', metadata.row_count ? metadata.row_count.toLocaleString() : 'N/A'],
                ['Row Groups', metadata.row_group_count ? metadata.row_group_count.toLocaleString() : 'N/A']
            ]);
        }

        // Compression Statistics
        if (metadata.compression_stats) {
            const compressionStats = metadata.compression_stats;
            html += this.generateInfoSection('Compression', [
                ['Compressed Size', formatBytes(compressionStats.total_compressed || 0)],
                ['Uncompressed Size', formatBytes(compressionStats.total_uncompressed || 0)],
                ['Compression Ratio', compressionStats.ratio ?
                    (compressionStats.ratio * 100).toFixed(1) + '%' : 'N/A'],
                ['Space Saved', compressionStats.space_saved_percent ?
                    compressionStats.space_saved_percent.toFixed(1) + '%' : 'N/A']
            ]);
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate organized info panel for column chunks
     */
    generateColumnInfoPanel(segment) {
        const logicalMeta = segment.logicalMetadata.metadata;
        const physicalMeta = segment.physicalMetadata;

        let html = '<div class="info-sections">';

        // Basic Information
        html += this.generateInfoSection('Basic Information', [
            ['Column Path', segment.columnPath],
            ['Start Offset', formatBytes(segment.start)],
            ['End Offset', formatBytes(segment.end)],
            ['Total Size', formatBytes(segment.size)]
        ]);

        // Data Types Section
        const typeInfo = [];

        // Physical Type
        if (logicalMeta.type !== undefined) {
            const typeNames = {
                0: 'BOOLEAN', 1: 'INT32', 2: 'INT64', 3: 'INT96',
                4: 'FLOAT', 5: 'DOUBLE', 6: 'BYTE_ARRAY', 7: 'FIXED_LEN_BYTE_ARRAY'
            };
            typeInfo.push(['Physical Type', typeNames[logicalMeta.type] || `TYPE_${logicalMeta.type}`]);
        } else {
            typeInfo.push(['Physical Type', 'Unknown']);
        }

        // Logical Type - always show
        const logicalTypeInfo = this.getLogicalTypeInfo(segment);
        typeInfo.push(['Logical Type', logicalTypeInfo || 'None']);

        // Converted Type - always show
        const convertedTypeInfo = this.getConvertedTypeInfo(segment);
        typeInfo.push(['Converted Type', convertedTypeInfo || 'None']);

        // Type parameters
        if (logicalMeta.type_length) typeInfo.push(['Type Length', logicalMeta.type_length]);
        if (logicalMeta.precision) typeInfo.push(['Precision', logicalMeta.precision]);
        if (logicalMeta.scale) typeInfo.push(['Scale', logicalMeta.scale]);

        html += this.generateInfoSection('Data Types', typeInfo);

        // Compression & Encoding
        const compressionInfo = [];
        if (physicalMeta.codec !== undefined) {
            const compressionNames = {
                0: 'UNCOMPRESSED', 1: 'SNAPPY', 2: 'GZIP', 3: 'LZO',
                4: 'BROTLI', 5: 'LZ4', 6: 'ZSTD'
            };
            compressionInfo.push(['Algorithm', compressionNames[physicalMeta.codec] || `CODEC_${physicalMeta.codec}`]);
        }

        if (logicalMeta.total_compressed_size && logicalMeta.total_uncompressed_size) {
            compressionInfo.push(['Compressed Size', formatBytes(logicalMeta.total_compressed_size)]);
            compressionInfo.push(['Uncompressed Size', formatBytes(logicalMeta.total_uncompressed_size)]);
            const ratio = (logicalMeta.total_compressed_size / logicalMeta.total_uncompressed_size * 100).toFixed(1);
            compressionInfo.push(['Compression Ratio', `${ratio}%`]);
            const savedBytes = logicalMeta.total_uncompressed_size - logicalMeta.total_compressed_size;
            compressionInfo.push(['Space Saved', formatBytes(savedBytes)]);
        }

        if (logicalMeta.encodings && logicalMeta.encodings.length > 0) {
            const encodingNames = {
                0: 'PLAIN', 2: 'DICTIONARY', 3: 'RLE', 4: 'BIT_PACKED',
                5: 'DELTA_BINARY_PACKED', 6: 'DELTA_LENGTH_BYTE_ARRAY',
                7: 'DELTA_BYTE_ARRAY', 8: 'RLE_DICTIONARY', 9: 'BYTE_STREAM_SPLIT'
            };
            const encodings = logicalMeta.encodings.map(enc =>
                encodingNames[enc] || `ENC_${enc}`
            ).join(', ');
            compressionInfo.push(['Encodings', encodings]);
        }

        if (compressionInfo.length > 0) {
            html += this.generateInfoSection('Compression & Encoding', compressionInfo);
        }

        // Page Layout
        const pageInfo = [];
        if (logicalMeta.data_page_offset) pageInfo.push(['Data Page Offset', formatBytes(logicalMeta.data_page_offset)]);
        if (logicalMeta.dictionary_page_offset) pageInfo.push(['Dictionary Page Offset', formatBytes(logicalMeta.dictionary_page_offset)]);
        if (logicalMeta.index_page_offset) pageInfo.push(['Index Page Offset', formatBytes(logicalMeta.index_page_offset)]);
        if (physicalMeta.num_values) pageInfo.push(['Total Values', formatNumber(physicalMeta.num_values)]);

        if (pageInfo.length > 0) {
            html += this.generateInfoSection('Page Layout', pageInfo);
        }

        // Statistics
        if (logicalMeta.statistics) {
            const stats = logicalMeta.statistics;
            const statsInfo = [];
            if (stats.min_value !== undefined) statsInfo.push(['Min Value', this.formatStatValue(stats.min_value)]);
            if (stats.max_value !== undefined) statsInfo.push(['Max Value', this.formatStatValue(stats.max_value)]);
            if (stats.null_count !== undefined) {
                const nullPercent = physicalMeta.num_values > 0 ?
                    ((stats.null_count / physicalMeta.num_values) * 100).toFixed(1) : '0';
                statsInfo.push(['Null Count', `${formatNumber(stats.null_count)} (${nullPercent}%)`]);
            }
            if (stats.distinct_count !== undefined) {
                const distinctPercent = physicalMeta.num_values > 0 ?
                    ((stats.distinct_count / physicalMeta.num_values) * 100).toFixed(1) : '0';
                statsInfo.push(['Distinct Count', `${formatNumber(stats.distinct_count)} (${distinctPercent}%)`]);
            }

            if (statsInfo.length > 0) {
                html += this.generateInfoSection('Column Statistics', statsInfo);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for row groups
     */
    generateRowGroupInfoPanel(segment) {
        const metadata = segment.metadata;
        let html = '<div class="info-sections">';

        // Basic Information
        html += this.generateInfoSection('Basic Information', [
            ['Row Group Index', segment.rowGroupIndex],
            ['Start Offset', formatBytes(segment.start)],
            ['End Offset', formatBytes(segment.end)],
            ['Total Size', formatBytes(segment.size)]
        ]);

        // Data Information
        const dataInfo = [
            ['Row Count', formatNumber(metadata.row_count)],
            ['Total Byte Size', formatBytes(metadata.total_byte_size)]
        ];

        if (metadata.compression_stats) {
            dataInfo.push(['Compressed Size', formatBytes(metadata.compression_stats.total_compressed)]);
            dataInfo.push(['Uncompressed Size', formatBytes(metadata.compression_stats.total_uncompressed)]);
            const ratio = (metadata.compression_stats.total_compressed / metadata.compression_stats.total_uncompressed * 100).toFixed(1);
            dataInfo.push(['Compression Ratio', `${ratio}%`]);
        }

        // Count columns
        const columnCount = Object.keys(metadata.column_chunks || {}).length;
        dataInfo.push(['Column Count', columnCount]);

        html += this.generateInfoSection('Data Information', dataInfo);

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for pages
     */
    generatePageInfoPanel(segment, pageType) {
        const metadata = segment.metadata;
        let html = '<div class="info-sections">';

        // Basic Information
        html += this.generateInfoSection('Basic Information', [
            ['Page Type', pageType],
            ['Start Offset', formatBytes(segment.start)],
            ['End Offset', formatBytes(segment.end)],
            ['Compressed Size', formatBytes(segment.size)]
        ]);

        // Page Details
        const pageInfo = [];
        if (metadata.uncompressed_page_size) pageInfo.push(['Uncompressed Size', formatBytes(metadata.uncompressed_page_size)]);
        if (metadata.num_values) pageInfo.push(['Values', formatNumber(metadata.num_values)]);
        if (metadata.num_rows) pageInfo.push(['Rows', formatNumber(metadata.num_rows)]);
        if (metadata.num_nulls) pageInfo.push(['Null Values', formatNumber(metadata.num_nulls)]);

        if (metadata.encoding !== undefined) {
            const encodingNames = {
                0: 'PLAIN', 2: 'DICTIONARY', 3: 'RLE', 4: 'BIT_PACKED',
                5: 'DELTA_BINARY_PACKED', 6: 'DELTA_LENGTH_BYTE_ARRAY',
                7: 'DELTA_BYTE_ARRAY', 8: 'RLE_DICTIONARY', 9: 'BYTE_STREAM_SPLIT'
            };
            pageInfo.push(['Encoding', encodingNames[metadata.encoding] || `ENC_${metadata.encoding}`]);
        }

        if (segment.type === 'dictionary' && metadata.is_sorted !== undefined) {
            pageInfo.push(['Is Sorted', metadata.is_sorted ? 'Yes' : 'No']);
        }

        if (pageInfo.length > 0) {
            html += this.generateInfoSection('Page Details', pageInfo);
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate organized info panel for metadata segments
     */
    generateMetadataInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Basic Information
        html += this.generateInfoSection('Basic Information', [
            ['Start Offset', formatBytes(segment.start)],
            ['End Offset', formatBytes(segment.end)],
            ['Size', formatBytes(segment.size)]
        ]);

        // Check if we have metadata available through the analyzer
        const metadata = this.analyzer?.metadata;
        if (!metadata) {
            html += this.generateInfoSection('Debug Info', [
                ['Analyzer Available', this.analyzer ? 'Yes' : 'No'],
                ['Analyzer Data', this.analyzer?.data ? 'Yes' : 'No'],
                ['Metadata Available', this.analyzer?.metadata ? 'Yes' : 'No']
            ]);
            html += '</div>';
            return html;
        }

        // File Information
        const fileInfo = [
            ['Version', metadata.version || 'Unknown'],
            ['Created By', metadata.created_by || 'Unknown'],
            ['Columns', metadata.column_count || 'N/A'],
            ['Rows', metadata.row_count ? metadata.row_count.toLocaleString() : 'N/A'],
            ['Row Groups', metadata.row_group_count || 'N/A']
        ];
        html += this.generateInfoSection('File Metadata', fileInfo);

        // Schema Information
        if (metadata.schema) {
            const schemaInfo = [
                ['Root Name', metadata.schema.name],
                ['Element Type', metadata.schema.element_type || 'group'],
                ['Repetition Type', this.getRepetitionType(metadata.schema.repetition_type)],
                ['Children Count', metadata.schema.children ? Object.keys(metadata.schema.children).length : 0]
            ];
            html += this.generateInfoSection('Schema Structure', schemaInfo);
        }

        // Compression Statistics
        if (metadata.compression_stats) {
            const compressionInfo = [
                ['Total Compressed', formatBytes(metadata.compression_stats.total_compressed || 0)],
                ['Total Uncompressed', formatBytes(metadata.compression_stats.total_uncompressed || 0)],
                ['Compression Ratio', metadata.compression_stats.ratio ?
                    (metadata.compression_stats.ratio * 100).toFixed(1) + '%' : 'N/A'],
                ['Space Saved', metadata.compression_stats.space_saved_percent ?
                    metadata.compression_stats.space_saved_percent.toFixed(1) + '%' : 'N/A']
            ];
            html += this.generateInfoSection('Compression Statistics', compressionInfo);
        }

        // Key-Value Metadata
        if (metadata.key_value_metadata && metadata.key_value_metadata.length > 0) {
            const kvPairs = metadata.key_value_metadata.map(kv => [
                kv.key || 'Unknown Key',
                kv.value ? (kv.value.length > 50 ? kv.value.substring(0, 47) + '...' : kv.value) : 'N/A'
            ]);
            html += this.generateInfoSection('Key-Value Metadata', kvPairs);
        }

        html += '</div>';
        return html;
    }

    /**
     * Get repetition type name
     */
    getRepetitionType(repetitionType) {
        const types = {
            0: 'REQUIRED',
            1: 'OPTIONAL',
            2: 'REPEATED'
        };
        return types[repetitionType] || `TYPE_${repetitionType}`;
    }

    /**
     * Generate basic info panel for simple segments
     */
    generateBasicInfoPanel(segment) {
        return this.generateInfoSection('Information', [
            ['Name', segment.name],
            ['Type', segment.type],
            ['Start Offset', formatBytes(segment.start)],
            ['End Offset', formatBytes(segment.end)],
            ['Size', formatBytes(segment.size)]
        ]);
    }

    /**
     * Generate a categorized info section
     */
    generateInfoSection(title, items) {
        return `
            <div class="info-section">
                <h5 class="info-section-title">${title}</h5>
                <div class="info-grid">
                    ${items.map(([label, value]) => `
                        <div class="info-item">
                            <span class="info-label">${label}:</span>
                            <span class="info-value">${value}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    /**
     * Get logical type information from schema
     */
    getLogicalTypeInfo(segment) {
        const logicalMeta = segment.logicalMetadata?.metadata;
        if (!logicalMeta) return null;

        // Check if there's a logical_type field in the metadata
        if (logicalMeta.logical_type !== undefined) {
            const logicalTypeNames = {
                1: 'STRING', 2: 'MAP', 3: 'LIST', 4: 'ENUM', 5: 'DECIMAL',
                6: 'DATE', 7: 'TIME', 8: 'TIMESTAMP', 10: 'INT', 11: 'UNKNOWN',
                12: 'JSON', 13: 'BSON', 14: 'UUID', 15: 'FLOAT16',
                16: 'VARIANT', 17: 'GEOMETRY', 18: 'GEOGRAPHY'
            };
            return logicalTypeNames[logicalMeta.logical_type] || `LOGICAL_${logicalMeta.logical_type}`;
        }

        // If no logical type is explicitly set, return null to show "None"
        return null;
    }

    /**
     * Get converted type information
     */
    getConvertedTypeInfo(segment) {
        const logicalMeta = segment.logicalMetadata?.metadata;
        if (!logicalMeta) return null;

        if (logicalMeta.converted_type === undefined || logicalMeta.converted_type === null) {
            return null;
        }

        const convertedTypeNames = {
            0: 'UTF8', 1: 'MAP', 2: 'MAP_KEY_VALUE', 3: 'LIST', 4: 'ENUM',
            5: 'DECIMAL', 6: 'DATE', 7: 'TIME_MILLIS', 8: 'TIME_MICROS', 9: 'TIMESTAMP_MILLIS',
            10: 'TIMESTAMP_MICROS', 11: 'UINT_8', 12: 'UINT_16', 13: 'UINT_32', 14: 'UINT_64',
            15: 'INT_8', 16: 'INT_16', 17: 'INT_32', 18: 'INT_64', 19: 'JSON', 20: 'BSON', 21: 'INTERVAL'
        };

        return convertedTypeNames[logicalMeta.converted_type] || `CONVERTED_${logicalMeta.converted_type}`;
    }

    /**
     * Format statistical values for display
     */
    formatStatValue(value) {
        if (value === null || value === undefined) return 'null';
        const str = String(value);
        return str.length > 50 ? str.substring(0, 47) + '...' : str;
    }

    /**
     * Show error message
     */
    showError(message) {
        this.container.innerHTML = `<p class="viz-error">${message}</p>`;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = StackedByteRangeVisualizer;
}
