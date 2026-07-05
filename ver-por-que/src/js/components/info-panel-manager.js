/**
 * Info Panel Manager
 * Manages the info panel content separately from the visualization
 * Refactored to use ParquetTypeResolver for consistent type resolution
 */
class InfoPanelManager {
    constructor(container, typeResolver = ParquetTypeResolver) {
        this.container = container;
        this.infoPanel = null;
        this.data = null;
        this.typeResolver = typeResolver;

        this.init();
    }

    init() {
        // Clear container to remove any previous content
        this.container.innerHTML = '';

        // Create info panel structure
        this.infoPanel = document.createElement('div');
        this.infoPanel.className = 'info-panel';
        this.infoPanel.style.display = 'none';
        this.container.appendChild(this.infoPanel);
    }

    /**
     * Show file overview when no segment is selected
     */
    showOverview(data) {
        this.data = data;
        this.infoPanel.style.display = 'block';

        const html = this.generateOverviewInfoPanel();
        this.infoPanel.innerHTML = html;
    }

    /**
     * Show info for selected segment using metadata-based logic
     */
    showSegment(segment) {
        this.infoPanel.style.display = 'block';

        // Generate organized content based on segment properties and metadata
        let html = `<h3>${segment.description}</h3>`;

        // Column chunk segments (have both physical and logical metadata)
        if (segment.logicalMetadata?.metadata && segment.physicalMetadata) {
            html += this.generateColumnInfoPanel(segment);
        }
        // Row group segments (have rowGroupIndex and row group metadata)
        else if (
            segment.rowGroupIndex !== undefined &&
            segment.chunkIndex === undefined &&
            segment.metadata
        ) {
            html += this.generateRowGroupInfoPanel(segment);
        }
        // Page segments (have pageIndex)
        else if (segment.pageIndex !== undefined && segment.metadata) {
            if (segment.name.includes('DICT')) {
                html += this.generatePageInfoPanel(segment, 'Dictionary Page');
            } else if (segment.name.includes('DATA')) {
                html += this.generatePageInfoPanel(segment, 'Data Page');
            } else if (segment.name.includes('IDX')) {
                html += this.generatePageInfoPanel(segment, 'Index Page');
            } else {
                html += this.generatePageInfoPanel(segment, 'Page');
            }
        }
        // Metadata container segment (id = 'metadata')
        else if (segment.id === 'metadata') {
            html += this.generateMetadataInfoPanel(segment);
        }
        // Schema root (id = 'schema_root')
        else if (segment.id === 'schema_root' && segment.metadata) {
            html += this.generateSchemaInfoPanel(segment);
        }
        // Row group metadata container (id = 'row_groups_metadata')
        else if (segment.id === 'row_groups_metadata' && segment.metadata) {
            html += this.generateRowGroupMetadataInfoPanel(segment);
        }
        // Schema elements (have element_type in metadata)
        else if (segment.metadata?.element_type === 'group') {
            html += this.generateSchemaGroupInfoPanel(segment);
        } else if (segment.metadata?.element_type === 'column') {
            html += this.generateSchemaElementInfoPanel(segment);
        }
        // Index metadata elements (have index_type in metadata)
        else if (segment.metadata?.index_type) {
            html += this.generateMetadataElementInfoPanel(segment);
        }
        // Row group metadata elements (have row_count in metadata and rowGroupIndex)
        else if (segment.metadata?.row_count && segment.rowGroupIndex !== undefined) {
            html += this.generateRowGroupMetadataInfoPanel(segment);
        }
        // Key-value metadata entries (individual key-value pairs)
        else if (segment.metadata?.key && segment.metadata?.value) {
            html += this.generateKeyValueMetadataInfoPanel(segment);
        }
        // Generic metadata elements
        else if (
            segment.metadata &&
            typeof segment.metadata === 'object' &&
            segment.id !== 'rowgroups'
        ) {
            html += this.generateMetadataElementInfoPanel(segment);
        }
        // Basic file structure or unknown segments
        else {
            html += this.generateBasicInfoPanel(segment);
        }

        this.infoPanel.innerHTML = html;
    }

    /**
     * Hide info panel
     */
    hide() {
        this.infoPanel.style.display = 'none';
    }

    /**
     * Generate overview info panel when no segment is selected (exact copy from old visualizer)
     */
    generateOverviewInfoPanel() {
        let html = '<h3>File Overview</h3><div class="info-sections">';

        const data = this.data;
        const metadata = this.data?.metadata;

        if (!data || !metadata) {
            html += this.generateInfoSection('Information', [['Status', 'No file data available']]);
            html += '</div>';
            return html;
        }

        // File Information
        html += this.generateInfoSection('File Information', [
            ['Source', data.source || 'Unknown'],
            ['Total Size', formatBytes(data.filesize || 0)],
            ['Parquet Version', metadata.version || 'Unknown'],
            ['Created By', metadata.created_by || 'Unknown'],
        ]);

        // Schema Summary
        const schema = metadata.schema_root;
        if (schema) {
            html += this.generateInfoSection('Schema Summary', [
                [
                    'Total Columns',
                    metadata.column_count ? metadata.column_count.toLocaleString() : 'N/A',
                ],
                ['Total Rows', metadata.row_count ? metadata.row_count.toLocaleString() : 'N/A'],
                [
                    'Row Groups',
                    metadata.row_group_count ? metadata.row_group_count.toLocaleString() : 'N/A',
                ],
            ]);
        }

        // Compression Statistics
        if (metadata.compression_stats) {
            const compressionStats = metadata.compression_stats;
            html += this.generateInfoSection('Compression Stats', [
                ['Compressed Size', formatBytes(compressionStats.total_compressed || 0)],
                ['Uncompressed Size', formatBytes(compressionStats.total_uncompressed || 0)],
                [
                    'Compression Ratio',
                    compressionStats.ratio
                        ? (compressionStats.ratio * 100).toFixed(1) + '%'
                        : 'N/A',
                ],
                [
                    'Space Saved',
                    compressionStats.space_saved_percent
                        ? compressionStats.space_saved_percent.toFixed(1) + '%'
                        : 'N/A',
                ],
            ]);
        }

        // Data Page Summary
        const pageSummary = this.generatePageSummaryData(data);
        if (pageSummary) {
            const pageSummaryInfo = [
                ['Total Page Count', pageSummary.totalPages.toLocaleString()],
                ['Average Page Size', formatBytes(pageSummary.avgPageSize)],
            ];

            // Add page type distribution as grouped multiline
            if (pageSummary.pageTypes && Object.keys(pageSummary.pageTypes).length > 0) {
                const pageTypeLines = Object.entries(pageSummary.pageTypes)
                    .map(
                        ([type, info]) =>
                            `${type}: ${info.count.toLocaleString()} (${info.percentage}%)`
                    )
                    .join('<br>');
                pageSummaryInfo.push(['Page Types', pageTypeLines]);
            }

            // Add encoding distribution as grouped multiline
            if (pageSummary.encodings && Object.keys(pageSummary.encodings).length > 0) {
                const encodingLines = Object.entries(pageSummary.encodings)
                    .map(
                        ([encoding, info]) =>
                            `${encoding}: ${info.count.toLocaleString()} (${info.percentage}%)`
                    )
                    .join('<br>');
                pageSummaryInfo.push(['Page Encodings', encodingLines]);
            }

            html += this.generateInfoSection('Data Page Summary', pageSummaryInfo);
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate basic info panel for simple segments using metadata-based logic
     */
    generateBasicInfoPanel(segment) {
        // For row groups container segment, add aggregate statistics split into two sections
        if (segment.id === 'rowgroups' && this.data?.metadata?.row_groups) {
            const rowGroups = this.data.metadata.row_groups;
            const totalRows = rowGroups.reduce((sum, rg) => sum + (rg.row_count || 0), 0);
            const avgRowsPerGroup =
                rowGroups.length > 0 ? Math.round(totalRows / rowGroups.length) : 0;

            let html = '<div class="info-sections">';

            // Physical layout information
            html += this.generateInfoSection('Physical Layout', [
                ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
                ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
                ['Total Size', formatBytes(segment.size)],
            ]);

            // Row group statistics
            html += this.generateInfoSection('Row Group Statistics', [
                ['Total Row Groups', rowGroups.length.toLocaleString()],
                ['Total Rows', formatNumber(totalRows)],
                ['Avg Rows per Group', formatNumber(avgRowsPerGroup)],
            ]);

            html += '</div>';
            return html;
        }

        // For other segments, use generic information
        return this.generateInfoSection('Segment Information', [
            ['Name', segment.name],
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);
    }

    /**
     * Generate organized info panel for column chunks (exact copy from old visualizer)
     */
    generateColumnInfoPanel(segment) {
        const logicalMeta = segment.logicalMetadata.metadata;
        const physicalMeta = segment.physicalMetadata;

        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Total Size', formatBytes(segment.size)],
        ]);

        // Data Types Section
        const typeInfo = [];

        // Physical Type
        const physicalType = this.typeResolver.getPhysicalTypeName(logicalMeta.type);
        typeInfo.push(['Physical Type', physicalType]);

        // Logical Type - always show
        const logicalTypeInfo = this.getLogicalTypeInfo(segment);
        typeInfo.push(['Logical Type', logicalTypeInfo || 'None']);

        // Converted Type - always show
        const convertedTypeInfo = this.getConvertedTypeInfo(segment);
        typeInfo.push(['Converted Type', convertedTypeInfo || 'None']);

        // Type parameters
        if (logicalMeta.type_length) {
            typeInfo.push(['Type Length', logicalMeta.type_length]);
        }
        if (logicalMeta.precision) {
            typeInfo.push(['Precision', logicalMeta.precision]);
        }
        if (logicalMeta.scale) {
            typeInfo.push(['Scale', logicalMeta.scale]);
        }

        html += this.generateInfoSection('Data Types', typeInfo);

        // Compression & Encoding
        const compressionInfo = [];
        if (physicalMeta.codec !== undefined) {
            const compressionName = this.typeResolver.getCompressionName(physicalMeta.codec);
            compressionInfo.push(['Algorithm', compressionName]);
        }

        if (logicalMeta.total_compressed_size && logicalMeta.total_uncompressed_size) {
            compressionInfo.push([
                'Compressed Size',
                formatBytes(logicalMeta.total_compressed_size),
            ]);
            compressionInfo.push([
                'Uncompressed Size',
                formatBytes(logicalMeta.total_uncompressed_size),
            ]);
            const ratio = (
                (logicalMeta.total_compressed_size / logicalMeta.total_uncompressed_size) *
                100
            ).toFixed(1);
            compressionInfo.push(['Compression Ratio', `${ratio}%`]);
            const savedBytes =
                logicalMeta.total_uncompressed_size - logicalMeta.total_compressed_size;
            compressionInfo.push(['Space Saved', formatBytes(savedBytes)]);
        }

        if (logicalMeta.encodings && logicalMeta.encodings.length > 0) {
            const encodings = this.typeResolver.getEncodingNames(logicalMeta.encodings);
            compressionInfo.push(['Encodings', encodings]);
        }

        if (compressionInfo.length > 0) {
            html += this.generateInfoSection('Compression & Encoding', compressionInfo);
        }

        // Page Layout
        const pageInfo = [];
        if (logicalMeta.data_page_offset) {
            pageInfo.push([
                'Data Page Offset',
                logicalMeta.data_page_offset.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_'),
            ]);
        }
        if (logicalMeta.dictionary_page_offset) {
            pageInfo.push([
                'Dictionary Page Offset',
                logicalMeta.dictionary_page_offset.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_'),
            ]);
        }
        if (logicalMeta.index_page_offset) {
            pageInfo.push([
                'Index Page Offset',
                logicalMeta.index_page_offset.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_'),
            ]);
        }
        if (physicalMeta.num_values) {
            pageInfo.push(['Total Values', formatNumber(physicalMeta.num_values)]);
        }

        // Calculate page statistics from physical data
        const pageStats = this.calculatePageStatistics(segment);
        if (pageStats.totalPages > 0) {
            pageInfo.push(['Total Pages', formatNumber(pageStats.totalPages)]);
        }
        if (pageStats.avgRowsPerPage > 0) {
            pageInfo.push([
                'Avg Values per Page',
                formatNumber(Math.round(pageStats.avgRowsPerPage)),
            ]);
        }

        if (pageInfo.length > 0) {
            html += this.generateInfoSection('Page Layout', pageInfo);
        }

        // Statistics - always show all fields
        if (logicalMeta.statistics) {
            const stats = logicalMeta.statistics;
            const statsInfo = [];

            // Min/Max values - show N/A if not available
            const minValue =
                stats.min_value !== undefined && stats.min_value !== null
                    ? this.formatStatValue(stats.min_value)
                    : 'N/A';
            const maxValue =
                stats.max_value !== undefined && stats.max_value !== null
                    ? this.formatStatValue(stats.max_value)
                    : 'N/A';

            statsInfo.push(['Min Value', minValue]);
            statsInfo.push(['Max Value', maxValue]);

            // Null count with percentage
            if (
                stats.null_count !== undefined &&
                stats.null_count !== null &&
                physicalMeta.num_values > 0
            ) {
                const nullPercent = ((stats.null_count / physicalMeta.num_values) * 100).toFixed(1);
                statsInfo.push([
                    'Null Count',
                    `${formatNumber(stats.null_count)} (${nullPercent}%)`,
                ]);
            } else {
                statsInfo.push(['Null Count', 'N/A']);
            }

            // Distinct count with percentage
            if (
                stats.distinct_count !== undefined &&
                stats.distinct_count !== null &&
                physicalMeta.num_values > 0
            ) {
                const distinctPercent = (
                    (stats.distinct_count / physicalMeta.num_values) *
                    100
                ).toFixed(1);
                statsInfo.push([
                    'Distinct Count',
                    `${formatNumber(stats.distinct_count)} (${distinctPercent}%)`,
                ]);
            } else {
                statsInfo.push(['Distinct Count', 'N/A']);
            }

            html += this.generateInfoSection('Column Statistics', statsInfo);
        }

        html += '</div>';
        return html;
    }

    /**
     * Format statistic values for display
     */
    formatStatValue(value) {
        if (value === null || value === undefined) {
            return 'N/A';
        }
        if (typeof value === 'string') {
            return value.length > 50 ? value.substring(0, 47) + '...' : value;
        }
        if (typeof value === 'number') {
            return formatNumber(value);
        }
        return String(value);
    }

    /**
     * Get logical type information from schema
     */
    getLogicalTypeInfo(segment) {
        const logicalMeta = segment.logicalMetadata?.metadata;
        if (!logicalMeta) {
            return null;
        }

        return this.typeResolver.getLogicalTypeName(logicalMeta.logical_type);
    }

    /**
     * Get converted type information
     */
    getConvertedTypeInfo(segment) {
        const logicalMeta = segment.logicalMetadata?.metadata;
        if (!logicalMeta) {
            return null;
        }

        return this.typeResolver.getConvertedTypeName(logicalMeta.converted_type);
    }

    /**
     * Generate info panel for row groups (exact copy from old visualizer)
     */
    generateRowGroupInfoPanel(segment) {
        const metadata = segment.metadata;
        let html = '<div class="info-sections">';

        // Physical layout information
        html += this.generateInfoSection('Physical Layout', [
            ['Row Group Index', segment.rowGroupIndex],
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Total Size', formatBytes(segment.size)],
        ]);

        // Data and compression statistics
        const dataInfo = [['Row Count', formatNumber(metadata.row_count)]];

        // Add average row size if we have the data
        if (metadata.compression_stats?.total_uncompressed && metadata.row_count > 0) {
            const avgRowSize = metadata.compression_stats.total_uncompressed / metadata.row_count;
            dataInfo.push(['Avg Row Size', formatBytes(avgRowSize)]);
        }

        // Count columns
        const columnCount = Object.keys(metadata.column_chunks || {}).length;
        dataInfo.push(['Column Count', columnCount]);

        if (metadata.compression_stats) {
            dataInfo.push([
                'Compressed Size',
                formatBytes(metadata.compression_stats.total_compressed),
            ]);
            dataInfo.push([
                'Uncompressed Size',
                formatBytes(metadata.compression_stats.total_uncompressed),
            ]);
            const ratio = (
                (metadata.compression_stats.total_compressed /
                    metadata.compression_stats.total_uncompressed) *
                100
            ).toFixed(1);
            dataInfo.push(['Compression Ratio', `${ratio}%`]);
        }

        html += this.generateInfoSection('Data & Compression', dataInfo);

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for pages (exact copy from old visualizer)
     */
    generatePageInfoPanel(segment, pageType) {
        const metadata = segment.metadata;
        let html = '<div class="info-sections">';

        // Page Overview
        const actualPageType =
            metadata.page_type !== undefined
                ? ParquetConstants.PAGE_TYPES[metadata.page_type] ||
                  `Unknown (${metadata.page_type})`
                : pageType;

        const encodingName =
            metadata.encoding !== undefined
                ? this.typeResolver.getEncodingName(metadata.encoding)
                : 'N/A';

        const overviewInfo = [
            ['Page Type', actualPageType],
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Encoding', encodingName],
        ];

        html += this.generateInfoSection('Page Overview', overviewInfo);

        // Size Information
        const headerSize =
            metadata.header_size !== undefined ? formatBytes(metadata.header_size) : 'N/A';
        const compressedSize =
            metadata.compressed_page_size !== undefined
                ? formatBytes(metadata.compressed_page_size)
                : 'N/A';
        const uncompressedSize =
            metadata.uncompressed_page_size !== undefined
                ? formatBytes(metadata.uncompressed_page_size)
                : 'N/A';

        const sizeInfo = [
            ['Total Page Size', formatBytes(segment.size)],
            ['Header Size', headerSize],
            ['Compressed Data Size', compressedSize],
            ['Uncompressed Size', uncompressedSize],
        ];

        // Calculate compression ratio if we have both sizes
        if (
            metadata.compressed_page_size !== undefined &&
            metadata.uncompressed_page_size !== undefined
        ) {
            const ratio = (
                (metadata.compressed_page_size / metadata.uncompressed_page_size) *
                100
            ).toFixed(1);
            sizeInfo.push(['Compression Ratio', `${ratio}%`]);
        } else {
            sizeInfo.push(['Compression Ratio', 'N/A']);
        }

        html += this.generateInfoSection('Size Information', sizeInfo);

        // Data Content
        const numValues =
            metadata.num_values !== undefined ? formatNumber(metadata.num_values) : 'N/A';
        const numRows = metadata.num_rows !== undefined ? formatNumber(metadata.num_rows) : 'N/A';

        const crcChecksum =
            metadata.crc !== undefined ? metadata.crc.toString(16).toUpperCase() : 'N/A';

        const dataInfo = [
            ['Values', numValues],
            ['Rows', numRows],
            ['CRC Checksum', crcChecksum],
        ];

        // Dictionary-specific fields
        if (segment.type === 'dictionary') {
            const isSorted =
                metadata.is_sorted !== undefined ? (metadata.is_sorted ? 'Yes' : 'No') : 'N/A';
            dataInfo.push(['Is Sorted', isSorted]);
        }

        html += this.generateInfoSection('Data Page Content', dataInfo);

        html += '</div>';
        return html;
    }

    /**
     * Generate organized info panel for metadata segments (exact copy from old visualizer)
     */
    generateMetadataInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        // Check if we have metadata available through the analyzer
        const metadata = this.data?.metadata;
        if (!metadata) {
            html += this.generateInfoSection('Debug Info', [
                ['Analyzer Available', this.data ? 'Yes' : 'No'],
                ['Metadata Available', metadata ? 'Yes' : 'No'],
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
            ['Row Groups', metadata.row_group_count || 'N/A'],
        ];
        html += this.generateInfoSection('File Metadata', fileInfo);

        // Compression Statistics
        if (metadata.compression_stats) {
            const compressionInfo = [
                ['Total Compressed', formatBytes(metadata.compression_stats.total_compressed || 0)],
                [
                    'Total Uncompressed',
                    formatBytes(metadata.compression_stats.total_uncompressed || 0),
                ],
                [
                    'Compression Ratio',
                    metadata.compression_stats.ratio
                        ? (metadata.compression_stats.ratio * 100).toFixed(1) + '%'
                        : 'N/A',
                ],
                [
                    'Space Saved',
                    metadata.compression_stats.space_saved_percent
                        ? metadata.compression_stats.space_saved_percent.toFixed(1) + '%'
                        : 'N/A',
                ],
            ];
            html += this.generateInfoSection('Compression Statistics', compressionInfo);
        }

        // Key-Value Metadata
        if (metadata.key_value_metadata && metadata.key_value_metadata.length > 0) {
            const kvPairs = metadata.key_value_metadata.map(kv => [
                kv.key || 'Unknown Key',
                kv.value
                    ? kv.value.length > 50
                        ? kv.value.substring(0, 47) + '...'
                        : kv.value
                    : 'N/A',
            ]);
            html += this.generateInfoSection('Key-Value Metadata', kvPairs);
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for schema segments
     */
    generateSchemaInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        if (segment.metadata) {
            const schema = segment.metadata;

            // Root Information (for schema root)
            const schemaInfo = [
                ['Name', schema.name || 'Unknown'],
                ['Element Type', schema.element_type || 'group'],
                ['Children Count', schema.num_children || '0'],
            ];

            html += this.generateInfoSection('Root Information', schemaInfo);

            // Count total columns recursively
            const totalColumns = this.countColumns(schema);
            if (totalColumns > 0) {
                html += this.generateInfoSection('Schema Statistics', [
                    ['Total Columns', totalColumns.toLocaleString()],
                ]);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for schema group segments
     */
    generateSchemaGroupInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        if (segment.metadata) {
            const group = segment.metadata;

            // Group Information
            const groupInfo = [
                ['Name', group.name || 'Unknown'],
                ['Element Type', 'group'],
                ['Repetition', this.getRepetitionType(group.repetition)],
                ['Children Count', group.num_children || Object.keys(group.children || {}).length],
            ];

            if (group.field_id !== null && group.field_id !== undefined) {
                groupInfo.push(['Field ID', group.field_id.toString()]);
            }

            html += this.generateInfoSection('Group Information', groupInfo);

            // Logical Type
            if (group.logical_type) {
                const logicalTypeInfo = this.typeResolver.getLogicalTypeInfo(group.logical_type);
                if (logicalTypeInfo) {
                    html += this.generateInfoSection('Logical Type', [
                        ['Type', logicalTypeInfo.name || 'Unknown'],
                        ['Description', logicalTypeInfo.description || 'No description available'],
                    ]);
                }
            }

            // Children summary
            if (group.children) {
                const children = Object.entries(group.children);
                const childrenInfo = children.map(([name, child]) => [
                    name,
                    child.element_type === 'group'
                        ? 'Group'
                        : this.typeResolver.getPhysicalTypeName(child.type),
                ]);
                html += this.generateInfoSection('Child Elements', childrenInfo);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for schema element (column) segments
     */
    generateSchemaElementInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        if (segment.metadata) {
            const element = segment.metadata;

            // Element Information
            const elementInfo = [
                ['Name', element.name || 'Unknown'],
                ['Element Type', 'column'],
                ['Physical Type', this.typeResolver.getPhysicalTypeName(element.type)],
                ['Repetition', this.getRepetitionType(element.repetition)],
            ];

            if (element.field_id !== null && element.field_id !== undefined) {
                elementInfo.push(['Field ID', element.field_id.toString()]);
            }
            if (element.type_length !== null && element.type_length !== undefined) {
                elementInfo.push(['Type Length', element.type_length.toString()]);
            }
            if (element.precision !== null && element.precision !== undefined) {
                elementInfo.push(['Precision', element.precision.toString()]);
            }
            if (element.scale !== null && element.scale !== undefined) {
                elementInfo.push(['Scale', element.scale.toString()]);
            }

            html += this.generateInfoSection('Column Information', elementInfo);

            // Type Information Section - always show both logical and converted types
            const typeInfo = [];

            // Logical Type
            if (element.logical_type) {
                const logicalTypeInfo = this.typeResolver.getLogicalTypeInfo(element.logical_type);
                if (logicalTypeInfo) {
                    typeInfo.push(['Logical Type', logicalTypeInfo.name || 'Unknown']);
                    if (
                        logicalTypeInfo.description &&
                        logicalTypeInfo.description !== 'No description available'
                    ) {
                        typeInfo.push(['Type Description', logicalTypeInfo.description]);
                    }
                } else {
                    const logicalTypeName = this.typeResolver.getLogicalTypeName(
                        element.logical_type
                    );
                    typeInfo.push(['Logical Type', logicalTypeName || 'Unknown']);
                }
            } else {
                typeInfo.push(['Logical Type', 'None']);
            }

            // Converted Type (legacy) - always show
            const convertedTypeName = this.typeResolver.getConvertedTypeName(
                element.converted_type
            );
            typeInfo.push(['Converted Type (Legacy)', convertedTypeName || 'None']);

            if (typeInfo.length > 0) {
                html += this.generateInfoSection('Type Information', typeInfo);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for row group metadata segments
     */
    generateRowGroupMetadataInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        if (segment.metadata) {
            const metadata = segment.metadata;

            // Row Group Metadata Overview
            const overviewInfo = [
                ['Element Type', 'Row Group Metadata'],
                ['Purpose', 'Metadata for all row groups in the file'],
            ];

            if (metadata.num_row_groups) {
                overviewInfo.push(['Total Row Groups', metadata.num_row_groups.toString()]);
            }
            if (metadata.row_groups && metadata.row_groups.length > 0) {
                overviewInfo.push(['Row Groups Available', metadata.row_groups.length.toString()]);

                const totalRows = metadata.row_groups.reduce(
                    (sum, rg) => sum + (rg.row_count || 0),
                    0
                );
                if (totalRows > 0) {
                    overviewInfo.push(['Total Rows', formatNumber(totalRows)]);
                }
            }

            html += this.generateInfoSection('Row Group Metadata', overviewInfo);

            // Summary statistics if we have row group data
            if (metadata.row_groups && metadata.row_groups.length > 0) {
                const rowGroups = metadata.row_groups;
                const totalCompressed = rowGroups.reduce(
                    (sum, rg) => sum + (rg.compression_stats?.total_compressed || 0),
                    0
                );
                const totalUncompressed = rowGroups.reduce(
                    (sum, rg) => sum + (rg.compression_stats?.total_uncompressed || 0),
                    0
                );
                const avgRowsPerGroup =
                    rowGroups.reduce((sum, rg) => sum + (rg.row_count || 0), 0) / rowGroups.length;

                const summaryInfo = [
                    ['Average Rows per Group', formatNumber(Math.round(avgRowsPerGroup))],
                ];

                if (totalCompressed > 0 && totalUncompressed > 0) {
                    summaryInfo.push(['Total Compressed Size', formatBytes(totalCompressed)]);
                    summaryInfo.push(['Total Uncompressed Size', formatBytes(totalUncompressed)]);
                    const ratio = ((totalCompressed / totalUncompressed) * 100).toFixed(1);
                    summaryInfo.push(['Overall Compression Ratio', `${ratio}%`]);
                }

                html += this.generateInfoSection('Summary Statistics', summaryInfo);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for generic metadata elements (indices, etc.)
     */
    generateMetadataElementInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        // Type-specific information
        const elementType = segment.type
            ? segment.type.replace('_', ' ').toUpperCase()
            : 'METADATA ELEMENT';
        const typeInfo = [
            ['Element Type', elementType],
            [
                'Purpose',
                segment.type
                    ? this.getMetadataElementPurpose(segment.type)
                    : 'File metadata structure',
            ],
        ];

        html += this.generateInfoSection('Metadata Element', typeInfo);

        // Additional metadata if available
        if (segment.metadata) {
            const additionalInfo = [];
            if (typeof segment.metadata === 'object') {
                Object.entries(segment.metadata).forEach(([key, value]) => {
                    if (key !== 'children' && value !== null && value !== undefined) {
                        additionalInfo.push([
                            key.replace(/_/g, ' ').toUpperCase(),
                            typeof value === 'object'
                                ? JSON.stringify(value).substring(0, 50) + '...'
                                : value.toString(),
                        ]);
                    }
                });
            }

            if (additionalInfo.length > 0) {
                html += this.generateInfoSection('Additional Information', additionalInfo);
            }
        }

        html += '</div>';
        return html;
    }

    /**
     * Generate info panel for key-value metadata entries
     */
    generateKeyValueMetadataInfoPanel(segment) {
        let html = '<div class="info-sections">';

        // Physical Layout
        html += this.generateInfoSection('Physical Layout', [
            ['Start Offset', segment.start.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['End Offset', segment.end.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '_')],
            ['Size', formatBytes(segment.size)],
        ]);

        // Key-Value Information (without Entry Index)
        const kvInfo = [['Key', segment.metadata.key]];

        html += this.generateInfoSection('Key-Value Metadata', kvInfo);

        // Full-width Value Viewer
        html += this.generateValueViewerSection(segment.metadata.value);

        html += '</div>';
        return html;
    }

    /**
     * Generate full-width value viewer section with JSON tree or code display
     */
    generateValueViewerSection(value) {
        if (!value) {
            return `
                <div class="info-section">
                    <h5 class="info-section-title">Value</h5>
                    <div class="value-viewer">
                        <div class="value-empty">No value</div>
                    </div>
                </div>
            `;
        }

        // Try to parse as JSON
        let parsedJson = null;
        let isJson = false;

        if (typeof value === 'string') {
            try {
                parsedJson = JSON.parse(value);
                isJson = true;
            } catch (e) {
                isJson = false;
            }
        }

        if (isJson && parsedJson && typeof parsedJson === 'object') {
            // Display as JSON tree viewer
            const jsonHtml = this.renderJsonTreeHtml(parsedJson);
            return `
                <div class="info-section large-card">
                    <h5 class="info-section-title">Value (JSON)</h5>
                    <div class="value-viewer json-viewer">
                        <div class="json-tree">${jsonHtml}</div>
                    </div>
                </div>
            `;
        } else {
            // Display as code string
            const escapedValue = this.escapeHtml(String(value));
            return `
                <div class="info-section large-card">
                    <h5 class="info-section-title">Value</h5>
                    <div class="value-viewer code-viewer">
                        <pre><code>${escapedValue}</code></pre>
                    </div>
                </div>
            `;
        }
    }

    /**
     * Render JSON as formatted HTML tree
     */
    renderJsonTreeHtml(obj, depth = 0) {
        const indent = '  '.repeat(depth);

        if (Array.isArray(obj)) {
            if (obj.length === 0) {
                return '<span class="json-empty-array">[]</span>';
            }

            let html = '<span class="json-bracket">[</span><br>';
            obj.forEach((item, index) => {
                const isLast = index === obj.length - 1;
                html += `${indent}  `;
                html += this.renderJsonTreeHtml(item, depth + 1);
                if (!isLast) {
                    html += '<span class="json-comma">,</span>';
                }
                html += '<br>';
            });
            html += `${indent}<span class="json-bracket">]</span>`;
            return html;
        } else if (obj !== null && typeof obj === 'object') {
            const keys = Object.keys(obj);
            if (keys.length === 0) {
                return '<span class="json-empty-object">{}</span>';
            }

            let html = '<span class="json-bracket">{</span><br>';
            keys.forEach((key, index) => {
                const isLast = index === keys.length - 1;
                html += `${indent}  `;
                html += `<span class="json-key">"${this.escapeHtml(key)}"</span>`;
                html += '<span class="json-colon">: </span>';
                html += this.renderJsonTreeHtml(obj[key], depth + 1);
                if (!isLast) {
                    html += '<span class="json-comma">,</span>';
                }
                html += '<br>';
            });
            html += `${indent}<span class="json-bracket">}</span>`;
            return html;
        } else {
            return this.renderJsonPrimitive(obj);
        }
    }

    /**
     * Render JSON primitive values
     */
    renderJsonPrimitive(value) {
        if (value === null) {
            return '<span class="json-null">null</span>';
        }
        if (typeof value === 'boolean') {
            return `<span class="json-boolean">${value}</span>`;
        }
        if (typeof value === 'number') {
            return `<span class="json-number">${value}</span>`;
        }
        if (typeof value === 'string') {
            return `<span class="json-string">"${this.escapeHtml(value)}"</span>`;
        }
        return `<span class="json-unknown">${this.escapeHtml(String(value))}</span>`;
    }

    /**
     * Escape HTML characters
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * Format key-value content for display, handling long values
     */
    formatKeyValueContent(value) {
        if (typeof value !== 'string') {
            return String(value);
        }

        // Truncate very long values for display
        const maxLength = 200;
        if (value.length > maxLength) {
            return value.substring(0, maxLength) + '...';
        }

        return value;
    }

    /**
     * Get purpose description for metadata element types
     */
    getMetadataElementPurpose(type) {
        const purposes = {
            column_index: 'Min/max statistics and null information for pages',
            offset_index: 'Page locations and sizes for efficient seeking',
            page_index: 'Combined column and offset index information',
        };
        return purposes[type] || 'Metadata element for file structure';
    }

    /**
     * Get repetition type name
     */
    getRepetitionType(repetitionType) {
        return this.typeResolver.getRepetitionTypeName(repetitionType);
    }

    /**
     * Generate a categorized info section (exact copy from old visualizer)
     */
    generateInfoSection(title, items) {
        return `
            <div class="info-section regular-card">
                <h5 class="info-section-title">${title}</h5>
                <div class="info-grid">
                    ${items
                        .map(
                            ([label, value]) => `
                        <div class="info-item">
                            <span class="info-label">${label}:</span>
                            <span class="info-value">${value}</span>
                        </div>
                    `
                        )
                        .join('')}
                </div>
            </div>
        `;
    }

    /**
     * Count columns in schema recursively
     */
    countColumns(node) {
        if (!node) {
            return 0;
        }

        if (node.element_type === 'column') {
            return 1;
        }

        if (node.children) {
            return Object.values(node.children).reduce(
                (sum, child) => sum + this.countColumns(child),
                0
            );
        }

        return 0;
    }

    /**
     * Generate page summary data by analyzing all column chunks
     */
    generatePageSummaryData(data) {
        if (!data.column_chunks || data.column_chunks.length === 0) {
            return null;
        }

        let totalPages = 0;
        let totalSize = 0;
        const pageTypes = {};
        const encodings = {};

        // Analyze all column chunks
        data.column_chunks.forEach(chunk => {
            // Process dictionary page if it exists
            if (chunk.dictionary_page) {
                totalPages++;
                const pageSize = chunk.dictionary_page.compressed_page_size || 0;
                totalSize += pageSize;

                // Count page type
                const pageType = 'DICTIONARY';
                if (!pageTypes[pageType]) {
                    pageTypes[pageType] = { count: 0 };
                }
                pageTypes[pageType].count++;

                // Count encoding
                const encoding = this.getEncodingName(chunk.dictionary_page.encoding) || 'UNKNOWN';
                if (!encodings[encoding]) {
                    encodings[encoding] = { count: 0 };
                }
                encodings[encoding].count++;
            }

            // Process data pages
            if (chunk.data_pages && chunk.data_pages.length > 0) {
                chunk.data_pages.forEach(page => {
                    totalPages++;
                    const pageSize = page.compressed_page_size || 0;
                    totalSize += pageSize;

                    // Determine page type
                    let pageType = 'DATA';
                    if (page.page_type !== undefined) {
                        const pageTypeMap = {
                            0: 'DATA_V1',
                            1: 'INDEX',
                            2: 'DICTIONARY',
                            3: 'DATA_V2',
                        };
                        pageType = pageTypeMap[page.page_type] || 'DATA';
                    }

                    // Count page type
                    if (!pageTypes[pageType]) {
                        pageTypes[pageType] = { count: 0 };
                    }
                    pageTypes[pageType].count++;

                    // Count encoding
                    const encoding = this.getEncodingName(page.encoding) || 'UNKNOWN';
                    if (!encodings[encoding]) {
                        encodings[encoding] = { count: 0 };
                    }
                    encodings[encoding].count++;
                });
            }

            // Process index pages if they exist
            if (chunk.index_pages && chunk.index_pages.length > 0) {
                chunk.index_pages.forEach(page => {
                    totalPages++;
                    const pageSize = page.compressed_page_size || 0;
                    totalSize += pageSize;

                    // Count page type
                    const pageType = 'INDEX';
                    if (!pageTypes[pageType]) {
                        pageTypes[pageType] = { count: 0 };
                    }
                    pageTypes[pageType].count++;

                    // Index pages don't typically have encodings, so we'll skip encoding count
                });
            }
        });

        // Calculate percentages for page types
        Object.keys(pageTypes).forEach(type => {
            const count = pageTypes[type].count;
            pageTypes[type].percentage =
                totalPages > 0 ? ((count / totalPages) * 100).toFixed(1) : '0.0';
        });

        // Calculate percentages for encodings
        Object.keys(encodings).forEach(encoding => {
            const count = encodings[encoding].count;
            encodings[encoding].percentage =
                totalPages > 0 ? ((count / totalPages) * 100).toFixed(1) : '0.0';
        });

        return {
            totalPages,
            avgPageSize: totalPages > 0 ? totalSize / totalPages : 0,
            pageTypes,
            encodings,
        };
    }

    /**
     * Get encoding name from encoding code (helper method)
     */
    getEncodingName(encodingCode) {
        if (typeof ParquetTypeResolver !== 'undefined') {
            return ParquetTypeResolver.getEncodingName(encodingCode);
        }

        // Fallback encoding mapping if ParquetTypeResolver is not available
        const encodingMap = {
            0: 'PLAIN',
            1: 'GROUP_VAR_INT',
            2: 'PLAIN_DICTIONARY',
            3: 'RLE',
            4: 'BIT_PACKED',
            5: 'DELTA_BINARY_PACKED',
            6: 'DELTA_LENGTH_BYTE_ARRAY',
            7: 'DELTA_BYTE_ARRAY',
            8: 'RLE_DICTIONARY',
        };

        return encodingMap[encodingCode] || `ENCODING_${encodingCode}`;
    }

    /**
     * Calculate page statistics for a column chunk segment
     * @param {object} segment - Column chunk segment
     * @returns {object} Page statistics with totalPages and avgRowsPerPage
     */
    calculatePageStatistics(segment) {
        const stats = { totalPages: 0, avgRowsPerPage: 0 };

        // Get the physical metadata which contains page information
        const physicalMeta = segment.physicalMetadata;
        if (!physicalMeta) {
            return stats;
        }

        let totalPages = 0;

        // Count dictionary page
        if (physicalMeta.dictionary_page) {
            totalPages++;
        }

        // Count data pages
        if (physicalMeta.data_pages && Array.isArray(physicalMeta.data_pages)) {
            totalPages += physicalMeta.data_pages.length;
        }

        // Count index pages
        if (physicalMeta.index_pages && Array.isArray(physicalMeta.index_pages)) {
            totalPages += physicalMeta.index_pages.length;
        }

        // Get row count from chunk level (pages don't typically have row counts)
        const chunkRows =
            physicalMeta.num_values || segment.logicalMetadata?.metadata?.num_values || 0;

        stats.totalPages = totalPages;
        stats.avgRowsPerPage = totalPages > 0 ? chunkRows / totalPages : 0;

        return stats;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = InfoPanelManager;
}
