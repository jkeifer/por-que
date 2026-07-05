/**
 * SVG-Based Byte Range Visualizer
 * Professional parquet file structure visualization with smooth Z-order animations
 * Uses SVG DOM manipulation for precise Z-order control during animations
 */
class SvgByteVisualizer {
    constructor(container, infoPanelManager = null, config = null) {
        this.container = container;
        this.svg = null;

        // Configuration (use provided config or default layout)
        this.config = config || VisualizationConfig.LAYOUT;

        // Data and state
        this.data = null;
        this.levels = [];
        this.selectedSegments = new Map(); // Map of levelIndex -> segmentId for hierarchical selection
        this.selectionPath = []; // Stack of selected segments in drill-down order
        this.hoveredSegment = null;

        // Layout properties
        this.width = 0;
        this.height = 0;

        // Animation system - using CSS transitions and simple timeouts

        // Interaction system
        this.tooltip = null;

        // Info panel manager (optional)
        this.infoPanelManager = infoPanelManager;

        this.init();
    }

    /**
     * Initialize the SVG visualizer
     */
    init() {
        this.createSVG();
        this.setupEventListeners();
        this.createTooltip();
    }

    /**
     * Create and configure SVG element
     */
    createSVG() {
        // Clear our dedicated container
        this.container.innerHTML = '';

        // Create SVG element
        this.svg = this.createSvgElement('svg', {
            width: '100%',
            style: `
                display: block;
                background: var(--bg-secondary);
                border-radius: 4px;
                cursor: pointer;
                height: ${this.calculateContentHeight()}px;
            `,
        });

        this.container.appendChild(this.svg);
        this.updateSvgSize();
    }

    /**
     * Create SVG element with attributes
     */
    createSvgElement(tag, attributes = {}) {
        const element = document.createElementNS('http://www.w3.org/2000/svg', tag);
        Object.entries(attributes).forEach(([key, value]) => {
            if (key === 'style') {
                element.style.cssText = value;
            } else {
                element.setAttribute(key, value);
            }
        });
        return element;
    }

    /**
     * Update SVG size and viewbox
     */
    updateSvgSize() {
        // Get the SVG's actual rendered size
        const rect = this.svg.getBoundingClientRect();
        this.width = rect.width;
        this.height = rect.height;

        // If no dimensions (hidden/not rendered), use fallback
        if (this.width === 0 || this.height === 0) {
            const containerRect = this.container.getBoundingClientRect();
            if (containerRect.width > 0) {
                this.width = containerRect.width;
                this.height = this.calculateContentHeight() || 80;
            } else {
                // Final fallback
                this.width = Math.min(window.innerWidth - 40, this.config.MAX_WIDTH || 1200);
                this.height = this.calculateContentHeight() || 80;
            }
        }

        // Set viewBox for responsive scaling
        this.svg.setAttribute('viewBox', `0 0 ${this.width} ${this.height}`);
        this.svg.setAttribute('preserveAspectRatio', 'xMidYMin meet');
    }

    /**
     * Calculate the height needed for current content
     */
    calculateContentHeight() {
        if (this.levels.length === 0) {
            return VisualizationConfig.LAYOUT.LEVEL_HEIGHT; // Use configured level height
        }

        let maxY = 0;
        this.levels.forEach(level => {
            if (level.layout) {
                const levelBottom = level.layout.y + level.layout.height;
                maxY = Math.max(maxY, levelBottom);
            }
        });

        return Math.max(VisualizationConfig.LAYOUT.LEVEL_HEIGHT, maxY);
    }

    /**
     * Update SVG height based on current content with smooth animation
     */
    updateSvgHeight() {
        const newHeight = this.calculateContentHeight();
        if (this.height !== newHeight) {
            this.height = newHeight;

            // Use CSS height instead of SVG height attribute to avoid scaling issues
            this.svg.style.height = `${newHeight}px`;

            // Keep viewBox stable - don't animate viewBox changes as they cause visual "growing"
            this.svg.setAttribute('viewBox', `0 0 ${this.width} ${newHeight}`);
        }
    }

    /**
     * Animate SVG height change in sync with level animations
     */
    animateSvgHeightChange(startHeight, endHeight, duration = 300) {
        if (startHeight === endHeight) {
            return;
        }

        this.height = endHeight;

        // Temporarily override the CSS transition to match our custom timing
        this.svg.style.transition = `height ${duration}ms cubic-bezier(0.4, 0, 0.2, 1)`;

        // Start from current height and animate to new height
        this.svg.style.height = `${startHeight}px`;

        const isExpanding = endHeight > startHeight;

        if (!isExpanding) {
            // When shrinking, update viewBox immediately to prevent scaling artifacts
            this.svg.setAttribute('viewBox', `0 0 ${this.width} ${endHeight}`);
        }

        // Trigger animation on next frame
        requestAnimationFrame(() => {
            this.svg.style.height = `${endHeight}px`;
        });

        // Clean up and handle viewBox for expansion
        setTimeout(() => {
            if (isExpanding) {
                // When expanding, update viewBox after animation to avoid scaling issues
                this.svg.setAttribute('viewBox', `0 0 ${this.width} ${endHeight}`);
            }
            this.svg.style.transition = '';
        }, duration);
    }

    /**
     * Set up event listeners
     */
    setupEventListeners() {
        // Resize handling
        window.addEventListener('resize', this.handleResize.bind(this));

        // Keyboard events
        document.addEventListener('keydown', this.handleKeyDown.bind(this));
    }

    /**
     * Create tooltip element
     */
    createTooltip() {
        const config = VisualizationConfig.TOOLTIP;
        this.tooltip = document.createElement('div');
        this.tooltip.className = 'svg-tooltip';
        this.tooltip.style.cssText = `
            position: absolute;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 12px;
            line-height: 1.4;
            pointer-events: none;
            z-index: 1000;
            visibility: hidden;
            max-width: min(500px, calc(100vw - 20px));
            min-width: ${config.MIN_WIDTH}px;
            white-space: normal;
            word-wrap: break-word;
            box-sizing: border-box;
        `;
        document.body.appendChild(this.tooltip);
    }

    /**
     * Initialize with parquet data
     */
    initWithData(data) {
        this.data = data;
        this.levels = [];
        this.selectedSegments.clear();
        this.selectionPath = [];

        // Initialize file structure analyzer
        this.analyzer = new FileStructureAnalyzer(data);

        // Wait for SVG to have proper dimensions
        requestAnimationFrame(() => {
            this.updateSvgSize();

            // Get overview segments and create first level
            const overviewSegments = this.analyzer.getSegmentsForLevel('overview');
            this.addLevel('overview', null, overviewSegments);

            // Show initial overview info panel
            if (this.infoPanelManager) {
                this.infoPanelManager.showOverview(this.data);
            }
        });
    }

    /**
     * Add a new level to the visualization with slide-down animation
     */
    addLevel(levelName, parentSegmentId, segments) {
        const level = {
            name: levelName,
            parentSegmentId: parentSegmentId,
            segments: segments || [],
            layout: null,
            svgGroup: null,
            animationState: 'appearing',
        };

        // Compute layout for this level
        const levelIndex = this.levels.length;
        level.layout = this.computeLevelLayout(level, levelIndex);

        // Create SVG group for this level
        level.svgGroup = this.createLevelGroup(level, levelIndex);

        // Calculate heights for synchronized animation
        const currentHeight = this.calculateContentHeight();
        this.levels.push(level);
        const newHeight = this.calculateContentHeight();

        // Start slide-down animation and synchronized height animation
        this.animateSlideDown(level, levelIndex);
        this.animateSvgHeightChange(currentHeight, newHeight, 300);

        // Update selection display after level is fully created
        requestAnimationFrame(() => {
            this.updateSelectionDisplay();
        });

        return level;
    }

    /**
     * Create SVG group element for a level
     */
    createLevelGroup(level, levelIndex) {
        const group = this.createSvgElement('g', {
            class: `level level-${levelIndex}`,
            'data-level': level.name,
            'data-level-index': levelIndex,
            transform: `translate(0, ${level.layout.y})`, // Position group at level's Y
        });

        // Create segments within the group
        level.layout.segments.forEach((segmentLayout, segmentIndex) => {
            this.createSegmentElements(
                segmentLayout,
                group,
                levelIndex,
                segmentIndex,
                level.layout.segments
            );
        });

        // Funnel connection will be created during animation

        return group;
    }

    /**
     * Create SVG elements for a segment (rectangle and label)
     */
    createSegmentElements(segmentLayout, group, levelIndex, segmentIndex, allSegmentsInLevel) {
        const segment = segmentLayout.segment;
        const allSegments = allSegmentsInLevel.map(s => s.segment);

        // Get CSS variable name for segment color
        const colorVar = VisualizationConfig.getSegmentColor(segment, segmentIndex, allSegments);

        // Get resolved color value for SVG fill
        const fillColor =
            this.getCSSVariable(colorVar) ||
            this.getCSSVariable(VisualizationConfig.COLORS.DEFAULT);

        // Get contrast class for this segment
        const contrastClass = VisualizationConfig.getContrastClass(fillColor);

        // Create wrapper group for this segment with contrast class
        const segmentGroup = this.createSvgElement('g', {
            class: contrastClass,
        });

        // Create main rectangle element (fill)
        const rect = this.createSvgElement('rect', {
            x: segmentLayout.x,
            y: segmentLayout.y, // Relative to group
            width: segmentLayout.width,
            height: segmentLayout.height,
            fill: fillColor,
            rx: this.config.CORNER_RADIUS || 0,
            class: `segment ${contrastClass}`,
            'data-segment-id': segment.id,
            'data-level-index': levelIndex,
        });

        // Add event listeners to main rectangle
        this.setupSegmentEventListeners(rect, segment, levelIndex);

        // Add rectangle to segment wrapper group (not directly to level group)
        segmentGroup.appendChild(rect);

        // Always create label initially to measure text
        const centerX = segmentLayout.x + segmentLayout.width / 2;
        const centerY = segmentLayout.y + segmentLayout.height / 2; // Relative to group

        const text = this.createSvgElement('text', {
            x: centerX,
            y: centerY,
            'text-anchor': 'middle',
            'dominant-baseline': 'central',
            'font-family': 'var(--font-sans)',
            'font-size': '12px',
            class: 'segment-label',
            'pointer-events': 'none', // Let clicks pass through to rectangle
            style: 'user-select: none;',
        });

        text.textContent = segment.name;
        // Add label to segment wrapper group (not directly to level group)
        segmentGroup.appendChild(text);

        // Add the complete segment wrapper group to the level group
        group.appendChild(segmentGroup);

        // Check if text actually fits in segment with some padding
        requestAnimationFrame(() => {
            const textBBox = text.getBBox();
            const availableWidth = segmentLayout.width - 8; // 4px padding each side

            if (textBBox.width <= availableWidth) {
                // Text fits - store reference to label on main rectangle
                rect.labelElement = text;
            } else {
                // Text doesn't fit - remove the label element
                text.remove();
                // rect.labelElement remains undefined, indicating no visible label
            }
        });
    }

    /**
     * Create funnel connection between parent segment and child level
     */
    createFunnelConnection(childLevel, childLevelIndex) {
        const parentLevel = this.levels[childLevelIndex - 1];
        if (!parentLevel) {
            return;
        }

        // Find the parent segment layout
        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        // Calculate funnel coordinates
        // Top of funnel: bottom edge of parent segment
        const parentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        // Bottom of funnel: top edge of child level
        const childY = childLevel.layout.y + this.config.SEGMENT_MARGIN;

        // Parent segment boundaries (exact segment width)
        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        // Child level spans full width
        const childLeft = 0;
        const childRight = this.width;

        // Create funnel polygon points
        const points = [
            `${parentLeft},${parentY}`, // Parent top-left
            `${parentRight},${parentY}`, // Parent top-right
            `${childRight},${childY}`, // Child bottom-right
            `${childLeft},${childY}`, // Child bottom-left
        ].join(' ');

        // Create funnel element
        const funnel = this.createSvgElement('polygon', {
            points: points,
            fill: this.getCSSVariable('--funnel-fill'),
            class: 'funnel-connection',
            'data-parent-segment': childLevel.parentSegmentId,
            'data-child-level': childLevelIndex,
            style: 'pointer-events: none;', // Don't intercept mouse events
        });

        // Add funnel to SVG (it should render behind levels due to DOM order)
        // Insert before all level groups so it renders behind them
        const firstLevel = this.svg.querySelector('.level');
        if (firstLevel) {
            this.svg.insertBefore(funnel, firstLevel);
        } else {
            this.svg.appendChild(funnel);
        }

        // Store funnel element on level for cleanup
        childLevel.funnelElement = funnel;

        // Create funnel label for the selected parent segment
        this.createFunnelLabel(
            parentSegmentLayout,
            parentLeft,
            parentRight,
            parentY,
            parentY,
            funnel
        );
    }

    /**
     * Set up event listeners for a segment
     */
    setupSegmentEventListeners(rect, segment, levelIndex) {
        // Mouse enter - show tooltip and hover outline (if not selected)
        rect.addEventListener('mouseenter', e => {
            this.hoveredSegment = segment.id;
            if (!this.isSegmentSelected(segment.id, levelIndex)) {
                rect.classList.add('segment-hover');
            }
            this.showSegmentTooltip(e, segment);
        });

        // Mouse leave - hide tooltip and hover outline
        rect.addEventListener('mouseleave', _e => {
            this.hoveredSegment = null;
            rect.classList.remove('segment-hover');
            this.hideTooltip();
        });

        // Mouse move - update tooltip position
        rect.addEventListener('mousemove', e => {
            if (this.hoveredSegment === segment.id) {
                this.updateTooltipPosition(e);
            }
        });

        // Click - handle segment selection and drill-down
        rect.addEventListener('click', e => {
            e.stopPropagation();
            this.handleSegmentClick(segment, levelIndex);
        });
    }

    /**
     * Compute layout for a level (delegates to SegmentLayoutCalculator)
     */
    computeLevelLayout(level, levelIndex) {
        return SegmentLayoutCalculator.computeLevelLayout(
            level.name,
            level.parentSegmentId,
            level.segments,
            levelIndex,
            this.width,
            this.config
        );
    }

    /**
     * Start slide-down animation
     */
    animateSlideDown(level, levelIndex) {
        const group = level.svgGroup;

        // Find parent level for animation coordination
        const parentLevel = level.parentSegmentId
            ? this.levels.find(l =>
                  l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
              )
            : null;

        // Calculate animation positions
        let startY = level.layout.y; // Default to final position
        if (parentLevel) {
            startY = parentLevel.layout.y; // Start behind parent
        }

        // Create animated funnel if this level has a parent
        if (parentLevel && level.parentSegmentId) {
            this.createAnimatedFunnel(level, levelIndex, parentLevel, startY, level.layout.y);
        }

        // Set initial position (overriding the transform from createLevelGroup)
        group.setAttribute('transform', `translate(0, ${startY})`);
        group.style.transition = 'transform 300ms cubic-bezier(0.4, 0, 0.2, 1)';

        // CRITICAL: Add group to SVG BEHIND its parent for proper Z-order
        if (parentLevel && parentLevel.svgGroup) {
            // Insert this level's group BEFORE the parent's group in DOM
            // This makes it render behind the parent during slide-down
            this.svg.insertBefore(group, parentLevel.svgGroup);
        } else {
            // No parent - add to end
            this.svg.appendChild(group);
        }

        // Start animation to final position
        requestAnimationFrame(() => {
            group.setAttribute('transform', `translate(0, ${level.layout.y})`);
        });

        // Mark level as visible after animation completes
        setTimeout(() => {
            level.animationState = 'visible';
        }, 300);
    }

    /**
     * Create animated funnel connection that slides down with the child level
     */
    createAnimatedFunnel(childLevel, childLevelIndex, parentLevel, startY, finalY) {
        // Find the parent segment layout
        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        // Calculate initial funnel coordinates (when child level is at startY)
        const initialParentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const initialChildY = startY + this.config.SEGMENT_MARGIN;

        // Calculate final funnel coordinates (when child level reaches finalY)
        const finalParentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const finalChildY = finalY + this.config.SEGMENT_MARGIN;

        // Parent segment boundaries (remain constant)
        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        // Child level spans full width (remains constant)
        const childLeft = 0;
        const childRight = this.width;

        // Create initial funnel polygon points
        const initialPoints = [
            `${parentLeft},${initialParentY}`, // Parent top-left
            `${parentRight},${initialParentY}`, // Parent top-right
            `${childRight},${initialChildY}`, // Child bottom-right
            `${childLeft},${initialChildY}`, // Child bottom-left
        ].join(' ');

        // Create final funnel polygon points
        const finalPoints = [
            `${parentLeft},${finalParentY}`, // Parent top-left
            `${parentRight},${finalParentY}`, // Parent top-right
            `${childRight},${finalChildY}`, // Child bottom-right
            `${childLeft},${finalChildY}`, // Child bottom-left
        ].join(' ');

        // Create funnel element with initial coordinates
        const funnel = this.createSvgElement('polygon', {
            points: initialPoints,
            fill: this.getCSSVariable('--funnel-fill'),
            class: 'funnel-connection animated-funnel',
            'data-parent-segment': childLevel.parentSegmentId,
            'data-child-level': childLevelIndex,
            style: 'pointer-events: none; transition: points 300ms cubic-bezier(0.4, 0, 0.2, 1);',
        });

        // Insert funnel before all level groups so it renders behind them
        const firstLevel = this.svg.querySelector('.level');
        if (firstLevel) {
            this.svg.insertBefore(funnel, firstLevel);
        } else {
            this.svg.appendChild(funnel);
        }

        // Store funnel element on level for cleanup
        childLevel.funnelElement = funnel;

        // Create funnel label for the selected parent segment
        this.createFunnelLabel(
            parentSegmentLayout,
            parentLeft,
            parentRight,
            initialParentY,
            finalParentY,
            funnel
        );

        // Animate funnel coordinates to final position
        requestAnimationFrame(() => {
            funnel.setAttribute('points', finalPoints);
        });
    }

    /**
     * Create label for funnel connection, especially for narrow segments
     */
    createFunnelLabel(
        parentSegmentLayout,
        parentLeft,
        parentRight,
        initialParentY,
        finalParentY,
        funnelElement
    ) {
        const segment = parentSegmentLayout.segment;

        // Find the actual segment rectangle to check if it has a visible label
        const segmentRect = this.svg.querySelector(`.segment[data-segment-id="${segment.id}"]`);

        // Only create funnel labels for segments that don't have visible labels
        if (segmentRect && segmentRect.labelElement) {
            return; // Segment already has a visible label, no need for funnel label
        }

        // Calculate label position - center of the funnel area
        const segmentWidth = parentRight - parentLeft;
        const labelX = parentLeft + segmentWidth / 2;
        const funnelMidY =
            initialParentY + ((finalParentY || initialParentY) - initialParentY) / 2 + 15; // Position in middle of funnel
        const labelY = funnelMidY;

        // Create the label text element
        const label = this.createSvgElement('text', {
            x: labelX,
            y: labelY,
            'text-anchor': 'middle',
            'dominant-baseline': 'central',
            fill: 'var(--text-primary)',
            'font-family': 'var(--font-sans)',
            'font-size': '11px',
            'font-weight': '500',
            class: 'funnel-label',
            'pointer-events': 'none',
            style: 'user-select: none; transition: opacity 300ms ease;',
        });

        label.textContent = segment.name;

        // Insert label after the funnel but before level groups
        const firstLevel = this.svg.querySelector('.level');
        if (firstLevel) {
            this.svg.insertBefore(label, firstLevel);
        } else {
            this.svg.appendChild(label);
        }

        // Store label reference on the funnel element for cleanup
        funnelElement.labelElement = label;

        // Check if label fits within funnel bounds and adjust positioning
        requestAnimationFrame(() => {
            const textBBox = label.getBBox();
            const svgWidth = this.width;

            // Calculate funnel bounds at the label position
            // The funnel expands from parent segment (top) to full width (bottom)
            const parentTop = initialParentY;
            const childTop = initialParentY + 30; // Approximate child level top (funnel bottom)
            const labelProgress = Math.min(
                1,
                Math.max(0, (labelY - parentTop) / Math.max(1, childTop - parentTop))
            );

            // At label position, funnel interpolates between parent segment width and full width
            const funnelLeftAtLabel = parentLeft - parentLeft * labelProgress;
            const funnelRightAtLabel =
                parentLeft + segmentWidth + (svgWidth - parentLeft - segmentWidth) * labelProgress;

            // Start with label centered on parent segment
            const labelLeft = labelX - textBBox.width / 2;
            const labelRight = labelX + textBBox.width / 2;

            let adjustedX = labelX;
            let anchor = 'middle';
            const margin = 5; // Small margin from funnel edges

            // Check if label overhangs funnel bounds and adjust if needed
            if (labelRight > funnelRightAtLabel - margin) {
                // Label extends beyond right funnel edge - right-align to funnel edge
                adjustedX = funnelRightAtLabel - margin;
                anchor = 'end';
            } else if (labelLeft < funnelLeftAtLabel + margin) {
                // Label extends beyond left funnel edge - left-align to funnel edge
                adjustedX = funnelLeftAtLabel + margin;
                anchor = 'start';
            }

            // Apply position adjustments
            if (adjustedX !== labelX || anchor !== 'middle') {
                label.setAttribute('x', adjustedX);
                label.setAttribute('text-anchor', anchor);
            }

            // Only truncate if label is still too wide for funnel after repositioning
            const funnelWidth = funnelRightAtLabel - funnelLeftAtLabel;
            const maxLabelWidth = funnelWidth - 2 * margin;
            if (textBBox.width > maxLabelWidth) {
                let text = segment.name;
                label.textContent = text;

                // Progressively truncate until it fits within funnel
                while (text.length > 5 && label.getBBox().width > maxLabelWidth) {
                    text = text.slice(0, -1);
                    label.textContent = text + '…';
                }
            }
        });
    }

    /**
     * Remove levels from a given index with slide-up animation
     */
    removeLevelsFrom(index) {
        if (index >= this.levels.length) {
            return;
        }

        const levelsToRemove = this.levels.slice(index);

        // Early return if no levels to remove
        if (levelsToRemove.length === 0) {
            return;
        }

        // Calculate heights for synchronized animation
        const currentHeight = this.calculateContentHeight();

        // Remove levels from array immediately (but keep SVG elements for animation)
        this.levels = this.levels.slice(0, index);
        const newHeight = this.calculateContentHeight();

        // Start synchronized height animation immediately
        const totalAnimationTime = 300 + levelsToRemove.length * 50;
        this.animateSvgHeightChange(currentHeight, newHeight, totalAnimationTime);

        // Animate each level sliding up (deepest first for proper visual effect)
        levelsToRemove.reverse().forEach((level, i) => {
            setTimeout(() => {
                this.animateSlideUp(level, index + levelsToRemove.length - 1 - i);
            }, i * 50); // Stagger animations slightly
        });
    }

    /**
     * Start slide-up animation with Z-order fix
     */
    animateSlideUp(level, _originalLevelIndex) {
        const group = level.svgGroup;
        if (!group) {
            return;
        }

        // CRITICAL: Reorder DOM elements so this level renders BEHIND its parent
        const parentLevel = level.parentSegmentId
            ? this.levels.find(l =>
                  l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
              )
            : null;

        if (parentLevel && parentLevel.svgGroup) {
            // Insert this level's group BEFORE the parent's group in DOM
            // This makes it render behind the parent
            this.svg.insertBefore(group, parentLevel.svgGroup);
        }

        // Calculate slide-up target Y position
        let targetY = level.layout.y; // Default to current position
        if (parentLevel) {
            targetY = parentLevel.layout.y; // Slide to parent's Y (behind it)
        } else {
            // No parent - slide up off screen
            targetY = -level.layout.height - 50;
        }

        // Animate funnel if it exists
        if (level.funnelElement && parentLevel) {
            this.animateFunnelSlideUp(level, parentLevel, targetY);
        }

        // Set up animation
        group.style.transition = 'transform 300ms cubic-bezier(0.4, 0, 0.2, 1)';

        // Start slide-up animation
        requestAnimationFrame(() => {
            group.setAttribute('transform', `translate(0, ${targetY})`);
        });

        // Remove element when animation completes
        setTimeout(() => {
            if (group.parentNode) {
                group.parentNode.removeChild(group);
            }
            // Also remove the funnel element and its label if they exist
            if (level.funnelElement && level.funnelElement.parentNode) {
                // Remove funnel label if it exists
                if (
                    level.funnelElement.labelElement &&
                    level.funnelElement.labelElement.parentNode
                ) {
                    level.funnelElement.labelElement.parentNode.removeChild(
                        level.funnelElement.labelElement
                    );
                }
                level.funnelElement.parentNode.removeChild(level.funnelElement);
            }
        }, 300);
    }

    /**
     * Start slide-up animation with explicit parent level (for multi-level switching)
     */
    animateSlideUpWithParent(level, originalLevelIndex, parentLevel) {
        const group = level.svgGroup;
        if (!group) {
            return;
        }

        // CRITICAL: Reorder DOM elements so this level renders BEHIND its parent
        if (parentLevel && parentLevel.svgGroup) {
            // Insert this level's group BEFORE the parent's group in DOM
            // This makes it render behind the parent
            this.svg.insertBefore(group, parentLevel.svgGroup);
        }

        // Calculate slide-up target Y position
        let targetY = level.layout.y; // Default to current position
        if (parentLevel) {
            targetY = parentLevel.layout.y; // Slide to parent's Y (behind it)
        } else {
            // No parent - slide up off screen
            targetY = -level.layout.height - 50;
        }

        // Animate funnel if it exists
        if (level.funnelElement && parentLevel) {
            this.animateFunnelSlideUp(level, parentLevel, targetY);
        }

        // Set up animation
        group.style.transition = 'transform 300ms cubic-bezier(0.4, 0, 0.2, 1)';

        // Start slide-up animation
        requestAnimationFrame(() => {
            group.setAttribute('transform', `translate(0, ${targetY})`);
        });

        // Remove element when animation completes
        setTimeout(() => {
            if (group.parentNode) {
                group.parentNode.removeChild(group);
            }
            // Also remove the funnel element and its label if they exist
            if (level.funnelElement && level.funnelElement.parentNode) {
                // Remove funnel label if it exists
                if (
                    level.funnelElement.labelElement &&
                    level.funnelElement.labelElement.parentNode
                ) {
                    level.funnelElement.labelElement.parentNode.removeChild(
                        level.funnelElement.labelElement
                    );
                }
                level.funnelElement.parentNode.removeChild(level.funnelElement);
            }
        }, 300);
    }

    /**
     * Animate funnel during slide-up
     */
    animateFunnelSlideUp(childLevel, parentLevel, targetY) {
        const funnel = childLevel.funnelElement;
        if (!funnel) {
            return;
        }

        // Find the parent segment layout
        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        // Calculate target funnel coordinates (when child level reaches targetY)
        const targetParentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const targetChildY = targetY + this.config.SEGMENT_MARGIN;

        // Parent segment boundaries (remain constant)
        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        // Child level spans full width (remains constant)
        const childLeft = 0;
        const childRight = this.width;

        // Create target funnel polygon points
        const targetPoints = [
            `${parentLeft},${targetParentY}`, // Parent top-left
            `${parentRight},${targetParentY}`, // Parent top-right
            `${childRight},${targetChildY}`, // Child bottom-right
            `${childLeft},${targetChildY}`, // Child bottom-left
        ].join(' ');

        // Animate funnel coordinates to target position
        requestAnimationFrame(() => {
            funnel.setAttribute('points', targetPoints);
        });
    }

    /**
     * Get color for segment with adjacency-aware coloring
     */
    getSegmentColor(segment, segmentIndex, allSegmentsInLevel) {
        const colorVar = VisualizationConfig.getSegmentColor(
            segment,
            segmentIndex,
            allSegmentsInLevel
        );
        return (
            this.getCSSVariable(colorVar) || this.getCSSVariable(VisualizationConfig.COLORS.DEFAULT)
        );
    }

    /**
     * Get CSS variable value
     */
    getCSSVariable(name) {
        return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }

    /**
     * Handle segment click for hierarchical selection and drill-down
     */
    handleSegmentClick(segment, levelIndex) {
        const isCurrentlySelected = this.isSegmentSelected(segment.id, levelIndex);

        // Clear selections at this level and all levels below
        for (let i = levelIndex; i < this.levels.length; i++) {
            this.selectedSegments.delete(i);
        }

        // Update selection path - trim to current level and add if selecting
        this.selectionPath = this.selectionPath.slice(0, levelIndex);

        if (!isCurrentlySelected) {
            this.selectedSegments.set(levelIndex, segment.id);
            this.selectionPath.push(segment);
        }

        // Update visual selection state
        this.updateSelectionDisplay();

        // Update info panel - show the last segment in selection path
        if (this.infoPanelManager) {
            if (this.selectionPath.length > 0) {
                const lastSelected = this.selectionPath[this.selectionPath.length - 1];
                this.infoPanelManager.showSegment(lastSelected);
            } else {
                this.infoPanelManager.showOverview(this.data);
            }
        }

        // Handle child levels with optimized switching logic
        const hasChildLevels = this.levels.length > levelIndex + 1;
        const willAddNewChild = !isCurrentlySelected && this.segmentHasChildren(segment);

        if (willAddNewChild) {
            // Special handling for row groups metadata
            // Use the segment's childLevelName property (now metadata-based)
            const childLevelName = segment.childLevelName;
            const childSegments = this.analyzer.getSegmentsForLevel(childLevelName, segment.id);

            if (childSegments && childSegments.length > 0) {
                if (hasChildLevels) {
                    const numLevelsToRemove = this.levels.length - (levelIndex + 1);
                    const newChildLevelIndex = levelIndex + 1;

                    if (numLevelsToRemove === 1) {
                        // Same-level switching: slide up old child and slide down new child simultaneously (no height change)
                        this.switchChildLevels(
                            newChildLevelIndex,
                            childLevelName,
                            segment.id,
                            childSegments
                        );
                    } else {
                        // Multi-level switching: remove multiple levels and add one (with height animation)
                        this.switchMultipleLevels(
                            newChildLevelIndex,
                            childLevelName,
                            segment.id,
                            childSegments
                        );
                    }
                } else {
                    // No levels to remove, add immediately
                    this.addLevel(childLevelName, segment.id, childSegments);
                }
            }
        } else if (hasChildLevels) {
            // Just removing child levels, no replacement
            this.removeLevelsFrom(levelIndex + 1);
        }
    }

    /**
     * Switch child levels with simultaneous slide-up/slide-down animation
     */
    switchChildLevels(childIndex, newLevelName, newParentSegmentId, newSegments) {
        const levelsToRemove = this.levels.slice(childIndex);

        // Immediately remove funnel labels to prevent overlapping during animation
        levelsToRemove.forEach(level => {
            if (level.funnelElement && level.funnelElement.labelElement) {
                if (level.funnelElement.labelElement.parentNode) {
                    level.funnelElement.labelElement.parentNode.removeChild(
                        level.funnelElement.labelElement
                    );
                }
                level.funnelElement.labelElement = null;
            }
        });

        // Remove levels from array immediately (but keep SVG elements for animation)
        this.levels = this.levels.slice(0, childIndex);

        // Create new level
        const newLevel = {
            name: newLevelName,
            parentSegmentId: newParentSegmentId,
            segments: newSegments || [],
            layout: null,
            svgGroup: null,
            animationState: 'appearing',
        };

        // Compute layout for new level (will occupy same position as old level)
        newLevel.layout = this.computeLevelLayout(newLevel, childIndex);

        // Create SVG group for new level
        newLevel.svgGroup = this.createLevelGroup(newLevel, childIndex);

        // Add new level to array
        this.levels.push(newLevel);

        // No height change needed since we're replacing at same position

        // Start simultaneous animations
        // 1. Slide up old levels
        levelsToRemove.reverse().forEach((level, i) => {
            setTimeout(() => {
                this.animateSlideUp(level, childIndex + levelsToRemove.length - 1 - i);
            }, i * 50); // Stagger removals slightly
        });

        // 2. Slide down new level immediately (no delay)
        this.animateSlideDown(newLevel, childIndex);

        // Update selection display after level is created
        requestAnimationFrame(() => {
            this.updateSelectionDisplay();
        });
    }

    /**
     * Switch multiple levels with synchronized height animation
     * Remove N levels and add 1 level, with height animation to match final size
     */
    switchMultipleLevels(childIndex, newLevelName, newParentSegmentId, newSegments) {
        const levelsToRemove = this.levels.slice(childIndex);

        // Immediately remove funnel labels to prevent overlapping during animation
        levelsToRemove.forEach(level => {
            if (level.funnelElement && level.funnelElement.labelElement) {
                if (level.funnelElement.labelElement.parentNode) {
                    level.funnelElement.labelElement.parentNode.removeChild(
                        level.funnelElement.labelElement
                    );
                }
                level.funnelElement.labelElement = null;
            }
        });

        // Calculate heights for synchronized animation
        const currentHeight = this.calculateContentHeight();

        // Remove levels from array immediately (but keep SVG elements for animation)
        this.levels = this.levels.slice(0, childIndex);

        // Create new level
        const newLevel = {
            name: newLevelName,
            parentSegmentId: newParentSegmentId,
            segments: newSegments || [],
            layout: null,
            svgGroup: null,
            animationState: 'appearing',
        };

        // Compute layout for new level
        newLevel.layout = this.computeLevelLayout(newLevel, childIndex);

        // Create SVG group for new level
        newLevel.svgGroup = this.createLevelGroup(newLevel, childIndex);

        // Add new level to array and calculate final height
        this.levels.push(newLevel);
        const newHeight = this.calculateContentHeight();

        // Start synchronized height animation
        const totalAnimationTime = 300 + levelsToRemove.length * 50;
        this.animateSvgHeightChange(currentHeight, newHeight, totalAnimationTime);

        // Start simultaneous animations
        // 1. Slide up old levels (staggered) - but preserve parent relationships
        levelsToRemove.reverse().forEach((level, i) => {
            setTimeout(() => {
                // For multi-level removal, we need to preserve parent-child relationships
                // Find parent in the original levels array before removal
                const levelIndex = childIndex + levelsToRemove.length - 1 - i;
                let parentLevel = null;

                if (level.parentSegmentId) {
                    // Look for parent in the levels we're keeping OR in the levels being removed
                    parentLevel =
                        this.levels.find(l =>
                            l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
                        ) ||
                        levelsToRemove.find(l =>
                            l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
                        );
                }

                this.animateSlideUpWithParent(level, levelIndex, parentLevel);
            }, i * 50); // Stagger removals slightly
        });

        // 2. Slide down new level immediately (no delay)
        this.animateSlideDown(newLevel, childIndex);

        // Update selection display after level is created
        requestAnimationFrame(() => {
            this.updateSelectionDisplay();
        });
    }

    /**
     * Update visual selection state on all segments using CSS classes
     */
    updateSelectionDisplay() {
        // Remove selection classes from all segments (but preserve hover state)
        this.svg.querySelectorAll('.segment').forEach(segment => {
            segment.classList.remove('segment-selected');
        });

        // Add selection class to selected segments
        this.selectedSegments.forEach((segmentId, levelIndex) => {
            const rect = this.svg.querySelector(
                `.segment[data-segment-id="${segmentId}"][data-level-index="${levelIndex}"]`
            );
            if (rect) {
                rect.classList.add('segment-selected');
            }
        });
    }

    /**
     * Check if a segment is selected at its level
     */
    isSegmentSelected(segmentId, levelIndex) {
        return this.selectedSegments.get(levelIndex) === segmentId;
    }

    /**
     * Check if a segment has children that can be drilled down into
     */
    segmentHasChildren(segment) {
        // Use the segment's childLevelName property (now metadata-based)
        const childLevelName = segment.childLevelName;
        if (!childLevelName) {
            return false;
        }

        const childSegments = this.analyzer.getSegmentsForLevel(childLevelName, segment.id);
        return childSegments && childSegments.length > 0;
    }

    /**
     * Show tooltip for a segment
     */
    showSegmentTooltip(event, segment) {
        let content = '';

        if (segment.description && segment.description !== segment.name) {
            content += `<strong>${segment.description}</strong><br/>`;
        } else {
            content += `<strong>${segment.name}</strong><br/>`;
        }

        content += `Range: ${formatBytes(segment.start)} - ${formatBytes(segment.end)}<br/>`;
        content += `Size: ${formatBytes(segment.size)}`;

        this.showTooltip(event, content);
    }

    /**
     * Show tooltip with content
     */
    showTooltip(event, content) {
        // Always clear any existing tooltip first
        this.clearTooltip();

        this.tooltip.innerHTML = content;
        this.updateTooltipPosition(event);
        this.tooltip.style.visibility = 'visible';
    }

    /**
     * Update tooltip position
     */
    updateTooltipPosition(event) {
        const config = VisualizationConfig.TOOLTIP;
        const tooltipRect = this.tooltip.getBoundingClientRect();
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;

        let left = event.pageX + config.OFFSET_X;
        let top = event.pageY + config.OFFSET_Y;

        // Check boundaries and adjust (using viewport-relative clientX/Y for boundary checks)
        if (
            event.clientX + config.OFFSET_X + tooltipRect.width >
            viewportWidth - config.BOUNDARY_PADDING
        ) {
            left = event.pageX - tooltipRect.width - config.OFFSET_X;
        }
        if (
            event.clientY + config.OFFSET_Y + tooltipRect.height >
            viewportHeight - config.BOUNDARY_PADDING
        ) {
            top = event.pageY - tooltipRect.height - config.OFFSET_Y;
        }
        if (left < window.pageXOffset + config.BOUNDARY_PADDING) {
            left = window.pageXOffset + config.BOUNDARY_PADDING;
        }
        if (top < window.pageYOffset + config.BOUNDARY_PADDING) {
            top = window.pageYOffset + config.BOUNDARY_PADDING;
        }

        this.tooltip.style.left = left + 'px';
        this.tooltip.style.top = top + 'px';
    }

    /**
     * Hide tooltip
     */
    hideTooltip() {
        this.tooltip.style.visibility = 'hidden';
    }

    /**
     * Clear any existing tooltips (defensive cleanup)
     */
    clearTooltip() {
        // Hide our own tooltip
        if (this.tooltip) {
            this.tooltip.style.visibility = 'hidden';
        }

        // Clear any orphaned tooltips that might exist
        const existingTooltips = document.querySelectorAll('.svg-tooltip');
        existingTooltips.forEach(tooltip => {
            if (tooltip !== this.tooltip) {
                tooltip.style.visibility = 'hidden';
                tooltip.remove();
            }
        });
    }

    /**
     * Handle resize events
     */
    handleResize() {
        clearTimeout(this.resizeTimeout);
        this.resizeTimeout = setTimeout(() => {
            this.updateSvgSize();
            this.recalculateAllLayouts();
        }, 100);
    }

    /**
     * Recalculate layouts for all levels after resize
     */
    recalculateAllLayouts() {
        // Recalculate layout for each level with new SVG width
        this.levels.forEach((level, levelIndex) => {
            level.layout = this.computeLevelLayout(level, levelIndex);

            // Update the SVG group elements
            if (level.svgGroup) {
                this.updateLevelGroupLayout(level, levelIndex);
            }
        });

        // Update SVG height
        this.updateSvgHeight();
    }

    /**
     * Update SVG group layout after recalculation
     */
    updateLevelGroupLayout(level, levelIndex) {
        // Remove old funnel and its label if they exist
        if (level.funnelElement && level.funnelElement.parentNode) {
            // Remove funnel label if it exists
            if (level.funnelElement.labelElement && level.funnelElement.labelElement.parentNode) {
                level.funnelElement.labelElement.parentNode.removeChild(
                    level.funnelElement.labelElement
                );
            }
            level.funnelElement.parentNode.removeChild(level.funnelElement);
        }

        // Remove old elements
        level.svgGroup.innerHTML = '';

        // Update group position
        level.svgGroup.setAttribute('transform', `translate(0, ${level.layout.y})`);

        // Recreate elements with new layout
        level.layout.segments.forEach((segmentLayout, segmentIndex) => {
            this.createSegmentElements(
                segmentLayout,
                level.svgGroup,
                levelIndex,
                segmentIndex,
                level.layout.segments
            );
        });

        // Recreate funnel connection (only for static layout, not during animation)
        if (level.parentSegmentId && levelIndex > 0 && level.animationState === 'visible') {
            this.createFunnelConnection(level, levelIndex);
        }

        // Reapply selection state
        this.updateSelectionDisplay();
    }

    /**
     * Handle keyboard events
     */
    handleKeyDown(event) {
        switch (event.key) {
            case 'Escape':
                // Clear all selections and go back to overview
                this.selectedSegments.clear();
                this.selectionPath = [];
                this.removeLevelsFrom(1);
                this.updateSelectionDisplay();
                if (this.infoPanelManager) {
                    this.infoPanelManager.showOverview(this.data);
                }
                event.preventDefault();
                break;
            case 'Backspace':
                // Go back one level
                if (this.levels.length > 1) {
                    this.removeLevelsFrom(this.levels.length - 1);
                }
                event.preventDefault();
                break;
        }
    }

    /**
     * Clean up resources
     */
    destroy() {
        if (this.tooltip && this.tooltip.parentNode) {
            this.tooltip.parentNode.removeChild(this.tooltip);
        }

        // Remove event listeners
        window.removeEventListener('resize', this.handleResize);
        document.removeEventListener('keydown', this.handleKeyDown);
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SvgByteVisualizer;
}
