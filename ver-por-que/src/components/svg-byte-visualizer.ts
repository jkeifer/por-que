/**
 * SVG-Based Byte Range Visualizer
 * Parquet file structure visualization with slide animations and drill-down.
 */
import { formatBytes } from '../format';
import { VisualizationConfig, type LayoutConfig } from '../config/visualization-config';
import {
    SegmentLayoutCalculator,
    type LevelLayout,
    type SegmentLayout,
} from '../business/segment-layout-calculator';
import { projectDump, describe, type SegmentNode } from '../business/segment-tree';
import type { Dump } from '../types';
import type { InfoPanelManager } from './info-panel-manager';

type FunnelElement = SVGElement & { labelElement?: SVGElement | null };
type SegmentRect = SVGElement & { labelElement?: SVGElement | null };

interface Level {
    name: string;
    parentSegmentId: string | null;
    segments: SegmentNode[];
    layout: LevelLayout;
    svgGroup: SVGGElement;
    animationState: 'appearing' | 'visible';
    funnelElement?: FunnelElement | null;
}

const ANIM_MS = 300;

export class SvgByteVisualizer {
    private container: HTMLElement;
    private svg!: SVGSVGElement;
    private config: LayoutConfig;

    private dump: Dump | null = null;
    private root: SegmentNode | null = null;
    private levels: Level[] = [];
    private selectedSegments = new Map<number, string>();
    private selectionPath: SegmentNode[] = [];
    private hoveredSegment: string | null = null;

    private width = 0;
    private height = 0;

    private tooltip!: HTMLDivElement;
    private infoPanelManager: InfoPanelManager | null;
    private resizeTimeout?: ReturnType<typeof setTimeout>;

    // Bound once so they can be removed on destroy.
    private onResize = (): void => this.handleResize();
    private onKeyDown = (e: KeyboardEvent): void => this.handleKeyDown(e);

    constructor(
        container: HTMLElement,
        infoPanelManager: InfoPanelManager | null = null,
        config: LayoutConfig | null = null
    ) {
        this.container = container;
        this.config = config || VisualizationConfig.LAYOUT;
        this.infoPanelManager = infoPanelManager;
        this.init();
    }

    private init(): void {
        this.createSVG();
        this.setupEventListeners();
        this.createTooltip();
    }

    private createSVG(): void {
        this.container.innerHTML = '';
        this.svg = this.createSvgElement('svg', {
            width: '100%',
            style: `
                display: block;
                background: var(--bg-secondary);
                border-radius: 4px;
                cursor: pointer;
                height: ${this.calculateContentHeight()}px;
            `,
        }) as SVGSVGElement;

        this.container.appendChild(this.svg);
        this.updateSvgSize();
    }

    private createSvgElement(
        tag: string,
        attributes: Record<string, string | number> = {}
    ): SVGElement {
        const element = document.createElementNS('http://www.w3.org/2000/svg', tag);
        Object.entries(attributes).forEach(([key, value]) => {
            if (key === 'style') {
                element.style.cssText = String(value);
            } else {
                element.setAttribute(key, String(value));
            }
        });
        return element;
    }

    private updateSvgSize(): void {
        const rect = this.svg.getBoundingClientRect();
        this.width = rect.width;
        this.height = rect.height;

        if (this.width === 0 || this.height === 0) {
            const containerRect = this.container.getBoundingClientRect();
            if (containerRect.width > 0) {
                this.width = containerRect.width;
                this.height = this.calculateContentHeight() || 80;
            } else {
                this.width = Math.min(window.innerWidth - 40, this.config.MAX_WIDTH || 1200);
                this.height = this.calculateContentHeight() || 80;
            }
        }

        this.svg.setAttribute('viewBox', `0 0 ${this.width} ${this.height}`);
        this.svg.setAttribute('preserveAspectRatio', 'xMidYMin meet');
    }

    private calculateContentHeight(): number {
        if (this.levels.length === 0) {
            return VisualizationConfig.LAYOUT.LEVEL_HEIGHT;
        }

        let maxY = 0;
        this.levels.forEach(level => {
            if (level.layout) {
                maxY = Math.max(maxY, level.layout.y + level.layout.height);
            }
        });

        return Math.max(VisualizationConfig.LAYOUT.LEVEL_HEIGHT, maxY);
    }

    private updateSvgHeight(): void {
        const newHeight = this.calculateContentHeight();
        if (this.height !== newHeight) {
            this.height = newHeight;
            this.svg.style.height = `${newHeight}px`;
            this.svg.setAttribute('viewBox', `0 0 ${this.width} ${newHeight}`);
        }
    }

    private animateSvgHeightChange(
        startHeight: number,
        endHeight: number,
        duration = ANIM_MS
    ): void {
        if (startHeight === endHeight) {
            return;
        }

        this.height = endHeight;
        this.svg.style.transition = `height ${duration}ms cubic-bezier(0.4, 0, 0.2, 1)`;
        this.svg.style.height = `${startHeight}px`;

        const isExpanding = endHeight > startHeight;
        if (!isExpanding) {
            this.svg.setAttribute('viewBox', `0 0 ${this.width} ${endHeight}`);
        }

        requestAnimationFrame(() => {
            this.svg.style.height = `${endHeight}px`;
        });

        setTimeout(() => {
            if (isExpanding) {
                this.svg.setAttribute('viewBox', `0 0 ${this.width} ${endHeight}`);
            }
            this.svg.style.transition = '';
        }, duration);
    }

    private setupEventListeners(): void {
        window.addEventListener('resize', this.onResize);
        document.addEventListener('keydown', this.onKeyDown);
    }

    private createTooltip(): void {
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

    /** Initialize with parquet data. */
    initWithData(data: Dump): void {
        this.dump = data;
        this.root = projectDump(data);
        this.levels = [];
        this.selectedSegments.clear();
        this.selectionPath = [];

        requestAnimationFrame(() => {
            this.updateSvgSize();

            this.addLevel('overview', null, this.root!.children);

            if (this.infoPanelManager && this.dump && this.root) {
                this.infoPanelManager.show(this.root, this.dump);
            }
        });
    }

    private addLevel(
        levelName: string,
        parentSegmentId: string | null,
        segments: SegmentNode[]
    ): Level {
        const levelIndex = this.levels.length;
        const layout = this.computeLevelLayout(levelName, parentSegmentId, segments, levelIndex);
        const level: Level = {
            name: levelName,
            parentSegmentId: parentSegmentId,
            segments: segments || [],
            layout: layout,
            svgGroup: this.createLevelGroup(layout, levelIndex),
            animationState: 'appearing',
        };

        const currentHeight = this.calculateContentHeight();
        this.levels.push(level);
        const newHeight = this.calculateContentHeight();

        this.animateSlideDown(level, levelIndex);
        this.animateSvgHeightChange(currentHeight, newHeight, ANIM_MS);

        requestAnimationFrame(() => this.updateSelectionDisplay());

        return level;
    }

    private createLevelGroup(layout: LevelLayout, levelIndex: number): SVGGElement {
        const group = this.createSvgElement('g', {
            class: `level level-${levelIndex}`,
            'data-level': layout.levelName,
            'data-level-index': levelIndex,
            transform: `translate(0, ${layout.y})`,
        }) as SVGGElement;

        layout.segments.forEach((segmentLayout, segmentIndex) => {
            this.createSegmentElements(segmentLayout, group, levelIndex, segmentIndex);
        });

        return group;
    }

    private createSegmentElements(
        segmentLayout: SegmentLayout,
        group: SVGGElement,
        levelIndex: number,
        _segmentIndex: number
    ): void {
        const segment = segmentLayout.segment;

        const colorVar = VisualizationConfig.getSegmentColor(segment.kind);
        const fillColor =
            this.getCSSVariable(colorVar) ||
            this.getCSSVariable(VisualizationConfig.COLORS.DEFAULT);
        const contrastClass = VisualizationConfig.getContrastClass(fillColor);

        const segmentGroup = this.createSvgElement('g', { class: contrastClass });

        const rect = this.createSvgElement('rect', {
            x: segmentLayout.x,
            y: segmentLayout.y,
            width: segmentLayout.width,
            height: segmentLayout.height,
            fill: fillColor,
            rx: this.config.CORNER_RADIUS || 0,
            class: `segment ${contrastClass}`,
            'data-segment-id': segment.id,
            'data-level-index': levelIndex,
        }) as SegmentRect;

        this.setupSegmentEventListeners(rect, segment, levelIndex);
        segmentGroup.appendChild(rect);

        const centerX = segmentLayout.x + segmentLayout.width / 2;
        const centerY = segmentLayout.y + segmentLayout.height / 2;

        const text = this.createSvgElement('text', {
            x: centerX,
            y: centerY,
            'text-anchor': 'middle',
            'dominant-baseline': 'central',
            'font-family': 'var(--font-sans)',
            'font-size': '12px',
            class: 'segment-label',
            'pointer-events': 'none',
            style: 'user-select: none;',
        });

        text.textContent = segment.name;
        segmentGroup.appendChild(text);
        group.appendChild(segmentGroup);

        requestAnimationFrame(() => {
            const textBBox = (text as SVGGraphicsElement).getBBox();
            const availableWidth = segmentLayout.width - 8;

            if (textBBox.width <= availableWidth) {
                rect.labelElement = text;
            } else {
                text.remove();
            }
        });
    }

    private createFunnelConnection(childLevel: Level, childLevelIndex: number): void {
        const parentLevel = this.levels[childLevelIndex - 1];
        if (!parentLevel) {
            return;
        }

        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        const parentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const childY = childLevel.layout.y + this.config.SEGMENT_MARGIN;
        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        const funnel = this.createFunnel(
            this.funnelPoints(parentLeft, parentRight, parentY, this.width, childY),
            childLevel.parentSegmentId,
            childLevelIndex,
            false
        );

        childLevel.funnelElement = funnel;
        this.createFunnelLabel(
            parentSegmentLayout,
            parentLeft,
            parentRight,
            parentY,
            parentY,
            funnel
        );
    }

    /** Build the trapezoid points string shared by all funnel shapes. */
    private funnelPoints(
        parentLeft: number,
        parentRight: number,
        parentY: number,
        childRight: number,
        childY: number
    ): string {
        return [
            `${parentLeft},${parentY}`,
            `${parentRight},${parentY}`,
            `${childRight},${childY}`,
            `0,${childY}`,
        ].join(' ');
    }

    /** Create + insert a funnel polygon behind the level groups. */
    private createFunnel(
        points: string,
        parentSegmentId: string | null,
        childLevelIndex: number,
        animated: boolean
    ): FunnelElement {
        const funnel = this.createSvgElement('polygon', {
            points: points,
            fill: this.getCSSVariable('--funnel-fill'),
            class: animated ? 'funnel-connection animated-funnel' : 'funnel-connection',
            'data-parent-segment': parentSegmentId ?? '',
            'data-child-level': childLevelIndex,
            style: animated
                ? 'pointer-events: none; transition: points 300ms cubic-bezier(0.4, 0, 0.2, 1);'
                : 'pointer-events: none;',
        }) as FunnelElement;

        const firstLevel = this.svg.querySelector('.level');
        if (firstLevel) {
            this.svg.insertBefore(funnel, firstLevel);
        } else {
            this.svg.appendChild(funnel);
        }
        return funnel;
    }

    /** Remove a level's funnel element and its label from the DOM. */
    private removeFunnel(level: Level): void {
        const funnel = level.funnelElement;
        if (funnel && funnel.parentNode) {
            if (funnel.labelElement && funnel.labelElement.parentNode) {
                funnel.labelElement.parentNode.removeChild(funnel.labelElement);
            }
            funnel.parentNode.removeChild(funnel);
        }
    }

    /** Remove only a level's funnel label (keeps the funnel). */
    private removeFunnelLabel(level: Level): void {
        const funnel = level.funnelElement;
        if (funnel && funnel.labelElement) {
            if (funnel.labelElement.parentNode) {
                funnel.labelElement.parentNode.removeChild(funnel.labelElement);
            }
            funnel.labelElement = null;
        }
    }

    private setupSegmentEventListeners(
        rect: SVGElement,
        segment: SegmentNode,
        levelIndex: number
    ): void {
        rect.addEventListener('mouseenter', (e: MouseEvent) => {
            this.hoveredSegment = segment.id;
            if (!this.isSegmentSelected(segment.id, levelIndex)) {
                rect.classList.add('segment-hover');
            }
            this.showSegmentTooltip(e, segment);
        });

        rect.addEventListener('mouseleave', () => {
            this.hoveredSegment = null;
            rect.classList.remove('segment-hover');
            this.hideTooltip();
        });

        rect.addEventListener('mousemove', (e: MouseEvent) => {
            if (this.hoveredSegment === segment.id) {
                this.updateTooltipPosition(e);
            }
        });

        rect.addEventListener('click', (e: MouseEvent) => {
            e.stopPropagation();
            this.handleSegmentClick(segment, levelIndex);
        });
    }

    private computeLevelLayout(
        levelName: string,
        parentSegmentId: string | null,
        segments: SegmentNode[],
        levelIndex: number
    ): LevelLayout {
        return SegmentLayoutCalculator.computeLevelLayout(
            levelName,
            parentSegmentId,
            segments,
            levelIndex,
            this.width,
            this.config
        );
    }

    private animateSlideDown(level: Level, levelIndex: number): void {
        const group = level.svgGroup;
        const parentLevel = this._findParentLevel(level);

        let startY = level.layout.y;
        if (parentLevel) {
            startY = parentLevel.layout.y;
        }

        if (parentLevel && level.parentSegmentId) {
            this.createAnimatedFunnel(level, levelIndex, parentLevel, startY, level.layout.y);
        }

        group.setAttribute('transform', `translate(0, ${startY})`);
        group.style.transition = 'transform 300ms cubic-bezier(0.4, 0, 0.2, 1)';

        if (parentLevel && parentLevel.svgGroup) {
            this.svg.insertBefore(group, parentLevel.svgGroup);
        } else {
            this.svg.appendChild(group);
        }

        requestAnimationFrame(() => {
            group.setAttribute('transform', `translate(0, ${level.layout.y})`);
        });

        setTimeout(() => {
            level.animationState = 'visible';
        }, ANIM_MS);
    }

    private createAnimatedFunnel(
        childLevel: Level,
        childLevelIndex: number,
        parentLevel: Level,
        startY: number,
        finalY: number
    ): void {
        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        const parentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const initialChildY = startY + this.config.SEGMENT_MARGIN;
        const finalChildY = finalY + this.config.SEGMENT_MARGIN;

        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        const funnel = this.createFunnel(
            this.funnelPoints(parentLeft, parentRight, parentY, this.width, initialChildY),
            childLevel.parentSegmentId,
            childLevelIndex,
            true
        );

        childLevel.funnelElement = funnel;
        this.createFunnelLabel(
            parentSegmentLayout,
            parentLeft,
            parentRight,
            parentY,
            parentY,
            funnel
        );

        requestAnimationFrame(() => {
            funnel.setAttribute(
                'points',
                this.funnelPoints(parentLeft, parentRight, parentY, this.width, finalChildY)
            );
        });
    }

    private createFunnelLabel(
        parentSegmentLayout: SegmentLayout,
        parentLeft: number,
        parentRight: number,
        initialParentY: number,
        finalParentY: number,
        funnelElement: FunnelElement
    ): void {
        const segment = parentSegmentLayout.segment;

        const segmentRect = this.svg.querySelector(
            `.segment[data-segment-id="${segment.id}"]`
        ) as SegmentRect | null;

        if (segmentRect && segmentRect.labelElement) {
            return;
        }

        const segmentWidth = parentRight - parentLeft;
        const labelX = parentLeft + segmentWidth / 2;
        const funnelMidY =
            initialParentY + ((finalParentY || initialParentY) - initialParentY) / 2 + 15;
        const labelY = funnelMidY;

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

        const firstLevel = this.svg.querySelector('.level');
        if (firstLevel) {
            this.svg.insertBefore(label, firstLevel);
        } else {
            this.svg.appendChild(label);
        }

        funnelElement.labelElement = label;

        requestAnimationFrame(() => {
            const textBBox = (label as SVGGraphicsElement).getBBox();
            const svgWidth = this.width;

            const parentTop = initialParentY;
            const childTop = initialParentY + 30;
            const labelProgress = Math.min(
                1,
                Math.max(0, (labelY - parentTop) / Math.max(1, childTop - parentTop))
            );

            const funnelLeftAtLabel = parentLeft - parentLeft * labelProgress;
            const funnelRightAtLabel =
                parentLeft + segmentWidth + (svgWidth - parentLeft - segmentWidth) * labelProgress;

            const labelLeft = labelX - textBBox.width / 2;
            const labelRight = labelX + textBBox.width / 2;

            let adjustedX = labelX;
            let anchor = 'middle';
            const margin = 5;

            if (labelRight > funnelRightAtLabel - margin) {
                adjustedX = funnelRightAtLabel - margin;
                anchor = 'end';
            } else if (labelLeft < funnelLeftAtLabel + margin) {
                adjustedX = funnelLeftAtLabel + margin;
                anchor = 'start';
            }

            if (adjustedX !== labelX || anchor !== 'middle') {
                label.setAttribute('x', String(adjustedX));
                label.setAttribute('text-anchor', anchor);
            }

            const funnelWidth = funnelRightAtLabel - funnelLeftAtLabel;
            const maxLabelWidth = funnelWidth - 2 * margin;
            if (textBBox.width > maxLabelWidth) {
                let text = segment.name;
                label.textContent = text;

                while (
                    text.length > 5 &&
                    (label as SVGGraphicsElement).getBBox().width > maxLabelWidth
                ) {
                    text = text.slice(0, -1);
                    label.textContent = text + '…';
                }
            }
        });
    }

    private removeLevelsFrom(index: number): void {
        if (index >= this.levels.length) {
            return;
        }

        const levelsToRemove = this.levels.slice(index);
        if (levelsToRemove.length === 0) {
            return;
        }

        const currentHeight = this.calculateContentHeight();
        this.levels = this.levels.slice(0, index);
        const newHeight = this.calculateContentHeight();

        const totalAnimationTime = ANIM_MS + levelsToRemove.length * 50;
        this.animateSvgHeightChange(currentHeight, newHeight, totalAnimationTime);

        levelsToRemove.reverse().forEach((level, i) => {
            setTimeout(() => {
                this.animateSlideUp(level);
            }, i * 50);
        });
    }

    /**
     * Slide a level up and remove it. When `explicitParent` is undefined the
     * parent is looked up in the current levels; pass an explicit parent (which
     * may live in a batch being removed) to preserve funnel positioning.
     */
    private animateSlideUp(level: Level, explicitParent?: Level | null): void {
        const group = level.svgGroup;
        if (!group) {
            return;
        }

        const parentLevel =
            explicitParent !== undefined ? explicitParent : this._findParentLevel(level);

        if (parentLevel && parentLevel.svgGroup) {
            this.svg.insertBefore(group, parentLevel.svgGroup);
        }

        let targetY = level.layout.y;
        if (parentLevel) {
            targetY = parentLevel.layout.y;
        } else {
            targetY = -level.layout.height - 50;
        }

        if (level.funnelElement && parentLevel) {
            this.animateFunnelSlideUp(level, parentLevel, targetY);
        }

        group.style.transition = 'transform 300ms cubic-bezier(0.4, 0, 0.2, 1)';

        requestAnimationFrame(() => {
            group.setAttribute('transform', `translate(0, ${targetY})`);
        });

        setTimeout(() => {
            if (group.parentNode) {
                group.parentNode.removeChild(group);
            }
            this.removeFunnel(level);
        }, ANIM_MS);
    }

    private animateFunnelSlideUp(childLevel: Level, parentLevel: Level, targetY: number): void {
        const funnel = childLevel.funnelElement;
        if (!funnel) {
            return;
        }

        const parentSegmentLayout = parentLevel.layout.segments.find(
            s => s.segment.id === childLevel.parentSegmentId
        );
        if (!parentSegmentLayout) {
            return;
        }

        const targetParentY =
            parentLevel.layout.y + parentLevel.layout.height - this.config.SEGMENT_MARGIN;
        const targetChildY = targetY + this.config.SEGMENT_MARGIN;
        const parentLeft = parentSegmentLayout.x;
        const parentRight = parentSegmentLayout.x + parentSegmentLayout.width;

        requestAnimationFrame(() => {
            funnel.setAttribute(
                'points',
                this.funnelPoints(parentLeft, parentRight, targetParentY, this.width, targetChildY)
            );
        });
    }

    private getCSSVariable(name: string): string {
        return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    }

    private handleSegmentClick(segment: SegmentNode, levelIndex: number): void {
        const isCurrentlySelected = this.isSegmentSelected(segment.id, levelIndex);

        for (let i = levelIndex; i < this.levels.length; i++) {
            this.selectedSegments.delete(i);
        }

        this.selectionPath = this.selectionPath.slice(0, levelIndex);

        if (!isCurrentlySelected) {
            this.selectedSegments.set(levelIndex, segment.id);
            this.selectionPath.push(segment);
        }

        this.updateSelectionDisplay();

        if (this.infoPanelManager && this.dump) {
            const selected = this.selectionPath[this.selectionPath.length - 1];
            this.infoPanelManager.show(selected ?? this.root!, this.dump);
        }

        const hasChildLevels = this.levels.length > levelIndex + 1;
        const willAddNewChild = !isCurrentlySelected && segment.children.length > 0;

        if (willAddNewChild) {
            const childSegments = segment.children;
            if (hasChildLevels) {
                const numLevelsToRemove = this.levels.length - (levelIndex + 1);
                const newChildLevelIndex = levelIndex + 1;
                this._replaceLevels(
                    newChildLevelIndex,
                    segment.kind,
                    segment.id,
                    childSegments,
                    numLevelsToRemove !== 1
                );
            } else {
                this.addLevel(segment.kind, segment.id, childSegments);
            }
        } else if (hasChildLevels) {
            this.removeLevelsFrom(levelIndex + 1);
        }
    }

    /**
     * Replace all levels from `childIndex` down with a single new level,
     * sliding the old ones up and the new one down. `animateHeight` drives the
     * synchronized SVG height animation (only needed when the level count
     * changes).
     */
    private _replaceLevels(
        childIndex: number,
        newLevelName: string,
        newParentSegmentId: string,
        newSegments: SegmentNode[],
        animateHeight: boolean
    ): void {
        const levelsToRemove = this.levels.slice(childIndex);

        // Drop funnel labels up front so they don't overlap during animation.
        levelsToRemove.forEach(level => this.removeFunnelLabel(level));

        const currentHeight = this.calculateContentHeight();
        this.levels = this.levels.slice(0, childIndex);

        const layout = this.computeLevelLayout(
            newLevelName,
            newParentSegmentId,
            newSegments,
            childIndex
        );
        const newLevel: Level = {
            name: newLevelName,
            parentSegmentId: newParentSegmentId,
            segments: newSegments || [],
            layout: layout,
            svgGroup: this.createLevelGroup(layout, childIndex),
            animationState: 'appearing',
        };

        this.levels.push(newLevel);

        if (animateHeight) {
            const newHeight = this.calculateContentHeight();
            const totalAnimationTime = ANIM_MS + levelsToRemove.length * 50;
            this.animateSvgHeightChange(currentHeight, newHeight, totalAnimationTime);
        }

        levelsToRemove.reverse().forEach((level, i) => {
            setTimeout(() => {
                let parentLevel: Level | null = null;
                if (level.parentSegmentId) {
                    parentLevel =
                        this.levels.find(l =>
                            l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
                        ) ??
                        levelsToRemove.find(l =>
                            l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
                        ) ??
                        null;
                }
                this.animateSlideUp(level, parentLevel);
            }, i * 50);
        });

        this.animateSlideDown(newLevel, childIndex);

        requestAnimationFrame(() => this.updateSelectionDisplay());
    }

    private updateSelectionDisplay(): void {
        this.svg.querySelectorAll('.segment').forEach(segment => {
            segment.classList.remove('segment-selected');
        });

        this.selectedSegments.forEach((segmentId, levelIndex) => {
            const rect = this.svg.querySelector(
                `.segment[data-segment-id="${segmentId}"][data-level-index="${levelIndex}"]`
            );
            if (rect) {
                rect.classList.add('segment-selected');
            }
        });
    }

    private isSegmentSelected(segmentId: string, levelIndex: number): boolean {
        return this.selectedSegments.get(levelIndex) === segmentId;
    }

    private showSegmentTooltip(event: MouseEvent, segment: SegmentNode): void {
        const label = describe(segment);
        let content =
            label && label !== segment.name
                ? `<strong>${label}</strong><br/>`
                : `<strong>${segment.name}</strong><br/>`;

        content += `Range: ${formatBytes(segment.start)} - ${formatBytes(segment.end)}<br/>`;
        content += `Size: ${formatBytes(segment.end - segment.start)}`;

        this.showTooltip(event, content);
    }

    private showTooltip(event: MouseEvent, content: string): void {
        this.tooltip.innerHTML = content;
        this.updateTooltipPosition(event);
        this.tooltip.style.visibility = 'visible';
    }

    private updateTooltipPosition(event: MouseEvent): void {
        const config = VisualizationConfig.TOOLTIP;
        const tooltipRect = this.tooltip.getBoundingClientRect();
        const viewportWidth = window.innerWidth;
        const viewportHeight = window.innerHeight;

        let left = event.pageX + config.OFFSET_X;
        let top = event.pageY + config.OFFSET_Y;

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
        if (left < window.scrollX + config.BOUNDARY_PADDING) {
            left = window.scrollX + config.BOUNDARY_PADDING;
        }
        if (top < window.scrollY + config.BOUNDARY_PADDING) {
            top = window.scrollY + config.BOUNDARY_PADDING;
        }

        this.tooltip.style.left = left + 'px';
        this.tooltip.style.top = top + 'px';
    }

    private hideTooltip(): void {
        this.tooltip.style.visibility = 'hidden';
    }

    private handleResize(): void {
        clearTimeout(this.resizeTimeout);
        this.resizeTimeout = setTimeout(() => {
            this.updateSvgSize();
            this.recalculateAllLayouts();
        }, 100);
    }

    private recalculateAllLayouts(): void {
        this.levels.forEach((level, levelIndex) => {
            level.layout = this.computeLevelLayout(
                level.name,
                level.parentSegmentId,
                level.segments,
                levelIndex
            );
            if (level.svgGroup) {
                this.updateLevelGroupLayout(level, levelIndex);
            }
        });

        this.updateSvgHeight();
    }

    private updateLevelGroupLayout(level: Level, levelIndex: number): void {
        this.removeFunnel(level);

        level.svgGroup.innerHTML = '';
        level.svgGroup.setAttribute('transform', `translate(0, ${level.layout.y})`);

        level.layout.segments.forEach((segmentLayout, segmentIndex) => {
            this.createSegmentElements(segmentLayout, level.svgGroup, levelIndex, segmentIndex);
        });

        if (level.parentSegmentId && levelIndex > 0 && level.animationState === 'visible') {
            this.createFunnelConnection(level, levelIndex);
        }

        this.updateSelectionDisplay();
    }

    private handleKeyDown(event: KeyboardEvent): void {
        switch (event.key) {
            case 'Escape':
                this.selectedSegments.clear();
                this.selectionPath = [];
                this.removeLevelsFrom(1);
                this.updateSelectionDisplay();
                if (this.infoPanelManager && this.dump && this.root) {
                    this.infoPanelManager.show(this.root, this.dump);
                }
                event.preventDefault();
                break;
            case 'Backspace':
                if (this.levels.length > 1) {
                    this.removeLevelsFrom(this.levels.length - 1);
                }
                event.preventDefault();
                break;
        }
    }

    private _findParentLevel(level: Level): Level | null {
        if (!level.parentSegmentId) {
            return null;
        }
        return (
            this.levels.find(l =>
                l.layout?.segments?.some(s => s.segment.id === level.parentSegmentId)
            ) ?? null
        );
    }

    /** Clean up resources: unbind listeners and remove the tooltip element. */
    destroy(): void {
        window.removeEventListener('resize', this.onResize);
        document.removeEventListener('keydown', this.onKeyDown);
        clearTimeout(this.resizeTimeout);

        if (this.tooltip && this.tooltip.parentNode) {
            this.tooltip.parentNode.removeChild(this.tooltip);
        }
    }
}
