/**
 * Main application logic for the Parquet Explorer (JSON Mode).
 */
import { InfoPanelManager } from './components/info-panel-manager';
import { SvgByteVisualizer } from './components/svg-byte-visualizer';
import type { FileData } from './types';

const DB_NAME = 'ParquetExplorerDB';

interface StoredFile {
    id: string;
    data: FileData;
    source: string;
    timestamp: number;
}

class ParquetExplorer {
    private parquetData: FileData | null = null;
    private infoPanelManager: InfoPanelManager | null = null;
    private fileStructureViz: SvgByteVisualizer | null = null;

    /** Initialize the application (event listeners are bound exactly once). */
    async init(): Promise<void> {
        try {
            this.setupEventListeners();
            await this.tryLoadFromStorage();
            this.hideLoadingScreen();
        } catch (error) {
            this.showError(`Initialization failed: ${(error as Error).message}`);
        }
    }

    private setupEventListeners(): void {
        const dropZone = document.getElementById('drop-zone')!;
        const fileInput = document.getElementById('file-input') as HTMLInputElement;

        dropZone.addEventListener('click', () => fileInput.click());
        dropZone.addEventListener('dragover', e => this.handleDragOver(e as DragEvent));
        dropZone.addEventListener('dragleave', e => this.handleDragLeave(e as DragEvent));
        dropZone.addEventListener('drop', e => this.handleDrop(e as DragEvent));

        document.body.addEventListener('dragover', e => this.handleGlobalDragOver(e as DragEvent));
        document.body.addEventListener('dragleave', e =>
            this.handleGlobalDragLeave(e as DragEvent)
        );
        document.body.addEventListener('drop', e => this.handleGlobalDrop(e as DragEvent));

        fileInput.addEventListener('change', e => {
            const files = (e.target as HTMLInputElement).files;
            if (files && files.length > 0) {
                void this.loadFile(files[0]!);
            }
        });

        const urlInput = document.getElementById('url-input') as HTMLInputElement;
        const loadUrlBtn = document.getElementById('load-url-btn')!;

        loadUrlBtn.addEventListener('click', () => {
            const url = urlInput.value.trim();
            if (url) {
                void this.loadURL(url);
            }
        });

        urlInput.addEventListener('keypress', e => {
            if ((e as KeyboardEvent).key === 'Enter') {
                const url = urlInput.value.trim();
                if (url) {
                    void this.loadURL(url);
                }
            }
        });

        document.getElementById('reset-btn')!.addEventListener('click', () => this.handleReset());
        document
            .getElementById('error-reset-btn')!
            .addEventListener('click', () => this.handleReset());
    }

    private handleDragOver(e: DragEvent): void {
        e.preventDefault();
        (e.currentTarget as HTMLElement).classList.add('drag-over');
    }

    private handleDragLeave(e: DragEvent): void {
        e.preventDefault();
        (e.currentTarget as HTMLElement).classList.remove('drag-over');
    }

    private handleDrop(e: DragEvent): void {
        e.preventDefault();
        (e.currentTarget as HTMLElement).classList.remove('drag-over');

        const files = e.dataTransfer?.files;
        if (files && files.length > 0) {
            void this.loadFile(files[0]!);
        }
    }

    private handleGlobalDragOver(e: DragEvent): void {
        if (e.dataTransfer?.types.includes('Files')) {
            e.preventDefault();
            document.body.classList.add('global-drag-over');
        }
    }

    private handleGlobalDragLeave(e: DragEvent): void {
        if (e.target === document.body) {
            document.body.classList.remove('global-drag-over');
        }
    }

    private handleGlobalDrop(e: DragEvent): void {
        e.preventDefault();
        document.body.classList.remove('global-drag-over');

        const files = e.dataTransfer?.files;
        if (files && files.length > 0) {
            void this.loadFile(files[0]!);
        }
    }

    /** Load a file into the existing app instance. */
    async loadFile(file: File): Promise<void> {
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
            this.showError(`Failed to parse file: ${(error as Error).message}`);
        }
    }

    /** Load a remote JSON file into the existing app instance. */
    async loadURL(url: string): Promise<void> {
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
            this.showError(`Failed to load URL: ${(error as Error).message}`);
        }
    }

    private async parseJSON(jsonText: string, source: string): Promise<void> {
        try {
            const data = JSON.parse(jsonText) as FileData;
            this.validateParquetJSON(data);

            if (!data.source) {
                data.source = source;
            }

            this.parquetData = data;
            await this.saveToStorage(data, source);

            this.showExplorer();
            this.populateUI();
            this.hideLoadingScreen();
        } catch (error) {
            throw new Error(`JSON parsing failed: ${(error as Error).message}`);
        }
    }

    private validateParquetJSON(data: FileData): void {
        const requiredFields: (keyof FileData)[] = [
            'source',
            'filesize',
            'column_chunks',
            'metadata',
        ];
        for (const field of requiredFields) {
            if (!(field in data)) {
                throw new Error(`Missing required field: ${field}`);
            }
        }

        if (!data.metadata) {
            throw new Error('Invalid metadata structure');
        }
        if (!data.metadata.schema_root) {
            throw new Error('Missing schema in metadata');
        }
    }

    private populateUI(): void {
        if (!this.parquetData) {
            return;
        }

        try {
            this.initializeFileStructureViz(this.parquetData);
        } catch (error) {
            console.error('Error populating UI:', error);
            this.showError(`Failed to populate UI: ${(error as Error).message}`);
        }
    }

    private initializeFileStructureViz(data: FileData): void {
        const container = document.getElementById('rowgroup-chart');
        if (!container || !data.column_chunks) {
            return;
        }

        try {
            const canvasContainer = document.getElementById('canvas-container');
            const infoPanelContainer = document.getElementById('info-panel-container');

            if (!canvasContainer || !infoPanelContainer) {
                throw new Error('Required containers not found');
            }

            // Tear down any previous visualizer so its listeners/tooltip don't leak.
            this.fileStructureViz?.destroy();

            this.infoPanelManager = new InfoPanelManager(infoPanelContainer);
            this.fileStructureViz = new SvgByteVisualizer(canvasContainer, this.infoPanelManager);
            this.fileStructureViz.initWithData(data);
        } catch (error) {
            console.error('Error creating file structure visualization:', error);
            container.innerHTML =
                '<p class="viz-error">Unable to create file structure visualization</p>';
        }
    }

    private async handleReset(): Promise<void> {
        this.parquetData = null;
        await this.clearStorage();
        this.clearFileStructureContent();
        this.showFileInput();
    }

    private clearFileStructureContent(): void {
        this.fileStructureViz?.destroy();
        this.fileStructureViz = null;
        this.infoPanelManager = null;

        const canvasContainer = document.getElementById('canvas-container');
        const infoPanelContainer = document.getElementById('info-panel-container');
        if (canvasContainer) {
            canvasContainer.innerHTML = '';
        }
        if (infoPanelContainer) {
            infoPanelContainer.innerHTML = '';
        }
    }

    // UI state management

    private showLoadingScreen(): void {
        document.getElementById('loading-screen')!.style.display = 'flex';
        document.getElementById('app-main')!.style.display = 'none';
    }

    private hideLoadingScreen(): void {
        document.getElementById('loading-screen')!.style.display = 'none';
        document.getElementById('app-main')!.style.display = 'block';
    }

    private updateLoadingStatus(status: string): void {
        const el = document.getElementById('loading-status');
        if (el) {
            el.textContent = status;
        }
    }

    private showFileInput(): void {
        this.setDisplay('no-file-state', 'block');
        this.setDisplay('file-loaded-state', 'none');
        this.setDisplay('file-content-section', 'none');
        this.setDisplay('error-section', 'none');
    }

    private showExplorer(): void {
        this.setDisplay('no-file-state', 'none');
        this.setDisplay('file-loaded-state', 'block');
        this.setDisplay('file-content-section', 'block');
        this.setDisplay('error-section', 'none');

        const sourceElement = document.getElementById('loaded-file-source');
        if (sourceElement && this.parquetData?.source) {
            sourceElement.textContent = this.parquetData.source;
        }
    }

    private showError(message: string): void {
        console.error('App Error:', message);

        const errorMessage = document.getElementById('error-message');
        if (errorMessage) {
            errorMessage.textContent = message;
        }

        this.setDisplay('error-section', 'block');
        this.hideLoadingScreen();
    }

    private setDisplay(id: string, value: string): void {
        const el = document.getElementById(id);
        if (el) {
            el.style.display = value;
        }
    }

    // Storage

    private async tryLoadFromStorage(): Promise<void> {
        try {
            const storedFile = await this.loadFromIndexedDB();
            if (storedFile) {
                this.parquetData = storedFile.data;
                this.showExplorer();
                this.populateUI();
            }
        } catch (error) {
            console.warn('Failed to load from IndexedDB:', error);
            void this.clearStorage();
        }
    }

    private loadFromIndexedDB(): Promise<StoredFile | null> {
        return new Promise(resolve => {
            const request = indexedDB.open(DB_NAME, 1);

            request.onerror = () => resolve(null);

            request.onupgradeneeded = event => {
                const db = (event.target as IDBOpenDBRequest).result;
                if (!db.objectStoreNames.contains('files')) {
                    db.createObjectStore('files', { keyPath: 'id' });
                }
            };

            request.onsuccess = event => {
                const db = (event.target as IDBOpenDBRequest).result;

                if (!db.objectStoreNames.contains('files')) {
                    db.close();
                    const deleteRequest = indexedDB.deleteDatabase(DB_NAME);
                    deleteRequest.onsuccess = () => resolve(null);
                    deleteRequest.onerror = () => resolve(null);
                    return;
                }

                try {
                    const transaction = db.transaction(['files'], 'readonly');
                    const store = transaction.objectStore('files');
                    const getRequest = store.get('current-file');

                    getRequest.onsuccess = () => {
                        db.close();
                        resolve((getRequest.result as StoredFile | undefined) ?? null);
                    };
                    getRequest.onerror = () => {
                        db.close();
                        resolve(null);
                    };
                } catch {
                    db.close();
                    resolve(null);
                }
            };
        });
    }

    private async saveToStorage(data: FileData, source: string): Promise<void> {
        try {
            await this.saveToIndexedDB(data, source);
        } catch (error) {
            console.warn('Failed to save to IndexedDB:', error);
        }
    }

    private saveToIndexedDB(data: FileData, source: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, 1);

            request.onerror = () => reject(request.error);

            request.onupgradeneeded = event => {
                const db = (event.target as IDBOpenDBRequest).result;
                if (!db.objectStoreNames.contains('files')) {
                    db.createObjectStore('files', { keyPath: 'id' });
                }
            };

            request.onsuccess = event => {
                const db = (event.target as IDBOpenDBRequest).result;
                const transaction = db.transaction(['files'], 'readwrite');
                const store = transaction.objectStore('files');

                const fileData: StoredFile = {
                    id: 'current-file',
                    data: data,
                    source: source,
                    timestamp: Date.now(),
                };

                const putRequest = store.put(fileData);
                putRequest.onsuccess = () => {
                    db.close();
                    resolve();
                };
                putRequest.onerror = () => {
                    db.close();
                    reject(putRequest.error);
                };
            };
        });
    }

    private async clearStorage(): Promise<void> {
        try {
            await this.clearIndexedDB();
        } catch (error) {
            console.warn('Failed to clear IndexedDB:', error);
        }
    }

    private clearIndexedDB(): Promise<void> {
        return new Promise(resolve => {
            const deleteRequest = indexedDB.deleteDatabase(DB_NAME);
            deleteRequest.onsuccess = () => resolve();
            deleteRequest.onerror = () => resolve();
            deleteRequest.onblocked = () => resolve();
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const app = new ParquetExplorer();
    app.init().catch((error: Error) => {
        console.error('Failed to initialize app:', error);
        const status = document.getElementById('loading-status');
        if (status) {
            status.textContent = `Error: ${error.message}`;
        }
    });
});
