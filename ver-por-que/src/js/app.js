/**
 * Main application logic for the Parquet Explorer (JSON Mode)
 */
class ParquetExplorer {
    constructor() {
        this.currentFile = null;
        this.parquetData = null;

        // Bind methods
        this.handleFileSelect = this.handleFileSelect.bind(this);
        this.handleURLLoad = this.handleURLLoad.bind(this);
        this.handleReset = this.handleReset.bind(this);
    }

    /**
     * Initialize the application
     */
    async init() {
        try {
            this.setupEventListeners();
            await this.tryLoadFromStorage();
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

        // Drag and drop on drop zone
        dropZone.addEventListener('click', () => fileInput.click());
        dropZone.addEventListener('dragover', this.handleDragOver);
        dropZone.addEventListener('dragleave', this.handleDragLeave);
        dropZone.addEventListener('drop', this.handleDrop.bind(this));

        // Global drag and drop for file replacement
        document.body.addEventListener('dragover', this.handleGlobalDragOver.bind(this));
        document.body.addEventListener('dragleave', this.handleGlobalDragLeave.bind(this));
        document.body.addEventListener('drop', this.handleGlobalDrop.bind(this));

        // File input change
        fileInput.addEventListener('change', e => {
            if (e.target.files.length > 0) {
                loadNewFile(e.target.files[0]);
            }
        });

        // URL input
        const urlInput = document.getElementById('url-input');
        const loadUrlBtn = document.getElementById('load-url-btn');

        loadUrlBtn.addEventListener('click', () => {
            const url = urlInput.value.trim();
            if (url) {
                loadNewURL(url);
            }
        });

        urlInput.addEventListener('keypress', e => {
            if (e.key === 'Enter') {
                const url = urlInput.value.trim();
                if (url) {
                    loadNewURL(url);
                }
            }
        });

        // Reset button
        document.getElementById('reset-btn').addEventListener('click', this.handleReset);
        document.getElementById('error-reset-btn').addEventListener('click', this.handleReset);
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
            loadNewFile(files[0]);
        }
    }

    /**
     * Handle global drag over event
     */
    handleGlobalDragOver(e) {
        // Only handle if a file is being dragged
        if (e.dataTransfer.types.includes('Files')) {
            e.preventDefault();
            document.body.classList.add('global-drag-over');
        }
    }

    /**
     * Handle global drag leave event
     */
    handleGlobalDragLeave(e) {
        // Only remove if we're leaving the body entirely
        if (e.target === document.body) {
            document.body.classList.remove('global-drag-over');
        }
    }

    /**
     * Handle global drop event
     */
    handleGlobalDrop(e) {
        e.preventDefault();
        document.body.classList.remove('global-drag-over');

        const files = e.dataTransfer.files;
        if (files.length > 0) {
            loadNewFile(files[0]);
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

            // Add the source to the data object (preserve existing source if available)
            if (!data.source) {
                data.source = source;
            }

            this.parquetData = data;

            // Store in IndexedDB
            await this.saveToStorage(data, source);

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
        if (!data.metadata) {
            throw new Error('Invalid metadata structure');
        }

        const metadata = data.metadata;
        if (!metadata.schema_root) {
            throw new Error('Missing schema in metadata');
        }

        console.log('JSON validation passed');
    }

    /**
     * Populate the UI with parsed data
     */
    populateUI() {
        if (!this.parquetData) {
            return;
        }

        try {
            console.log('Parsed data:', this.parquetData);

            // Initialize file structure visualization
            console.log('About to initialize file structure viz...');
            this.initializeFileStructureViz(this.parquetData);
            console.log('File structure viz initialized successfully');
        } catch (error) {
            console.error('Error populating UI:', error);
            console.error('Error stack:', error.stack);
            this.showError(`Failed to populate UI: ${error.message}`);
        }
    }

    /**
     * Get compression algorithm name from code
     */
    getCompressionName(code) {
        return ParquetTypeResolver.getCompressionName(code);
    }

    /**
     * Get encoding name from code
     */
    getEncodingName(code) {
        return ParquetTypeResolver.getEncodingName(code);
    }

    /**
     * Find a column in the schema tree by path
     */
    findColumnInSchema(schema, path) {
        if (!schema || !path) {
            return null;
        }

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
     * Initialize file structure visualization
     */
    initializeFileStructureViz(data) {
        const container = document.getElementById('rowgroup-chart');
        if (!container || !data.column_chunks) {
            return;
        }

        try {
            // Get dedicated containers for each component
            const canvasContainer = document.getElementById('canvas-container');
            const infoPanelContainer = document.getElementById('info-panel-container');

            if (!canvasContainer || !infoPanelContainer) {
                throw new Error('Required containers not found');
            }

            // Create info panel manager with its own container
            this.infoPanelManager = new InfoPanelManager(infoPanelContainer);

            // Initialize the SVG visualizer with its own container
            this.fileStructureViz = new SvgByteVisualizer(canvasContainer, this.infoPanelManager);
            this.fileStructureViz.initWithData(data);
        } catch (error) {
            console.error('Error creating file structure visualization:', error);
            container.innerHTML =
                '<p class="viz-error">Unable to create file structure visualization</p>';
        }
    }

    /**
     * Handle reset
     */
    async handleReset() {
        this.currentFile = null;
        this.parquetData = null;
        await this.clearStorage();

        // Clear the file structure content
        this.clearFileStructureContent();

        this.showFileInput();
    }

    /**
     * Clear file structure visualization content
     */
    clearFileStructureContent() {
        const canvasContainer = document.getElementById('canvas-container');
        const infoPanelContainer = document.getElementById('info-panel-container');

        // Clear any lingering tooltips
        const tooltips = document.querySelectorAll('.svg-tooltip');
        tooltips.forEach(tooltip => {
            tooltip.style.visibility = 'hidden';
            tooltip.remove();
        });

        if (canvasContainer) {
            canvasContainer.innerHTML = '';
        }
        if (infoPanelContainer) {
            infoPanelContainer.innerHTML = '';
        }
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
        const noFileState = document.getElementById('no-file-state');
        const fileLoadedState = document.getElementById('file-loaded-state');
        const fileContentSection = document.getElementById('file-content-section');
        const errorSection = document.getElementById('error-section');

        if (noFileState) {
            noFileState.style.display = 'block';
        }
        if (fileLoadedState) {
            fileLoadedState.style.display = 'none';
        }
        if (fileContentSection) {
            fileContentSection.style.display = 'none';
        }
        if (errorSection) {
            errorSection.style.display = 'none';
        }

        // Fallback to old structure if new elements don't exist
        const fileInputSection = document.getElementById('file-input-section');
        const explorerSection = document.getElementById('explorer-section');
        if (fileInputSection && explorerSection) {
            fileInputSection.style.display = 'block';
            explorerSection.style.display = 'none';
        }
    }

    showExplorer() {
        const noFileState = document.getElementById('no-file-state');
        const fileLoadedState = document.getElementById('file-loaded-state');
        const fileContentSection = document.getElementById('file-content-section');
        const errorSection = document.getElementById('error-section');

        if (noFileState) {
            noFileState.style.display = 'none';
        }
        if (fileLoadedState) {
            fileLoadedState.style.display = 'block';
        }
        if (fileContentSection) {
            fileContentSection.style.display = 'block';
        }
        if (errorSection) {
            errorSection.style.display = 'none';
        }

        // Update the file source display
        const sourceElement = document.getElementById('loaded-file-source');
        if (sourceElement && this.parquetData && this.parquetData.source) {
            sourceElement.textContent = this.parquetData.source;
        }

        // Fallback to old structure if new elements don't exist
        const fileInputSection = document.getElementById('file-input-section');
        const explorerSection = document.getElementById('explorer-section');
        if (fileInputSection && explorerSection) {
            fileInputSection.style.display = 'none';
            explorerSection.style.display = 'block';
        }
    }

    showError(message) {
        console.error('App Error:', message);

        const errorMessage = document.getElementById('error-message');
        if (errorMessage) {
            errorMessage.textContent = message;
        }

        const fileInputSection = document.getElementById('file-input-section');
        if (fileInputSection) {
            fileInputSection.style.display = 'none';
        }

        const explorerSection = document.getElementById('explorer-section');
        if (explorerSection) {
            explorerSection.style.display = 'none';
        }

        const errorSection = document.getElementById('error-section');
        if (errorSection) {
            errorSection.style.display = 'block';
        }

        this.hideLoadingScreen();
    }

    /**
     * Try to load data from localStorage
     */
    async tryLoadFromStorage() {
        try {
            console.log('Checking IndexedDB...');
            const storedFile = await this.loadFromIndexedDB();

            if (storedFile) {
                console.log('Restoring from IndexedDB...');
                this.parquetData = storedFile.data;

                // Show the explorer interface
                this.showExplorer();

                // Populate the UI with data
                this.populateUI();

                console.log(`Restored file from storage: ${storedFile.source}`);
            } else {
                console.log('No stored data found, showing file input');
            }
        } catch (error) {
            console.warn('Failed to load from IndexedDB:', error);
            this.clearStorage();
        }
    }

    /**
     * Load from IndexedDB
     */
    loadFromIndexedDB() {
        return new Promise((resolve, _reject) => {
            const request = indexedDB.open('ParquetExplorerDB', 1);

            request.onerror = () => resolve(null); // No data available

            request.onupgradeneeded = event => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains('files')) {
                    db.createObjectStore('files', { keyPath: 'id' });
                }
            };

            request.onsuccess = event => {
                const db = event.target.result;

                // If the object store doesn't exist, the database was never properly initialized
                if (!db.objectStoreNames.contains('files')) {
                    db.close();
                    // Try to recreate the database by deleting it and calling this function again
                    const deleteRequest = indexedDB.deleteDatabase('ParquetExplorerDB');
                    deleteRequest.onsuccess = () => {
                        // Database deleted, resolve with null (no stored data)
                        resolve(null);
                    };
                    deleteRequest.onerror = () => resolve(null);
                    return;
                }

                try {
                    const transaction = db.transaction(['files'], 'readonly');
                    const store = transaction.objectStore('files');
                    const getRequest = store.get('current-file');

                    getRequest.onsuccess = () => {
                        db.close();
                        resolve(getRequest.result || null);
                    };

                    getRequest.onerror = () => {
                        db.close();
                        resolve(null);
                    };
                } catch (error) {
                    db.close();
                    resolve(null);
                }
            };
        });
    }

    /**
     * Save data to IndexedDB (fallback to localStorage for smaller data)
     */
    async saveToStorage(data, source) {
        try {
            // Use IndexedDB for larger storage capacity
            await this.saveToIndexedDB(data, source);
            console.log('Stored data to IndexedDB');
        } catch (error) {
            console.warn('Failed to save to IndexedDB:', error);
            // Fallback: don't store anything if it's too large
            console.log('Data too large to store persistently');
        }
    }

    /**
     * Save to IndexedDB
     */
    saveToIndexedDB(data, source) {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open('ParquetExplorerDB', 1);

            request.onerror = () => reject(request.error);

            request.onupgradeneeded = event => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains('files')) {
                    db.createObjectStore('files', { keyPath: 'id' });
                }
            };

            request.onsuccess = event => {
                const db = event.target.result;
                const transaction = db.transaction(['files'], 'readwrite');
                const store = transaction.objectStore('files');

                const fileData = {
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

    /**
     * Clear stored data
     */
    async clearStorage() {
        try {
            // Clear IndexedDB
            await this.clearIndexedDB();
        } catch (error) {
            console.warn('Failed to clear IndexedDB:', error);
        }
    }

    /**
     * Clear IndexedDB
     */
    clearIndexedDB() {
        return new Promise((resolve, _reject) => {
            // Simple approach: just delete the entire database
            const deleteRequest = indexedDB.deleteDatabase('ParquetExplorerDB');

            deleteRequest.onsuccess = () => {
                console.log('IndexedDB cleared successfully');
                resolve();
            };

            deleteRequest.onerror = () => {
                console.log('Failed to clear IndexedDB, but continuing');
                resolve(); // Don't fail the operation
            };

            deleteRequest.onblocked = () => {
                console.log('IndexedDB delete blocked, but continuing');
                resolve(); // Don't fail the operation
            };
        });
    }
}

// Utility functions
function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) {
        return '0 Bytes';
    }
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatNumber(num) {
    if (num === null || num === undefined) {
        return 'N/A';
    }
    return num.toLocaleString();
}

// Make utility functions globally available
window.formatBytes = formatBytes;
window.formatNumber = formatNumber;

// Global app instance
let app = null;

// Global function to handle new file loading (creates new app instance)
async function loadNewFile(file) {
    // Create completely new app instance
    app = new ParquetExplorer();
    await app.init();
    return app.handleFileSelect(file);
}

// Global function to handle new URL loading (creates new app instance)
async function loadNewURL(url) {
    // Create completely new app instance
    app = new ParquetExplorer();
    await app.init();
    return app.handleURLLoad(url);
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    app = new ParquetExplorer();
    app.init().catch(error => {
        console.error('Failed to initialize app:', error);
        document.getElementById('loading-status').textContent = `Error: ${error.message}`;
    });
});
