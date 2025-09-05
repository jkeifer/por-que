/**
 * BrowserFileAdapter - Adapts browser File objects to the ReadableSeekable protocol
 * used by the por_que library. This allows the Python library to read from browser files
 * as if they were regular file-like objects.
 */
class BrowserFileAdapter {
    constructor(file, buffer = null) {
        this.file = file;
        this.position = 0;
        this.size = file.size;
        this.name = file.name;
        this.buffer = buffer; // Pre-loaded buffer for synchronous reads
    }

    /**
     * Initialize the adapter by loading the entire file into memory
     * @returns {Promise<BrowserFileAdapter>} The initialized adapter
     */
    static async create(file) {
        const arrayBuffer = await file.arrayBuffer();
        const buffer = new Uint8Array(arrayBuffer);
        return new BrowserFileAdapter(file, buffer);
    }

    /**
     * Read bytes from the file starting at current position
     * @param {number} size - Number of bytes to read (-1 for all remaining)
     * @returns {Uint8Array} The bytes read (synchronous)
     */
    read(size = -1) {
        if (!this.buffer) {
            throw new Error('Adapter not initialized - use BrowserFileAdapter.create()');
        }

        if (size === 0) {
            return new Uint8Array(0);
        }

        let bytesToRead = size;
        if (size === -1) {
            bytesToRead = this.size - this.position;
        } else {
            bytesToRead = Math.min(size, this.size - this.position);
        }

        if (bytesToRead <= 0) {
            return new Uint8Array(0);
        }

        const result = this.buffer.slice(this.position, this.position + bytesToRead);
        this.position += bytesToRead;
        return result;
    }

    /**
     * Change stream position
     * @param {number} offset - Byte offset
     * @param {number} whence - How to interpret offset (0=absolute, 1=relative, 2=from end)
     * @returns {number} New absolute position
     */
    seek(offset, whence = 0) {
        let newPos;

        switch (whence) {
            case 0: // Absolute position
                newPos = offset;
                break;
            case 1: // Relative to current position
                newPos = this.position + offset;
                break;
            case 2: // Relative to end
                newPos = this.size + offset;
                break;
            default:
                throw new Error(`Invalid whence value: ${whence}`);
        }

        // Clamp position to valid range
        if (newPos < 0) {
            newPos = 0;
        } else if (newPos > this.size) {
            newPos = this.size;
        }

        this.position = newPos;
        return this.position;
    }

    /**
     * Get current stream position
     * @returns {number} Current byte position
     */
    tell() {
        return this.position;
    }

    /**
     * Close the file (no-op for browser files)
     */
    close() {
        // No-op for browser files
    }

    /**
     * Check if file is readable
     * @returns {boolean} Always true for browser files
     */
    readable() {
        return true;
    }

    /**
     * Check if file is writable
     * @returns {boolean} Always false for browser files
     */
    writable() {
        return false;
    }

    /**
     * Check if file is seekable
     * @returns {boolean} Always true for browser files
     */
    seekable() {
        return true;
    }
}

/**
 * URLFileAdapter - Adapts remote URLs to the ReadableSeekable protocol
 * Uses HTTP range requests to provide random access to remote files
 */
class URLFileAdapter {
    constructor(url) {
        this.url = url;
        this.position = 0;
        this.size = null;
        this.cache = new Map();
        this.chunkSize = 64 * 1024; // 64KB chunks
    }

    /**
     * Initialize the adapter by determining file size
     */
    async initialize() {
        try {
            const response = await fetch(this.url, {
                method: 'HEAD'
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const contentLength = response.headers.get('content-length');
            if (contentLength) {
                this.size = parseInt(contentLength, 10);
            } else {
                // Fallback: try a range request to get file size
                const rangeResponse = await fetch(this.url, {
                    headers: { 'Range': 'bytes=0-0' }
                });

                const contentRange = rangeResponse.headers.get('content-range');
                if (contentRange) {
                    // Parse "bytes 0-0/12345" format
                    const match = contentRange.match(/bytes \d+-\d+\/(\d+)/);
                    if (match) {
                        this.size = parseInt(match[1], 10);
                    }
                }
            }

            if (this.size === null) {
                throw new Error('Cannot determine file size - server may not support range requests');
            }

            return this;
        } catch (error) {
            throw new Error(`Failed to initialize URL file: ${error.message}`);
        }
    }

    /**
     * Read bytes from the remote file
     * @param {number} size - Number of bytes to read (-1 for all remaining)
     * @returns {Promise<Uint8Array>} The bytes read
     */
    async read(size = -1) {
        if (this.size === null) {
            throw new Error('Adapter not initialized');
        }

        if (size === 0) {
            return new Uint8Array(0);
        }

        let bytesToRead = size;
        if (size === -1) {
            bytesToRead = this.size - this.position;
        } else {
            bytesToRead = Math.min(size, this.size - this.position);
        }

        if (bytesToRead <= 0) {
            return new Uint8Array(0);
        }

        const data = await this._fetchRange(this.position, this.position + bytesToRead);
        this.position += bytesToRead;
        return data;
    }

    /**
     * Fetch a range of bytes from the remote file with caching
     * @param {number} start - Start byte (inclusive)
     * @param {number} end - End byte (exclusive)
     * @returns {Promise<Uint8Array>} The bytes
     */
    async _fetchRange(start, end) {
        // Align to chunk boundaries for better caching
        const chunkStart = Math.floor(start / this.chunkSize) * this.chunkSize;
        const chunkEnd = Math.ceil(end / this.chunkSize) * this.chunkSize;

        const cacheKey = `${chunkStart}-${chunkEnd}`;

        if (this.cache.has(cacheKey)) {
            const cached = this.cache.get(cacheKey);
            const offset = start - chunkStart;
            const length = end - start;
            return cached.slice(offset, offset + length);
        }

        try {
            const response = await fetch(this.url, {
                headers: {
                    'Range': `bytes=${chunkStart}-${Math.min(chunkEnd - 1, this.size - 1)}`
                }
            });

            if (!response.ok && response.status !== 206) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const arrayBuffer = await response.arrayBuffer();
            const chunk = new Uint8Array(arrayBuffer);

            // Cache the chunk
            this.cache.set(cacheKey, chunk);

            // Limit cache size (keep last 10 chunks)
            if (this.cache.size > 10) {
                const firstKey = this.cache.keys().next().value;
                this.cache.delete(firstKey);
            }

            // Return the requested portion
            const offset = start - chunkStart;
            const length = end - start;
            return chunk.slice(offset, offset + length);

        } catch (error) {
            throw new Error(`Failed to fetch range ${start}-${end}: ${error.message}`);
        }
    }

    seek(offset, whence = 0) {
        if (this.size === null) {
            throw new Error('Adapter not initialized');
        }

        let newPos;

        switch (whence) {
            case 0: // Absolute position
                newPos = offset;
                break;
            case 1: // Relative to current position
                newPos = this.position + offset;
                break;
            case 2: // Relative to end
                newPos = this.size + offset;
                break;
            default:
                throw new Error(`Invalid whence value: ${whence}`);
        }

        // Clamp position to valid range
        if (newPos < 0) {
            newPos = 0;
        } else if (newPos > this.size) {
            newPos = this.size;
        }

        this.position = newPos;
        return this.position;
    }

    tell() {
        return this.position;
    }

    close() {
        this.cache.clear();
    }

    readable() {
        return true;
    }

    writable() {
        return false;
    }

    seekable() {
        return true;
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { BrowserFileAdapter, URLFileAdapter };
}
