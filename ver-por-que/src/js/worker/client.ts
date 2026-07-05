/**
 * Main-thread handle to the parquet worker. Spins the worker up on first use
 * (so the JSON path pays zero pyodide cost) and reuses it thereafter.
 */
import type { ParseRequest, WorkerResponse } from './protocol';

interface Pending {
    resolve: (dump: string) => void;
    reject: (error: Error) => void;
}

export class ParquetWorkerClient {
    private worker: Worker | null = null;
    private nextId = 1;
    private readonly pending = new Map<number, Pending>();

    constructor(private readonly onStatus: (status: string) => void) {}

    private ensureWorker(): Worker {
        if (!this.worker) {
            this.worker = new Worker(new URL('./worker.ts', import.meta.url), {
                type: 'module',
            });
            this.worker.addEventListener('message', (event: MessageEvent<WorkerResponse>) =>
                this.handle(event.data)
            );
        }
        return this.worker;
    }

    private handle(msg: WorkerResponse): void {
        if ('status' in msg) {
            this.onStatus(msg.status);
            return;
        }
        const entry = this.pending.get(msg.id);
        if (!entry) {
            return;
        }
        this.pending.delete(msg.id);
        if (msg.ok) {
            entry.resolve(msg.dump);
        } else {
            entry.reject(new Error(msg.error));
        }
    }

    /** Parses raw parquet bytes into a por-que dump JSON string. */
    parse(bytes: ArrayBuffer, name: string): Promise<string> {
        const worker = this.ensureWorker();
        const id = this.nextId++;
        const manifestUrl = new URL('vendor/manifest.json', document.baseURI).href;
        return new Promise<string>((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            const req: ParseRequest = { id, name, bytes, manifestUrl };
            worker.postMessage(req, [bytes]);
        });
    }
}
