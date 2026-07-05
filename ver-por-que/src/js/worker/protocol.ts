/** Message shapes exchanged between main thread and the parquet worker. */

export interface ParseRequest {
    id: number;
    name: string;
    /** Raw parquet bytes; transferred (not copied) to the worker. */
    bytes: ArrayBuffer;
    /** Absolute URL of the wheel manifest, resolved against the document. */
    manifestUrl: string;
}

export interface ParseSuccess {
    id: number;
    ok: true;
    dump: string;
}

export interface ParseFailure {
    id: number;
    ok: false;
    error: string;
}

/** Unsolicited boot/parse progress, surfaced in the loading-status element. */
export interface StatusEvent {
    status: string;
}

export type WorkerResponse = ParseSuccess | ParseFailure | StatusEvent;
