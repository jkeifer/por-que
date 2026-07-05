/**
 * Boots pyodide and runs por-que against raw parquet bytes, returning the same
 * dump JSON string that `por-que dump` produces server-side.
 *
 * This module is deliberately worker-agnostic: it takes `loadPyodide` and a
 * wheel loader as parameters so it can run under a real Web Worker in the
 * browser AND under plain node in vitest (test/pyodide-parquet.*.test.ts).
 * The actual worker file (worker.ts) is a thin postMessage shell around this.
 */

// Type-only: erased at build time, so the browser bundle never imports the
// npm pyodide package (it loads the runtime from the CDN instead).
import type { PyodideInterface } from 'pyodide';

export type LoadPyodide = (options?: { indexURL?: string }) => Promise<PyodideInterface>;

export interface WheelAsset {
    filename: string;
    bytes: Uint8Array;
}

export interface ParquetParserDeps {
    loadPyodide: LoadPyodide;
    /** Fetches the por-que wheel to install. Lazy: only called during boot. */
    loadWheel: () => Promise<WheelAsset>;
    /** Where pyodide's core files live. Omit under node (npm ships them). */
    indexURL?: string;
    /** Surfaces coarse boot/parse progress to the UI. */
    onStatus?: (status: string) => void;
}

/** Parses raw parquet bytes into a por-que dump JSON string. */
export type ParquetParser = (bytes: Uint8Array, name: string) => Promise<string>;

const PARSE_PY = `
import io
from por_que import ParquetFile

async def _dump(data, name):
    pf = await ParquetFile.from_reader(io.BytesIO(bytes(data.to_py())), name)
    return pf.to_json()
`;

/**
 * Boots pyodide once and returns a parser. Boot is expensive (~12MB runtime on
 * first browser load); reuse the returned parser across files.
 */
export async function createParquetParser(deps: ParquetParserDeps): Promise<ParquetParser> {
    const status = deps.onStatus ?? (() => {});

    status('Loading Python runtime...');
    const pyodide = await deps.loadPyodide(deps.indexURL ? { indexURL: deps.indexURL } : undefined);

    status('Installing dependencies...');
    // Compiled wheels bundled with the pyodide distribution. aiohttp is needed
    // because por_que/__init__ imports hctef.aio (-> aiohttp) at import time.
    await pyodide.loadPackage(['micropip', 'pydantic', 'zstandard', 'brotli', 'aiohttp']);

    // micropip's PyProxy is untyped; keep the surface we use narrow.
    const micropip = pyodide.pyimport('micropip') as {
        install: {
            (spec: string): Promise<void>;
            callKwargs(spec: string, kwargs: { deps: boolean }): Promise<void>;
        };
    };
    // hctef is pure-python and installs from PyPI (zero hard deps).
    await micropip.install('hctef');

    status('Installing por-que...');
    const wheel = await deps.loadWheel();
    const wheelPath = `/tmp/${wheel.filename}`;
    pyodide.FS.writeFile(wheelPath, wheel.bytes);
    // deps=False: everything por-que needs is already loaded above, and its
    // declared deps include no-pure-wheel packages (cramjam via hctef[async],
    // python-snappy) that we intentionally skip -- the structure dump never
    // decompresses page content. The wheel filename must stay intact (micropip
    // parses name/version from it).
    // ponytail: local wheel because the PyPI release predates the current dump
    // format; pin a PyPI version here once a release ships it.
    await micropip.install.callKwargs(`emfs:${wheelPath}`, { deps: false });

    await pyodide.runPythonAsync(PARSE_PY);
    const dump = pyodide.globals.get('_dump') as (
        data: Uint8Array,
        name: string
    ) => Promise<string>;

    return async (bytes: Uint8Array, name: string): Promise<string> => {
        status('Parsing parquet...');
        const result = await dump(bytes, name);
        return result;
    };
}
