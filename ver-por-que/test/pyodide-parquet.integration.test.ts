import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, it, expect } from 'vitest';
import { loadPyodide } from 'pyodide';
import { createParquetParser, type LoadPyodide } from '../src/js/worker/pyodide-parquet';
import validate from '../src/generated/validate';

/**
 * End-to-end: boot real pyodide, install the locally-built por-que wheel, parse
 * a real parquet file, and assert the dump passes the same ajv validator the
 * app uses. Skipped (with a message) when the wheel is absent -- run
 * `npm run wheel` first, which CI does.
 *
 * alltypes_plain.snappy.parquet is deliberate: it exercises the snappy-less
 * path (there is no wasm snappy wheel) and proves a SNAPPY file still dumps.
 */
const vendorDir = fileURLToPath(new URL('../static/vendor/', import.meta.url));
const manifestPath = `${vendorDir}manifest.json`;
const hasWheel = existsSync(manifestPath);

// Pinned to the same apache/parquet-testing ref as the python test fixtures.
const PARQUET_TESTING_REF = '1a2a75127be06fc0123f03ebd36c966f7beda27d';
const PARQUET_URL =
    `https://raw.githubusercontent.com/apache/parquet-testing/` +
    `${PARQUET_TESTING_REF}/data/alltypes_plain.snappy.parquet`;

describe.skipIf(!hasWheel)('createParquetParser (real pyodide)', () => {
    if (!hasWheel) {
        it('skipped: run `npm run wheel` to build the por-que wheel first', () => {});
        return;
    }

    it('parses a real parquet file into a schema-valid dump', async () => {
        const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as { wheel: string };
        const bytes = new Uint8Array(readFileSync(`${vendorDir}${manifest.wheel}`));

        // Under vitest, pyodide can't auto-locate its own dist files (the
        // transform breaks its __dirname detection); point it at the package.
        const indexURL = fileURLToPath(new URL('../node_modules/pyodide/', import.meta.url));
        const parse = await createParquetParser({
            loadPyodide: loadPyodide as unknown as LoadPyodide,
            indexURL,
            loadWheel: () => Promise.resolve({ filename: manifest.wheel, bytes }),
        });

        const data = new Uint8Array(await (await fetch(PARQUET_URL)).arrayBuffer());
        const dump = await parse(data, 'alltypes_plain.snappy.parquet');

        const parsed: unknown = JSON.parse(dump);
        const ok = validate(parsed);
        if (!ok) {
            throw new Error(
                `dump failed schema validation: ${JSON.stringify(validate.errors?.slice(0, 3))}`
            );
        }
        expect(ok).toBe(true);
        expect((parsed as { column_chunks?: unknown }).column_chunks).toBeDefined();
    }, 180_000);
});
