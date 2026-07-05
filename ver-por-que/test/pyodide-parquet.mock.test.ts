import { describe, it, expect, vi } from 'vitest';
import { createParquetParser, type LoadPyodide } from '../src/js/worker/pyodide-parquet';

/**
 * Drives createParquetParser against a fake pyodide so the boot sequence and
 * status protocol are covered without downloading a 12MB runtime.
 */
function fakePyodide() {
    const install = Object.assign(vi.fn().mockResolvedValue(undefined), {
        callKwargs: vi.fn().mockResolvedValue(undefined),
    });
    const dump = vi.fn().mockResolvedValue('{"dumped":true}');
    return {
        loadPackage: vi.fn().mockResolvedValue(undefined),
        pyimport: vi.fn().mockReturnValue({ install }),
        FS: { writeFile: vi.fn() },
        runPythonAsync: vi.fn().mockResolvedValue(undefined),
        globals: { get: vi.fn().mockReturnValue(dump) },
        _install: install,
        _dump: dump,
    };
}

describe('createParquetParser (mocked pyodide)', () => {
    it('boots, installs the wheel deps-free, and parses', async () => {
        const py = fakePyodide();
        const statuses: string[] = [];
        const bytes = new Uint8Array([1, 2, 3]);

        const parse = await createParquetParser({
            loadPyodide: (() => Promise.resolve(py)) as unknown as LoadPyodide,
            loadWheel: () => Promise.resolve({ filename: 'por_que-9.9.9-py3-none-any.whl', bytes }),
            onStatus: s => statuses.push(s),
        });

        // Wheel installed by exact filename, without resolving deps.
        expect(py.FS.writeFile).toHaveBeenCalledWith('/tmp/por_que-9.9.9-py3-none-any.whl', bytes);
        expect(py._install.callKwargs).toHaveBeenCalledWith(
            'emfs:/tmp/por_que-9.9.9-py3-none-any.whl',
            { deps: false }
        );
        expect(statuses).toContain('Loading Python runtime...');
        expect(statuses).toContain('Installing por-que...');

        const input = new Uint8Array([0x50, 0x41, 0x52, 0x31]);
        const dump = await parse(input, 'x.parquet');
        expect(dump).toBe('{"dumped":true}');
        expect(py._dump).toHaveBeenCalledWith(input, 'x.parquet');
        expect(statuses).toContain('Parsing parquet...');
    });
});
