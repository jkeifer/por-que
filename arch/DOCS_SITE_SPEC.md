# Documentation Site Spec (Zensical)

> **Status: historical spec (implemented).** This is the design the docs site
> was built from. Kept in `arch/` as the record; not published.

Spec for a por-que documentation site built with
[Zensical](https://zensical.org) — the static site generator from the
Material for MkDocs team. Written 2026-07-04; verify Zensical's release
notes before implementing, it is evolving quickly (mkdocstrings support
became available in v0.0.11 and the team has said API reference rendering
will be reworked during 2026).

## Goals and audiences

Three audiences, in priority order — the site structure below maps to them:

1. **Learners** — people who want to understand parquet itself. This is the
   project's reason to exist; the docstrings are already written as teaching
   material, and the site's job is to surface them.
2. **Library users** — people using por-que to inspect real files (API
   reference, how-tos, hctef usage).
3. **Maintainers/contributors** — architecture notes, fixture updating,
   notebook authoring (docs that already exist in `arch/`).

Non-goals for v1: versioned docs (Zensical has no mike-equivalent yet —
publish "latest" only and revisit), hctef's own site (separate repo; link
out), embedding ver-por-que (tracked as the CLI stretch goal instead).

## Tooling

- **Zensical**, installed via a `docs` dependency group:
  `uv add --group docs zensical mkdocstrings-python`. Pin both — Zensical is
  pre-1.0 and moving.
- **Config**: `zensical.toml` at repo root (native format; content is plain
  Markdown so a fallback to mkdocs+material would be config-only work if
  Zensical ever blocks us).
- **API reference**: mkdocstrings via Zensical's preliminary integration:

  ```toml
  [project.plugins.mkdocstrings.handlers.python]
  inventories = ["https://docs.python.org/3/objects.inv"]
  paths = ["src"]

  [project.plugins.mkdocstrings.handlers.python.options]
  docstring_style = "google"   # matches existing Args:/Returns: style
  show_source = true           # teaching library: the source IS the content
  ```

  Known limitation at spec time: backlinks unsupported. Acceptable.
- **Commands**: `uv run zensical serve` (author loop), `uv run zensical
  build` (CI).

## Information architecture

```
docs/
  index.md                  # what/why (¿por qué?), positioning, quick tour
  learn/                    # the teaching content — the site's heart
    anatomy.md              # magic bytes, footer, metadata-last design
    metadata-and-thrift.md  # thrift compact protocol, why parsers look how they look
    schema-tree.md          # schema elements, repetition/definition levels
    row-groups-and-chunks.md
    pages-and-encodings.md
    statistics-and-pruning.md   # stats, page index, the 3-stage pruning story
    reconstruction.md       # from columns back to records (structuring/)
    going-deeper.md         # bloom filters (post wave-2), encryption, pointers out
  workshop/
    index.md                # what the workshop covers, codespace setup
    notebook.md             # rendered notebook (see below)
  guides/
    reading-remote-files.md # hctef, range requests, disk cache env vars
    fast-metadata.md        # projection (columns=), staged access, process pools
    staged-access.md        # bounds -> metadata -> structure -> data
    serialization.md        # THE JSON CONTRACT: format_version, schema_path
                            # reference normalization — ver-por-que's contract page
  reference/                # mkdocstrings pages, one per public module:
                            # por_que, file_metadata, pages, physical,
                            # exceptions, protocols, parsers (curated subset)
  project/
    architecture.md         # curated from arch/ (see audit note)
    updating-fixtures.md    # the one-command flow from jak/fixtures-pin
    notebook-authoring.md   # arch/NOTEBOOK_AUTHOR_GUIDE.md moves here
    cli.md                  # status: design docs exist, ships as por-que[cli];
                            # ver-por-que local launch documented as stretch
    roadmap.md
```

- **`arch/` audit**: `arch/*.md` are point-in-time plans, some superseded
  (e.g. ARCHITECTURE.md describes a refactoring plan that has since landed
  differently). Migration rule: each file is either (a) rewritten into
  `docs/project/` as current-state documentation, (b) kept in `arch/` as a
  historical design record and NOT published, or (c) deleted. Do the audit as
  its own reviewable commit.
- **Learn section sourcing**: much of this prose already exists in module
  docstrings and "Teaching Points" comments. Pages should link INTO the API
  reference rather than duplicating docstrings; each learn page is narrative
  + pointers, not a second copy to keep in sync.
- **Notebook rendering**: Zensical has no mkdocs-jupyter equivalent. v1:
  a small build-prep script (`scripts/render-notebook.py`) runs
  `jupyter nbconvert --to markdown example-notebook.ipynb` into
  `docs/workshop/notebook.md` in CI (nbconvert in the docs dependency
  group). Fallback if conversion output is poor: link to the GitHub render.

## Build and deploy

- **CI**: new `.github/workflows/docs.yml`:
  - PRs touching `docs/`, `src/`, `zensical.toml`, or the notebook: build the
    site (`uv run zensical build`) as a check; fail on build errors.
  - Push to main: build + deploy to GitHub Pages via
    `actions/upload-pages-artifact` + `actions/deploy-pages` (no gh-pages
    branch juggling).
- Building runs mkdocstrings over `src/` — a broken docstring becomes a docs
  build failure on PR, which is the enforcement mechanism for docstring
  quality this teaching library wants anyway.
- `uv run zensical serve` documented in the contributor section for local
  preview. Note Zensical's dev-server limitation: files outside the project
  directory don't trigger rebuilds.

## Implementation plan (future agent; separate reviewable commits)

1. Scaffold: dependency group, `zensical.toml`, `docs/index.md`, nav, CI
   workflow building (not yet deploying). Site builds green.
2. `arch/` audit + `project/` section (includes moving
   NOTEBOOK_AUTHOR_GUIDE.md; leave a stub pointing at its new home).
3. Reference section + docstring pass: ensure every public module renders
   cleanly under mkdocstrings; fix formatting-only docstring issues (no
   content rewrites without review).
4. Learn + guides content (largest; can split per section). The
   serialization-contract page should land with or before the v0.3.0 release
   since ver-por-que depends on it.
5. Notebook rendering step + deploy flip.

## Risks / revisit triggers

- Zensical pre-1.0 churn: pin versions; expect config tweaks on upgrade.
- mkdocstrings integration is preliminary; the team announced a ground-up
  API-reference rework during 2026 — expect to revisit the reference section
  when that ships.
- No versioned docs until Zensical grows the feature; fine while por-que is
  0.x.
