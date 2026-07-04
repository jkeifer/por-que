# Workshop

The workshop teaches Parquet internals hands-on by progressively filtering the
[Overture Maps](https://overturemaps.org/) buildings dataset — hundreds of
~0.5-1 GB partition files — down to a handful of rows: file bounding boxes →
row-group statistics → pages → values. It is designed to run inside a
resource-constrained GitHub Codespace (2 cores, 4-8 GB RAM) on a workshop
clock.

## Setup

Open the repository in a GitHub Codespace and run `example-notebook.ipynb` top
to bottom. Maintenance notes for the notebook itself live in the
[notebook authoring](../project/notebook-authoring.md) guide.

## The rendered notebook

The [Notebook](notebook.md) page is **generated** from
`example-notebook.ipynb` — it is not written by hand and is not checked into
git. `scripts/render-notebook.py` runs `jupyter nbconvert` to produce it, both
locally and in CI before the site is built:

```bash
uv run python scripts/render-notebook.py
uv run zensical build
```

If the notebook is not present in your checkout (some branches do not carry
it), the render script writes a short placeholder in its place so the site
still builds.
