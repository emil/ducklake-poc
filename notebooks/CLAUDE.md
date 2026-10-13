# CLAUDE.md (notebooks/)

This file gives guidance to Claude Code for work on the marimo notebooks in this directory.
It adds to the root `CLAUDE.md` and `QUERYING.md`. The query rules there also apply to each
query in a notebook.

The marimo project publishes a general guide for AI assistants at
<https://docs.marimo.io/CLAUDE.md>. The rules below are the parts of it that apply here, with
the choices of this project.

## Workflow

Run the notebook in watch mode. marimo then reloads the browser when Claude Code changes the
file on disk:

```bash
cd src
uv run marimo edit --watch ../notebooks/querying_the_lake.py
```

Start `claude` in a second terminal. Point it at a working notebook as an example:
`See @notebooks/querying_the_lake.py for an example marimo notebook.`

After each edit, check the notebook:

```bash
uv run marimo check --fix ../notebooks/querying_the_lake.py   # formatting and common pitfalls
uv run python ../notebooks/querying_the_lake.py               # run as a script, catches runtime errors
```

## Rules for cells

- Edit only the body of a function with `@app.cell`. marimo adds the parameters and the
  `return` statement.
- A variable has one definition in the whole notebook. Do not redeclare it in another cell.
  Prefix a local name with an underscore (`_df`) to keep it inside its cell.
- The dependency graph must have no cycles.
- Do not read `.value` of a UI element in the cell that defines it.
- The last expression of a cell is its output. For matplotlib, end with `plt.gca()`. Do not call
  `plt.show()`.
- Import modules in the imports cell. Always import `marimo as mo` there.
- Do not use `global`.
- Keep the `layouts/querying_the_lake.slides.json` file in step with the cells. Do not delete a
  cell that a slide uses without a check.

## Rules for data

- Each query follows `QUERYING.md`: partition filters, one pinned `data_version`, bound
  parameters, and reduction in SQL before a plot.
- This notebook uses pandas and matplotlib. Do not add polars or altair only because the
  general marimo guide names them.
- Write dataframe logic as small functions. Then you can use `DataFrame.pipe()`.
- Write documentation in ASD-STE100 Simplified Technical English, like the rest of the repo.
