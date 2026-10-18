# Copied from tracklab@686508a: pipeline/src/tracklab/formats/__init__.py
# Changes: docstring: the freeze command is not part of ftbase.
"""Format helpers: the frozen CSV schemas that bronze imposes on the provider's CSVs.

`csvschema` loads a `*.schema.json` and reads a CSV against it. The freeze itself
(walking the raw bundle to write those files) stayed in tracklab; ftbase ships the
two football schemas it produced.
"""
