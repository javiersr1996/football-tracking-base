# Copied from tracklab@686508a: pipeline/src/tracklab/bronze/__init__.py
# Changes: module name in the docstring.
"""Bronze layer: the provider's raw files as typed Parquet, one file per match and table.

Native names, native rows, nothing filtered and nothing derived; the only
transformations are type casts declared in each table's schema. See
``ftbase.bronze.common`` for the layout, lineage and writer every builder shares.
"""
