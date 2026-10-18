"""Format probes: measure a provider bundle and freeze its wide CSV schemas.

`tracklab freeze-schema <sport>` lives here. Each sport module walks the raw
bundle once, writes one report row per match and the `*.schema.json` files
that bronze later imposes on the CSVs (see tasks/todo.md, "Capa bronze").
"""
