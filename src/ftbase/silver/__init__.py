"""Silver: conformed frames and events in metres and silver keys, built from bronze with DuckDB.

``ftbase silver`` builds it (:func:`ftbase.silver.build.build_silver`); the readers below
are the public way to read it back.
"""

from ftbase.silver.read import read_dim, read_events, read_frames

__all__ = ["read_dim", "read_events", "read_frames"]
