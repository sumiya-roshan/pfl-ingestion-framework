from .lookup_query_builder import build_lookup_query, detect_pattern_type

__all__ = ["LookupExecutor", "build_lookup_query", "detect_pattern_type"]


def __getattr__(name):
    # Lazy: importing lookup.lookup_query_builder must not pull in
    # LookupExecutor (and the whole connector factory) as a side effect.
    if name == "LookupExecutor":
        from .lookup_executor import LookupExecutor

        return LookupExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
