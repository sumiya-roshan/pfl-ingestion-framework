__all__ = ["LookupExecutor"]


def __getattr__(name):
    # Lazy re-export: importing lookup.lookup_query_builder must not drag in
    # LookupExecutor (and the whole connector factory) as a side effect.
    if name == "LookupExecutor":
        from .lookup_executor import LookupExecutor
        return LookupExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
