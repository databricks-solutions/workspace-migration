"""SDK-drift guard (findings #5, #17).

Asserts the databricks-sdk API surface the tool depends on still exists in
the *installed* SDK. When a future SDK bump renames a method the code calls
(as happened with ``SharesAPI.list()`` -> ``list_shares()`` in 0.6x, and the
missing ``ModelVersionsAPI.create``), this fails in CI before it reaches a
customer's silent WARN / swallowed error.

Introspects the API *classes* directly — no WorkspaceClient instantiation,
no network. Keep this in sync with the pinned ``databricks-sdk`` version:
when you bump the pin, run this; if it goes red, a method was renamed and a
shim (or call-site update) is needed.
"""

import pytest
from databricks.sdk.service.catalog import (
    CatalogsAPI,
    ConnectionsAPI,
    MetastoresAPI,
    SchemasAPI,
)
from databricks.sdk.service.sharing import SharesAPI

# (label, api class, method names — AT LEAST ONE must exist).
# One-of semantics let a shim span the old and new name (e.g. shares).
REQUIRED_SURFACE = [
    ("shares.list (#5 shim: list_shares|list)", SharesAPI, ("list_shares", "list")),
    ("connections.list (#6)", ConnectionsAPI, ("list",)),
    ("catalogs.list (foreign-catalog discovery)", CatalogsAPI, ("list",)),
    ("schemas.list", SchemasAPI, ("list",)),
    ("metastores.summary (pre_check)", MetastoresAPI, ("summary",)),
]


@pytest.mark.parametrize("label,api_cls,methods", REQUIRED_SURFACE)
def test_sdk_method_surface(label, api_cls, methods):
    present = [m for m in methods if hasattr(api_cls, m)]
    assert present, (
        f"SDK drift: {api_cls.__name__} has none of {methods} for {label!r}. "
        f"The installed databricks-sdk renamed/removed it — add a compat shim "
        f"and update the call site (see finding #5)."
    )


def test_shares_rename_is_covered_by_shim():
    """SharesAPI.list was renamed to list_shares — the pre_check shim
    (_list_shares) and catalog_utils both prefer list_shares with a .list
    fallback. If BOTH ever disappear, the shim breaks: fail loudly here."""
    assert hasattr(SharesAPI, "list_shares") or hasattr(SharesAPI, "list")
