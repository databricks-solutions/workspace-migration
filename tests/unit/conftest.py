"""Unit test setup: mock pyspark.sql.functions for databricks-connect compatibility.

databricks-connect provides a stub pyspark that doesn't include pyspark.sql.functions.
We inject a mock module so that runtime imports in tracking.py and summary.py succeed
during unit tests.
"""

import sys
from types import ModuleType
from unittest.mock import MagicMock

if "pyspark.sql.functions" not in sys.modules:
    _mock_funcs = ModuleType("pyspark.sql.functions")
    _mock_funcs.current_timestamp = MagicMock()  # type: ignore[attr-defined]
    _mock_funcs.col = MagicMock()  # type: ignore[attr-defined]
    _mock_funcs.count = MagicMock()  # type: ignore[attr-defined]
    _mock_funcs.when = MagicMock()  # type: ignore[attr-defined]
    _mock_funcs.sum = MagicMock()  # type: ignore[attr-defined]
    sys.modules["pyspark.sql.functions"] = _mock_funcs

if "pyspark.sql.types" not in sys.modules:
    _mock_types = ModuleType("pyspark.sql.types")
    for _t in (
        "BooleanType",
        "DoubleType",
        "LongType",
        "StringType",
        "TimestampType",
        "StructField",
        "StructType",
    ):
        setattr(_mock_types, _t, MagicMock())
    sys.modules["pyspark.sql.types"] = _mock_types
