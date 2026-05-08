import sys
from unittest.mock import MagicMock

# pyspark가 테스트 환경에 없으므로 import 시점에 stub으로 대체
for mod in [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.streaming",
]:
    sys.modules.setdefault(mod, MagicMock())
