import sys
from pathlib import Path
from unittest.mock import MagicMock

# metrics-exporter 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

# trino 클라이언트가 테스트 환경에 없으므로 stub 처리
sys.modules.setdefault("trino", MagicMock())
sys.modules.setdefault("trino.dbapi", MagicMock())

import pytest
from fastapi.testclient import TestClient
from main import app


@pytest.fixture
def client():
    return TestClient(app)
