import os
import tempfile

import pytest

# Point the data directory at a throwaway location *before* importing the app.
_TMP = tempfile.mkdtemp(prefix="batchlog-tests-")
os.environ.setdefault("BATCHLOG_DATA_DIR", os.path.join(_TMP, "data"))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c
