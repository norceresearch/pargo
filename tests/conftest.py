import pytest


@pytest.fixture(autouse=True)
def set_tmp_pargo_dir(tmp_path, monkeypatch):
    pargo_path = tmp_path / ".pargo"
    pargo_path.mkdir()
    monkeypatch.setenv("PARGO_DIR", str(pargo_path))
    yield
