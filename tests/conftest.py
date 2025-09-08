import pytest


@pytest.fixture(autouse=True)
def set_tmp_argus_dir(tmp_path, monkeypatch):
    argus_path = tmp_path / ".argus"
    argus_path.mkdir()
    monkeypatch.setenv("ARGUS_DIR", str(argus_path))
    yield
