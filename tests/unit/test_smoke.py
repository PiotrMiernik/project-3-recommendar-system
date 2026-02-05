# tests/unit/test_smoke.py
import importlib

def test_important_common_modules():
    """
    Smoke test: the project should import in a clean environment.
    If imports fail here, CI should fail fast. 
    """
    moduls = [
        "src.common.config",
        "src.common.hashing",
        "src.common.logging"
    ]

    for m in moduls:
        importlib.import_module(m)