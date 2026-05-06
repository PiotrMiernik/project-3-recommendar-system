import importlib


def test_important_common_modules():
    """
    Smoke test: important lightweight project modules should import correctly.
    If imports fail here, CI should fail fast.
    """
    modules = [
        "src.common.logging",
        "src.common.embedding_utils",
    ]

    for module in modules:
        importlib.import_module(module)