import doctest
import pytest
from importlib import import_module
from pathlib import Path

def path_to_module_name(path):
    if path.name == "__init__.py":
        path = path.parent

    return str(path.with_suffix("")).replace("/", ".")

lib = Path(__file__).parent.parent / "lib"

modules = [
    path_to_module_name(path.relative_to(lib))
        for path in lib.glob("**/*.py") ]

@pytest.mark.parametrize("module_name", modules)
def test_doc(module_name):
    module = import_module(module_name)

    failures, tests = doctest.testmod(module)

    assert failures == 0, f"{module_name} failed doctest"
