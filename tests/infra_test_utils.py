import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch


def load_infra_api():
    # Loading Arrow schemas and runtime config is unnecessary for health tests.
    dependencies = {}
    for name, exports in {
        "cartageddon.common": {"RunTimeContext": SimpleNamespace},
        "cartageddon.schema": {"OpenCartSchema": Mock()},
        "utils": {"get_runtime_context": Mock()},
    }.items():
        module = ModuleType(name)
        module.__dict__.update(exports)
        dependencies[name] = module

    path = Path(__file__).resolve().parents[1] / "infra" / "api.py"
    spec = importlib.util.spec_from_file_location("infra_health_under_test", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict("sys.modules", dependencies):
        spec.loader.exec_module(module)
    return module

