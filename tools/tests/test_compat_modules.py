from compat import ExpressionProxy
from compat import TSBridge
from compat import register_fake_sqlglot
from compat.bridge import PROJECT_ROOT
from compat.proxy import ExpressionProxy as ProxyModuleExpressionProxy
from compat.registration import register_fake_sqlglot as register_from_module


def test_compat_package_reexports_split_modules() -> None:
    assert PROJECT_ROOT.name == "sqlglot-ts"
    assert TSBridge.__module__ == "compat.bridge"
    assert ExpressionProxy is ProxyModuleExpressionProxy
    assert register_fake_sqlglot is register_from_module
