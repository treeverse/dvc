import inspect
import os
from pathlib import Path

from dvc import daemon


def test_no_stdlib_types_shadow():
    # Regression for https://github.com/iterative/dvc/issues/11035.
    #
    # When the daemon worker is launched as `python <site-packages>/dvc/__main__.py`
    # (the path the daemon takes on Windows -- see `_get_dvc_args` and
    # `_spawn_worker` in dvc/repo/experiments/queue/celery.py), PEP 338 prepends
    # the script's directory to `sys.path[0]`. If the dvc package directory
    # contains a module whose name shadows a stdlib module the interpreter
    # imports during startup (notably `types`, on Python 3.14+ where
    # stdlib `typing.py` does `from types import GenericAlias` very early),
    # the shadowed import resolves to the dvc-package copy and the worker
    # dies before any user code runs -- on the user side `dvc queue start`
    # reports success but no worker actually comes up.
    #
    # The structural fix is simply: don't ship a top-level `dvc/types.py`.
    # The internal aliases that used to live there now live in
    # `dvc/_types.py` (leading underscore, no stdlib name to collide with).
    # This test fails fast if someone reintroduces `dvc/types.py`.
    dvc_pkg = Path(daemon.__file__).parent
    assert not (dvc_pkg / "types.py").exists(), (
        "dvc/types.py shadows stdlib `types` and breaks the Windows-path "
        "daemon worker on Python 3.14+ (issue #11035). Use dvc/_types.py."
    )
    # Defensive: even if someone re-imports stdlib `types` via the dvc
    # package, it must be the stdlib one.
    import types as types_mod

    assert "dvc" not in (types_mod.__file__ or ""), (
        f"stdlib `types` resolved to a dvc-shadowed module: {types_mod.__file__}"
    )


def test_daemon(mocker):
    mock = mocker.patch("dvc.daemon._spawn")
    daemon.daemon(["updater"])

    mock.assert_called()
    args = mock.call_args[0]
    env = args[2]
    assert "PYTHONPATH" in env

    file_path = os.path.abspath(inspect.stack()[0][1])
    file_dir = os.path.dirname(file_path)
    test_dir = os.path.dirname(file_dir)
    dvc_dir = os.path.dirname(test_dir)
    assert env["PYTHONPATH"] == dvc_dir
    assert env[daemon.DVC_DAEMON] == "1"


def test_no_recursive_spawn(mocker):
    mocker.patch.dict(os.environ, {daemon.DVC_DAEMON: "1"})
    mock_spawn = mocker.patch("dvc.daemon._spawn")
    daemon.daemon(["updater"])
    mock_spawn.assert_not_called()
