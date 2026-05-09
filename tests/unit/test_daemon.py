import inspect
import os
import sys

from dvc import daemon


def test_get_dvc_args_uses_inline_bootstrap(mocker):
    # Regression for https://github.com/iterative/dvc/issues/11035.
    # The daemon must not be launched as `python <site-packages>/dvc/__main__.py`,
    # which puts the dvc package directory on sys.path[0] and shadows the stdlib
    # `types` module via dvc/types.py -- fatal on Python 3.14+ at interpreter
    # startup (stdlib re/enum import from types).
    #
    # We also avoid `python -m dvc`, which sets
    # `__main__.__spec__.name == "dvc.__main__"` and breaks billiard /
    # celery's main-module fixup on Windows (the celery worker backing
    # `dvc queue start` crashes -- see #11037 CI).
    #
    # The accepted shape is `python -c "<inline bootstrap>"`: keeps
    # sys.path[0] empty (no stdlib shadow) AND keeps `__main__` script-
    # shaped (no `__spec__`), so billiard's existing path is preserved.
    mocker.patch("dvc.daemon.is_binary", return_value=False)
    args = daemon._get_dvc_args()
    assert args[0] == sys.executable
    assert args[1] == "-c"
    assert len(args) == 3
    bootstrap = args[2]
    # The inline bootstrap must import via the regular package path
    # (`from dvc.cli import main`) -- never reference __main__.py, never
    # use `runpy` / `-m dvc`, never inject the package dir onto sys.path.
    assert "from dvc.cli import main" in bootstrap
    assert "__main__" not in bootstrap
    assert "runpy" not in bootstrap
    assert not any(a.endswith("__main__.py") for a in args)
    assert "-m" not in args


def test_get_dvc_args_binary(mocker):
    # When packaged as a PyInstaller binary, args is just the executable.
    mocker.patch("dvc.daemon.is_binary", return_value=True)
    args = daemon._get_dvc_args()
    assert args == [sys.executable]


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
