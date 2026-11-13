import pytest

from dvc.cli import parse_args
from dvc.commands.experiments.run import CmdExperimentsRun
from dvc.commands.repro import CmdRepro

common_arguments = {
    "all_pipelines": False,
    "downstream": False,
    "dry": False,
    "force": False,
    "interactive": False,
    "pipeline": False,
    "single_item": False,
    "recursive": False,
    "force_downstream": False,
    "pull": False,
    "allow_missing": False,
    "targets": [],
    "on_error": "fail",
}
repro_arguments = {
    "run_cache": True,
    "no_commit": False,
    "glob": False,
}


def test_default_arguments(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    cmd.repo.reproduce.assert_called_with(**common_arguments, **repro_arguments)


def test_downstream(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro", "--downstream"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    arguments = common_arguments.copy()
    arguments.update(repro_arguments)
    arguments.update({"downstream": True})
    cmd.repo.reproduce.assert_called_with(**arguments)


@pytest.mark.parametrize(
    "args, configured_pull, expected",
    [
        ([], False, False),
        ([], True, True),
        (["--pull"], False, True),
        (["--no-pull"], True, False),
    ],
)
def test_pull_option_precedence(dvc, mocker, args, configured_pull, expected):
    with dvc.config.edit() as config:
        config["core"]["repro_pull"] = configured_pull

    cmd = CmdRepro(parse_args(["repro", *args]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()

    arguments = common_arguments.copy()
    arguments.update(repro_arguments)
    arguments["pull"] = expected
    cmd.repo.reproduce.assert_called_with(**arguments)


def test_pull_option_respects_config_scope_precedence(dvc, mocker):
    with dvc.config.edit() as config:
        config["core"]["repro_pull"] = False
    with dvc.config.edit("local") as config:
        config["core"]["repro_pull"] = True

    cmd = CmdRepro(parse_args(["repro"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()

    arguments = common_arguments.copy()
    arguments.update(repro_arguments)
    arguments["pull"] = True
    cmd.repo.reproduce.assert_called_with(**arguments)


@pytest.mark.parametrize(
    "args, expected",
    [
        ([], False),
        (["--pull"], True),
    ],
)
def test_experiments_run_ignores_repro_pull_config(dvc, args, expected):
    with dvc.config.edit() as config:
        config["core"]["repro_pull"] = True

    cmd = CmdExperimentsRun(parse_args(["exp", "run", *args]))
    assert cmd._common_kwargs["pull"] is expected
