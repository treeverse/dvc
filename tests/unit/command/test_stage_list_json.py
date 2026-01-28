import json

import pytest

from dvc.cli import parse_args
from dvc.commands.stage import CmdStageList
from dvc.dependency import ParamsDependency
from dvc.output import Output
from dvc.stage import PipelineStage
from dvc_data.hashfile.hash_info import HashInfo


def _add_deps(stage, deps):
    from dvc.dependency import Dependency

    for dep_path in deps:
        stage.deps.append(Dependency(stage, dep_path))


def _add_params(stage, params):
    for param_file, param_values in params.items():
        param_dep = ParamsDependency(
            stage, param_file, params=list(param_values.keys())
        )
        param_dep.hash_info = HashInfo("params", param_values)
        stage.deps.append(param_dep)


def _add_outs(stage, outs, metric=False, plot=False):
    for out_path in outs:
        stage.outs.append(Output(stage, out_path, metric=metric, plot=plot))


def _create_mock_stage(
    dvc,
    name,
    cmd,
    deps=None,
    outs=None,
    metrics=None,
    plots=None,
    params=None,
    desc=None,
):
    stage = PipelineStage(dvc, "dvc.yaml", cmd=cmd, name=name)
    stage.desc = desc
    _add_deps(stage, deps or [])
    _add_params(stage, params or {})
    _add_outs(stage, outs or [])
    _add_outs(stage, metrics or [], metric=True)
    _add_outs(stage, plots or [], plot=True)
    return stage


@pytest.mark.parametrize(
    "stages_data, expected_json",
    [
        pytest.param(
            [
                {
                    "name": "train",
                    "cmd": "python train.py --lr 0.001",
                    "deps": ["data/train.csv", "src/train.py"],
                    "outs": ["model.pkl"],
                    "metrics": ["metrics.json"],
                    "desc": "Train the model",
                }
            ],
            {
                "train": {
                    "cmd": "python train.py --lr 0.001",
                    "deps": ["data/train.csv", "src/train.py"],
                    "outs": ["model.pkl"],
                    "metrics": ["metrics.json"],
                    "plots": [],
                    "params": {},
                    "desc": "Train the model",
                }
            },
            id="simple_stage",
        ),
        pytest.param(
            [
                {
                    "name": "preprocess",
                    "cmd": "python preprocess.py",
                    "deps": ["raw_data.csv"],
                    "outs": ["processed_data.csv"],
                    "params": {"params.yaml": {"preprocess.threshold": 0.5}},
                }
            ],
            {
                "preprocess": {
                    "cmd": "python preprocess.py",
                    "deps": ["raw_data.csv"],
                    "outs": ["processed_data.csv"],
                    "metrics": [],
                    "plots": [],
                    "params": {"params.yaml": {"preprocess.threshold": 0.5}},
                    "desc": None,
                }
            },
            id="stage_with_params",
        ),
        pytest.param(
            [
                {
                    "name": "evaluate",
                    "cmd": "python evaluate.py",
                    "plots": ["plots/confusion.png", "plots/roc.png"],
                }
            ],
            {
                "evaluate": {
                    "cmd": "python evaluate.py",
                    "deps": [],
                    "outs": [],
                    "metrics": [],
                    "plots": ["plots/confusion.png", "plots/roc.png"],
                    "params": {},
                    "desc": None,
                }
            },
            id="stage_with_plots",
        ),
    ],
)
def test_stage_list_json(dvc, mocker, capsys, stages_data, expected_json):
    cli_args = parse_args(["stage", "list", "--json"])
    assert cli_args.func == CmdStageList

    cmd = cli_args.func(cli_args)

    mock_stages = [_create_mock_stage(dvc, **data) for data in stages_data]
    mocker.patch.object(cmd, "_get_stages", return_value=mock_stages)

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)
    assert result == expected_json


def test_stage_list_json_multiple_stages(dvc, mocker, capsys):
    cli_args = parse_args(["stage", "list", "--json"])
    cmd = cli_args.func(cli_args)

    mock_stages = [
        _create_mock_stage(dvc, "prepare", "python prepare.py", deps=["raw.csv"]),
        _create_mock_stage(dvc, "train", "python train.py", outs=["model.pkl"]),
    ]
    mocker.patch.object(cmd, "_get_stages", return_value=mock_stages)

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert "prepare" in result
    assert "train" in result
    assert result["prepare"]["cmd"] == "python prepare.py"
    assert result["train"]["cmd"] == "python train.py"


def test_stage_list_json_empty(dvc, mocker, capsys):
    cli_args = parse_args(["stage", "list", "--json"])
    cmd = cli_args.func(cli_args)

    mocker.patch.object(cmd, "_get_stages", return_value=[])

    assert cmd.run() == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)
    assert result == {}


def test_stage_list_json_flag_parsing(dvc):
    cli_args = parse_args(["stage", "list", "--json"])
    assert cli_args.func == CmdStageList
    assert cli_args.json is True


def test_stage_list_without_json_flag(dvc):
    cli_args = parse_args(["stage", "list"])
    assert cli_args.func == CmdStageList
    assert cli_args.json is False
