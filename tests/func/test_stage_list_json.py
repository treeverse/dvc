import json

import pytest

from dvc.cli import main


@pytest.fixture
def simple_stage(tmp_dir, dvc):
    tmp_dir.gen("train.py", "print('training')")
    tmp_dir.gen("data.csv", "a,b,c")
    (tmp_dir / "dvc.yaml").dump(
        {
            "stages": {
                "train": {
                    "cmd": "python train.py",
                    "deps": ["data.csv"],
                    "outs": ["model.pkl"],
                    "metrics": [{"metrics.json": {"cache": False}}],
                    "desc": "Train the model",
                }
            }
        }
    )
    return dvc


def test_stage_list_json_simple(simple_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert "train" in result
    assert result["train"]["cmd"] == "python train.py"
    assert "data.csv" in result["train"]["deps"]
    assert "model.pkl" in result["train"]["outs"]
    assert "metrics.json" in result["train"]["metrics"]
    assert result["train"]["desc"] == "Train the model"


@pytest.fixture
def interpolated_stage(tmp_dir, dvc):
    tmp_dir.gen("train.py", "print('training')")
    (tmp_dir / "params.yaml").dump({"train": {"lr": 0.001, "epochs": 100}})
    (tmp_dir / "dvc.yaml").dump(
        {
            "stages": {
                "train": {
                    "cmd": "python train.py --lr ${train.lr} --epochs ${train.epochs}",
                    "params": ["train.lr", "train.epochs"],
                }
            }
        }
    )
    return dvc


def test_stage_list_json_interpolated_params(interpolated_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert "train" in result
    assert result["train"]["cmd"] == "python train.py --lr 0.001 --epochs 100"


@pytest.fixture
def matrix_stage(tmp_dir, dvc):
    tmp_dir.gen("train.py", "print('training')")
    (tmp_dir / "dvc.yaml").dump(
        {
            "stages": {
                "train": {
                    "matrix": {"lr": [0.001, 0.01], "epochs": [10, 100]},
                    "cmd": "python train.py --lr ${item.lr} --epochs ${item.epochs}",
                }
            }
        }
    )
    return dvc


def test_stage_list_json_matrix_stage(matrix_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert len(result) == 4
    for stage_name, stage_data in result.items():
        assert stage_name.startswith("train@")
        assert "python train.py" in stage_data["cmd"]
        assert "--lr" in stage_data["cmd"]
        assert "--epochs" in stage_data["cmd"]


@pytest.fixture
def foreach_stage(tmp_dir, dvc):
    tmp_dir.gen("process.py", "print('processing')")
    (tmp_dir / "dvc.yaml").dump(
        {
            "stages": {
                "process": {
                    "foreach": ["a", "b", "c"],
                    "do": {"cmd": "python process.py --file ${item}"},
                }
            }
        }
    )
    return dvc


def test_stage_list_json_foreach_stage(foreach_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    expected_stages = ["process@a", "process@b", "process@c"]
    for stage_name in expected_stages:
        assert stage_name in result
        assert "python process.py" in result[stage_name]["cmd"]


def test_stage_list_json_with_target(simple_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json", "train"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert "train" in result
    assert len(result) == 1


def test_stage_list_json_all_flag(simple_stage, tmp_dir, capsys):
    assert main(["stage", "list", "--json", "--all"]) == 0

    out, _ = capsys.readouterr()
    result = json.loads(out)

    assert "train" in result
