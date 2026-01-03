import pytest

from dvc.parsing import DataResolver, MatrixDefinition, ResolveError
from dvc.schema import COMPILED_MULTI_STAGE_SCHEMA

MATRIX_DATA = {
    "os": ["win", "linux"],
    "pyv": [3.7, 3.8],
    "dict": [{"arg1": 1}, {"arg2": 2}],
    "list": [["out1", "out11"], ["out2", "out22"]],
}


@pytest.mark.parametrize(
    "matrix",
    [
        MATRIX_DATA,
        {
            "os": "${os}",
            "pyv": "${pyv}",
            "dict": "${dict}",
            "list": "${list}",
        },
    ],
)
def test_matrix_interpolated(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump(MATRIX_DATA)
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "cmd": "echo ${item.os} ${item.pyv} ${item.dict}"
        " -- ${item.list.0} ${item.list.1}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@win-3.7-dict0-list0": {"cmd": "echo win 3.7 --arg1 1 -- out1 out11"},
        "build@win-3.7-dict0-list1": {"cmd": "echo win 3.7 --arg1 1 -- out2 out22"},
        "build@win-3.7-dict1-list0": {"cmd": "echo win 3.7 --arg2 2 -- out1 out11"},
        "build@win-3.7-dict1-list1": {"cmd": "echo win 3.7 --arg2 2 -- out2 out22"},
        "build@win-3.8-dict0-list0": {"cmd": "echo win 3.8 --arg1 1 -- out1 out11"},
        "build@win-3.8-dict0-list1": {"cmd": "echo win 3.8 --arg1 1 -- out2 out22"},
        "build@win-3.8-dict1-list0": {"cmd": "echo win 3.8 --arg2 2 -- out1 out11"},
        "build@win-3.8-dict1-list1": {"cmd": "echo win 3.8 --arg2 2 -- out2 out22"},
        "build@linux-3.7-dict0-list0": {"cmd": "echo linux 3.7 --arg1 1 -- out1 out11"},
        "build@linux-3.7-dict0-list1": {"cmd": "echo linux 3.7 --arg1 1 -- out2 out22"},
        "build@linux-3.7-dict1-list0": {"cmd": "echo linux 3.7 --arg2 2 -- out1 out11"},
        "build@linux-3.7-dict1-list1": {"cmd": "echo linux 3.7 --arg2 2 -- out2 out22"},
        "build@linux-3.8-dict0-list0": {"cmd": "echo linux 3.8 --arg1 1 -- out1 out11"},
        "build@linux-3.8-dict0-list1": {"cmd": "echo linux 3.8 --arg1 1 -- out2 out22"},
        "build@linux-3.8-dict1-list0": {"cmd": "echo linux 3.8 --arg2 2 -- out1 out11"},
        "build@linux-3.8-dict1-list1": {"cmd": "echo linux 3.8 --arg2 2 -- out2 out22"},
    }


@pytest.mark.parametrize(
    "matrix",
    [
        MATRIX_DATA,
        {
            "os": "${os}",
            "pyv": "${pyv}",
            "dict": "${dict}",
            "list": "${list}",
        },
    ],
)
def test_matrix_key_present(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump(MATRIX_DATA)
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "cmd": "echo ${key}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@win-3.7-dict0-list0": {"cmd": "echo win-3.7-dict0-list0"},
        "build@win-3.7-dict0-list1": {"cmd": "echo win-3.7-dict0-list1"},
        "build@win-3.7-dict1-list0": {"cmd": "echo win-3.7-dict1-list0"},
        "build@win-3.7-dict1-list1": {"cmd": "echo win-3.7-dict1-list1"},
        "build@win-3.8-dict0-list0": {"cmd": "echo win-3.8-dict0-list0"},
        "build@win-3.8-dict0-list1": {"cmd": "echo win-3.8-dict0-list1"},
        "build@win-3.8-dict1-list0": {"cmd": "echo win-3.8-dict1-list0"},
        "build@win-3.8-dict1-list1": {"cmd": "echo win-3.8-dict1-list1"},
        "build@linux-3.7-dict0-list0": {"cmd": "echo linux-3.7-dict0-list0"},
        "build@linux-3.7-dict0-list1": {"cmd": "echo linux-3.7-dict0-list1"},
        "build@linux-3.7-dict1-list0": {"cmd": "echo linux-3.7-dict1-list0"},
        "build@linux-3.7-dict1-list1": {"cmd": "echo linux-3.7-dict1-list1"},
        "build@linux-3.8-dict0-list0": {"cmd": "echo linux-3.8-dict0-list0"},
        "build@linux-3.8-dict0-list1": {"cmd": "echo linux-3.8-dict0-list1"},
        "build@linux-3.8-dict1-list0": {"cmd": "echo linux-3.8-dict1-list0"},
        "build@linux-3.8-dict1-list1": {"cmd": "echo linux-3.8-dict1-list1"},
    }


def test_matrix_schema_allows_mapping():
    data = {
        "stages": {
            "build": {
                "matrix": {"models": {"goo": {"val": 1}, "baz": {"val": 2}}},
                "cmd": "echo ${item.models.val}",
            }
        }
    }
    COMPILED_MULTI_STAGE_SCHEMA(data)


MAPPING_MATRIX_DATA = {"goo": {"val": 1}, "baz": {"val": 2}}


@pytest.mark.parametrize(
    "matrix",
    [
        {"models": MAPPING_MATRIX_DATA},
        {"models": "${map_param}"},
    ],
)
def test_matrix_with_mapping(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump({"map_param": MAPPING_MATRIX_DATA})
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"matrix": matrix, "cmd": "echo ${item.models.val}"}
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@goo": {"cmd": "echo 1"},
        "build@baz": {"cmd": "echo 2"},
    }


@pytest.mark.parametrize(
    "matrix",
    [
        {"models": MAPPING_MATRIX_DATA, "ver": [1, 2]},
        {"models": "${map_param}", "ver": "${ver}"},
    ],
)
def test_matrix_mixed_mapping_and_list(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump({"map_param": MAPPING_MATRIX_DATA, "ver": [1, 2]})
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"matrix": matrix, "cmd": "echo ${item.models.val} ${item.ver}"}
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@goo-1": {"cmd": "echo 1 1"},
        "build@goo-2": {"cmd": "echo 1 2"},
        "build@baz-1": {"cmd": "echo 2 1"},
        "build@baz-2": {"cmd": "echo 2 2"},
    }


@pytest.mark.parametrize(
    "matrix",
    [
        {"models": MAPPING_MATRIX_DATA},
        {"models": "${map_param}"},
    ],
)
def test_matrix_mapping_key_present(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump({"map_param": MAPPING_MATRIX_DATA})
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"matrix": matrix, "cmd": "echo ${key}"}
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@goo": {"cmd": "echo goo"},
        "build@baz": {"cmd": "echo baz"},
    }


@pytest.mark.parametrize("matrix_value", ["${foo}", "${dct.model1}", "foobar"])
def test_matrix_expects_list_or_dict(tmp_dir, dvc, matrix_value):
    (tmp_dir / "params.yaml").dump({"foo": "bar", "dct": {"model1": "a-out"}})
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"matrix": {"dim": matrix_value}, "cmd": "echo ${item.dim}"}
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    with pytest.raises(ResolveError) as exc_info:
        definition.resolve_all()
    assert "expected list/dictionary, got str" in str(exc_info.value)
    assert "stages.build.matrix.dim" in str(exc_info.value)
