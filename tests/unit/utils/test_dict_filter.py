from dvc.utils import dict_filter


def test_flat_exclusion():
    assert dict_filter({"a": 1, "b": 2}, exclude=["b"]) == {"a": 1}


def test_multiple_excludes():
    assert dict_filter({"a": 1, "b": 2, "c": 3}, exclude=["a", "c"]) == {"b": 2}


def test_nested_key_removed_recursively():
    assert dict_filter({"a": {"b": 2, "c": 3}}, exclude=["b"]) == {"a": {"c": 3}}


def test_excluded_key_removes_its_subtree():
    assert dict_filter({"a": 1, "b": {"x": 1}}, exclude=["b"]) == {"a": 1}


def test_list_of_dicts_is_filtered_elementwise():
    result = dict_filter([{"b": 1, "a": 2}, {"a": 3}], exclude=["b"])
    assert result == [{"a": 2}, {"a": 3}]


def test_no_exclude_returns_input_unchanged():
    assert dict_filter({"a": 1}) == {"a": 1}


def test_scalar_is_passed_through():
    assert dict_filter(5, exclude=["b"]) == 5
