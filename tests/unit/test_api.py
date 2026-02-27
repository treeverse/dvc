import re

import pytest

from dvc import api


def test_open_raises_error_if_no_context(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo-text")

    fd = api.open("foo")
    with pytest.raises(
        AttributeError, match=re.escape("should be used in a with statement.")
    ):
        fd.read()


def test_open_rev_raises_error_on_wrong_mode(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo-text")

    with pytest.raises(
        ValueError, match=re.escape("Only reading `mode` is supported.")
    ):
        with api.open("foo", mode="w"):
            pass


def test_api_read_from_subdir_with_repo_arg(tmp_dir, dvc):
    """Ensure relative paths are resolved from repo root, not cwd."""

    tmp_dir.dvc_gen({"data": {"data.xml": "contents"}})
    subdir = tmp_dir / "src"
    subdir.mkdir()

    with subdir.chdir():
        assert api.read("data/data.xml", repo=str(tmp_dir)) == "contents"
