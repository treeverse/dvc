from pathlib import Path

import pytest

from dvc.cli import main
from dvc.repo.purge import PurgeError


def test_purge_no_remote_configured_errors(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    with pytest.raises(PurgeError):
        dvc.purge()


def test_purge_no_remote_configured_with_force_warns(tmp_dir, dvc, caplog):
    tmp_dir.dvc_gen("foo", "foo")
    caplog.clear()
    dvc.purge(force=True)
    assert (
        "No default remote configured. Proceeding with purge due to --force"
        in caplog.text
    )


def test_purge_api_removes_file_and_cache(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert (tmp_dir / "foo").exists()
    assert Path(stage.outs[0].cache_path).exists()

    dvc.push("foo")  # ensure remote has backup

    dvc.purge()

    # workspace file gone, cache gone, metadata remains
    assert not (tmp_dir / "foo").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "foo.dvc").exists()


def test_purge_cli_removes_file_and_cache(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("bar", "bar")
    assert (tmp_dir / "bar").exists()
    assert Path(stage.outs[0].cache_path).exists()

    # force will skip check that remote has backup
    assert main(["purge", "--force"]) == 0

    assert not (tmp_dir / "bar").exists()
    assert not Path(stage.outs[0].cache_path).exists()
    assert (tmp_dir / "bar.dvc").exists()


def test_purge_targets_only(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen({"dir": {"a.txt": "A", "b.txt": "B"}})
    assert (tmp_dir / "dir" / "a.txt").exists()
    assert (tmp_dir / "dir" / "b.txt").exists()

    dvc.purge(targets=[str(tmp_dir / "dir")], force=True)

    assert not (tmp_dir / "dir").exists()
    assert (tmp_dir / "dir.dvc").exists()


def test_purge_recursive(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen({"nested": {"sub": {"file.txt": "content"}}})
    assert (tmp_dir / "nested" / "sub" / "file.txt").exists()

    dvc.purge(targets=["nested"], recursive=True, force=True)
    assert not (tmp_dir / "nested" / "sub" / "file.txt").exists()


def test_purge_individual_targets(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)

    # Generate two *separate* tracked files
    (stage_a,) = tmp_dir.dvc_gen("a.txt", "A")
    (stage_b,) = tmp_dir.dvc_gen("b.txt", "B")

    assert (tmp_dir / "a.txt").exists()
    assert (tmp_dir / "b.txt").exists()
    assert Path(stage_a.outs[0].cache_path).exists()
    assert Path(stage_b.outs[0].cache_path).exists()

    # Push both so purge passes remote safety
    dvc.push()

    # Purge only a.txt
    dvc.purge(targets=[str(tmp_dir / "a.txt")])

    # a.txt and its cache should be gone, but metadata intact
    assert not (tmp_dir / "a.txt").exists()
    assert not Path(stage_a.outs[0].cache_path).exists()
    assert (tmp_dir / "a.txt.dvc").exists()

    # b.txt and its cache should still exist
    assert (tmp_dir / "b.txt").exists()
    assert Path(stage_b.outs[0].cache_path).exists()
    assert (tmp_dir / "b.txt.dvc").exists()


def test_purge_dry_run_does_not_delete(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("baz", "baz")
    cache_path = Path(stage.outs[0].cache_path)

    dvc.purge(dry_run=True, force=True)

    assert (tmp_dir / "baz").exists()
    assert cache_path.exists()


def test_purge_dirty_file_requires_force(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").write_text("modified")

    with pytest.raises(PurgeError):
        dvc.purge()

    dvc.purge(force=True)
    assert not (tmp_dir / "foo").exists()


def test_purge_missing_remote_object_requires_force(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen("foo", "foo")
    dvc.push("foo")

    remote = dvc.cloud.get_remote_odb("backup")
    remote.fs.remove(remote.path, recursive=True)  # wipe remote

    with pytest.raises(PurgeError):
        dvc.purge()


def test_purge_missing_remote_object_with_force_warns(
    tmp_dir, dvc, make_remote, caplog
):
    make_remote("backup", default=True)
    tmp_dir.dvc_gen("foo", "foo")
    dvc.push("foo")

    remote = dvc.cloud.get_remote_odb("backup")
    remote.fs.remove(remote.path, recursive=True)  # wipe remote

    caplog.clear()
    dvc.purge(force=True)
    assert "Some outputs are not present in the remote cache" in caplog.text


def test_purge_unused_cache(tmp_dir, dvc, make_remote):
    """Basic behavior for `unused-cache` flag.

    Removes cache for files not checked out"""
    make_remote("backup", default=True)

    # tracked & checked out
    (stage_a,) = tmp_dir.dvc_gen("a.txt", "A")

    # tracked but workspace file removed
    (stage_b,) = tmp_dir.dvc_gen("b.txt", "B")
    (tmp_dir / "b.txt").unlink()

    dvc.push()  # ensure remote OK so purge doesn't fail

    cache_a = Path(stage_a.outs[0].cache_path)
    cache_b = Path(stage_b.outs[0].cache_path)

    assert cache_a.exists()
    assert cache_b.exists()

    # Remove unused cache only
    dvc.purge(unused_cache=True, force=False)

    # a.txt exists in workspace -> its cache kept
    assert cache_a.exists()

    # b.txt removed -> its cache purged
    assert not cache_b.exists()


def test_purge_unused_cache_does_not_delete_workspace_files(tmp_dir, dvc, make_remote):
    """Only cache files should be removed, not workspace files"""
    make_remote("backup", default=True)
    tmp_dir.dvc_gen("file.txt", "X")
    dvc.push()

    # Running --unused-cache alone must not delete the file itself
    dvc.purge(unused_cache=True, force=True)

    assert (tmp_dir / "file.txt").exists()
    assert (tmp_dir / "file.txt.dvc").exists()


def test_purge_unused_cache_dry_run(tmp_dir, dvc, make_remote):
    make_remote("backup", default=True)
    (stage,) = tmp_dir.dvc_gen("foo", "content")
    dvc.push()

    # Delete workspace file -> cache is now unused
    (tmp_dir / "foo").unlink()
    cache_path = Path(stage.outs[0].cache_path)
    assert cache_path.exists()

    dvc.purge(unused_cache=True, dry_run=True, force=True)

    # Dry run must NOT delete anything
    assert cache_path.exists()


def test_purge_and_unused_cache_together(tmp_dir, dvc, make_remote, caplog):
    make_remote("backup", default=True)

    (stage_a,) = tmp_dir.dvc_gen("a.txt", "A")
    (stage_b,) = tmp_dir.dvc_gen("b.txt", "B")
    dvc.push()

    cache_a = Path(stage_a.outs[0].cache_path)
    cache_b = Path(stage_b.outs[0].cache_path)

    # Purge only a.txt (should be ignored and raise warning)
    caplog.clear()
    dvc.purge(targets=[str(tmp_dir / "a.txt")], unused_cache=True, force=True)

    # unused-cache is exclusive; targets ignored
    assert "other args have been provided but will be ignored" in caplog.text

    # a.txt is NOT removed by purge
    assert (tmp_dir / "a.txt").exists()
    assert cache_a.exists()

    # b.txt still exists in workspace -> cache kept
    assert (tmp_dir / "b.txt").exists()
    assert cache_b.exists()


def test_unused_cache_ignores_dirty_outputs(tmp_dir, dvc, make_remote):
    """Unused-cache does not concern itself with dirty outputs."""
    make_remote("backup", default=True)
    tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").write_text("modified")  # dirty
    dvc.purge(unused_cache=True)
