import datetime
import logging
import os
import shutil
import textwrap
from typing import TYPE_CHECKING

import pytest

from dvc.cli import main
from dvc.exceptions import CollectCacheError, InvalidArgumentError, RevCollectionError
from dvc.fs import LocalFileSystem
from dvc.utils.fs import remove
from dvc_data.hashfile.db.local import LocalHashFileDB

if TYPE_CHECKING:
    from dvc.repo.gc import DryGCEntry


@pytest.fixture
def good_and_bad_cache(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    (stage,) = tmp_dir.dvc_gen(
        "data",
        {"sub": {"data_sub": "data_sub", "data": "data", "тест": "проверка"}},
    )
    raw_dir_hash = stage.outs[0].hash_info.as_raw().value
    odb = dvc.cache.local

    bad_cache = {raw_dir_hash}
    for i in ["123", "234", "345"]:
        odb.add_bytes(i, i.encode("utf8"))
        bad_cache.add(i)

    good_cache = {md5 for md5 in odb.all() if md5 not in bad_cache}
    return good_cache, bad_cache


def test_gc_api(dvc, good_and_bad_cache):
    dvc.gc(workspace=True)
    odb = dvc.cache.local
    good_cache, _ = good_and_bad_cache
    assert set(odb.all()) == good_cache


def test_gc_cli(dvc, good_and_bad_cache):
    assert main(["gc", "-wf"]) == 0
    odb = dvc.cache.local
    good_cache, _ = good_and_bad_cache
    assert set(odb.all()) == good_cache


def test_gc_branches_tags(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("file", "v1.0", commit="v1.0")
    scm.tag("v1.0")

    with tmp_dir.branch("test", new=True):
        dvc.remove("file.dvc")
        tmp_dir.dvc_gen("file", "test", commit="test")

    dvc.remove("file.dvc")
    tmp_dir.dvc_gen("file", "trash", commit="trash")

    dvc.remove("file.dvc")
    tmp_dir.dvc_gen("file", "master", commit="trash")

    odb = dvc.cache.local
    assert len(list(odb.all())) == 4

    dvc.gc(all_tags=True, all_branches=True)
    assert len(list(odb.all())) == 3

    dvc.gc(all_tags=False, all_branches=True)
    assert len(list(odb.all())) == 2

    dvc.gc(all_tags=True, all_branches=False)
    assert len(list(odb.all())) == 1


def test_gc_multiple_dvc_repos(tmp_dir, scm, dvc, erepo_dir):
    tmp_dir.dvc_gen("only_in_first", "only in main repo")
    tmp_dir.dvc_gen("in_both", "in both repos")

    erepo_dir.dvc.cache.local.path = dvc.cache.local.path
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("in_both", "in both repos")
        erepo_dir.dvc_gen("only_in_second", "only in additional repo")

    odb = dvc.cache.local
    assert len(list(odb.all())) == 3

    dvc.gc(repos=[erepo_dir], workspace=True)
    assert len(list(odb.all())) == 3

    dvc.gc(workspace=True)
    assert len(list(odb.all())) == 2


def test_all_commits(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("testfile", "uncommitted")
    tmp_dir.dvc_gen("testfile", "committed", commit="committed")
    tmp_dir.dvc_gen("testfile", "modified", commit="modified")
    tmp_dir.dvc_gen("testfile", "workspace")

    n = _count_files(dvc.cache.local.path)
    dvc.gc(all_commits=True)

    # Only one uncommitted file should go away
    assert _count_files(dvc.cache.local.path) == n - 1


def test_gc_no_dir_cache(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    (dir_stage,) = tmp_dir.dvc_gen({"dir": {"x": "x", "subdir": {"y": "y"}}})

    remove(dir_stage.outs[0].cache_path)

    with pytest.raises(RevCollectionError) as exc:
        dvc.gc(workspace=True)
    assert type(exc.value.__cause__) is CollectCacheError

    assert _count_files(dvc.cache.local.path) == 4
    dvc.gc(force=True, workspace=True)
    assert _count_files(dvc.cache.local.path) == 2


def _count_files(path):
    return sum(len(files) for _, _, files in os.walk(path))


def test_gc_no_unpacked_dir(tmp_dir, dvc):
    dir_stages = tmp_dir.dvc_gen({"dir": {"file": "text"}})
    dvc.status()

    os.remove("dir.dvc")
    unpackeddir = dir_stages[0].outs[0].cache_path + LocalHashFileDB.UNPACKED_DIR_SUFFIX

    # older (pre 1.0) versions of dvc used to generate this dir
    shutil.copytree("dir", unpackeddir)
    assert os.path.exists(unpackeddir)

    dvc.gc(force=True, workspace=True)
    assert not os.path.exists(unpackeddir)


def test_gc_without_workspace_raises_error(tmp_dir, dvc):
    dvc.gc(force=True, workspace=True)  # works without error

    from dvc.exceptions import InvalidArgumentError

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True)

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True, workspace=False)


def test_gc_cloud_with_or_without_specifier(tmp_dir, erepo_dir, local_cloud):
    erepo_dir.add_remote(config=local_cloud.config)
    dvc = erepo_dir.dvc
    from dvc.exceptions import InvalidArgumentError

    with pytest.raises(InvalidArgumentError):
        dvc.gc(force=True, cloud=True)

    dvc.gc(cloud=True, all_tags=True)
    dvc.gc(cloud=True, all_commits=True)
    dvc.gc(cloud=True, all_branches=True)
    dvc.gc(cloud=True, all_commits=False, all_branches=True, all_tags=True)


def test_gc_without_workspace_on_tags_branches_commits(tmp_dir, dvc):
    dvc.gc(force=True, all_tags=True)
    dvc.gc(force=True, all_commits=True)
    dvc.gc(force=False, all_branches=True)

    # even if workspace is disabled, and others are enabled, assume as if
    # workspace is enabled.
    dvc.gc(force=False, all_branches=True, all_commits=False, workspace=False)


@pytest.mark.parametrize("cloud", ["c", ""])
def test_gc_without_workspace(tmp_dir, dvc, caplog, cloud):
    with caplog.at_level(logging.WARNING, logger="dvc"):
        assert main(["gc", f"-{cloud}vf"]) == 255

    assert (
        "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
        "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
        "needs to be set." in caplog.text
    )


def test_gc_with_possible_args_positive(tmp_dir, dvc):
    for flag in ["-w", "-a", "-T", "--all-commits", "-aT", "-wa", "-waT"]:
        assert main(["gc", "-vf", flag]) == 0


def test_gc_cloud_positive(tmp_dir, dvc, tmp_path_factory, local_remote):
    for flag in ["-cw", "-ca", "-cT", "-caT", "-cwT"]:
        assert main(["gc", "-vf", flag]) == 0


def test_gc_cloud_remove_order(tmp_dir, scm, dvc, mocker, local_remote):
    (standalone, dir1, dir2) = tmp_dir.dvc_gen(
        {
            "file1": "standalone",
            "dir1": {"file2": "file2"},
            "dir2": {"file3": "file3", "file4": "file4"},
        }
    )
    dvc.push()
    dvc.remove(standalone.relpath)
    dvc.remove(dir1.relpath)
    dvc.remove(dir2.relpath)
    dvc.gc(workspace=True)

    mocked_remove = mocker.patch.object(LocalFileSystem, "remove", autospec=True)
    dvc.gc(workspace=True, cloud=True)
    assert len(mocked_remove.mock_calls) == 4
    # Unpacked dir should be the first removed
    for args in mocked_remove.call_args_list[:2]:
        checksum = str(args[0][1])
        assert checksum.endswith(".dir.unpacked")
    # Then, bulk remove should be applied

    # First to `.dir`
    checksums = mocked_remove.call_args_list[2][0][1]
    assert isinstance(checksums, list)
    assert all(x.endswith(".dir") for x in checksums)
    # And later to individual files
    checksums = mocked_remove.call_args_list[3][0][1]
    assert isinstance(checksums, list)
    assert not any(x.endswith(".dir") for x in checksums)


def test_gc_not_collect_pipeline_tracked_files(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PROJECT_FILE, load_file

    tmp_dir.gen("foo", "foo")
    tmp_dir.gen("bar", "bar")

    run_copy("foo", "foo2", name="copy")
    shutil.rmtree(dvc.stage_cache.cache_dir)
    assert _count_files(dvc.cache.local.path) == 1
    dvc.gc(workspace=True, force=True)
    assert _count_files(dvc.cache.local.path) == 1

    # remove pipeline file and lockfile and check
    load_file(dvc, PROJECT_FILE).remove(force=True)
    dvc.gc(workspace=True, force=True)
    assert _count_files(dvc.cache.local.path) == 0


def test_gc_all_experiments(tmp_dir, scm, dvc):
    from dvc.repo.experiments.refs import ExpRefInfo

    (foo,) = tmp_dir.dvc_gen("foo", "foo", commit="foo")
    foo_hash = foo.outs[0].hash_info.value

    tmp_dir.dvc_gen("foo", "bar", commit="bar")
    baseline = scm.get_rev()

    (baz,) = tmp_dir.dvc_gen("foo", "baz", commit="baz")
    baz_hash = baz.outs[0].hash_info.value

    ref = ExpRefInfo(baseline, "exp")
    scm.set_ref(str(ref), scm.get_rev())

    dvc.gc(all_experiments=True, force=True)

    assert not (
        tmp_dir / ".dvc" / "cache" / "files" / "md5" / foo_hash[:2] / foo_hash[2:]
    ).exists()
    assert (
        tmp_dir / ".dvc" / "cache" / "files" / "md5" / baz_hash[:2] / baz_hash[2:]
    ).read_text() == "baz"


def test_gc_rev_num(tmp_dir, scm, dvc):
    num = 2

    hashes = {}
    for i in range(4):
        i_str = str(i)
        f = tmp_dir.dvc_gen("foo", i_str, commit=i_str)
        hashes[i] = f[0].outs[0].hash_info.value

    dvc.gc(rev="HEAD", num=num, force=True)

    for n, i in enumerate(reversed(range(4))):
        cache = (
            tmp_dir / ".dvc" / "cache" / "files" / "md5" / hashes[i][:2] / hashes[i][2:]
        )
        if n >= num:
            assert not cache.exists()
        else:
            assert cache.read_text() == str(i)


def test_date(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("testfile", "content", commit="add testfile")

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    datestamp = (now.date() + datetime.timedelta(days=1)).isoformat()

    tmp_dir.dvc_gen("testfile", "modified", commit="modified")

    dvc.gc(commit_date=datestamp)

    assert _count_files(dvc.cache.local.path) == 1
    assert dvc.cache.local.exists("9ae73c65f418e6f79ceb4f0e4a4b98d5")  # "modified"

    tmp_dir.dvc_gen("testfile", "modified, again", commit="modify")

    datestamp = (now.date() - datetime.timedelta(days=1)).isoformat()
    dvc.gc(commit_date=datestamp)
    assert _count_files(dvc.cache.local.path) == 2
    assert dvc.cache.local.exists("9ae73c65f418e6f79ceb4f0e4a4b98d5")
    assert dvc.cache.local.exists(
        "3bcf3b1be3e794a97a5a6b93a005784c"
    )  # "modified, again"


def test_gc_not_in_remote(tmp_dir, scm, dvc, mocker, local_remote):
    (standalone, dir1, _) = tmp_dir.dvc_gen(
        {
            "file1": "standalone",
            "dir1": {"file2": "file2"},
            "dir2": {"file3": "file3", "file4": "file4"},
        }
    )
    mocked_remove = mocker.spy(LocalFileSystem, "remove")
    dvc.gc(workspace=True)
    assert not mocked_remove.call_args_list

    dvc.push(["file1", "dir1"])

    dvc.gc(workspace=True, not_in_remote=True)

    assert len(mocked_remove.mock_calls) == 3

    arg_list = mocked_remove.call_args_list

    standalone_hash = standalone.outs[0].hash_info.value
    dir1_hash = dir1.outs[0].hash_info.value
    assert f"{dir1_hash[2:]}.unpacked" in arg_list[0][0][1]
    assert f"{dir1_hash[2:]}" in arg_list[1][0][1][0]
    # We expect 2 calls: standalone_hash and dir1/file2/file2
    assert len(arg_list[2][0][1]) == 2
    # Order is not guaranteed here.
    assert (
        f"{standalone_hash[2:]}" in arg_list[2][0][1][0]
        or f"{standalone_hash[2:]}" in arg_list[2][0][1][1]
    )


def test_gc_not_in_remote_remote_arg(tmp_dir, scm, dvc, mocker, make_remote):
    make_remote("local_remote", typ="local")
    make_remote("other_remote", typ="local", default=False)

    tmp_dir.dvc_gen(
        {
            "file1": "standalone",
            "dir1": {"file2": "file2"},
            "dir2": {"file3": "file3", "file4": "file4"},
        }
    )
    mocked_remove = mocker.spy(LocalFileSystem, "remove")

    dvc.push(["file1", "dir1"], remote="other_remote")

    dvc.gc(workspace=True, not_in_remote=True)

    assert not mocked_remove.mock_calls

    dvc.gc(workspace=True, not_in_remote=True, remote="other_remote")

    assert len(mocked_remove.mock_calls) == 3


def test_gc_not_in_remote_with_remote_field(tmp_dir, scm, dvc, mocker, make_remote):
    make_remote("local_remote", typ="local")
    make_remote("other_remote", typ="local", default=False)

    text = textwrap.dedent(
        """\
        outs:
        - path: foo
          remote: other_remote
          hash: md5
    """
    )
    tmp_dir.gen("foo.dvc", text)
    tmp_dir.dvc_gen("foo", "foo")
    dvc.push()

    mocked_remove = mocker.spy(LocalFileSystem, "remove")
    dvc.gc(workspace=True, not_in_remote=True)
    assert len(mocked_remove.mock_calls) == 1


def test_gc_not_in_remote_cloud(tmp_dir, scm, dvc):
    with pytest.raises(
        InvalidArgumentError,
        match="`--not-in-remote` and `--cloud` are mutually exclusive",
    ):
        dvc.gc(workspace=True, not_in_remote=True, cloud=True)


def test_gc_cloud_remote_field(tmp_dir, scm, dvc, mocker, make_remote):
    make_remote("local_remote", typ="local")
    make_remote("other_remote", typ="local", default=False)

    text = textwrap.dedent(
        """\
        outs:
        - path: foo
          remote: other_remote
          hash: md5
    """
    )
    tmp_dir.gen("foo.dvc", text)
    tmp_dir.dvc_gen("foo", "foo")
    dvc.push()
    tmp_dir.dvc_gen("foo", "bar")

    mocked_remove = mocker.spy(LocalFileSystem, "remove")
    dvc.gc(workspace=True, cloud=True)
    assert len(mocked_remove.mock_calls) == 2  # local and other_remote


def test_gc_dry(dvc, good_and_bad_cache):
    dvc.gc(workspace=True, dry=True)
    odb = dvc.cache.local
    good_cache, _ = good_and_bad_cache
    assert set(odb.all()) != good_cache


def test_gc_logging(caplog, dvc, good_and_bad_cache):
    with caplog.at_level(logging.INFO, logger="dvc"):
        dvc.gc(workspace=True)

    assert "Removed 3 objects from repo cache." in caplog.text
    assert "No unused 'local' cache to remove." in caplog.text
    assert "No unused 'legacy' cache to remove." in caplog.text


def test_gc_skip_failed(tmp_dir, dvc):
    with open("dvc.yaml", mode="w") as f:
        f.write("\ninvalid")

    with pytest.raises(RevCollectionError):
        dvc.gc(force=True, workspace=True)

    dvc.gc(force=True, workspace=True, skip_failed=True)


@pytest.fixture
def make_garbage(tmp_dir):
    def _make_garbage(content, name):
        (stage,) = tmp_dir.dvc_gen(name, content)
        os.remove(stage.relpath)
        return stage.outs[0].hash_info.value

    return _make_garbage


def test_gc_dry_reports_correct_garbage_oid(tmp_dir, dvc, mocker, make_garbage):
    """Verify dry-run correctly identifies and reports garbage object IDs."""
    tmp_dir.dvc_gen("used", "content")
    garbage_oid = make_garbage("garbage", "trash")

    mock_print = mocker.patch("dvc.repo.gc._print_gc_report")

    dvc.gc(workspace=True, dry=True)

    results = mock_print.call_args.kwargs["results"]
    assert len(results) == 1
    assert results[0]["oid"] == garbage_oid


def test_gc_dry_with_cloud(tmp_dir, dvc, local_remote, mocker):
    (stage,) = tmp_dir.dvc_gen("file", "content")
    oid = stage.outs[0].hash_info.value
    dvc.push()
    os.remove(stage.relpath)

    remote_odb = dvc.cloud.get_remote_odb()
    local_odb = dvc.cache.local

    mock_print = mocker.patch("dvc.repo.gc._print_gc_report")

    dvc.gc(workspace=True, cloud=True, dry=True)

    results = mock_print.call_args.kwargs["results"]
    paths = [entry["path"] for entry in results]

    assert len(results) == 2

    assert any(local_odb.path in path for path in paths)
    assert any(remote_odb.path in path for path in paths)

    assert remote_odb.exists(oid)
    assert local_odb.exists(oid)


def test_gc_dry_with_corrupted_cache(tmp_dir, dvc):
    """
    Verifies `gc --dry` survives filesystem errors
    when fetching file metadata for its report.
    """
    (stage,) = tmp_dir.dvc_gen("file", "content")
    cache_path = stage.outs[0].cache_path
    os.remove(stage.relpath)

    os.remove(cache_path)

    # no error should be raised
    dvc.gc(workspace=True, dry=True, force=True)


def test_gc_dry_format_report_lines():
    """Verifies the complete GC report is assembled correctly."""
    import re

    from dvc.repo.gc import ObjectType, _format_report_lines

    now = datetime.datetime.now(tz=datetime.timezone.utc).astimezone()
    test_results: list[DryGCEntry] = [
        {
            "oid": "a1b2c3d4e5f6g7h8",
            "type": ObjectType.FILE,
            "size": 12345,  # Should be formatted as 12.1K
            "mtime": now.timestamp(),
            "path": "/fake/path/to/a1b2c3d4",
        },
        {
            "oid": "f9e8d7c6b5a4",
            "type": ObjectType.DIR,
            "size": None,  # Should be formatted as "-"
            "mtime": None,
            "path": "/fake/path/to/f9e8d7c6.dir",
        },
    ]

    lines = _format_report_lines(test_results)
    assert len(lines) == 5

    summary, headers, divider, first_row, second_row = lines

    assert summary.startswith("total 2 objects")
    assert "12.1k" in summary.lower()

    assert headers.startswith("Type  OID")
    assert divider.startswith("----  --------")

    parsed_first_row = re.split(r"\s{2,}", first_row.strip())
    parsed_second_row = re.split(r"\s{2,}", second_row.strip())

    expected_first_row = [
        ObjectType.FILE.value,
        "a1b2c3d4",
        "12.1k",
        now.strftime("%Y-%m-%d %H:%M:%S"),
        "/fake/path/to/a1b2c3d4",
    ]

    expected_second_row = [
        ObjectType.DIR.value,
        "f9e8d7c6",
        "-",
        "-",
        "/fake/path/to/f9e8d7c6.dir",
    ]

    assert parsed_first_row == expected_first_row
    assert parsed_second_row == expected_second_row


def test_gc_dry_report_empty():
    from dvc.repo.gc import _format_report_lines

    empty_results: list[DryGCEntry] = []
    lines = _format_report_lines(empty_results)
    assert lines == ["total 0 objects, 0B reclaimed"]
