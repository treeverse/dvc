import shutil

import pytest

from dvc.exceptions import DvcException, ReproductionError
from dvc.repo import Repo
from dvc.utils.fs import remove
from dvc.utils.serialize import modify_yaml
from dvc_objects.fs.local import LocalFileSystem


def test_repro_allow_missing(tmp_dir, dvc):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo, _) = dvc.reproduce()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(allow_missing=True)
    # both stages are skipped
    assert not ret


def test_repro_allow_missing_and_pull(tmp_dir, dvc, mocker, local_remote):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo,) = dvc.reproduce("create-foo")

    dvc.push()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(pull=True, allow_missing=True)
    # create-foo is skipped ; copy-foo pulls missing dep
    assert len(ret) == 1


@pytest.mark.parametrize("dry", [False, True])
def test_repro_allow_missing_upstream_stage_modified(
    tmp_dir, dvc, mocker, local_remote, dry
):
    """https://github.com/treeverse/dvc/issues/9530"""
    tmp_dir.gen("params.yaml", "param: 1")
    dvc.stage.add(
        name="create-foo", cmd="echo ${param} > foo", params=["param"], outs=["foo"]
    )
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    dvc.reproduce()

    dvc.push()

    tmp_dir.gen("params.yaml", "param: 2")
    (create_foo,) = dvc.reproduce("create-foo")
    dvc.push()
    remove("foo")
    remove(create_foo.outs[0].cache_path)

    ret = dvc.reproduce(pull=True, allow_missing=True, dry=dry)
    # create-foo is skipped ; copy-foo pulls modified dep
    assert len(ret) == 1


def test_repro_allow_missing_cached(tmp_dir, dvc):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    dvc.reproduce()

    remove("foo")

    ret = dvc.reproduce(allow_missing=True)
    # both stages are skipped
    assert not ret


@pytest.mark.parametrize("dependency", ["data/nested/file", "data/nested"])
@pytest.mark.parametrize("change", ["none", "sibling", "input", "deleted"])
@pytest.mark.parametrize("manifest_location", ["cache", "remote"])
def test_repro_allow_missing_directory_dependency(
    tmp_dir, dvc, mocker, local_remote, dependency, change, manifest_location
):
    tmp_dir.gen({"source": {"nested": {"file": "old"}, "sibling": "other"}})
    dvc.stage.add(
        name="produce", cmd="cp -r source data", deps=["source"], outs=["data"]
    )
    dvc.stage.add(
        name="consume",
        cmd="cp data/nested/file result",
        deps=[dependency],
        outs=["result"],
    )
    dvc.reproduce()
    if change == "input":
        tmp_dir.gen("source/nested/file", "new")
    elif change == "sibling":
        tmp_dir.gen("source/sibling", "changed sibling")
    elif change == "deleted":
        remove("source/nested/file")
    dvc.reproduce("produce")
    dvc.push()
    out = dvc.find_outs_by_path("data")[0]
    manifest = out.cache_path
    for _, _, hi in out.get_obj():
        remove(out.cache.oid_to_path(hi.value))
    if manifest_location == "remote":
        remove(manifest)
    remove("data")
    remove("result")
    lock = (tmp_dir / "dvc.lock").read_bytes()

    # Checking the recorded dependency may read a manifest, but never payloads.
    mocker.patch.object(dvc.cloud, "pull", side_effect=AssertionError("no pull"))
    original_open = LocalFileSystem.open
    remote_reads = []

    def open_metadata_only(fs, path, *args, **kwargs):
        if fs.isin(path, str(local_remote)):
            assert str(path).endswith(".dir"), "must not read remote payloads"
            remote_reads.append(path)
        return original_open(fs, path, *args, **kwargs)

    mocker.patch.object(LocalFileSystem, "open", open_metadata_only)
    ret = dvc.reproduce(allow_missing=True, dry=True)
    assert [stage.name for stage in ret] == (
        ["consume"] if change in {"input", "deleted"} else []
    )
    assert not (tmp_dir / "data").exists()
    assert not (tmp_dir / "result").exists()
    assert (tmp_dir / "dvc.lock").read_bytes() == lock
    assert len(remote_reads) == (1 if manifest_location == "remote" else 0)


@pytest.mark.parametrize("damage", ["missing", "invalid-json", "wrong-checksum"])
def test_repro_allow_missing_directory_manifest_unavailable(
    tmp_dir, dvc, local_remote, damage
):
    (producer,) = tmp_dir.dvc_gen({"data": {"file": "old"}})
    dvc.stage.add(
        name="consume", cmd="cp data/file result", deps=["data/file"], outs=["result"]
    )
    dvc.reproduce()
    dvc.push()
    out = producer.outs[0]
    odb = dvc.cloud.get_remote_odb()
    path = odb.oid_to_path(out.hash_info.value)
    remove("data")
    remove(out.cache_path)
    remove(path)
    if damage != "missing":
        with odb.fs.open(path, "w") as stream:
            stream.write("not JSON" if damage == "invalid-json" else "[]")

    with pytest.raises(ReproductionError) as error:
        dvc.reproduce(allow_missing=True, dry=True)
    assert isinstance(error.value.__cause__, DvcException)
    assert "manifest" in str(error.value.__cause__).lower()


def test_repro_allow_missing_directory_with_pull(tmp_dir, dvc, local_remote):
    tmp_dir.gen({"source": {"file": "old"}})
    dvc.stage.add(
        name="produce", cmd="cp -r source data", deps=["source"], outs=["data"]
    )
    dvc.stage.add(
        name="consume", cmd="cp data/file result", deps=["data/file"], outs=["result"]
    )
    dvc.reproduce()
    tmp_dir.gen("source/file", "new")
    dvc.reproduce("produce")
    dvc.push()
    remove("data")
    remove(dvc.cache.local.path)
    remove(dvc.stage_cache.cache_dir)

    # A new CLI process has no memoized cache-directory creation state.
    with Repo(str(tmp_dir)) as fresh:
        ret = fresh.reproduce(allow_missing=True, pull=True)
    assert [stage.name for stage in ret] == ["consume"]
    assert (tmp_dir / "result").read_text() == "new"


@pytest.mark.parametrize("dependency", ["data/nested/file", "data/nested"])
def test_repro_allow_missing_legacy_directory(tmp_dir, dvc, local_remote, dependency):
    tmp_dir.dvc_gen({"data": {"nested": {"file": "old"}}})
    dvc.stage.add(
        name="consume",
        cmd="cp data/nested/file result",
        deps=[dependency],
        outs=["result"],
    )
    dvc.reproduce()
    # DVC 2 locks use md5-dos2unix and the legacy cache/remote layout.
    with modify_yaml("data.dvc") as data:
        data["outs"][0].pop("hash")
    with modify_yaml("dvc.lock") as data:
        data["stages"]["consume"]["deps"][0].pop("hash")
    shutil.copytree(dvc.cache.local.path, dvc.cache.legacy.path, dirs_exist_ok=True)
    dvc.push()
    remove("data")
    remove(dvc.cache.legacy.path)

    assert not dvc.reproduce(allow_missing=True, dry=True)


def test_repro_allow_missing_directory_remote_error(tmp_dir, dvc, mocker):
    (producer,) = tmp_dir.dvc_gen({"data": {"file": "old"}})
    dvc.stage.add(
        name="consume", cmd="cp data/file result", deps=["data/file"], outs=["result"]
    )
    dvc.reproduce()
    remove("data")
    remove(producer.outs[0].cache_path)
    mocker.patch.object(
        dvc.cloud, "get_remote_odb", side_effect=PermissionError("access denied")
    )
    with pytest.raises(ReproductionError) as error:
        dvc.reproduce(allow_missing=True, dry=True)
    assert isinstance(error.value.__cause__, PermissionError)
