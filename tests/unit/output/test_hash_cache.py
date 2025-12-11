from unittest import mock

from dvc.dependency.base import Dependency
from dvc.output import Output
from dvc.stage import Stage
from dvc_data.index import HashInfo


def test_hash_cache_reuse(tmp_dir, dvc):
    "Ensure that outputs (non-dependencies) populate the cache."
    tmp_dir.gen("test", "sample content")
    stage1 = Stage(dvc)
    out1 = Output(stage1, "test", cache=False)

    with mock.patch.object(out1, "_build", wraps=out1._build) as mock_build:
        out1.save()
        assert mock_build.call_count == 1

        # Result must be stored in cache.
        cache_key = (out1.fs_path, out1.hash_name, out1.fs.protocol)
        assert cache_key in dvc._hash_cache

        # And cached hash must match actual output.
        cached_meta, cached_hash = dvc._hash_cache[cache_key]
        assert cached_hash == out1.hash_info
        assert cached_meta == out1.meta

    stage2 = Stage(dvc)
    out2 = Dependency(stage2, "test", cache=False)

    # This should now use the cached hash.
    with mock.patch.object(
        out2,
        "_build",
        wraps=out2._build,
        side_effect=Exception("Should not be called!"),
    ) as mock_build:
        out2.save()

        assert out2.hash_info == out1.hash_info
        cached_meta, cached_hash = dvc._hash_cache[cache_key]
        assert cached_hash == out2.hash_info
        assert cached_meta == out2.meta


def test_hash_cache_overwrite(tmp_dir, dvc):
    "Ensure that dependencies *do not* populate the cache."
    tmp_dir.gen("test", "sample content")
    stage = Stage(dvc)
    out = Output(stage, "test", cache=False)
    dvc._hash_cache.clear()
    # Pre-fill the cache with an existing entry.
    cache_key = (out.fs_path, out.hash_name, out.fs.protocol)
    dvc._hash_cache[cache_key] = (mock.Mock(), HashInfo("md5", "old_value"))

    out.save()

    assert out.hash_info is not None
    # Old cache entry must still be present...
    assert cache_key in dvc._hash_cache
    # ...but should now have a new hash (since this is an output).
    _, new_hash = dvc._hash_cache[cache_key]
    assert new_hash.value == out.hash_info.value
    assert new_hash.value != "old_value"


def test_hash_cache_reset(dvc):
    dvc._hash_cache[("some/path", "some_md5", "some_fs")] = (
        "some_meta",
        "some_hashinfo",
    )
    assert len(dvc._hash_cache) == 1
    dvc._reset()
    assert len(dvc._hash_cache) == 0
