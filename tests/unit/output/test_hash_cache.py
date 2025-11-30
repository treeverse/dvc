import pytest

from dvc.dependency import Dependency
from dvc.output import Output
from dvc.stage import PipelineStage


def test_hash_cache_prevents_rebuild(tmp_dir, dvc, mocker):
    tmp_dir.gen("data", "test content")

    stage1 = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage1")
    stage2 = PipelineStage(dvc, "dvc.yaml", cmd="echo 2", name="stage2")
    out1 = Output(stage1, "data")
    out2 = Output(stage2, "data")

    build_spy = mocker.spy(out1, "_build")

    hash1 = out1.get_hash()
    assert build_spy.call_count == 1

    hash2 = out2.get_hash()
    assert build_spy.call_count == 1  # Still 1 - used cache

    assert hash1 == hash2


def test_hash_cache_cleared_on_reset(tmp_dir, dvc):
    tmp_dir.gen("data", "test content")

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    out = Output(stage, "data")

    out.get_hash()
    assert len(dvc._hash_cache) == 1

    dvc._reset()
    assert len(dvc._hash_cache) == 0


def test_hash_cache_different_paths(tmp_dir, dvc):
    tmp_dir.gen({"data1": "content1", "data2": "content2"})

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    out1 = Output(stage, "data1")
    out2 = Output(stage, "data2")

    out1.get_hash()
    out2.get_hash()

    assert len(dvc._hash_cache) == 2


def test_hash_cache_with_directory(tmp_dir, dvc, mocker):
    tmp_dir.gen({"data_dir": {"file1.txt": "a", "file2.txt": "b"}})

    stage1 = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage1")
    stage2 = PipelineStage(dvc, "dvc.yaml", cmd="echo 2", name="stage2")
    out1 = Output(stage1, "data_dir")
    out2 = Output(stage2, "data_dir")

    build_spy = mocker.spy(out1, "_build")

    hash1 = out1.get_hash()
    assert build_spy.call_count == 1

    hash2 = out2.get_hash()
    assert build_spy.call_count == 1

    assert hash1 == hash2
    assert hash1.isdir


def test_hash_cache_key_structure(tmp_dir, dvc):
    tmp_dir.gen("data", "test content")

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    out = Output(stage, "data")
    out.get_hash()

    cache_key = next(iter(dvc._hash_cache.keys()))
    fs_path, hash_name, protocol = cache_key

    assert fs_path == out.fs_path
    assert hash_name == out.hash_name
    assert protocol == out.fs.protocol


def test_hash_cache_initialized_on_repo(dvc):
    assert hasattr(dvc, "_hash_cache")
    assert isinstance(dvc._hash_cache, dict)


def test_hash_cache_updated_on_save(tmp_dir, dvc):
    tmp_dir.gen("data", "initial content")

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    out = Output(stage, "data")

    initial_hash = out.get_hash()
    cache_key = (out.fs_path, out.hash_name, out.fs.protocol)
    _, cached_hash = dvc._hash_cache[cache_key]
    assert cached_hash == initial_hash

    tmp_dir.gen("data", "modified content")
    out.save()

    _, cached_hash = dvc._hash_cache[cache_key]
    assert cached_hash == out.hash_info
    assert cached_hash != initial_hash


def test_chained_stages_see_updated_hash(tmp_dir, dvc):
    tmp_dir.gen({"intermediate": {"file.txt": "initial"}})

    stage_a = PipelineStage(dvc, "dvc.yaml", cmd="echo a", name="stage_a")
    stage_b = PipelineStage(dvc, "dvc.yaml", cmd="echo b", name="stage_b")
    out_a = Output(stage_a, "intermediate")
    dep_b = Output(stage_b, "intermediate")

    hash_before = out_a.get_hash()

    tmp_dir.gen({"intermediate": {"file.txt": "modified", "new_file.txt": "new"}})
    out_a.save()

    hash_from_dep = dep_b.get_hash()

    assert hash_from_dep == out_a.hash_info
    assert hash_from_dep != hash_before


def test_dependency_save_uses_cache(tmp_dir, dvc, mocker):
    tmp_dir.gen({"data_dir": {"file1.txt": "a", "file2.txt": "b"}})

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    dep = Dependency(stage, "data_dir")

    hash1 = dep.get_hash()
    assert len(dvc._hash_cache) == 1

    build_spy = mocker.spy(dep, "_build")
    dep.save()

    assert build_spy.call_count == 0
    assert dep.hash_info == hash1


def test_output_save_always_rebuilds(tmp_dir, dvc, mocker):
    tmp_dir.gen("output.txt", "initial content")

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    out = Output(stage, "output.txt")

    out.get_hash()

    build_spy = mocker.spy(out, "_build")
    out.save()

    assert build_spy.call_count == 1


def test_dependency_save_without_cache_builds(tmp_dir, dvc, mocker):
    tmp_dir.gen("data", "test content")

    stage = PipelineStage(dvc, "dvc.yaml", cmd="echo 1", name="stage")
    dep = Dependency(stage, "data")

    assert len(dvc._hash_cache) == 0

    build_spy = mocker.spy(dep, "_build")
    dep.save()

    assert build_spy.call_count == 1
    assert len(dvc._hash_cache) == 1


def test_repro_chained_pipeline(tmp_dir, dvc, scm):
    tmp_dir.gen("input.txt", "input data")
    tmp_dir.gen(
        "dvc.yaml",
        """\
stages:
  stage1:
    cmd: mkdir -p intermediate && cp input.txt intermediate/data.txt
    deps:
      - input.txt
    outs:
      - intermediate
  stage2:
    cmd: cat intermediate/data.txt > output.txt
    deps:
      - intermediate
    outs:
      - output.txt
  stage3:
    cmd: cat intermediate/data.txt > output2.txt
    deps:
      - intermediate
    outs:
      - output2.txt
""",
    )

    dvc.reproduce()

    assert (tmp_dir / "intermediate" / "data.txt").exists()
    assert (tmp_dir / "output.txt").read_text().strip() == "input data"

    tmp_dir.gen("input.txt", "modified input data")
    dvc._hash_cache = {}

    stages = dvc.reproduce()

    assert len(stages) == 3
    assert (tmp_dir / "output.txt").read_text().strip() == "modified input data"
