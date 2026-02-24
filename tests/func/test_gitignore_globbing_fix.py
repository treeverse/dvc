import os

import pytest

from dvc.repo import Repo


def test_gitignore_globbing_with_dvc_files(tmp_dir, scm, dvc):
    """Test that ** globbing patterns in .gitignore with negations work correctly.
    
    This is a regression test for issue #10987: dvc status reports "no data tracked"
    when using ** globbing patterns in .gitignore.
    
    The issue occurs when .gitignore has patterns like:
    - data/raw/**         (ignore everything in data/raw/)
    - !data/raw/**/*.dvc  (except .dvc files)
    """
    # Create directory structure
    data_dir = tmp_dir / "data" / "raw"
    data_dir.mkdir(parents=True)
    
    # Create data file
    data_file = data_dir / "example.nc"
    data_file.write_text("test data")
    
    # Create .gitignore with ** globbing patterns
    gitignore_content = """
# Ignore all data files  
data/raw/**
data/interim/**
data/processed/**

# But keep DVC metafiles
!data/raw/**/*.dvc
!data/interim/**/*.dvc
!data/processed/**/*.dvc

.dvc/cache/
""".strip()
    
    gitignore = tmp_dir / ".gitignore"
    gitignore.write_text(gitignore_content)
    
    # Add data file to DVC
    dvc.add(str(data_file))
    
    # The .dvc file should exist
    dvc_file = data_dir / "example.nc.dvc"
    assert dvc_file.exists()
    
    # Add to git
    scm.add([str(dvc_file), str(gitignore)])
    scm.commit("Add data file and gitignore")
    
    # Refresh DVC to re-read gitignore
    dvc._reset()
    
    # The key test: DVC should recognize the .dvc file even with ** patterns
    # Before the fix, this would return an empty list
    assert len(dvc.index.stages) > 0, "DVC should find stages even with ** globbing patterns"
    
    # The .dvc file should not be ignored by DVC's ignore system
    assert not dvc.dvcignore.is_ignored_file(str(dvc_file))
    
    # The data file itself should be ignored by git (as expected)
    assert scm.is_ignored(str(data_file))
    
    # But the .dvc file should not be ignored by git (due to negation pattern)
    assert not scm.is_ignored(str(dvc_file))
    
    # DVC status should work correctly
    status = dvc.status()
    # status should be empty (up to date) or have status info, but not fail
    assert isinstance(status, dict)


def test_gitignore_globbing_specific_vs_double_star(tmp_dir, scm, dvc):
    """Test the difference between specific patterns and ** patterns.
    
    This verifies that the workaround mentioned in the issue
    (using specific patterns instead of **) works.
    """
    # Create test files
    (tmp_dir / "data").mkdir()
    (tmp_dir / "data" / "file1.txt").write_text("content")
    (tmp_dir / "data" / "file2.csv").write_text("content")
    
    # Add to DVC
    dvc.add("data/file1.txt")
    dvc.add("data/file2.csv")
    
    # Test 1: With ** patterns (the problematic case)
    gitignore_star = """
data/**
!data/**/*.dvc
""".strip()
    
    gitignore = tmp_dir / ".gitignore"
    gitignore.write_text(gitignore_star)
    scm.add([".dvc", "data/file1.txt.dvc", "data/file2.csv.dvc", ".gitignore"])
    scm.commit("Test with ** patterns")
    
    dvc._reset()
    
    # Should work with the fix
    assert len(dvc.index.stages) == 2
    
    # Test 2: With specific patterns (the workaround)
    gitignore_specific = """
data/*.txt
data/*.csv
""".strip()
    
    gitignore.write_text(gitignore_specific)
    scm.add([".gitignore"])
    scm.commit("Test with specific patterns")
    
    dvc._reset()
    
    # Should also work
    assert len(dvc.index.stages) == 2


def test_collect_files_with_complex_gitignore(tmp_dir, scm, dvc):
    """Test collect_files function directly with complex gitignore patterns."""
    from dvc.repo.index import collect_files
    
    # Create nested structure
    nested_dir = tmp_dir / "project" / "data" / "raw" / "subdir"
    nested_dir.mkdir(parents=True)
    
    # Create multiple data files
    files = [
        nested_dir / "file1.nc",
        nested_dir / "file2.nc", 
        (tmp_dir / "project" / "data" / "processed" / "result.csv"),
    ]
    
    # Ensure processed dir exists
    files[2].parent.mkdir(parents=True)
    
    for f in files:
        f.write_text(f"data in {f.name}")
    
    # Add all to DVC
    for f in files:
        dvc.add(str(f))
    
    # Complex gitignore with nested ** patterns
    gitignore_content = """
# Ignore data directories with ** patterns
project/data/raw/**
project/data/interim/**  
project/data/processed/**

# Keep DVC files
!project/data/raw/**/*.dvc
!project/data/interim/**/*.dvc
!project/data/processed/**/*.dvc

# Also ignore some other patterns
*.log
temp/
.cache/
""".strip()
    
    gitignore = tmp_dir / ".gitignore"
    gitignore.write_text(gitignore_content)
    
    # Add all .dvc files and gitignore to git
    dvc_files = list(tmp_dir.rglob("*.dvc"))
    scm.add([str(f) for f in dvc_files] + [str(gitignore)])
    scm.commit("Add complex gitignore with nested structure")
    
    dvc._reset()
    
    # Test collect_files function
    collected = list(collect_files(dvc))
    
    # Should find all 3 DVC files
    assert len(collected) == 3
    
    # Verify the paths are correct
    collected_paths = [path for path, _ in collected]
    for dvc_file in dvc_files:
        assert str(dvc_file) in collected_paths


def test_is_ignored_function_behavior(tmp_dir, scm, dvc):
    """Test the is_ignored function behavior directly."""
    from dvc.repo.index import collect_files
    
    # Create test structure
    data_dir = tmp_dir / "data"
    data_dir.mkdir()
    test_file = data_dir / "test.txt"
    test_file.write_text("test")
    
    dvc.add(str(test_file))
    dvc_file = data_dir / "test.txt.dvc"
    
    # Gitignore that ignores data dir but keeps .dvc files
    gitignore = tmp_dir / ".gitignore"
    gitignore.write_text("data/**\n!data/**/*.dvc")
    
    scm.add([str(dvc_file), str(gitignore)])
    scm.commit("Test ignore behavior")
    
    dvc._reset()
    
    # Test ignore behavior
    assert dvc.scm.is_ignored(str(test_file))  # Data file should be ignored by git
    assert not dvc.scm.is_ignored(str(dvc_file))  # DVC file should not be ignored by git
    assert not dvc.dvcignore.is_ignored_file(str(dvc_file))  # DVC file should not be ignored by DVC
    
    # The key test: collect_files should find the DVC file
    collected = list(collect_files(dvc))
    assert len(collected) == 1
    assert str(dvc_file) in [path for path, _ in collected]