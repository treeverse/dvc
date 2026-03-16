"""Test for issue #11010: dvc diff --targets reports unchanged file as modified."""
import os

import pytest


def digest(text):
    import hashlib
    return hashlib.md5(bytes(text, "utf-8"), usedforsecurity=False).hexdigest()


def test_diff_targets_unchanged_file_in_modified_dir(tmp_dir, scm, dvc):
    """
    When using --targets to check a specific file inside a tracked directory,
    if the directory's .dir manifest changes (even just by adding new files),
    the unchanged file should NOT be reported as modified.
    
    Regression test for https://github.com/iterative/dvc/issues/11010
    """
    # Create directory with files and track it
    tmp_dir.dvc_gen(
        {"data": {"file1": "content1", "file2": "content2"}},
        commit="initial commit",
    )
    
    # Add a new file to the directory (don't modify existing files)
    tmp_dir.dvc_gen(
        {"data": {"file3": "content3"}},
        commit="added new file",
    )
    
    # Run dvc diff --targets for the unchanged file1
    result = dvc.diff(targets=[os.path.join("data", "file1")])
    
    # The unchanged file should NOT be reported as modified
    assert result == {
        "added": [],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }, f"Unchanged file should not appear in diff. Got: {result}"


def test_diff_targets_unchanged_file_in_modified_dir_with_revs(tmp_dir, scm, dvc):
    """
    Same as above but with explicit revisions.
    
    When the parent directory's .dir manifest changes between commits,
    unchanged files should not be reported as modified.
    """
    # Create directory with files and track it
    tmp_dir.dvc_gen(
        {"data": {"file1": "content1", "file2": "content2"}},
        commit="initial commit",
    )
    
    # Add a new file to the directory (don't modify existing files)
    tmp_dir.dvc_gen(
        {"data": {"file3": "content3"}},
        commit="added new file",
    )
    
    # Run dvc diff between commits for the unchanged file
    result = dvc.diff("HEAD~1", "HEAD", targets=[os.path.join("data", "file1")])
    
    # The unchanged file should NOT be reported as modified
    assert result == {
        "added": [],
        "deleted": [],
        "modified": [],
        "not in cache": [],
        "renamed": [],
    }, f"Unchanged file should not appear in diff. Got: {result}"
