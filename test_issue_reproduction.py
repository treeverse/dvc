#!/usr/bin/env python3

"""
Reproduction test for issue #10987:
dvc status reports "no data tracked" when using ** globbing patterns in .gitignore
"""

import os
import tempfile
from pathlib import Path

from dvc.repo import Repo


def test_gitignore_globbing_reproduction():
    """Reproduce the ** globbing pattern issue from #10987"""

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Initialize git and dvc
        os.chdir(tmp_path)
        os.system("git init")
        os.system("dvc init --no-scm")

        # Create directory structure
        data_dir = tmp_path / "data" / "raw"
        data_dir.mkdir(parents=True)

        # Create data file
        data_file = data_dir / "example.nc"
        data_file.write_text("test data")

        # Create .gitignore with problematic ** patterns
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
"""
        gitignore = tmp_path / ".gitignore"
        gitignore.write_text(gitignore_content.strip())

        # Add data file to DVC
        os.system(f"dvc add {data_file}")

        # Add to git
        os.system(f"git add {data_file}.dvc .gitignore")
        os.system('git commit -m "Add data file"')

        # Now test the issue
        repo = Repo(".")

        print(f"Number of stages in index: {len(repo.index.stages)}")
        print(f"DVC files in git: {list(tmp_path.rglob('*.dvc'))}")

        # Check if the .dvc file is being ignored
        dvc_file_path = str(data_file) + ".dvc"
        print(
            f"Is {dvc_file_path} ignored by git? {repo.scm.is_ignored(dvc_file_path)}"
        )

        # Check collect_files output
        from dvc.repo.index import collect_files

        collected_files = list(collect_files(repo))
        print(f"Collected files: {collected_files}")

        # The bug: index should have stages but doesn't
        assert len(repo.index.stages) > 0, (
            f"Expected stages in index, but got {len(repo.index.stages)}"
        )


if __name__ == "__main__":
    test_gitignore_globbing_reproduction()
    print("Test passed!")
