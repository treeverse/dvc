#!/usr/bin/env python3

"""
Verification script for the gitignore ** globbing fix.

This script simulates the exact issue scenario to verify the fix works.
"""

import os
import tempfile
from pathlib import Path


def verify_fix():
    """Verify that the fix works for the original issue scenario."""

    print("🔍 Testing gitignore ** globbing fix...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Change to test directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            # Initialize git and dvc
            print("  📁 Setting up test repository...")
            os.system("git init >/dev/null 2>&1")
            os.system("dvc init --no-scm >/dev/null 2>&1")

            # Create directory structure exactly like the issue
            data_dir = tmp_path / "data" / "raw"
            data_dir.mkdir(parents=True)

            # Create data file
            data_file = data_dir / "example.nc"
            data_file.write_text("test data")

            # Create .gitignore with problematic ** patterns from the issue
            gitignore_content = """# Ignore all data files
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
            print("  📦 Adding file to DVC...")
            result = os.system(f"dvc add {data_file} >/dev/null 2>&1")
            if result != 0:
                print("  ❌ Failed to add file to DVC")
                return False

            # Add to git
            dvc_file = data_file.with_suffix(".nc.dvc")
            os.system(f"git add {dvc_file} .gitignore >/dev/null 2>&1")
            os.system('git commit -m "Add data file" >/dev/null 2>&1')

            # Now test the fix by importing DVC and checking status
            print("  🧪 Testing DVC index recognition...")

            # Import here to use our modified version
            try:
                from dvc.repo import Repo

                repo = Repo(".")

                # The critical test: repo.index.stages should NOT be empty
                stages_count = len(repo.index.stages)
                print(f"  📊 Found {stages_count} stages in DVC index")

                if stages_count == 0:
                    print("  ❌ FAIL: No stages found (original bug persists)")
                    return False
                print(
                    "  ✅ SUCCESS: DVC correctly recognizes .dvc files with ** patterns!"
                )

                # Additional verification: check ignore behavior
                dvc_file_str = str(dvc_file)
                data_file_str = str(data_file)

                print("  🔍 Verifying ignore behavior:")

                # Data file should be ignored by git
                if repo.scm.is_ignored(data_file_str):
                    print("    ✅ Data file correctly ignored by git")
                else:
                    print("    ⚠️  Data file not ignored by git (unexpected)")

                # DVC file should NOT be ignored by git (due to negation)
                if not repo.scm.is_ignored(dvc_file_str):
                    print("    ✅ DVC file correctly NOT ignored by git")
                else:
                    print("    ❌ DVC file incorrectly ignored by git")
                    return False

                # DVC file should NOT be ignored by DVC
                if not repo.dvcignore.is_ignored_file(dvc_file_str):
                    print("    ✅ DVC file correctly NOT ignored by DVC")
                else:
                    print("    ❌ DVC file incorrectly ignored by DVC")
                    return False

                return True

            except ImportError as e:
                print(f"  ❌ Could not import DVC: {e}")
                return False
            except Exception as e:
                print(f"  ❌ Error during test: {e}")
                return False

        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    success = verify_fix()

    if success:
        print("\n🎉 Fix verification PASSED!")
        print("   The gitignore ** globbing issue has been resolved.")
    else:
        print("\n💥 Fix verification FAILED!")
        print("   The issue may still exist.")
        exit(1)
