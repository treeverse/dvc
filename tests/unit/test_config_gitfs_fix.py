from dvc.config import Config
from dvc.fs import GitFileSystem


def test_config_loads_from_workspace_with_gitfs(tmp_dir, scm):
    """Config should load from workspace, not git history when using GitFileSystem."""
    dvc_dir = tmp_dir / ".dvc"
    dvc_dir.mkdir()

    # Create complete config in workspace
    (dvc_dir / "config").write_text(
        """\
        [core]
            remote = test-webdav
        [remote "test-webdav"]
            url = webdav://localhost:9000/
        """
    )

    (dvc_dir / "config.local").write_text(
        """\
        [remote "test-webdav"]
            password = 12345678
        """
    )

    # Need at least one commit for HEAD to exist
    tmp_dir.scm_gen("foo", "foo", commit="init")

    # Create Config with GitFileSystem
    git_fs = GitFileSystem(scm=scm, rev="HEAD")
    config = Config(
        dvc_dir="/.dvc", local_dvc_dir=str(dvc_dir), fs=git_fs, validate=True
    )

    # Should load from workspace
    assert config["remote"]["test-webdav"]["url"] == "webdav://localhost:9000/"
    assert config["remote"]["test-webdav"]["password"] == "12345678"
