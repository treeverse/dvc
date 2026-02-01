import logging
import os
import textwrap

import pytest

from dvc.config import Config, ConfigError
from dvc.fs import LocalFileSystem


@pytest.mark.parametrize(
    "path, expected",
    [
        ("cache", "../cache"),
        (os.path.join("..", "cache"), "../../cache"),
        (os.getcwd(), os.getcwd()),
        ("ssh://some/path", "ssh://some/path"),
    ],
)
def test_to_relpath(path, expected):
    assert Config._to_relpath(os.path.join(".", "config"), path) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("cache", os.path.abspath(os.path.join("conf_dir", "cache"))),
        ("dir/cache", os.path.abspath(os.path.join("conf_dir", "dir", "cache"))),
        ("../cache", os.path.abspath("cache")),
        (os.getcwd(), os.getcwd()),
        ("ssh://some/path", "ssh://some/path"),
    ],
)
def test_resolve(path, expected):
    conf_dir = os.path.abspath("conf_dir")
    assert Config._resolve(conf_dir, path) == expected


def test_resolve_homedir():
    # NOTE: our test suit patches $HOME, but that only works within the
    # test itself, so we can't use expanduser in @parametrize here.
    conf_dir = os.path.abspath("conf_dir")
    expected = os.path.expanduser(os.path.join("~", "cache"))
    assert Config._resolve(conf_dir, "~/cache") == expected


def test_get_fs(tmp_dir, scm):
    tmp_dir.scm_gen("foo", "foo", commit="add foo")

    fs = scm.get_fs("master")
    config = Config.from_cwd(fs=fs)

    assert config.fs == fs
    assert config.wfs != fs
    assert isinstance(config.wfs, LocalFileSystem)

    assert config._get_fs("repo") == fs
    assert config._get_fs("local") == config.wfs
    assert config._get_fs("global") == config.wfs
    assert config._get_fs("system") == config.wfs


def test_should_prefer_local_dvc_dir(tmp_dir, scm):
    """Test _should_prefer_local_dvc_dir property logic."""
    tmp_dir.scm_gen("foo", "foo", commit="add foo")

    # Scenario 1: Normal workspace (local_dvc_dir == dvc_dir)
    config = Config(dvc_dir=str(tmp_dir / ".dvc"), local_dvc_dir=str(tmp_dir / ".dvc"))
    assert not config._should_prefer_local_dvc_dir

    # Scenario 2: Repo(rev="...") (local_dvc_dir is None)
    fs = scm.get_fs("master")
    config = Config(dvc_dir="/.dvc", local_dvc_dir=None, fs=fs)
    assert not config._should_prefer_local_dvc_dir

    # Scenario 3: Brancher (local_dvc_dir != dvc_dir, workspace has config)
    dvc_dir = tmp_dir / ".dvc"
    dvc_dir.mkdir()
    (dvc_dir / "config").write_text(
        """
        [core]
            analytics = false
        """
    )

    fs = scm.get_fs("master")
    config = Config(dvc_dir="/.dvc", local_dvc_dir=str(dvc_dir), fs=fs)
    assert config._should_prefer_local_dvc_dir

    # Scenario 4: Repo(rev="...") with workspace dir but no config
    dvc_dir2 = tmp_dir / ".dvc2"
    dvc_dir2.mkdir()

    config = Config(dvc_dir="/.dvc", local_dvc_dir=str(dvc_dir2), fs=fs)
    assert not config._should_prefer_local_dvc_dir


def test_get_fs_brancher_scenario(tmp_dir, scm):
    """Test _get_fs returns wfs when in brancher scenario."""
    tmp_dir.scm_gen("foo", "foo", commit="add foo")

    # Create workspace config
    dvc_dir = tmp_dir / ".dvc"
    dvc_dir.mkdir()
    (dvc_dir / "config").write_text(
        """
        [core]
            analytics = false
        """
    )

    # Simulate brancher scenario: fs is GitFileSystem but
    # local_dvc_dir points to workspace
    fs = scm.get_fs("master")
    config = Config(
        dvc_dir="/.dvc",  # git path
        local_dvc_dir=str(dvc_dir),  # workspace path
        fs=fs,
    )

    # Should prefer local_dvc_dir
    assert config._should_prefer_local_dvc_dir
    assert config._get_fs("repo") == config.wfs
    assert config._get_fs("local") == config.wfs
    assert config._get_fs("global") == config.wfs
    assert config._get_fs("system") == config.wfs


def test_get_fs_invalid_level(tmp_dir, dvc):
    """Test _get_fs raises ValueError for invalid config level."""
    config = Config.from_cwd(validate=False)

    # Test that passing an invalid level raises ValueError
    with pytest.raises(ValueError, match="Invalid config level: 'invalid'"):
        config._get_fs("invalid")


def test_s3_ssl_verify(tmp_dir, dvc):
    config = Config.from_cwd(validate=False)
    with config.edit() as conf:
        conf["remote"]["remote-name"] = {"url": "s3://bucket/dvc"}

    assert "ssl_verify" not in config["remote"]["remote-name"]

    with config.edit() as conf:
        section = conf["remote"]["remote-name"]
        section["ssl_verify"] = False

    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        ['remote "remote-name"']
            url = s3://bucket/dvc
            ssl_verify = False
        """
    )

    with config.edit() as conf:
        section = conf["remote"]["remote-name"]
        section["ssl_verify"] = "/path/to/custom/cabundle.pem"

    assert (tmp_dir / ".dvc" / "config").read_text() == textwrap.dedent(
        """\
        [core]
            no_scm = True
        ['remote "remote-name"']
            url = s3://bucket/dvc
            ssl_verify = /path/to/custom/cabundle.pem
        """
    )


def test_load_unicode_error(tmp_dir, dvc, mocker):
    config = Config.from_cwd(validate=False)
    mocker.patch(
        "configobj.ConfigObj", side_effect=UnicodeDecodeError("", b"", 0, 0, "")
    )
    with pytest.raises(ConfigError):
        with config.edit():
            pass


def test_load_configob_error(tmp_dir, dvc, mocker):
    from configobj import ConfigObjError

    config = Config.from_cwd(validate=False)
    mocker.patch("configobj.ConfigObj", side_effect=ConfigObjError())
    with pytest.raises(ConfigError):
        with config.edit():
            pass


def test_feature_section_supports_arbitrary_values(caplog):
    with caplog.at_level(logging.WARNING, logger="dvc.config_schema"):
        data = Config.validate(
            {
                "feature": {
                    "random_key_1": "random_value_1",
                    "random_key_2": 42,
                }
            }
        )

    assert "random_key_1" not in data
    assert "random_key_2" not in data
    assert (
        "'feature.random_key_1', 'feature.random_key_2' config options are unsupported"
    ) in caplog.text
