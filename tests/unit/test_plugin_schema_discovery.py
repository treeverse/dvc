"""Tests for plugin-based remote config schema discovery.

Verifies that DVC filesystem plugins can declare a ``REMOTE_CONFIG``
class attribute to register their URL scheme and config options with
DVC's config validation, without requiring changes to DVC core.
"""

from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest


class FakePluginFS:
    """Minimal filesystem class that declares REMOTE_CONFIG."""

    protocol = "myplugin"
    REMOTE_CONFIG: ClassVar[dict] = {
        "token": str,
        "endpoint_url": str,
    }


class FakePluginNoConfig:
    """Filesystem class without REMOTE_CONFIG — should be skipped."""

    protocol = "noplugin"


class FakePluginMultiProtocol:
    """Filesystem class with tuple protocol."""

    protocol = ("myproto", "myprotos")
    REMOTE_CONFIG: ClassVar[dict] = {
        "api_key": str,
    }


def _make_entry_point(name, cls):
    """Create a mock entry point that returns the given class on load()."""
    ep = MagicMock()
    ep.name = name
    ep.load.return_value = cls
    return ep


class TestDiscoverPluginSchemas:
    """Tests for _discover_plugin_schemas."""

    def test_plugin_schema_registered(self):
        """A plugin with REMOTE_CONFIG gets its scheme added to REMOTE_SCHEMAS."""
        from dvc.config_schema import REMOTE_SCHEMAS

        eps = [_make_entry_point("myplugin", FakePluginFS)]
        with patch("dvc.config_schema.entry_points", return_value=eps):
            # Clear any prior registration from this test key
            REMOTE_SCHEMAS.pop("myplugin", None)

            from dvc.config_schema import _discover_plugin_schemas

            _discover_plugin_schemas()

        assert "myplugin" in REMOTE_SCHEMAS
        schema = REMOTE_SCHEMAS["myplugin"]
        # Should contain plugin-specific keys
        assert "token" in schema
        assert "endpoint_url" in schema
        # Should contain REMOTE_COMMON keys
        assert "url" in schema

        # Cleanup
        REMOTE_SCHEMAS.pop("myplugin", None)

    def test_plugin_without_remote_config_skipped(self):
        """A plugin without REMOTE_CONFIG is silently skipped."""
        from dvc.config_schema import REMOTE_SCHEMAS

        eps = [_make_entry_point("noplugin", FakePluginNoConfig)]
        with patch("dvc.config_schema.entry_points", return_value=eps):
            REMOTE_SCHEMAS.pop("noplugin", None)

            from dvc.config_schema import _discover_plugin_schemas

            _discover_plugin_schemas()

        assert "noplugin" not in REMOTE_SCHEMAS

    def test_existing_scheme_not_overwritten(self):
        """Hardcoded schemes like 's3' are never overwritten by plugins."""
        from dvc.config_schema import REMOTE_SCHEMAS

        original_s3 = REMOTE_SCHEMAS["s3"].copy()

        class FakeS3:
            protocol = "s3"
            REMOTE_CONFIG: ClassVar[dict] = {"fake_key": str}

        eps = [_make_entry_point("s3", FakeS3)]
        with patch("dvc.config_schema.entry_points", return_value=eps):
            from dvc.config_schema import _discover_plugin_schemas

            _discover_plugin_schemas()

        # s3 schema should be unchanged
        assert "fake_key" not in REMOTE_SCHEMAS["s3"]
        assert REMOTE_SCHEMAS["s3"] == original_s3

    def test_plugin_load_failure_skipped(self):
        """Plugins that fail to load are silently skipped."""
        from dvc.config_schema import REMOTE_SCHEMAS

        ep = MagicMock()
        ep.name = "broken"
        ep.load.side_effect = ImportError("missing dependency")

        with patch("dvc.config_schema.entry_points", return_value=[ep]):
            REMOTE_SCHEMAS.pop("broken", None)

            from dvc.config_schema import _discover_plugin_schemas

            _discover_plugin_schemas()

        assert "broken" not in REMOTE_SCHEMAS

    def test_multi_protocol_plugin(self):
        """A plugin with tuple protocol registers all schemes."""
        from dvc.config_schema import REMOTE_SCHEMAS

        eps = [_make_entry_point("myproto", FakePluginMultiProtocol)]
        with patch("dvc.config_schema.entry_points", return_value=eps):
            REMOTE_SCHEMAS.pop("myproto", None)
            REMOTE_SCHEMAS.pop("myprotos", None)

            from dvc.config_schema import _discover_plugin_schemas

            _discover_plugin_schemas()

        assert "myproto" in REMOTE_SCHEMAS
        assert "myprotos" in REMOTE_SCHEMAS
        assert "api_key" in REMOTE_SCHEMAS["myproto"]
        assert "api_key" in REMOTE_SCHEMAS["myprotos"]

        # Cleanup
        REMOTE_SCHEMAS.pop("myproto", None)
        REMOTE_SCHEMAS.pop("myprotos", None)


class TestByUrlWithPlugin:
    """Integration test: ByUrl accepts plugin-registered schemes."""

    def test_byurl_validates_plugin_scheme(self):
        """ByUrl should accept a URL with a plugin-registered scheme."""
        from dvc.config_schema import REMOTE_COMMON, REMOTE_SCHEMAS, ByUrl

        # Register a fake scheme
        REMOTE_SCHEMAS["testplugin"] = {"token": str, **REMOTE_COMMON}
        validator = ByUrl(REMOTE_SCHEMAS)

        # Should not raise
        result = validator({"url": "testplugin://myhost/path", "token": "abc"})
        assert result["url"] == "testplugin://myhost/path"
        assert result["token"] == "abc"

        # Cleanup
        REMOTE_SCHEMAS.pop("testplugin", None)

    def test_byurl_rejects_unknown_scheme(self):
        """ByUrl should reject an unregistered scheme."""
        from voluptuous import Invalid as VoluptuousInvalid

        from dvc.config_schema import REMOTE_SCHEMAS, ByUrl

        validator = ByUrl(REMOTE_SCHEMAS)

        with pytest.raises(VoluptuousInvalid, match="Unsupported URL type"):
            validator({"url": "unknownscheme://host/path"})
