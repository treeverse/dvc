import pytest

from dvc.utils.serialize import (
    EncodingError,
    FastYAMLParseError,
    YAMLFileCorruptedError,
    dump_yaml_fast,
    load_yaml,
    load_yaml_fast,
    parse_yaml,
    parse_yaml_fast,
)


def test_parse_yaml_duplicate_key_error():
    text = """\
    mykey:
    - foo
    mykey:
    - bar
    """
    with pytest.raises(YAMLFileCorruptedError):
        parse_yaml(text, "mypath")


def test_parse_yaml_invalid_unicode(tmp_dir):
    filename = "invalid_utf8.yaml"
    tmp_dir.gen(filename, b"\x80some: stuff")

    with pytest.raises(EncodingError) as excinfo:
        load_yaml(tmp_dir / filename)

    assert filename in excinfo.value.path
    assert excinfo.value.encoding == "utf-8"


class TestFastYAML:
    def test_fast_parser_uses_c_extension(self):
        import yaml

        from dvc.utils.serialize._yaml import _get_safe_loader

        loader = _get_safe_loader()
        if hasattr(yaml, "CSafeLoader"):
            assert issubclass(loader, yaml.CSafeLoader)
        else:
            assert issubclass(loader, yaml.SafeLoader)

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("key: value", {"key": "value"}),
            ("num: 42", {"num": 42}),
            ("flag: true", {"flag": True}),
            ("list:\n  - a\n  - b", {"list": ["a", "b"]}),
            ("nested:\n  key: val", {"nested": {"key": "val"}}),
        ],
    )
    def test_parse_yaml_fast_basic(self, text, expected):
        result = parse_yaml_fast(text, "test.yaml")
        assert result == expected

    @pytest.mark.parametrize(
        "text",
        [
            "on: true\noff: false",
            "yes: 1\nno: 0",
        ],
    )
    def test_fast_yaml_boolean_compatibility(self, text):
        fast_result = parse_yaml_fast(text, "test.yaml")
        slow_result = parse_yaml(text, "test.yaml", typ="safe")
        assert fast_result == slow_result

    def test_fast_yaml_octal_compatibility(self):
        text = "value: 010"
        fast_result = parse_yaml_fast(text, "test.yaml")
        slow_result = parse_yaml(text, "test.yaml", typ="safe")
        assert fast_result == slow_result

    def test_parse_yaml_fast_error(self):
        text = "key: [unclosed"
        with pytest.raises(FastYAMLParseError):
            parse_yaml_fast(text, "test.yaml")

    def test_load_yaml_fast(self, tmp_dir):
        tmp_dir.gen("test.yaml", "key: value\nnum: 42")
        result = load_yaml_fast(tmp_dir / "test.yaml")
        assert result == {"key": "value", "num": 42}

    def test_load_yaml_fast_invalid_unicode(self, tmp_dir):
        tmp_dir.gen("invalid.yaml", b"\x80some: stuff")
        with pytest.raises(EncodingError):
            load_yaml_fast(tmp_dir / "invalid.yaml")

    def test_dump_yaml_fast_roundtrip(self, tmp_dir):
        data = {"schema": "2.0", "stages": {"build": {"cmd": "echo hello"}}}
        path = tmp_dir / "test.yaml"
        dump_yaml_fast(path, data)
        loaded = load_yaml_fast(path)
        assert loaded == data

    def test_dump_yaml_fast_preserves_order(self, tmp_dir):
        data = {"z": 1, "a": 2, "m": 3}
        path = tmp_dir / "test.yaml"
        dump_yaml_fast(path, data)
        content = path.read_text()
        lines = [line.strip() for line in content.strip().split("\n")]
        assert lines == ["z: 1", "a: 2", "m: 3"]

    def test_parse_yaml_empty(self):
        assert parse_yaml_fast("", "test.yaml") == {}
        assert parse_yaml_fast("---", "test.yaml") == {}

    def test_fast_parser_error_has_text(self):
        text = "key: [unclosed"
        with pytest.raises(FastYAMLParseError) as excinfo:
            parse_yaml_fast(text, "test.yaml")
        assert excinfo.value.text == text


class TestStrictYAMLFastPath:
    def test_strictyaml_load_returns_tuple(self, tmp_dir):
        from dvc.utils.strictyaml import load

        tmp_dir.gen("test.yaml", "key: value")
        result = load(str(tmp_dir / "test.yaml"), use_fast_yaml=True)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == {"key": "value"}
        assert result[1] == "key: value"

    def test_strictyaml_load_fast_matches_slow(self, tmp_dir):
        from dvc.utils.strictyaml import load

        tmp_dir.gen("test.yaml", "key: value\nnested:\n  inner: 42")
        fast = load(str(tmp_dir / "test.yaml"), use_fast_yaml=True)
        slow = load(str(tmp_dir / "test.yaml"), use_fast_yaml=False)
        assert fast[0] == slow[0]
        assert fast[1] == slow[1]

    def test_strictyaml_load_fast_error_fallback(self, tmp_dir):
        from dvc.utils.strictyaml import YAMLSyntaxError, load

        tmp_dir.gen("bad.yaml", "key: [unclosed")
        with pytest.raises(YAMLSyntaxError):
            load(str(tmp_dir / "bad.yaml"), use_fast_yaml=True)

    def test_strictyaml_load_fast_with_schema(self, tmp_dir):
        from voluptuous import Required, Schema

        from dvc.utils.strictyaml import load

        schema = Schema({Required("key"): str})
        tmp_dir.gen("test.yaml", "key: value")
        data, _ = load(str(tmp_dir / "test.yaml"), schema=schema, use_fast_yaml=True)
        assert data == {"key": "value"}

    def test_load_path_with_fast_yaml(self, tmp_dir):
        from dvc.fs import LocalFileSystem
        from dvc.utils.serialize import load_path

        tmp_dir.gen("test.yaml", "key: value")
        fs = LocalFileSystem()
        result = load_path(str(tmp_dir / "test.yaml"), fs=fs, use_fast_yaml=True)
        assert result == {"key": "value"}
