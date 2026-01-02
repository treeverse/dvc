import functools
import io
from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, TextIO

from funcy import reraise

from ._common import ParseError, _dump_data, _load_data, _modify_data


class YAMLError(ParseError):
    pass


class YAMLFileCorruptedError(YAMLError):
    def __init__(self, path):
        super().__init__(path, "YAML file structure is corrupted")


class FastYAMLParseError(YAMLError):
    def __init__(self, path, text, exc):
        self.text = text
        self.original_exc = exc
        super().__init__(path, str(exc))


def load_yaml(path, fs=None, **kwargs):
    return _load_data(path, parser=parse_yaml, fs=fs, **kwargs)


def parse_yaml(text, path, typ="safe"):
    from ruamel.yaml import YAML
    from ruamel.yaml import YAMLError as _YAMLError

    yaml = YAML(typ=typ)
    with reraise(_YAMLError, YAMLFileCorruptedError(path)):
        return yaml.load(text) or {}


def parse_yaml_for_update(text, path):
    """Parses text into Python structure.

    Unlike `parse_yaml()` this returns ordered dicts, values have special
    attributes to store comments and line breaks. This allows us to preserve
    all of those upon dump.

    This one is, however, several times slower than simple `parse_yaml()`.
    """
    return parse_yaml(text, path, typ="rt")


def _get_yaml():
    from ruamel.yaml import YAML

    yaml = YAML()
    yaml.default_flow_style = False

    # tell Dumper to represent OrderedDict as normal dict
    yaml_repr_cls = yaml.Representer
    yaml_repr_cls.add_representer(OrderedDict, yaml_repr_cls.represent_dict)
    return yaml


def _dump(data: Any, stream: TextIO) -> Any:
    yaml = _get_yaml()
    return yaml.dump(data, stream)


def dump_yaml(path, data, fs=None, **kwargs):
    return _dump_data(path, data, dumper=_dump, fs=fs, **kwargs)


def loads_yaml(s, typ="safe"):
    from ruamel.yaml import YAML

    return YAML(typ=typ).load(s)


def dumps_yaml(d):
    stream = io.StringIO()
    _dump(d, stream)
    return stream.getvalue()


@contextmanager
def modify_yaml(path, fs=None):
    with _modify_data(path, parse_yaml_for_update, _dump, fs=fs) as d:
        yield d


@functools.cache
def _get_safe_loader():
    import re

    import yaml

    base_loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

    class YAML12SafeLoader(base_loader):  # type: ignore[valid-type,misc]
        pass

    YAML12SafeLoader.yaml_implicit_resolvers = {
        k: [
            (tag, regexp)
            for tag, regexp in v
            if tag not in ("tag:yaml.org,2002:bool", "tag:yaml.org,2002:int")
        ]
        for k, v in base_loader.yaml_implicit_resolvers.copy().items()
    }

    bool_pattern = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")
    YAML12SafeLoader.add_implicit_resolver(
        "tag:yaml.org,2002:bool",
        bool_pattern,
        list("tTfF"),
    )

    int_pattern = re.compile(r"^[-+]?(?:[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+)$")
    YAML12SafeLoader.add_implicit_resolver(
        "tag:yaml.org,2002:int",
        int_pattern,
        list("-+0123456789"),
    )

    def construct_yaml_int_yaml12(loader, node):
        value = loader.construct_scalar(node)
        value = value.replace("_", "")
        sign = 1
        if value.startswith("-"):
            sign = -1
            value = value[1:]
        elif value.startswith("+"):
            value = value[1:]
        if value.startswith(("0x", "0o")):
            return sign * int(value, 0)
        return sign * int(value.lstrip("0") or "0")

    YAML12SafeLoader.add_constructor("tag:yaml.org,2002:int", construct_yaml_int_yaml12)

    return YAML12SafeLoader


def _get_safe_dumper():
    import yaml

    try:
        return yaml.CSafeDumper
    except AttributeError:
        return yaml.SafeDumper


def parse_yaml_fast(text, path):
    import yaml

    try:
        return yaml.load(text, Loader=_get_safe_loader()) or {}  # noqa: S506
    except yaml.YAMLError as exc:
        raise FastYAMLParseError(path, text, exc) from exc


def load_yaml_fast(path, fs=None):
    from ._common import EncodingError

    open_fn = fs.open if fs else open
    try:
        with open_fn(path, encoding="utf-8") as fd:
            text = fd.read()
    except UnicodeDecodeError as exc:
        raise EncodingError(path, "utf-8") from exc
    return parse_yaml_fast(text, path)


def dump_yaml_fast(path, data, fs=None):
    import yaml

    open_fn = fs.open if fs else open
    with open_fn(path, "w", encoding="utf-8") as fd:
        yaml.dump(
            data,
            fd,
            Dumper=_get_safe_dumper(),
            sort_keys=False,
            default_flow_style=False,
        )
