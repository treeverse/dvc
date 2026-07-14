import pytest

from dvc.utils import env2bool

VAR = "DVC_TEST_ENV2BOOL"


def test_unset_returns_undefined(monkeypatch):
    monkeypatch.delenv(VAR, raising=False)
    assert env2bool(VAR) is False
    assert env2bool(VAR, undefined=True) is True


@pytest.mark.parametrize("value", ["1", "y", "yes", "true", "TRUE", "Yes", " true "])
def test_truthy_values(monkeypatch, value):
    monkeypatch.setenv(VAR, value)
    assert env2bool(VAR) is True


@pytest.mark.parametrize("value", ["0", "n", "no", "false", "off", ""])
def test_falsy_values(monkeypatch, value):
    monkeypatch.setenv(VAR, value)
    assert env2bool(VAR) is False


@pytest.mark.parametrize("value", ["deny", "only", "anything", "my-value", "10", "1a"])
def test_values_merely_containing_a_truthy_token_are_false(monkeypatch, value):
    # The value is compared as a whole: a substring search would treat anything
    # containing "1" or "y" as true, so "deny" and "10" would enable a flag.
    monkeypatch.setenv(VAR, value)
    assert env2bool(VAR) is False
