from typing import TYPE_CHECKING, TypedDict, Union

from funcy import compact

from dvc.utils.diff import diff as _diff_dict
from dvc.utils.diff import format_dict

if TYPE_CHECKING:
    from dvc.repo import Repo

    from .show import Result


class DiffResult(TypedDict, total=False):
    errors: dict[str, Union[Exception, dict[str, Exception]]]
    diff: dict[str, dict[str, dict]]


def _resolve_rev(result: dict[str, "Result"], rev: str) -> "Result":
    # Direct hit when brancher emitted this rev as its own key.
    if rev in result:
        return result[rev]
    # Brancher groups revisions resolving to the same SHA under a single
    # comma-joined composite key (e.g. "main,HEAD" when both resolve to the
    # same commit). Fall back to lookup by membership so callers passing two
    # distinct ref names that happen to point at the same commit still get a
    # result. See https://github.com/iterative/dvc/issues/10429.
    for key, value in result.items():
        if rev in key.split(","):
            return value
    return {}


def _diff(
    result: dict[str, "Result"],
    old_rev: str,
    new_rev: str,
    **kwargs,
) -> DiffResult:
    old = _resolve_rev(result, old_rev)
    new = _resolve_rev(result, new_rev)

    old_data = old.get("data", {})
    new_data = new.get("data", {})

    res = DiffResult()
    errors = res.setdefault("errors", {})

    if old_error := old.get("error"):
        errors[old_rev] = old_error
    else:
        errors[old_rev] = {f: d["error"] for f, d in old_data.items() if "error" in d}

    if new_error := new.get("error"):
        errors[new_rev] = new_error
    else:
        errors[new_rev] = {f: d["error"] for f, d in new_data.items() if "error" in d}

    diff_data = _diff_dict(format_dict(old_data), format_dict(new_data), **kwargs)
    res = DiffResult(errors=errors, diff=diff_data)
    res["errors"] = compact(res.get("errors", {}))  # type: ignore[assignment]
    return compact(res)  # type: ignore[no-any-return]


def diff(
    repo: "Repo",
    a_rev: str = "HEAD",
    b_rev: str = "workspace",
    all: bool = False,  # noqa: A002
    **kwargs,
) -> DiffResult:
    if repo.scm.no_commits:
        return {}

    metrics = repo.metrics.show(revs=[a_rev, b_rev], hide_workspace=False, **kwargs)
    return _diff(metrics, a_rev, b_rev, with_unchanged=all)
