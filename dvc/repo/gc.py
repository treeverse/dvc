from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional, TypedDict

from dvc.exceptions import InvalidArgumentError
from dvc.log import logger

from . import locked

if TYPE_CHECKING:
    from collections.abc import Iterable

    from dvc.repo import Repo
    from dvc.repo.index import ObjectContainer
    from dvc_data.hashfile import HashInfo

logger = logger.getChild(__name__)


def _validate_args(**kwargs):
    not_in_remote = kwargs.pop("not_in_remote", None)
    cloud = kwargs.pop("cloud", None)
    remote = kwargs.pop("remote", None)
    if remote and not (cloud or not_in_remote):
        raise InvalidArgumentError("`--remote` requires `--cloud` or `--not-in-remote`")
    if not_in_remote and cloud:
        raise InvalidArgumentError(
            "`--not-in-remote` and `--cloud` are mutually exclusive"
        )
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "`--all-experiments`, `--all-commits`, `--date` or `--rev` "
            "needs to be set."
        )
    if kwargs.get("num") and not kwargs.get("rev"):
        raise InvalidArgumentError("`--num` can only be used alongside `--rev`")


def _used_obj_ids_not_in_remote(
    remote_odb_to_obj_ids: "ObjectContainer", jobs: Optional[int] = None
):
    used_obj_ids = set()
    remote_oids = set()
    for remote_odb, obj_ids in remote_odb_to_obj_ids.items():
        assert remote_odb
        remote_oids.update(
            remote_odb.list_oids_exists(
                {x.value for x in obj_ids if x.value},
                jobs=jobs,
            )
        )
        used_obj_ids.update(obj_ids)
    return {obj for obj in used_obj_ids if obj.value not in remote_oids}


def _iter_unique_odbs(odbs):
    """
    The local cache and repo cache often point to the same ObjectDB instance.
    Without deduplication, we would scan the same directory twice
    """
    seen = set()
    for scheme, odb in odbs:
        if odb and odb not in seen:
            seen.add(odb)
            yield scheme, odb


class ObjectType(str, Enum):
    """Defines the types of objects recognized by GC report."""

    FILE = "file"
    DIR = "dir"

    def __str__(self) -> str:
        return self.value


class DryGCEntry(TypedDict):
    """Represents an entry in the GC dry run report."""

    oid: str
    type: ObjectType
    size: Optional[int]
    mtime: Optional[float]
    path: str


@dataclass
class DryGCContext:
    repo: "Repo"
    used_obj_ids: "Iterable[HashInfo]"
    odb_to_obj_ids: "ObjectContainer"
    jobs: Optional[int]
    cloud: bool
    not_in_remote: bool


def _scan_odb_for_garbage(odb, used_oids, jobs) -> list[DryGCEntry]:
    """Scans a single ODB for garbage and returns a list of entries."""
    if not odb:
        return []

    from dvc.fs import LocalFileSystem
    from dvc_data.hashfile.gc import is_dir_hash, iter_garbage

    results = []
    for oid in iter_garbage(odb, used_oids, jobs=jobs):
        path = odb.oid_to_path(oid)
        size = None
        mtime = None
        if isinstance(odb.fs, LocalFileSystem):
            try:
                info = odb.fs.info(path)
                size = info.get("size", 0)
                mtime = info.get("mtime")
            except OSError as e:
                logger.debug(
                    "Could not retrieve info for '%s': %s", path, e, exc_info=True
                )

        entry = DryGCEntry(
            oid=oid,
            type=ObjectType.DIR if is_dir_hash(oid) else ObjectType.FILE,
            size=size,
            mtime=mtime,
            path=path,
        )
        results.append(entry)
    return results


def _collect_dry_run_garbage(context: "DryGCContext") -> list[DryGCEntry]:
    results = []

    # Scan local cache
    for _, odb in _iter_unique_odbs(context.repo.cache.by_scheme()):
        results.extend(_scan_odb_for_garbage(odb, context.used_obj_ids, context.jobs))

    # Scan remotes if requested
    if context.cloud or context.not_in_remote:
        for remote_odb, obj_ids in context.odb_to_obj_ids.items():
            results.extend(_scan_odb_for_garbage(remote_odb, obj_ids, context.jobs))

    return results


def _fmt_size(s: Optional[int]) -> str:
    from dvc.utils.humanize import naturalsize

    if s is None:
        return "-"
    suffix = "B" if s < 1024 else ""
    return f"{naturalsize(s)}{suffix}"


_GC_REPORT_HEADER_FMT = "total {count} objects, {size} reclaimed"
_GC_REPORT_ROW_FMT = "{:<4}  {:<8}  {:>10}  {:<19}  {}"
_GC_REPORT_COLUMN_HEADER = _GC_REPORT_ROW_FMT.format(
    "Type", "OID", "Size", "Modified", "Path"
)
_GC_REPORT_COLUMN_DIVIDER = _GC_REPORT_ROW_FMT.format(
    "----", "--------", "----------", "-------------------", "----"
)


def _format_summary_header(results: list[DryGCEntry]) -> str:
    """Generates the summary line for the GC report."""
    count = len(results)
    total_size = sum((r["size"] or 0) for r in results)
    size_str = _fmt_size(total_size)
    return _GC_REPORT_HEADER_FMT.format(count=count, size=size_str)


def _format_data_rows(results: list[DryGCEntry]) -> list[str]:
    """Formats a list of DryGCEntry into report strings."""
    data_lines = []
    for row in results:
        ts = "-"
        if row["mtime"]:
            try:
                dt = datetime.fromtimestamp(row["mtime"]).astimezone()
                ts = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                pass

        data_lines.append(
            _GC_REPORT_ROW_FMT.format(
                row["type"].value,
                row["oid"][:8],
                _fmt_size(row["size"]),
                ts,
                row["path"],
            )
        )
    return data_lines


def _format_report_lines(results: list[DryGCEntry]) -> list[str]:
    """Assembles all parts of the GC report into a list of lines.

    Output format:
        total 1 objects, 21B reclaimed
        Type  OID             Size  Modified             Path
        ----  --------  ----------  -------------------  ----
        file  a1b2c3d4        1.2 KB  2024-12-23 10:30:45  /path/to/file
        dir   e5f6g7h8          512 B  2024-12-22 15:20:10  /path/to/dir

    Columns: Type(4) | OID(8) | Size(10,right-aligned) | Modified(19,ISO) |
             Path(variable)
    """
    lines = [
        _format_summary_header(results),
    ]

    if not results:
        return lines

    lines.append(_GC_REPORT_COLUMN_HEADER)
    lines.append(_GC_REPORT_COLUMN_DIVIDER)
    lines.extend(_format_data_rows(results))

    return lines


def _print_gc_report(results=None):
    """
    Display a tabular report of objects to be removed.
    """
    from dvc.ui import ui

    lines = _format_report_lines(results or [])
    for line in lines:
        ui.write(line)


def _gc_dry_run(context: "DryGCContext") -> None:
    """
    Perform a dry run of garbage collection.

    Scans for unused objects and collects metadata (size, mtime) for the report
    without performing actual deletion.
    """
    results = _collect_dry_run_garbage(context)
    _print_gc_report(results=results)


@locked
def gc(  # noqa: C901, PLR0912, PLR0913
    self: "Repo",
    all_branches: bool = False,
    cloud: bool = False,
    remote: Optional[str] = None,
    with_deps: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    all_experiments: bool = False,
    force: bool = False,
    jobs: Optional[int] = None,
    repos: Optional[list[str]] = None,
    workspace: bool = False,
    commit_date: Optional[str] = None,
    rev: Optional[str] = None,
    num: Optional[int] = None,
    not_in_remote: bool = False,
    dry: bool = False,
    skip_failed: bool = False,
):
    # require `workspace` to be true to come into effect.
    # assume `workspace` to be enabled if any of `all_tags`, `all_commits`,
    # `all_experiments` or `all_branches` are enabled.
    _validate_args(
        workspace=workspace,
        all_tags=all_tags,
        all_commits=all_commits,
        all_branches=all_branches,
        all_experiments=all_experiments,
        commit_date=commit_date,
        rev=rev,
        num=num,
        cloud=cloud,
        not_in_remote=not_in_remote,
    )

    from contextlib import ExitStack

    from dvc.repo import Repo
    from dvc_data.hashfile.db import get_index
    from dvc_data.hashfile.gc import gc as ogc

    if not repos:
        repos = []
    all_repos = [Repo(path) for path in repos]

    odb_to_obj_ids: ObjectContainer = {}
    with ExitStack() as stack:
        for repo in all_repos:
            stack.enter_context(repo.lock)

        for repo in [*all_repos, self]:
            for odb, obj_ids in repo.used_objs(
                all_branches=all_branches,
                with_deps=with_deps,
                all_tags=all_tags,
                all_commits=all_commits,
                all_experiments=all_experiments,
                commit_date=commit_date,
                remote=remote,
                force=force,
                jobs=jobs,
                revs=[rev] if rev else None,
                num=num or 1,
                skip_failed=skip_failed,
            ).items():
                if odb not in odb_to_obj_ids:
                    odb_to_obj_ids[odb] = set()
                odb_to_obj_ids[odb].update(obj_ids)

    if cloud or not_in_remote:
        _merge_remote_obj_ids(self, remote, odb_to_obj_ids)
    if not_in_remote:
        used_obj_ids = _used_obj_ids_not_in_remote(odb_to_obj_ids, jobs=jobs)
    else:
        used_obj_ids = set()
        used_obj_ids.update(*odb_to_obj_ids.values())

    if dry:
        context = DryGCContext(
            repo=self,
            used_obj_ids=used_obj_ids,
            odb_to_obj_ids=odb_to_obj_ids,
            jobs=jobs,
            cloud=cloud,
            not_in_remote=not_in_remote,
        )
        _gc_dry_run(context)
        return

    for scheme, odb in self.cache.by_scheme():
        if not odb:
            continue
        num_removed = ogc(odb, used_obj_ids, jobs=jobs, dry=dry)
        if num_removed:
            logger.info("Removed %d objects from %s cache.", num_removed, scheme)
        else:
            logger.info("No unused '%s' cache to remove.", scheme)

    if not cloud:
        return

    for remote_odb, obj_ids in odb_to_obj_ids.items():
        assert remote_odb is not None
        num_removed = ogc(remote_odb, obj_ids, jobs=jobs, dry=dry)
        if num_removed:
            get_index(remote_odb).clear()
            logger.info("Removed %d objects from remote.", num_removed)
        else:
            logger.info("No unused cache to remove from remote.")


def _merge_remote_obj_ids(
    repo: "Repo", remote: Optional[str], used_objs: "ObjectContainer"
):
    # Merge default remote used objects with remote-per-output used objects
    default_obj_ids = used_objs.pop(None, set())
    remote_odb = repo.cloud.get_remote_odb(remote, "gc -c", hash_name="md5")
    if remote_odb not in used_objs:
        used_objs[remote_odb] = set()
    used_objs[remote_odb].update(default_obj_ids)
    legacy_odb = repo.cloud.get_remote_odb(remote, "gc -c", hash_name="md5-dos2unix")
    if legacy_odb not in used_objs:
        used_objs[legacy_odb] = set()
    used_objs[legacy_odb].update(default_obj_ids)
