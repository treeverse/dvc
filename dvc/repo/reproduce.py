import concurrent.futures
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, NoReturn, Optional, TypeVar, Union, cast

from funcy import ldistinct

from dvc.exceptions import ReproductionError
from dvc.log import logger
from dvc.repo.scm_context import scm_context
from dvc.stage.cache import RunCacheNotSupported
from dvc.utils import humanize
from dvc.utils.collections import ensure_list

from . import locked

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.stage import Stage

    from . import Repo

logger = logger.getChild(__name__)
T = TypeVar("T")


def collect_stages(
    repo: "Repo",
    targets: Iterable[str],
    recursive: bool = False,
    glob: bool = False,
) -> list["Stage"]:
    stages: list[Stage] = []
    for target in targets:
        stages.extend(repo.stage.collect(target, recursive=recursive, glob=glob))
    return ldistinct(stages)


def get_subgraph(
    graph: "DiGraph",
    nodes: Optional[list] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> "DiGraph":
    import networkx as nx

    from .graph import get_pipeline, get_pipelines, get_subgraph_of_nodes

    if not pipeline or not nodes:
        return get_subgraph_of_nodes(graph, nodes, downstream=downstream)

    pipelines = get_pipelines(graph)
    used_pipelines = [get_pipeline(pipelines, node) for node in nodes]
    return nx.compose_all(used_pipelines)


def get_active_graph(graph: "DiGraph") -> "DiGraph":
    g = cast("DiGraph", graph.copy())
    for stage in graph:
        if stage.frozen:
            # NOTE: disconnect frozen stage from its dependencies
            g.remove_edges_from(graph.out_edges(stage))
    return g


def plan_repro(
    graph: "DiGraph",
    stages: Optional[list["T"]] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> list["T"]:
    r"""Derive the evaluation of the given node for the given graph.

    When you _reproduce a stage_, you want to _evaluate the descendants_
    to know if it make sense to _recompute_ it. A post-ordered search
    will give us an order list of the nodes we want.

    For example, let's say that we have the following pipeline:

                               E
                              / \
                             D   F
                            / \   \
                           B   C   G
                            \ /
                             A

    The derived evaluation of D would be: [A, B, C, D]

    In case that `downstream` option is specified, the desired effect
    is to derive the evaluation starting from the given stage up to the
    ancestors. However, the `networkx.ancestors` returns a set, without
    any guarantee of any order, so we are going to reverse the graph and
    use a reverse post-ordered search using the given stage as a starting
    point.

                   E                                   A
                  / \                                 / \
                 D   F                               B   C   G
                / \   \        --- reverse -->        \ /   /
               B   C   G                               D   F
                \ /                                     \ /
                 A                                       E

    The derived evaluation of _downstream_ B would be: [B, D, E]
    """
    import networkx as nx

    sub = get_subgraph(graph, stages, pipeline=pipeline, downstream=downstream)
    return list(nx.dfs_postorder_nodes(sub))


def _reproduce_stage(stage: "Stage", **kwargs) -> Optional["Stage"]:
    if stage.frozen and not stage.is_import:
        msg = "%s is frozen. Its dependencies are not going to be reproduced."
        logger.warning(msg, stage)

    ret = stage.reproduce(**kwargs)
    if ret and not kwargs.get("dry", False):
        stage.dump(update_pipeline=False)
    return ret


def _repr(stages: Iterable["Stage"]) -> str:
    return humanize.join(repr(stage.addressing) for stage in stages)


def handle_error(
    graph: Optional["DiGraph"], on_error: str, exc: Exception, stage: "Stage"
) -> set["Stage"]:
    import networkx as nx

    logger.warning("%s%s", exc, " (ignored)" if on_error == "ignore" else "")
    if not graph or on_error == "ignore":
        return set()

    dependents = set(nx.dfs_postorder_nodes(graph.reverse(), stage)) - {stage}
    if dependents:
        names = _repr(dependents)
        msg = "%s %s will be skipped due to this failure"
        logger.warning(msg, "Stages" if len(dependents) > 1 else "Stage", names)
    return dependents


def _raise_error(exc: Optional[Exception], *stages: "Stage") -> NoReturn:
    names = _repr(stages)
    segment = " stages:" if len(stages) > 1 else ""
    raise ReproductionError(f"failed to reproduce{segment} {names}") from exc


class ReproStatus(Enum):
    READY = "ready"
    IN_PROGRESS = "in-progress"
    COMPLETE = "complete"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass
class StageInfo:
    upstream: list["Stage"]
    upstream_unfinished: set["Stage"]
    downstream: list["Stage"]
    force: bool
    status: ReproStatus
    result: Optional["Stage"]


def _start_ready_stages(
    to_repro: dict["Stage", StageInfo],
    executor: concurrent.futures.ThreadPoolExecutor,
    max_stages: int,
    repro_fn: Callable = _reproduce_stage,
    **kwargs,
) -> dict[concurrent.futures.Future["Stage"], "Stage"]:
    ready = [
        (stage, stage_info)
        for stage, stage_info in to_repro.items()
        if stage_info.status == ReproStatus.READY and not stage_info.upstream_unfinished
    ]
    if not ready:
        return {}

    futures = {
        executor.submit(
            repro_fn,
            stage,
            upstream=stage_info.upstream,
            force=stage_info.force,
            **kwargs,
        ): stage
        for stage, stage_info in ready[:max_stages]
    }
    for stage in futures.values():
        to_repro[stage].status = ReproStatus.IN_PROGRESS
    return futures


def _result_or_raise(
    to_repro: dict["Stage", StageInfo], stages: list["Stage"], on_error: str
) -> list["Stage"]:
    result: list[Stage] = []
    failed: list[Stage] = []
    # Preserve original order
    for stage in stages:
        stage_info = to_repro[stage]
        if stage_info.status == ReproStatus.FAILED:
            failed.append(stage)
        elif stage_info.result:
            result.append(stage_info.result)

    if on_error != "ignore" and failed:
        _raise_error(None, *failed)

    return result


def _handle_result(
    to_repro: dict["Stage", StageInfo],
    future: concurrent.futures.Future["Stage"],
    stage: "Stage",
    stage_info: StageInfo,
    graph: Optional["DiGraph"],
    on_error: str,
    force_downstream: bool,
):
    ret: Optional[Stage] = None
    success = False
    try:
        ret = future.result()
    except Exception as exc:  # noqa: BLE001
        if on_error == "fail":
            _raise_error(exc, stage)

        stage_info.status = ReproStatus.FAILED
        dependents = handle_error(graph, on_error, exc, stage)
        for dependent in dependents:
            to_repro[dependent].status = ReproStatus.SKIPPED
    else:
        stage_info.status = ReproStatus.COMPLETE
        success = True

    for dependent in stage_info.downstream:
        if dependent not in to_repro:
            continue
        dependent_info = to_repro[dependent]
        if stage in dependent_info.upstream_unfinished:
            dependent_info.upstream_unfinished.remove(stage)
        if success and force_downstream and (ret or stage_info.force):
            dependent_info.force = True

    if success and ret:
        stage_info.result = ret


def _reproduce(
    stages: list["Stage"],
    graph: Optional["DiGraph"] = None,
    force_downstream: bool = False,
    on_error: str = "fail",
    force: bool = False,
    jobs: int = 1,
    **kwargs,
) -> list["Stage"]:
    assert on_error in ("fail", "keep-going", "ignore")

    to_repro = {
        stage: StageInfo(
            upstream=(upstream := list(graph.successors(stage)) if graph else []),
            upstream_unfinished=set(upstream).intersection(stages),
            downstream=list(graph.predecessors(stage)) if graph else [],
            force=force,
            status=ReproStatus.READY,
            result=None,
        )
        for stage in stages
    }

    if jobs == -1:
        jobs = len(stages)
    max_workers = max(1, min(jobs, len(stages)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = _start_ready_stages(to_repro, executor, max_workers, **kwargs)
        while futures:
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                stage = futures.pop(future)
                stage_info = to_repro[stage]
                _handle_result(
                    to_repro,
                    future,
                    stage,
                    stage_info,
                    graph,
                    on_error,
                    force_downstream,
                )

            futures.update(_start_ready_stages(to_repro, executor, len(done), **kwargs))

    return _result_or_raise(to_repro, stages, on_error)


@locked
@scm_context
def reproduce(
    self: "Repo",
    targets: Union[Iterable[str], str, None] = None,
    recursive: bool = False,
    pipeline: bool = False,
    all_pipelines: bool = False,
    downstream: bool = False,
    single_item: bool = False,
    glob: bool = False,
    on_error: Optional[str] = "fail",
    **kwargs,
):
    from dvc.dvcfile import PROJECT_FILE

    if all_pipelines or pipeline:
        single_item = False
        downstream = False

    if not kwargs.get("interactive", False):
        kwargs["interactive"] = self.config["core"].get("interactive", False)

    stages: list[Stage] = []
    if not all_pipelines:
        targets_list = ensure_list(targets or PROJECT_FILE)
        stages = collect_stages(self, targets_list, recursive=recursive, glob=glob)

    if kwargs.get("pull", False) and kwargs.get("run_cache", True):
        logger.debug("Pulling run cache")
        try:
            self.stage_cache.pull(None)
        except RunCacheNotSupported as e:
            logger.warning("Failed to pull run cache: %s", e)

    graph = None
    steps = stages
    if not single_item:
        graph = get_active_graph(self.index.graph)
        steps = plan_repro(graph, stages, pipeline=pipeline, downstream=downstream)
    return _reproduce(steps, graph=graph, on_error=on_error or "fail", **kwargs)
