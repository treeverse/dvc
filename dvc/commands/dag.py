from typing import TYPE_CHECKING

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

if TYPE_CHECKING:
    from networkx import DiGraph


def _show_ascii(graph: "DiGraph"):
    from dvc.dagascii import draw
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(graph)

    ret = []
    for pipeline in pipelines:
        ret.append(draw(pipeline.nodes, pipeline.edges))  # noqa: PERF401

    return "\n".join(ret)


def _quote_label(node):
    label = str(node)
    # Node names should not contain ":" unless they are quoted with "".
    # See: https://github.com/pydot/pydot/issues/258.
    if label[0] != '"' and label[-1] != '"':
        return f'"{label}"'
    return label


def _show_dot(graph: "DiGraph"):
    import io

    import networkx as nx
    from networkx.drawing.nx_pydot import write_dot

    dot_file = io.StringIO()

    nx.relabel_nodes(graph, _quote_label, copy=False)
    write_dot(graph.reverse(), dot_file)
    return dot_file.getvalue()


def _show_mermaid(graph, markdown: bool = False):
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(graph)

    graph = "flowchart TD"

    total_nodes = 0
    for pipeline in pipelines:
        node_ids = {}
        nodes = sorted(str(x) for x in pipeline.nodes)
        for node in nodes:
            total_nodes += 1
            node_id = f"node{total_nodes}"
            graph += f'\n\t{node_id}["{node}"]'
            node_ids[node] = node_id
        edges = sorted((str(a), str(b)) for b, a in pipeline.edges)
        for a, b in edges:
            graph += f"\n\t{node_ids[str(a)]}-->{node_ids[str(b)]}"

    if markdown:
        return f"```mermaid\n{graph}\n```"

    return graph


def _collect_targets(repo, target, outs):
    if not target:
        return []

    pairs = repo.stage.collect_granular(target)
    if not outs:
        return [stage.addressing for stage, _ in pairs]

    targets = []

    outs_trie = repo.index.outs_trie
    for stage, path in pairs:
        if not path:
            targets.extend([str(out) for out in stage.outs])
            continue

        for out in outs_trie.itervalues(prefix=repo.fs.parts(path)):
            targets.extend(str(out))

    return targets


def _transform(index, outs):
    import networkx as nx

    from dvc.stage import Stage

    def _relabel(node) -> str:
        return node.addressing if isinstance(node, Stage) else str(node)

    graph = index.outs_graph if outs else index.graph
    return nx.relabel_nodes(graph, _relabel, copy=True)


def _filter(graph, targets, full):
    import networkx as nx

    if not targets:
        return graph

    new_graph = graph.copy()
    if not full:
        descendants = set()
        for target in targets:
            descendants.update(nx.descendants(graph, target))
            descendants.add(target)
        new_graph.remove_nodes_from(set(graph.nodes()) - descendants)

    undirected = new_graph.to_undirected()
    connected = set()
    for target in targets:
        connected.update(nx.node_connected_component(undirected, target))

    new_graph.remove_nodes_from(set(new_graph.nodes()) - connected)
    return new_graph


def _is_foreach_matrix_stage(node, join_string):
    if node.endswith(".dvc"):
        return False
    return join_string in node


def _collapse_foreach_matrix_get_nodes(graph):
    from dvc.parsing import JOIN

    new_nodes = set()
    nodes_to_remove = set()
    for _node in list(graph.nodes):
        if not _is_foreach_matrix_stage(_node, JOIN):
            continue
        nodes_to_remove.add(_node)
        new_nodes.add(_node.split(JOIN)[0])
    return new_nodes, nodes_to_remove


def _collapse_foreach_matrix_get_edges(graph):
    from dvc.parsing import JOIN

    new_edges = set()
    edges_to_remove = set()
    for _e1, _e2 in list(graph.edges):
        _replace = False
        _new_e1 = _e1
        _new_e2 = _e2
        if _is_foreach_matrix_stage(_e1, JOIN):
            _new_e1 = _e1.split(JOIN)[0]
            _replace = True
        if _is_foreach_matrix_stage(_e2, JOIN):
            _new_e2 = _e2.split(JOIN)[0]
            _replace = True
        if _replace:
            edges_to_remove.add((_e1, _e2))
            new_edges.add((_new_e1, _new_e2))
    return new_edges, edges_to_remove


def _collapse_foreach_matrix(graph):
    new_nodes, nodes_to_remove = _collapse_foreach_matrix_get_nodes(graph)
    new_edges, edges_to_remove = _collapse_foreach_matrix_get_edges(graph)
    new_graph = graph.copy()
    new_graph.remove_edges_from(edges_to_remove)
    new_graph.add_nodes_from(new_nodes)
    new_graph.add_edges_from(new_edges)
    new_graph.remove_nodes_from(nodes_to_remove)
    return new_graph


def _build(repo, target=None, full=False, outs=False, collapse_foreach_matrix=False):
    targets = _collect_targets(repo, target, outs)
    graph = _transform(repo.index, outs)
    filtered_graph = _filter(graph, targets, full)
    if collapse_foreach_matrix:
        filtered_graph = _collapse_foreach_matrix(filtered_graph)
    return filtered_graph


class CmdDAG(CmdBase):
    def run(self):
        from dvc.exceptions import InvalidArgumentError

        if self.args.outs and self.args.collapse_foreach_matrix:
            raise InvalidArgumentError(
                "`--outs` and `--collapse-foreach-matrix` are mutually exclusive"
            )
        graph = _build(
            self.repo,
            target=self.args.target,
            full=self.args.full,
            outs=self.args.outs,
            collapse_foreach_matrix=self.args.collapse_foreach_matrix,
        )

        if self.args.dot:
            ui.write(_show_dot(graph))
        elif self.args.mermaid or self.args.markdown:
            ui.write(_show_mermaid(graph, self.args.markdown))
        else:
            with ui.pager():
                ui.write(_show_ascii(graph))

        return 0


def add_parser(subparsers, parent_parser):
    DAG_HELP = "Visualize DVC project DAG."
    dag_parser = subparsers.add_parser(
        "dag",
        parents=[parent_parser],
        description=append_doc_link(DAG_HELP, "dag"),
        help=DAG_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    dag_parser.add_argument(
        "--dot",
        action="store_true",
        default=False,
        help="Print DAG with .dot format.",
    )
    dag_parser.add_argument(
        "--mermaid",
        action="store_true",
        default=False,
        help="Print DAG with mermaid format.",
    )
    dag_parser.add_argument(
        "--md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Print DAG with mermaid format wrapped in Markdown block.",
    )
    dag_parser.add_argument(
        "--collapse-foreach-matrix",
        action="store_true",
        default=False,
        help=(
            "Collapse stages from each foreach/matrix definition into a single node."
        ),
    )
    dag_parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help=(
            "Show full DAG that the target belongs too, instead of "
            "showing DAG consisting only of ancestors."
        ),
    )
    dag_parser.add_argument(
        "-o",
        "--outs",
        action="store_true",
        default=False,
        help="Print output files instead of stages.",
    )
    dag_parser.add_argument(
        "target",
        nargs="?",
        help=(
            "Stage name or output to show pipeline for. "
            "Finds all stages in the workspace by default."
        ),
    )
    dag_parser.set_defaults(func=CmdDAG)
