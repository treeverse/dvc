import argparse

from dvc.cli.command import CmdBase
from dvc.repo.logs import show_logs


def add_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "metrics-diff",
        parents=[parent_parser],
        description="Show metrics difference between two commits.",
        help="Show metrics difference between two commits.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("commit1", type=str, help="First commit hash")
    parser.add_argument("commit2", type=str, help="Second commit hash")
    parser.add_argument(
        "--internal",
        action="store_true",
        help="Show only internal (repo) push history instead of global history",
    )
    parser.set_defaults(func=CmdMetricsDiff)
    return parser


class CmdMetricsDiff(CmdBase):
    def run(self):
        commit1 = self.args.commit1
        commit2 = self.args.commit2
        use_internal = getattr(self.args, "internal", False)
        history = show_logs(global_history=not use_internal)
        m1 = m2 = None
        for entry in history:
            if entry.get("commit", "").startswith(commit1):
                m1 = entry.get("metrics")
                if m1 is None or not isinstance(m1, dict):
                    m1 = {}
            if entry.get("commit", "").startswith(commit2):
                m2 = entry.get("metrics")
                if m2 is None or not isinstance(m2, dict):
                    m2 = {}
        if m1 is None or m2 is None:
            print("One or both commits not found in push history.")
            return 1
        print(f"Metrics diff between {commit1} and {commit2}:")
        all_keys = set(m1.keys()) | set(m2.keys())
        for k in all_keys:
            v1 = m1.get(k)
            v2 = m2.get(k)
            print(f"  {k}: {v1} -> {v2}")
        return 0
