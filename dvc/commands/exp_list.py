import argparse

from dvc.cli.command import CmdBase
from dvc.repo.logs import show_logs


def add_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "exp-list",
        parents=[parent_parser],
        description="List all experiments and their metrics.",
        help="List all experiments and their metrics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--internal",
        action="store_true",
        help="Show only internal (repo) push history instead of global history",
    )
    parser.set_defaults(func=CmdExpList)
    return parser


class CmdExpList(CmdBase):
    def run(self):
        use_internal = getattr(self.args, "internal", False)
        history = show_logs(global_history=not use_internal)
        exps = {}
        for entry in history:
            exp = entry.get("experiment_name") or ""
            metrics = entry.get("metrics") or {}
            if not isinstance(metrics, dict):
                metrics = {}
            commit = entry.get("commit", "")
            if exp:
                exps.setdefault(exp, []).append((commit, metrics))
        if not exps:
            print("No experiments found.")
            return 0
        print("Experiment List:")
        for exp, runs in exps.items():
            print(f"Experiment: {exp}")
            for commit, metrics in runs:
                print(f"  Commit: {commit[:7]}  Metrics: {metrics}")
        return 0
