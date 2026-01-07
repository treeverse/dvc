import argparse

from dvc.cli.command import CmdBase
from dvc.repo.logs import show_logs


def add_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "dataset-list",
        parents=[parent_parser],
        description="List all datasets tracked in push history.",
        help="List all datasets tracked in push history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--internal",
        action="store_true",
        help="Show only internal (repo) push history instead of global history",
    )
    parser.set_defaults(func=CmdDatasetList)
    return parser


class CmdDatasetList(CmdBase):
    def run(self):
        use_internal = getattr(self.args, "internal", False)
        history = show_logs(global_history=not use_internal)
        datasets = set()
        for entry in history:
            for art in entry.get("artifacts", []):
                path = art.get("repo_path", "")
                if path:
                    datasets.add(path.split("/")[0])
        if not datasets:
            print("No datasets found.")
            return 0
        print("Datasets:")
        for ds in sorted(datasets):
            print(f"  {ds}")
        return 0
