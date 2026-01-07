# dvc/cli/logs.py

import argparse

from dvc.repo.logs import show_logs

try:
    from rich.console import Console
    from rich.table import Table
except ImportError:
    Console = None
    Table = None


def add_parser(subparsers, parent_parser):
    LOGS_HELP = "Show the history of DVC pushes."

    parser = subparsers.add_parser(
        "logs",
        parents=[parent_parser],
        description=LOGS_HELP,
        help=LOGS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        default=0,
        help="Number of commits to show (default: all)",
    )
    parser.add_argument(
        "--dataset", type=str, default=None, help="Filter logs by specific dataset name"
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Show all available information (wider table)",
    )
    parser.add_argument(
        "--internal",
        action="store_true",
        help="Show only internal (repo) push history instead of global history",
    )
    parser.add_argument(
        "--tag",
        nargs="+",
        type=str,
        default=None,
        help="Filter logs by one or more tag names (space separated)",
    )

    parser.set_defaults(func=CmdLogs)
    return parser


from dvc.cli.command import CmdBase


class CmdLogs(CmdBase):
    def run(self):
        number = getattr(self.args, "number", 0)
        dataset = getattr(self.args, "dataset", None)
        show_all = getattr(self.args, "show_all", False)
        use_internal = getattr(self.args, "internal", False)

        tag_list = getattr(self.args, "tag", None)

        # By default, show global (external) history; if --internal, show internal (repo) history
        history = show_logs(global_history=not use_internal)
        if not history:
            print("No DVC push history found.")
            return 0

        # If --tag is used, print only commit hash and tag name for each match, then exit
        if tag_list:
            found = False
            for entry in history:
                tags = entry.get("tags", [])
                if not isinstance(tags, list):
                    continue
                for t in tag_list:
                    if t in tags:
                        print(f"{entry.get('commit', '')} {t}")
                        found = True
            if not found:
                print(f"No DVC push history found for tag(s): {' '.join(tag_list)}.")
            return 0

        # Filter by dataset if requested
        if dataset:

            def has_dataset(entry):
                for art in entry.get("artifacts", []):
                    if dataset in art.get("repo_path", ""):
                        return True
                return False

            history = [h for h in history if has_dataset(h)]
            if not history:
                print(f"No DVC push history found for dataset '{dataset}'.")
                return 0

        # Show only the last N entries, or all if number is 0 or less
        if number > 0:
            history = history[-number:][::-1]
        else:
            history = history[::-1]

        def summarize_subfolders_count_only(changed_files):
            import os

            summary = {}
            for change_type in ["added", "removed", "modified"]:
                files = changed_files.get(change_type, [])
                for f in files:
                    parts = f.split(os.sep)
                    if len(parts) > 2:
                        subfolder = parts[2]
                    elif len(parts) > 1:
                        subfolder = parts[1]
                    else:
                        subfolder = "root"
                    summary.setdefault(
                        subfolder, {"added": 0, "removed": 0, "modified": 0}
                    )
                    summary[subfolder][change_type] += 1
            return summary

        def artifact_str(a):
            base = f"{a.get('repo_path', '')!s} (md5={a.get('md5', '')!s}, size={a.get('size', '')!s}, files={a.get('nfiles', '')!s})\n  → {a.get('s3_path', '')!s}"
            changes = a.get("changed_files_count", {})
            changed_files = a.get("changed_files", {})
            # Per-subfolder summary for directories, counts only
            if changed_files and a.get("repo_path", "").endswith("train"):
                sub_summary = summarize_subfolders_count_only(changed_files)
                for sub, counts in sub_summary.items():
                    base += f"\n    [{sub}] Added: {counts['added']}, Removed: {counts['removed']}, Modified: {counts['modified']}"
                total = {
                    k: sum(v[k] for v in sub_summary.values())
                    for k in ["added", "removed", "modified"]
                }
                base += f"\n    [Total] Added: {total['added']}, Removed: {total['removed']}, Modified: {total['modified']}"
            elif changes:
                base += f"\n    [Changes] Added: {changes.get('added', 0)}, Removed: {changes.get('removed', 0)}, Modified: {changes.get('modified', 0)}"
            return base

        if Console and Table:
            console = Console()
            table = Table(title="DVC Push History", show_lines=True)

            table.add_column("Commit", style="cyan", no_wrap=True)
            table.add_column("Message", style="magenta")
            table.add_column("Author", style="green")
            table.add_column("Commit Date", style="yellow")
            table.add_column("Push Time", style="yellow")
            table.add_column("Tags", style="white")
            table.add_column("Artifacts", style="white")
            if show_all:
                table.add_column("Experiment", style="blue")
                table.add_column("Metrics", style="white")
                table.add_column("MD5s", style="white")
                table.add_column("Sizes", style="white")
                table.add_column("NFiles", style="white")
                table.add_column("S3 Paths", style="white")

            for entry in history:
                artifacts = "\n".join(
                    [artifact_str(a) for a in entry.get("artifacts", [])]
                )
                tags = entry.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                tags_str = ", ".join(str(t) for t in tags)
                row = [
                    str(entry.get("commit", ""))[:7],
                    str(entry.get("message", "")),
                    str(entry.get("author", "")),
                    str(entry.get("commit_date", "")),
                    str(entry.get("push_time", "")),
                    tags_str,
                    artifacts,
                ]
                if show_all:
                    experiment = entry.get("experiment_name") or ""
                    metrics = entry.get("metrics") or {}
                    if not isinstance(metrics, dict):
                        metrics = {}
                    metrics_str = ", ".join(f"{k}={v}" for k, v in metrics.items())
                    md5s = "\n".join(
                        [str(a.get("md5", "")) for a in entry.get("artifacts", [])]
                    )
                    sizes = "\n".join(
                        [str(a.get("size", "")) for a in entry.get("artifacts", [])]
                    )
                    nfiles = "\n".join(
                        [str(a.get("nfiles", "")) for a in entry.get("artifacts", [])]
                    )
                    s3paths = "\n".join(
                        [str(a.get("s3_path", "")) for a in entry.get("artifacts", [])]
                    )
                    row += [experiment, metrics_str, md5s, sizes, nfiles, s3paths]
                table.add_row(*row)

            console.print(table)
        else:
            print("\nDVC Push History:\n")
            for entry in history:
                print(f"Commit: {entry.get('commit', '')!s}")
                print(f"Message: {entry.get('message', '')!s}")
                print(f"Author: {entry.get('author', '')!s}")
                print(f"Commit Date: {entry.get('commit_date', '')!s}")
                print(f"Push Time: {entry.get('push_time', '')!s}")
                tags = entry.get("tags") or []
                if not isinstance(tags, list):
                    tags = []
                print(f"Tags: {', '.join(str(t) for t in tags)}")
                if show_all:
                    print(f"Experiment: {entry.get('experiment_name', '')!s}")
                    metrics = entry.get("metrics") or {}
                    if not isinstance(metrics, dict):
                        metrics = {}
                    print(f"Metrics: {metrics}")
                print("Artifacts:")
                for art in entry.get("artifacts", []):
                    print(f"  - Path: {art.get('repo_path', '')!s}")
                    print(f"    MD5: {art.get('md5', '')!s}")
                    print(f"    Size: {art.get('size', '')!s}")
                    print(f"    Files: {art.get('nfiles', '')!s}")
                    print(f"    Remote Path: {art.get('s3_path', '')!s}")
                    changes = art.get("changed_files_count", {})
                    changed_files = art.get("changed_files", {})
                    if changed_files and art.get("repo_path", "").endswith("train"):
                        sub_summary = summarize_subfolders_count_only(changed_files)
                        for sub, counts in sub_summary.items():
                            print(
                                f"    [{sub}] Added: {counts['added']}, Removed: {counts['removed']}, Modified: {counts['modified']}"
                            )
                        total = {
                            k: sum(v[k] for v in sub_summary.values())
                            for k in ["added", "removed", "modified"]
                        }
                        print(
                            f"    [Total] Added: {total['added']}, Removed: {total['removed']}, Modified: {total['modified']}"
                        )
                    elif changes:
                        print(
                            f"    [Changes] Added: {changes.get('added', 0)}, Removed: {changes.get('removed', 0)}, Modified: {changes.get('modified', 0)}"
                        )
                print("-" * 50)
        return 0
