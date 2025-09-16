import argparse
from dvc.cli.command import CmdBase
from dvc.repo.logs import show_logs

def add_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "tag",
        parents=[parent_parser],
        description="Tag a specific commit in DVC push history.",
        help="Tag a specific commit in DVC push history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("name", type=str, help="Tag name")
    parser.add_argument("commit", type=str, help="Commit hash to tag")
    parser.set_defaults(func=CmdTag)
    return parser

class CmdTag(CmdBase):
    def run(self):
        name = self.args.name
        commit = self.args.commit
        from dvc.repo.logs import _load_history, _save_history,_save_global_history, _load_local_history
        history = _load_history()
        
        found = False
        for entry in history:
            if entry.get("commit", "").startswith(commit):
                tags = entry.get("tags")
                if tags is None or not isinstance(tags, list):
                    tags = []
                if name not in tags:
                    tags.append(name)
                entry["tags"] = tags
                found = True
        if found:
            _save_global_history(history)
            print(f"Tag '{name}' added to commit {commit}.")
        else:
            print(f"Commit {commit} not found.")

        history2 = _load_local_history()

        found2 = False
        for entry in history2:
            if entry.get("commit", "").startswith(commit):
                tags = entry.get("tags")
                if tags is None or not isinstance(tags, list):
                    tags = []
                if name not in tags:
                    tags.append(name)
                entry["tags"] = tags
                found2 = True
        if found2:
            _save_history(history2)
            print(f"Tag '{name}' added to commit {commit}.")
        else:
            print(f"Commit {commit} not found.")


        return 0
