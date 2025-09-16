# dvc/repo/logs.py
import os
import json
import subprocess
from datetime import datetime
from dvc.utils import relpath



HISTORY_FILE = ".dvc/push_history.json"  # Internal, versioned

def get_global_history_file():
    config_path = os.path.expanduser("~/.dvc_enhance_config.json")
    if os.path.exists(config_path):
        import json
        with open(config_path, "r") as f:
            cfg = json.load(f)
        if "global_log_path" in cfg:
            return os.path.expanduser(cfg["global_log_path"])
    return os.path.expanduser("~/.dvc_push_history_global.json")


def _run(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True, text=True).strip()



def _ensure_history_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump([], f)




def _load_history(path=None):
    if path is None:
        path = get_global_history_file()
    _ensure_history_file(path)
    with open(path, "r") as f:
        return json.load(f)

def _load_local_history(path=HISTORY_FILE):
    _ensure_history_file(path)
    with open(path, "r") as f:
        return json.load(f)

def _save_history(history, path=HISTORY_FILE):
    with open(path, "w") as f:
        json.dump(history, f, indent=2)


def _save_global_history(history, path=None):
    if path is None:
        path = get_global_history_file()
    with open(path, "w") as f:
        json.dump(history, f, indent=2)

def add_push_entry(repo, outs, experiment_name=None, metrics=None, tags=None):
    commit = _run("git rev-parse HEAD")
    message = _run("git log -1 --pretty=%s")
    author = _run("git log -1 --pretty=%an")
    commit_date = _run("git log -1 --pretty=%ad --date=iso")
    push_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")



    # Auto-capture experiment_name from params.yaml if present, else from branch name
    if not experiment_name:
        try:
            import yaml
            with open("params.yaml") as f:
                params = yaml.safe_load(f)
            experiment_name = params.get("experiment") or params.get("exp_name")
        except Exception:
            experiment_name = None
    if not experiment_name:
        # Try to use current git branch name
        try:
            experiment_name = _run("git rev-parse --abbrev-ref HEAD")
        except Exception:
            experiment_name = ""
    if not experiment_name:
        print("[DVC] WARNING: experiment_name is missing. Set 'experiment' in params.yaml for better tracking.")

    # Auto-capture metrics from metrics.json if present
    if not metrics:
        try:
            import json
            with open("metrics.json") as f:
                metrics = json.load(f)
            if not isinstance(metrics, dict):
                metrics = {}
        except Exception:
            metrics = {}

    # Ensure tags is always a list
    if not tags or not isinstance(tags, list):
        tags = []

    import os
    import hashlib
    def compute_size_nfiles(path):
        if os.path.isfile(path):
            return os.path.getsize(path), 1, {os.path.relpath(path): _file_md5(path)}
        elif os.path.isdir(path):
            total_size = 0
            total_files = 0
            file_md5s = {}
            for root, _, files in os.walk(path):
                for f in files:
                    fp = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(fp)
                        total_files += 1
                        file_md5s[os.path.relpath(fp)] = _file_md5(fp)
                    except Exception:
                        pass
            return total_size, total_files, file_md5s
        return "", "", {}

    def _file_md5(path):
        try:
            hash_md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
            return ""

    # Load previous history for diffing (from internal file only)
    history = _load_local_history(HISTORY_FILE)
    prev_artifacts = {}
    if history:
        last_entry = history[-1]
        for art in last_entry.get("artifacts", []):
            prev_artifacts[art.get("repo_path", "")] = art

    artifacts = []
    for o in outs:
        repo_path = relpath(o.fs_path) or ""
        md5 = o.hash_info.value or ""
        # Try to get size/nfiles from hash_info, else compute
        size = getattr(o.hash_info, "size", None)
        nfiles = getattr(o.hash_info, "nfiles", None)
        computed_size, computed_nfiles, file_md5s = compute_size_nfiles(o.fs_path)
        size = size or computed_size
        nfiles = nfiles or computed_nfiles

        # Try to resolve s3_path more robustly
        try:
            remote = repo.cloud.get_remote()
            odb = repo.cloud.get_remote_odb(remote)
            s3_path = odb.hash_to_path(md5) if md5 else ""
        except Exception:
            s3_path = ""

        # Compute changed files (added/removed/modified)
        prev = prev_artifacts.get(repo_path, {})
        prev_file_md5s = prev.get("file_md5s", {}) if isinstance(prev.get("file_md5s", {}), dict) else {}
        added = []
        removed = []
        modified = []
        for f, m in file_md5s.items():
            if f not in prev_file_md5s:
                added.append(f)
            elif prev_file_md5s[f] != m:
                modified.append(f)
        for f in prev_file_md5s:
            if f not in file_md5s:
                removed.append(f)

        artifacts.append(
            {
                "repo_path": repo_path,
                "md5": md5,
                "size": size if size is not None else "",
                "nfiles": nfiles if nfiles is not None else "",
                "s3_path": s3_path,
                "changed_files_count": {"added": len(added), "removed": len(removed), "modified": len(modified)},
                "changed_files": {"added": added, "removed": removed, "modified": modified},
                "file_md5s": file_md5s,
            }
        )


    new_entry = {
        "commit": commit or "",
        "message": message or "",
        "author": author or "",
        "commit_date": commit_date or "",
        "push_time": push_time or "",
        "artifacts": artifacts,
        "experiment_name": experiment_name or "",
        "metrics": metrics if isinstance(metrics, dict) else {},
        "tags": tags,
    }
    # Append to internal (repo) history only
    history.append(new_entry)
    _save_history(history, HISTORY_FILE)

    # Append to global (external) history only, never read from internal for diffing or merging
    global_history = _load_history()  # uses configured path
    global_history.append(new_entry)
    _save_global_history(global_history)



def show_logs(global_history=True):
    """
    By default, show logs from the global (external) file for full history.
    If global_history is False, show from the internal (repo) file.
    """
    if global_history:
        return _load_history()  # uses configured path
    else:
        return _load_history(HISTORY_FILE)
