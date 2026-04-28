"""Tests for dvc.repo.experiments.push (queued experiments)."""

from unittest.mock import MagicMock

import pytest
from scmrepo.exceptions import AuthError

from dvc.repo.experiments.push import _push_queued
from dvc.repo.experiments.queue.base import QueueEntry
from dvc.repo.experiments.refs import CELERY_QUEUE


def _make_queue_entry(stash_rev, name=None):
    return QueueEntry(
        dvc_root="/repo",
        scm_root="/repo",
        stash_ref="refs/exps/celery/stash",
        stash_rev=stash_rev,
        baseline_rev="abc123",
        branch=None,
        name=name,
        head_rev="def456",
    )


class TestPushQueued:
    def test_push_queued_empty(self):
        """When no experiments are queued, returns empty list."""
        repo = MagicMock()
        repo.experiments.celery_queue.iter_queued.return_value = iter([])

        result = _push_queued(repo, "origin", force=False)

        assert result == {"queued": []}
        repo.scm.set_ref.assert_not_called()
        repo.scm.push_refspecs.assert_not_called()

    def test_push_queued_with_named_entries(self):
        """Named queued experiments are pushed as temp refs and names returned."""
        repo = MagicMock()
        entries = [
            _make_queue_entry("aaa111bbb222ccc333", name="exp-1"),
            _make_queue_entry("ddd444eee555fff666", name="exp-2"),
        ]
        repo.experiments.celery_queue.iter_queued.return_value = iter(entries)
        repo.scm.push_refspecs.return_value = {}

        result = _push_queued(repo, "origin", force=False)

        assert result == {"queued": ["exp-1", "exp-2"]}

        # Verify temp refs were created
        assert repo.scm.set_ref.call_count == 2
        repo.scm.set_ref.assert_any_call(
            f"{CELERY_QUEUE}/aaa111bbb222ccc333", "aaa111bbb222ccc333"
        )
        repo.scm.set_ref.assert_any_call(
            f"{CELERY_QUEUE}/ddd444eee555fff666", "ddd444eee555fff666"
        )

        # Verify push was called with correct refspecs
        repo.scm.push_refspecs.assert_called_once()
        call_args = repo.scm.push_refspecs.call_args
        assert call_args[0][0] == "origin"
        refspecs = call_args[0][1]
        assert len(refspecs) == 2
        assert call_args[1]["force"] is False

        # Verify temp refs were cleaned up
        assert repo.scm.remove_ref.call_count == 2

    def test_push_queued_unnamed_entries_use_short_rev(self):
        """Unnamed experiments fall back to short stash_rev as name."""
        repo = MagicMock()
        entries = [_make_queue_entry("aaa111bbb222ccc333", name=None)]
        repo.experiments.celery_queue.iter_queued.return_value = iter(entries)
        repo.scm.push_refspecs.return_value = {}

        result = _push_queued(repo, "origin", force=False)

        assert result == {"queued": ["aaa111b"]}

    def test_push_queued_force_flag(self):
        """Force flag is forwarded to push_refspecs."""
        repo = MagicMock()
        entries = [_make_queue_entry("aaa111bbb222ccc333", name="exp-1")]
        repo.experiments.celery_queue.iter_queued.return_value = iter(entries)
        repo.scm.push_refspecs.return_value = {}

        _push_queued(repo, "origin", force=True)

        call_args = repo.scm.push_refspecs.call_args
        assert call_args[1]["force"] is True

    def test_push_queued_cleans_up_refs_on_error(self):
        """Temp refs are cleaned up even if push_refspecs raises."""
        repo = MagicMock()
        entries = [_make_queue_entry("aaa111bbb222ccc333", name="exp-1")]
        repo.experiments.celery_queue.iter_queued.return_value = iter(entries)
        repo.scm.push_refspecs.side_effect = Exception("network error")

        with pytest.raises(Exception, match="network error"):
            _push_queued(repo, "origin", force=False)

        # Temp refs should still be cleaned up
        repo.scm.remove_ref.assert_called_once_with(
            f"{CELERY_QUEUE}/aaa111bbb222ccc333"
        )

    def test_push_queued_auth_error(self):
        """AuthError is wrapped in GitAuthError."""
        from dvc.scm import GitAuthError

        repo = MagicMock()
        entries = [_make_queue_entry("aaa111bbb222ccc333", name="exp-1")]
        repo.experiments.celery_queue.iter_queued.return_value = iter(entries)
        repo.scm.push_refspecs.side_effect = AuthError("bad credentials")

        with pytest.raises(GitAuthError):
            _push_queued(repo, "origin", force=False)

        # Temp refs should still be cleaned up
        repo.scm.remove_ref.assert_called_once()
