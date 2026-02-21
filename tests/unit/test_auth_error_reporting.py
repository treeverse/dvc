from unittest.mock import MagicMock, patch

import pytest
from scmrepo.exceptions import CloneError as InternalCloneError
from scmrepo.exceptions import SCMError

from dvc.repo.open_repo import _pull
from dvc.scm import GitAuthError, clone


class TestAuthErrorReporting:
    """Test that authentication errors are properly reported instead of silently ignored."""

    def test_clone_auth_error_detection(self):
        """Test that clone function detects and reports authentication failures."""
        # Test various authentication error messages
        auth_error_messages = [
            "Authentication failed for 'https://github.com/user/repo.git'",
            "could not read Username for 'https://github.com': terminal prompts disabled",
            "could not read Password for 'https://user@github.com': terminal prompts disabled",
            "remote: Invalid username or password.",
            "fatal: Authentication failed for 'https://github.com/user/repo.git/'",
            "remote: Repository not found.",
            "fatal: repository 'https://github.com/user/private-repo.git/' not found",
            "Permission denied (publickey).",
            "fatal: Could not read from remote repository.",
            "error: The requested URL returned error: 401 Unauthorized",
            "error: The requested URL returned error: 403 Forbidden",
            "fatal: unable to access 'https://github.com/user/repo.git/': The requested URL returned error: 401",
        ]

        for auth_msg in auth_error_messages:
            with patch("dvc.scm.Git.clone") as mock_git_clone:
                mock_git_clone.side_effect = InternalCloneError(auth_msg)

                with pytest.raises(GitAuthError) as exc_info:
                    clone("https://github.com/user/repo.git", "/tmp/test")

                # Verify the error message contains the original error and is descriptive
                assert "Git authentication failed" in str(exc_info.value)
                assert auth_msg in str(exc_info.value)
                assert "https://dvc.org/doc/user-guide/troubleshooting#git-auth" in str(
                    exc_info.value
                )

    def test_clone_non_auth_error_passthrough(self):
        """Test that non-authentication errors are still raised as CloneError."""
        non_auth_errors = [
            "fatal: destination path 'repo' already exists and is not an empty directory.",
            "fatal: repository 'https://github.com/nonexistent/repo.git' not found",
            "fatal: unable to connect to github.com",
            "Network unreachable",
        ]

        for error_msg in non_auth_errors:
            with patch("dvc.scm.Git.clone") as mock_git_clone:
                mock_git_clone.side_effect = InternalCloneError(error_msg)

                with pytest.raises(Exception) as exc_info:
                    clone("https://github.com/user/repo.git", "/tmp/test")

                # Should not be a GitAuthError, but still should be an error
                assert not isinstance(exc_info.value, GitAuthError)
                assert error_msg in str(exc_info.value)

    def test_pull_auth_error_detection(self):
        """Test that _pull function detects and reports authentication failures."""
        mock_git = MagicMock()

        auth_error_messages = [
            "Authentication failed",
            "could not read Password",
            "fatal: unable to access 'https://github.com/user/repo.git/': The requested URL returned error: 401",
        ]

        for auth_msg in auth_error_messages:
            mock_git.fetch.side_effect = SCMError(auth_msg)

            with pytest.raises(GitAuthError) as exc_info:
                _pull(mock_git)

            assert "Git authentication failed during fetch" in str(exc_info.value)
            assert auth_msg in str(exc_info.value)

    def test_pull_non_auth_error_passthrough(self):
        """Test that non-authentication SCM errors in _pull are re-raised."""
        mock_git = MagicMock()

        non_auth_error = SCMError("Network unreachable")
        mock_git.fetch.side_effect = non_auth_error

        with pytest.raises(SCMError) as exc_info:
            _pull(mock_git)

        assert exc_info.value is non_auth_error
        assert not isinstance(exc_info.value, GitAuthError)

    def test_auth_error_case_insensitive_detection(self):
        """Test that authentication error detection is case-insensitive."""
        case_variants = [
            "AUTHENTICATION FAILED",
            "Authentication Failed",
            "Could Not Read Username",
            "BAD CREDENTIALS",
            "Access DENIED",
        ]

        for auth_msg in case_variants:
            with patch("dvc.scm.Git.clone") as mock_git_clone:
                mock_git_clone.side_effect = InternalCloneError(auth_msg)

                with pytest.raises(GitAuthError):
                    clone("https://github.com/user/repo.git", "/tmp/test")
