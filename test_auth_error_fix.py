#!/usr/bin/env python3
"""
Test that git authentication errors are properly reported in DVC fetch/import operations.
"""

import tempfile
import unittest.mock
from unittest.mock import MagicMock, patch

import pytest
from scmrepo.exceptions import AuthError

from dvc.repo.open_repo import _pull, clone
from dvc.scm import GitAuthError


def test_pull_auth_error_propagation():
    """Test that _pull properly converts AuthError to GitAuthError."""
    mock_git = MagicMock()
    mock_git.fetch.side_effect = AuthError("Authentication failed")
    
    with pytest.raises(GitAuthError) as exc_info:
        _pull(mock_git)
    
    assert "Authentication failed" in str(exc_info.value)
    assert "See https://dvc.org/doc/user-guide/troubleshooting#git-auth" in str(exc_info.value)


def test_pull_fetch_all_exps_auth_error():
    """Test that _pull handles AuthError from fetch_all_exps."""
    mock_git = MagicMock()
    mock_git.fetch.return_value = None  # fetch succeeds
    
    with patch("dvc.repo.open_repo.fetch_all_exps") as mock_fetch_all_exps:
        mock_fetch_all_exps.side_effect = AuthError("Authentication failed for experiments")
        
        with pytest.raises(GitAuthError) as exc_info:
            _pull(mock_git)
        
        assert "Authentication failed for experiments" in str(exc_info.value)


def test_clone_auth_error_propagation():
    """Test that clone properly converts AuthError to GitAuthError."""
    with patch("dvc.scm.Git.clone") as mock_git_clone:
        mock_git_clone.side_effect = AuthError("Bad PAT token")
        
        with pytest.raises(GitAuthError) as exc_info:
            clone("https://github.com/test/repo.git", "/tmp/test")
        
        assert "Bad PAT token" in str(exc_info.value)
        assert "See https://dvc.org/doc/user-guide/troubleshooting#git-auth" in str(exc_info.value)


def test_clone_fetch_all_exps_auth_error():
    """Test that clone handles AuthError from fetch_all_exps."""
    mock_git = MagicMock()
    
    with patch("dvc.scm.Git.clone", return_value=mock_git):
        with patch("dvc.repo.experiments.utils.fetch_all_exps") as mock_fetch_all_exps:
            mock_fetch_all_exps.side_effect = AuthError("Experiments fetch auth failed")
            
            with pytest.raises(GitAuthError) as exc_info:
                clone("https://github.com/test/repo.git", "/tmp/test")
            
            assert "Experiments fetch auth failed" in str(exc_info.value)


if __name__ == "__main__":
    # Run basic tests
    test_pull_auth_error_propagation()
    test_pull_fetch_all_exps_auth_error() 
    test_clone_auth_error_propagation()
    test_clone_fetch_all_exps_auth_error()
    print("✅ All tests passed!")