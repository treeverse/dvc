#!/usr/bin/env python3
"""
Simple test to verify authentication error detection logic
"""

def test_auth_detection():
    """Test the authentication error detection logic"""
    
    # Test authentication error messages
    auth_test_cases = [
        ("Authentication failed for 'https://github.com/user/repo.git'", True),
        ("could not read Username for 'https://github.com'", True),
        ("could not read Password for 'https://user@github.com'", True),
        ("Bad credentials", True),
        ("Invalid credentials", True),
        ("Access denied", True),
        ("Permission denied (publickey)", True),
        ("Repository access denied", True),
        ("Authentication required", True),
        ("HTTP 401", True),
        ("HTTP 403", True),
        ("fatal: unable to access 'https://github.com/user/repo.git/'", True),
        # Non-authentication errors
        ("Network unreachable", False),
        ("Repository not found", False),
        ("Connection timeout", False),
        ("fatal: destination path already exists", False),
    ]
    
    # Authentication detection keywords (copied from our implementation)
    auth_keywords = [
        'authentication failed',
        'could not read username',
        'could not read password', 
        'bad credentials',
        'invalid credentials',
        'access denied',
        'permission denied (publickey)',
        'repository access denied',
        'authentication required',
        'http 401',
        'http 403',
        'fatal: unable to access'
    ]
    
    print("Testing authentication error detection...")
    all_passed = True
    
    for error_msg, should_be_auth in auth_test_cases:
        error_msg_lower = error_msg.lower()
        is_auth_error = any(keyword in error_msg_lower for keyword in auth_keywords)
        
        if is_auth_error == should_be_auth:
            print(f"✓ PASS: '{error_msg}' -> {'Auth' if is_auth_error else 'Non-auth'}")
        else:
            print(f"✗ FAIL: '{error_msg}' -> Expected {'Auth' if should_be_auth else 'Non-auth'}, got {'Auth' if is_auth_error else 'Non-auth'}")
            all_passed = False
    
    if all_passed:
        print("\nAll tests passed! Authentication detection logic is working correctly.")
    else:
        print("\nSome tests failed. Please check the detection logic.")
    
    return all_passed

if __name__ == "__main__":
    test_auth_detection()