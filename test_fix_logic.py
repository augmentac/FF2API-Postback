#!/usr/bin/env python3
"""
Test the correct logic for the refresh_result fix
"""

def test_error_message_logic():
    """Test the correct error message generation logic"""
    
    # Test cases
    test_cases = [
        (None, "Token refresh returned None"),
        ({}, "Unknown token refresh error"),
        ({'message': 'Token expired'}, "Token expired"),
        ({'success': False}, "Unknown token refresh error"),
        ({'success': False, 'message': 'Invalid token'}, "Invalid token")
    ]
    
    for refresh_result, expected in test_cases:
        # Current (incorrect) logic
        error_message_old = f'Authentication failed: {refresh_result.get("message", "Unknown token refresh error") if refresh_result else "Token refresh returned None"}'
        
        # Corrected logic
        if refresh_result is None:
            error_part = "Token refresh returned None"
        else:
            error_part = refresh_result.get("message", "Unknown token refresh error")
        error_message_new = f'Authentication failed: {error_part}'
        
        print(f"Input: {refresh_result}")
        print(f"Expected: Authentication failed: {expected}")
        print(f"Old logic: {error_message_old}")
        print(f"New logic: {error_message_new}")
        print(f"New correct: {'✓' if error_message_new == f'Authentication failed: {expected}' else '✗'}")
        print()

if __name__ == "__main__":
    test_error_message_logic()