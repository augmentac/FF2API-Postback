#!/usr/bin/env python3
"""
Simple authentication test to isolate the API key issue.
"""

import requests
import json
from urllib.parse import quote

# Test credentials
API_KEY = "augment-brokerage|YOUR_API_KEY_HERE"
BASE_URL = "https://load.prod.goaugment.com"

def test_bearer_token_direct():
    """Test using the full API key as a bearer token."""
    print("=== Testing Full API Key as Bearer Token ===")
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {API_KEY}'
    }
    
    # Try a simple GET request to test authentication
    try:
        response = requests.get(f"{BASE_URL}/v2/loads", headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        if response.text:
            print(f"Response Body (first 500 chars): {response.text[:500]}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_token_part_only():
    """Test using only the part after the pipe as bearer token."""
    print("\n=== Testing Token Part Only as Bearer Token ===")
    
    token_part = API_KEY.split('|')[1] if '|' in API_KEY else API_KEY
    print(f"Using token part: {token_part[:20]}...")
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token_part}'
    }
    
    try:
        response = requests.get(f"{BASE_URL}/v2/loads", headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        if response.text:
            print(f"Response Body (first 500 chars): {response.text[:500]}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_token_refresh():
    """Test the token refresh endpoint."""
    print("\n=== Testing Token Refresh Endpoint ===")
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    # Try with full API key
    payload = {'refreshToken': API_KEY}
    
    try:
        response = requests.post(f"{BASE_URL}/token/refresh", headers=headers, json=payload, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        if response.text:
            print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                access_token = data.get('accessToken')
                if access_token:
                    print(f"Received access token: {access_token[:20]}...")
                    return access_token
            except json.JSONDecodeError:
                print("Could not parse JSON response")
        
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def test_with_refreshed_token(access_token):
    """Test using a refreshed access token."""
    print(f"\n=== Testing with Refreshed Token ===")
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    
    try:
        response = requests.get(f"{BASE_URL}/v2/loads", headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        if response.text:
            print(f"Response Body (first 500 chars): {response.text[:500]}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    print("Starting Authentication Tests")
    print("=" * 60)
    
    # Test 1: Direct bearer token (full key)
    success1 = test_bearer_token_direct()
    
    # Test 2: Bearer token (token part only)  
    success2 = test_token_part_only()
    
    # Test 3: Token refresh
    access_token = test_token_refresh()
    
    # Test 4: Using refreshed token
    success4 = False
    if access_token:
        success4 = test_with_refreshed_token(access_token)
    
    print("\n" + "=" * 60)
    print("AUTHENTICATION TEST RESULTS:")
    print(f"Full API Key as Bearer: {'✓' if success1 else '✗'}")
    print(f"Token Part as Bearer: {'✓' if success2 else '✗'}")
    print(f"Token Refresh: {'✓' if access_token else '✗'}")
    print(f"Refreshed Token: {'✓' if success4 else '✗'}")
    
    if any([success1, success2, success4]):
        print("\n✓ At least one authentication method worked!")
        return True
    else:
        print("\n✗ No authentication methods worked")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)