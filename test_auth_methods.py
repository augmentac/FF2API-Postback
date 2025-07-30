#!/usr/bin/env python3
"""
Test additional authentication methods with the confirmed valid API key.
"""

import requests
import json
import base64

API_KEY = "augment-brokerage|YOUR_API_KEY_HERE"
BASE_URL = "https://load.prod.goaugment.com"
BROKERAGE_KEY = "augment-brokerage"
TOKEN_PART = API_KEY.split('|')[1]

def test_basic_auth():
    """Test HTTP Basic Authentication."""
    print("=== Testing Basic Authentication ===")
    
    # Try with brokerage-key:token format
    credentials = f"{BROKERAGE_KEY}:{TOKEN_PART}"
    encoded = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(f"{BASE_URL}/v2/loads", headers=headers, timeout=10)
        print(f"Basic Auth (brokerage:token): {response.status_code}")
        if response.status_code != 401:
            print(f"Response: {response.text[:200]}")
            return True
    except Exception as e:
        print(f"Basic auth error: {e}")
    
    return False

def test_custom_headers():
    """Test custom authentication headers."""
    print("\n=== Testing Custom Headers ===")
    
    test_cases = [
        # Brokerage key in header with token
        {
            'headers': {
                'brokerage-key': BROKERAGE_KEY,
                'Authorization': f'Bearer {TOKEN_PART}',
                'Content-Type': 'application/json'
            },
            'description': 'Brokerage header + Bearer token'
        },
        # API key in custom header
        {
            'headers': {
                'x-api-key': API_KEY,
                'Content-Type': 'application/json'
            },
            'description': 'Full API key in x-api-key'
        },
        # Token in custom header with brokerage
        {
            'headers': {
                'x-api-key': TOKEN_PART,
                'x-brokerage-key': BROKERAGE_KEY,
                'Content-Type': 'application/json'
            },
            'description': 'Token + brokerage in separate headers'
        },
        # Authorization with custom scheme
        {
            'headers': {
                'Authorization': f'API-Key {API_KEY}',
                'Content-Type': 'application/json'
            },
            'description': 'Authorization: API-Key'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        try:
            response = requests.get(f"{BASE_URL}/v2/loads", headers=test_case['headers'], timeout=10)
            print(f"Test {i}: {response.status_code:3d} | {test_case['description']}")
            if response.status_code not in [401, 403]:
                print(f"         Success! Response: {response.text[:100]}")
                return True
        except Exception as e:
            print(f"Test {i}: ERR | {test_case['description']} - {str(e)[:50]}")
    
    return False

def test_load_api_variations():
    """Test different load API endpoint variations."""
    print("\n=== Testing Load API Endpoint Variations ===")
    
    endpoints = [
        "/loads",
        "/api/loads", 
        "/api/v1/loads",
        "/api/v2/loads",
        "/v1/loads",
        "/v2/loads",
        f"/loads/brokerage/{BROKERAGE_KEY}",
        f"/api/loads/brokerage/{BROKERAGE_KEY}",
        "/unstable/loads"
    ]
    
    # Use the most promising auth method from earlier tests
    headers = {
        'Authorization': f'Bearer {TOKEN_PART}',
        'brokerage-key': BROKERAGE_KEY,
        'Content-Type': 'application/json'
    }
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=5)
            print(f"{endpoint:30} -> {response.status_code:3d}")
            if response.status_code not in [401, 403, 404]:
                print(f"                              Success! {response.text[:100]}")
                return endpoint
        except Exception as e:
            print(f"{endpoint:30} -> ERR | {str(e)[:40]}")
    
    return None

def test_post_request():
    """Test POST request to see if GET vs POST makes a difference."""
    print("\n=== Testing POST Request ===")
    
    headers = {
        'Authorization': f'Bearer {TOKEN_PART}',
        'brokerage-key': BROKERAGE_KEY,
        'Content-Type': 'application/json'
    }
    
    # Simple payload for load creation
    payload = {
        'loadNumber': 'TEST_AUTH_001',
        'mode': 'FTL',
        'rateType': 'SPOT'
    }
    
    try:
        response = requests.post(f"{BASE_URL}/v2/loads", headers=headers, json=payload, timeout=10)
        print(f"POST /v2/loads: {response.status_code}")
        print(f"Response: {response.text[:300]}")
        
        if response.status_code in [200, 201]:
            return True
        elif response.status_code == 400:
            print("Bad request - but authentication may be working!")
            return True
            
    except Exception as e:
        print(f"POST error: {e}")
    
    return False

def test_different_base_urls():
    """Test different base URL patterns."""
    print("\n=== Testing Different Base URLs ===")
    
    base_urls = [
        "https://load.prod.goaugment.com",
        "https://api.prod.goaugment.com", 
        "https://loads.prod.goaugment.com",
        "https://ff2api.prod.goaugment.com"
    ]
    
    headers = {
        'Authorization': f'Bearer {TOKEN_PART}',
        'Content-Type': 'application/json'
    }
    
    for base_url in base_urls:
        try:
            response = requests.get(f"{base_url}/v2/loads", headers=headers, timeout=5)
            print(f"{base_url:35} -> {response.status_code:3d}")
            if response.status_code not in [401, 403]:
                print(f"                                   Success! {response.text[:50]}")
                return base_url
        except Exception as e:
            print(f"{base_url:35} -> ERR | {str(e)[:30]}")
    
    return None

def main():
    print("Advanced Authentication Method Testing")
    print("=" * 80)
    print(f"API Key: {API_KEY[:30]}...")
    print(f"Token Part: {TOKEN_PART[:30]}...")
    print("=" * 80)
    
    success_methods = []
    
    if test_basic_auth():
        success_methods.append("Basic Authentication")
    
    if test_custom_headers():
        success_methods.append("Custom Headers")
    
    working_endpoint = test_load_api_variations()
    if working_endpoint:
        success_methods.append(f"Endpoint: {working_endpoint}")
    
    if test_post_request():
        success_methods.append("POST Request")
    
    working_base_url = test_different_base_urls()
    if working_base_url:
        success_methods.append(f"Base URL: {working_base_url}")
    
    print("\n" + "=" * 80)
    print("RESULTS:")
    if success_methods:
        print("✓ Working authentication methods found:")
        for method in success_methods:
            print(f"  - {method}")
    else:
        print("✗ No working authentication methods found")
        print("\nPossible next steps:")
        print("1. Verify the API key is active and has correct permissions")
        print("2. Check if there are additional setup steps required")
        print("3. Confirm the correct base URL and endpoint structure")
        print("4. Check if there are IP restrictions or other security measures")
    
    return len(success_methods) > 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)