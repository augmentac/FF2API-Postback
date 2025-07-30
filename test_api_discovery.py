#!/usr/bin/env python3
"""
API Discovery test to understand the authentication and endpoint structure.
"""

import requests
import json

BASE_URL = "https://load.prod.goaugment.com"
TRACKING_URL = "https://track-and-trace-agent.prod.goaugment.com"
API_KEY = "augment-brokerage|YOUR_API_KEY_HERE"

def test_endpoint_discovery():
    """Test various endpoint patterns to understand API structure."""
    print("=== API Endpoint Discovery ===")
    
    endpoints_to_test = [
        "/",
        "/health",
        "/status", 
        "/api",
        "/v1",
        "/v2",
        "/unstable",
        "/unstable/loads",
        "/v2/loads",
        "/loads"
    ]
    
    headers = {'Content-Type': 'application/json'}
    
    for endpoint in endpoints_to_test:
        try:
            url = f"{BASE_URL}{endpoint}"
            response = requests.get(url, headers=headers, timeout=5)
            print(f"{endpoint:20} -> {response.status_code:3d} | {response.text[:100] if response.text else 'No content'}")
        except Exception as e:
            print(f"{endpoint:20} -> ERR | {str(e)[:80]}")

def test_tracking_api_discovery():
    """Test tracking API endpoints."""
    print("\n=== Tracking API Discovery ===")
    
    endpoints_to_test = [
        "/",
        "/health",
        "/unstable",
        "/unstable/completed-browser-task",
        "/unstable/completed-browser-task/pro-number/0968391969"
    ]
    
    headers = {'Content-Type': 'application/json'}
    
    for endpoint in endpoints_to_test:
        try:
            url = f"{TRACKING_URL}{endpoint}"
            response = requests.get(url, headers=headers, timeout=5)
            print(f"{endpoint:50} -> {response.status_code:3d} | {response.text[:100] if response.text else 'No content'}")
        except Exception as e:
            print(f"{endpoint:50} -> ERR | {str(e)[:80]}")

def test_auth_headers():
    """Test different authentication header formats."""
    print("\n=== Authentication Header Testing ===")
    
    test_url = f"{BASE_URL}/v2/loads"
    
    auth_variations = [
        {'Authorization': f'Bearer {API_KEY}'},
        {'Authorization': f'Bearer {API_KEY.split("|")[1]}'},
        {'X-API-Key': API_KEY},
        {'X-API-Key': API_KEY.split("|")[1]},
        {'Authorization': f'ApiKey {API_KEY}'},
        {'Authorization': f'Token {API_KEY}'},
        {'brokerage-key': 'augment-brokerage', 'Authorization': f'Bearer {API_KEY.split("|")[1]}'},
    ]
    
    base_headers = {'Content-Type': 'application/json'}
    
    for i, auth_header in enumerate(auth_variations, 1):
        headers = {**base_headers, **auth_header}
        try:
            response = requests.get(test_url, headers=headers, timeout=5)
            auth_desc = str(auth_header)[:60] + "..." if len(str(auth_header)) > 60 else str(auth_header)
            print(f"Test {i:2d}: {response.status_code:3d} | {auth_desc}")
            if response.status_code != 401:
                print(f"         Response: {response.text[:100]}")
        except Exception as e:
            print(f"Test {i:2d}: ERR | {str(e)[:60]}")

def test_tracking_auth():
    """Test tracking API with different auth approaches."""
    print("\n=== Tracking API Authentication Testing ===")
    
    test_url = f"{TRACKING_URL}/unstable/completed-browser-task/pro-number/0968391969"
    params = {
        'brokerageKey': 'eshipping',
        'browserTask': 'ESTES'
    }
    
    auth_variations = [
        {'Authorization': f'Bearer {API_KEY}'},
        {'Authorization': f'Bearer {API_KEY.split("|")[1]}'},
        {'X-API-Key': API_KEY},
        {'X-API-Key': API_KEY.split("|")[1]}
    ]
    
    base_headers = {'Content-Type': 'application/json'}
    
    for i, auth_header in enumerate(auth_variations, 1):
        headers = {**base_headers, **auth_header}
        try:
            response = requests.get(test_url, params=params, headers=headers, timeout=10)
            auth_desc = str(auth_header)[:60] + "..." if len(str(auth_header)) > 60 else str(auth_header)
            print(f"Test {i:2d}: {response.status_code:3d} | {auth_desc}")
            if response.status_code not in [401, 403]:
                print(f"         Response: {response.text[:200]}")
        except Exception as e:
            print(f"Test {i:2d}: ERR | {str(e)[:60]}")

def main():
    print("API Discovery and Authentication Testing")
    print("=" * 80)
    
    test_endpoint_discovery()
    test_tracking_api_discovery()
    test_auth_headers()
    test_tracking_auth()
    
    print("\n" + "=" * 80)
    print("Discovery complete. Check results above for working endpoints/auth methods.")

if __name__ == "__main__":
    main()