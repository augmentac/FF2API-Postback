#!/usr/bin/env python3
"""
Simple test to match the exact configuration that works in the app.
"""

import requests
import json

API_KEY = "augment-brokerage|YOUR_API_KEY_HERE"

# Test the exact endpoints from the working app
def test_token_refresh_endpoints():
    """Test token refresh with different base URLs."""
    print("=== Testing Token Refresh Endpoints ===")
    
    base_urls = [
        "https://load.prod.goaugment.com",
        "https://api.prod.goaugment.com", 
        "https://load.prod.goaugment.com/unstable/loads"
    ]
    
    for base_url in base_urls:
        print(f"\nTesting: {base_url}")
        
        try:
            # Strip trailing path for token refresh
            token_base = base_url.split('/unstable')[0] if '/unstable' in base_url else base_url
            token_url = f"{token_base}/token/refresh"
            
            headers = {'Content-Type': 'application/json'}
            payload = {'refreshToken': API_KEY}
            
            response = requests.post(token_url, headers=headers, json=payload, timeout=10)
            print(f"  Token refresh: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    access_token = data.get('accessToken')
                    if access_token:
                        print(f"  ✓ Got access token: {access_token[:20]}...")
                        
                        # Test using the access token
                        auth_headers = {
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {access_token}'
                        }
                        
                        # Test different load endpoints
                        load_endpoints = [
                            f"{base_url}/v2/loads",
                            f"{token_base}/v2/loads", 
                            f"{token_base}/unstable/loads"
                        ]
                        
                        for endpoint in load_endpoints:
                            try:
                                load_resp = requests.get(endpoint, headers=auth_headers, timeout=5)
                                print(f"    {endpoint}: {load_resp.status_code}")
                                if load_resp.status_code not in [401, 403]:
                                    print(f"    ✓ SUCCESS! Working endpoint found")
                                    return endpoint, access_token
                            except Exception as e:
                                print(f"    {endpoint}: ERR - {str(e)[:50]}")
                        
                except json.JSONDecodeError:
                    print(f"  Invalid JSON response: {response.text[:100]}")
            else:
                print(f"  Response: {response.text[:100]}")
                
        except Exception as e:
            print(f"  Error: {str(e)[:80]}")
    
    return None, None

def test_direct_bearer_variations():
    """Test using the API key directly as bearer token."""
    print("\n=== Testing Direct Bearer Token Variations ===")
    
    base_urls = [
        "https://load.prod.goaugment.com",
        "https://api.prod.goaugment.com"
    ]
    
    token_variations = [
        API_KEY,  # Full key
        API_KEY.split('|')[1] if '|' in API_KEY else API_KEY  # Token part only
    ]
    
    for base_url in base_urls:
        for i, token in enumerate(token_variations):
            print(f"\nTesting {base_url} with token variant {i+1}")
            
            headers = {
                'Content-Type': 'application/json', 
                'Authorization': f'Bearer {token}'
            }
            
            endpoints = [
                f"{base_url}/v2/loads",
                f"{base_url}/unstable/loads",
                f"{base_url}/loads"
            ]
            
            for endpoint in endpoints:
                try:
                    response = requests.get(endpoint, headers=headers, timeout=5)
                    print(f"  {endpoint}: {response.status_code}")
                    if response.status_code not in [401, 403]:
                        print(f"  ✓ SUCCESS! {endpoint} works with token variant {i+1}")
                        return endpoint, token
                except Exception as e:
                    print(f"  {endpoint}: ERR - {str(e)[:40]}")
    
    return None, None

def main():
    print("Simple Authentication Test - Matching Working App Configuration")
    print("=" * 80)
    
    # Test 1: Token refresh flow
    working_endpoint, access_token = test_token_refresh_endpoints()
    
    if working_endpoint:
        print(f"\n✓ SUCCESS: Found working configuration!")
        print(f"  Endpoint: {working_endpoint}")
        print(f"  Method: Token refresh → Bearer token")
        return True
    
    # Test 2: Direct bearer token
    working_endpoint, bearer_token = test_direct_bearer_variations()
    
    if working_endpoint:
        print(f"\n✓ SUCCESS: Found working configuration!")
        print(f"  Endpoint: {working_endpoint}")
        print(f"  Method: Direct bearer token")
        return True
    
    print("\n✗ No working authentication method found")
    print("This suggests the API key may need to be activated or configured differently.")
    return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)