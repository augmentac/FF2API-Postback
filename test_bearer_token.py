#!/usr/bin/env python3
"""
Test the enhanced workflow with the provided bearer token
"""

import requests
import json

# Test with provided bearer token
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFjVUZKbnV5TS1RbHVSNHdYUGZWViJ9.eyJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3JvbGVzIjpbImFkbWluIl0sImF1Z21lbnQtcHJvZHVjdGlvbi51cy5hdXRoMC5jb20vYnJva2VyYWdlS2V5IjoiYXVnbWVudC1icm9rZXJhZ2UiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3VzZXJJZCI6IjAxanoxMGZyZ2I1eTFuNGFmZ3p6bmFzaGp6IiwiYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS9vbmJvYXJkaW5nU3RhZ2UiOiJDT01QTEVURUQiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL2VtYWlsIjoiYW50aG9ueS5jYWZhcm9AZ29hdWdtZW50LmNvbSIsImlzcyI6Imh0dHBzOi8vYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNDk0MjA1NDY3Mjc0MzMxNDk4MiIsImF1ZCI6Imh0dHBzOi8vZ29hdWdtZW50LmNvbSIsImlhdCI6MTc1MzkwMDU2NywiZXhwIjoxNzUzOTA3NzY3LCJzY29wZSI6IiIsImF6cCI6IjNaOTBlTVBFZk5qUVlsak5TMzA4aXk5YWlIY3d3Y2dJIn0.jlx4Lfxs0ORVOdh_6iTvEnNx_f11PRSNUYN6EvPoIlsvpO5ok58Abst2a29wTYURQYr1iHCOjjCsuaNJrypTf3i9Xiu9WDzn83pCsBO8D62vJWKbAyk2P6VzjEZOeZouSJRanwoTDsUcjPrY2e1KWQb4Ek2tBjxiKZoIUv3KeUMf6l0Oicb8tO2kJqY4meEXdgyzsgoXIlDEa0Rm9NWRi0T7UTd8l8XtjLxI1a6tA9S6MA53IAkH_Rk0b-aeY6b_EqEMQkLndhwX0vKtB0jW9ZPR7VB_9CIVJG8hFwNudHloGNIl95HkowbUoxfxl5Z4xCT0NtHwyhBp6rgq0nCZcg"

def test_tracking_api():
    """Test the tracking API with the provided bearer token"""
    print("=== Testing Tracking API ===")
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json',
        'User-Agent': 'FF2API-TrackingEnrichment/1.0'
    }
    
    # Test PRO number from the user's test data
    pro_numbers = ['0968391969', '1400266820', '2121130165']
    
    for pro in pro_numbers:
        try:
            # Test tracking endpoint
            url = f'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/{pro}'
            params = {
                'brokerageKey': 'augment-brokerage',
                'browserTask': 'ESTES'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=15)
            print(f"PRO {pro}: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"  ✓ SUCCESS - Response: {json.dumps(data, indent=2)[:200]}...")
                except:
                    print(f"  ✓ SUCCESS - Text response: {response.text[:200]}...")
            elif response.status_code == 401:
                print(f"  ✗ Authentication failed")
                print(f"  Response: {response.text[:100]}")
            elif response.status_code == 404:
                print(f"  ⚠ Not found (may be expected for test data)")
            else:
                print(f"  Response: {response.text[:100]}")
                
        except Exception as e:
            print(f"PRO {pro}: ERROR - {str(e)}")

def test_load_api():
    """Test the load retrieval API with the provided bearer token"""
    print("\n=== Testing Load Retrieval API ===")
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    try:
        # Test load retrieval endpoint
        url = 'https://load.prod.goaugment.com/v2/loads/brokerage/augment-brokerage'
        
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Load retrieval: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"  ✓ SUCCESS - Found {len(data.get('loads', []))} loads")
                if data.get('loads'):
                    first_load = data['loads'][0]
                    print(f"  Sample load: {first_load.get('loadNumber', 'N/A')} - {first_load.get('status', 'N/A')}")
            except:
                print(f"  ✓ SUCCESS - Text response: {response.text[:200]}...")
        elif response.status_code == 401:
            print(f"  ✗ Authentication failed")
            print(f"  Response: {response.text[:100]}")
        else:
            print(f"  Response: {response.text[:100]}")
            
    except Exception as e:
        print(f"Load retrieval: ERROR - {str(e)}")

def test_ff2api_with_token_refresh():
    """Test FF2API load processing with token refresh (should use API key)"""
    print("\n=== Testing FF2API Load Processing (Token Refresh) ===")
    
    # This should use the API key method, not the bearer token
    api_key = "augment-brokerage|vd9P0-YNU2zNtCadcMDRsvNVfU5RntJYMOI-qI6sBd_XQ"
    
    try:
        # First get access token
        token_response = requests.post(
            'https://api.prod.goaugment.com/token/refresh',
            headers={'Content-Type': 'application/json'},
            json={'refreshToken': api_key},
            timeout=10
        )
        
        print(f"Token refresh: {token_response.status_code}")
        
        if token_response.status_code == 200:
            token_data = token_response.json()
            access_token = token_data.get('accessToken')
            print(f"  ✓ Got access token: {access_token[:20]}...")
            
            # Test load creation with proper payload structure
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'load': {
                    'loadNumber': 'TEST_BEARER_001',
                    'mode': 'FTL',
                    'rateType': 'SPOT',
                    'status': 'DRAFT',
                    'equipment': {'equipmentType': 'DRY_VAN'},
                    'items': [],
                    'route': [
                        {
                            'sequence': 1,
                            'stopActivity': 'PICKUP',
                            'address': {
                                'street1': '123 Test St',
                                'city': 'Test City',
                                'stateOrProvince': 'CA',
                                'country': 'US',
                                'postalCode': '90210'
                            },
                            'expectedArrivalWindowStart': '2024-01-01T08:00:00Z',
                            'expectedArrivalWindowEnd': '2024-01-01T17:00:00Z'
                        },
                        {
                            'sequence': 2,
                            'stopActivity': 'DELIVERY',
                            'address': {
                                'street1': '456 Test Ave',
                                'city': 'Test Town',
                                'stateOrProvince': 'TX',
                                'country': 'US',
                                'postalCode': '75001'
                            },
                            'expectedArrivalWindowStart': '2024-01-02T08:00:00Z',
                            'expectedArrivalWindowEnd': '2024-01-02T17:00:00Z'
                        }
                    ]
                },
                'customer': {'name': 'Test Customer'},
                'brokerage': {
                    'contacts': [
                        {
                            'name': 'Test Broker',
                            'email': 'test@example.com',
                            'phone': '555-123-4567',
                            'role': 'ACCOUNT_MANAGER'
                        }
                    ]
                }
            }
            
            load_response = requests.post(
                'https://api.prod.goaugment.com/v2/loads',
                headers=headers,
                json=payload,
                timeout=15
            )
            
            print(f"Load creation: {load_response.status_code}")
            if load_response.status_code in [200, 201]:
                print("  ✓ FF2API load processing successful!")
                try:
                    data = load_response.json()
                    print(f"  Load ID: {data.get('id', 'N/A')}")
                except:
                    pass
            else:
                print(f"  Response: {load_response.text[:200]}")
                
        else:
            print(f"  ✗ Token refresh failed: {token_response.text[:100]}")
            
    except Exception as e:
        print(f"FF2API test: ERROR - {str(e)}")

def main():
    print("Testing Enhanced Workflow with Provided Bearer Token")
    print("=" * 60)
    
    # Test 1: Tracking API with bearer token
    test_tracking_api()
    
    # Test 2: Load retrieval API with bearer token  
    test_load_api()
    
    # Test 3: FF2API with API key (token refresh method)
    test_ff2api_with_token_refresh()
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("- Tracking API: Uses provided bearer token")
    print("- Load retrieval: Uses provided bearer token") 
    print("- FF2API loads: Uses API key with token refresh")
    print("This demonstrates the dual authentication approach.")

if __name__ == "__main__":
    main()