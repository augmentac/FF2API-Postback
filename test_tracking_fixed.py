#!/usr/bin/env python3
"""
Test tracking with corrected brokerageKey = "eshipping"
"""

import requests
import json

# Bearer token from user
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFjVUZKbnV5TS1RbHVSNHdYUGZWViJ9.eyJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3JvbGVzIjpbImFkbWluIl0sImF1Z21lbnQtcHJvZHVjdGlvbi51cy5hdXRoMC5jb20vYnJva2VyYWdlS2V5IjoiYXVnbWVudC1icm9rZXJhZ2UiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3VzZXJJZCI6IjAxanoxMGZyZ2I1eTFuNGFmZ3p6bmFzaGp6IiwiYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS9vbmJvYXJkaW5nU3RhZ2UiOiJDT01QTEVURUQiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL2VtYWlsIjoiYW50aG9ueS5jYWZhcm9AZ29hdWdtZW50LmNvbSIsImlzcyI6Imh0dHBzOi8vYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNDk0MjA1NDY3Mjc0MzMxNDk4MiIsImF1ZCI6Imh0dHBzOi8vZ29hdWdtZW50LmNvbSIsImlhdCI6MTc1MzkwMDU2NywiZXhwIjoxNzUzOTA3NzY3LCJzY29wZSI6IiIsImF6cCI6IjNaOTBlTVBFZk5qUVlsak5TMzA4aXk5YWlIY3d3Y2dJIn0.jlx4Lfxs0ORVOdh_6iTvEnNx_f11PRSNUYN6EvPoIlsvpO5ok58Abst2a29wTYURQYr1iHCOjjCsuaNJrypTf3i9Xiu9WDzn83pCsBO8D62vJWKbAyk2P6VzjEZOeZouSJRanwoTDsUcjPrY2e1KWQb4Ek2tBjxiKZoIUv3KeUMf6l0Oicb8tO2kJqY4meEXdgyzsgoXIlDEa0Rm9NWRi0T7UTd8l8XtjLxI1a6tA9S6MA53IAkH_Rk0b-aeY6b_EqEMQkLndhwX0vKtB0jW9ZPR7VB_9CIVJG8hFwNudHloGNIl95HkowbUoxfxl5Z4xCT0NtHwyhBp6rgq0nCZcg"

def test_tracking_with_correct_brokerage():
    """Test tracking with brokerageKey = 'eshipping'"""
    print("=== Testing Tracking with brokerageKey = 'eshipping' ===")
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json',
        'User-Agent': 'FF2API-TrackingEnrichment/1.0'
    }
    
    # Test the real PRO numbers with correct brokerageKey
    pro_numbers = ['0968391969', '1400266820', '2121130165', '2121130168', '2121130170']
    
    for pro in pro_numbers:
        print(f"\n--- Testing PRO: {pro} ---")
        
        try:
            # Test main tracking endpoint with correct brokerageKey
            url = f'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/{pro}'
            params = {
                'brokerageKey': 'eshipping',  # Fixed to use 'eshipping'
                'browserTask': 'ESTES'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=15)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"✓ SUCCESS - Tracking data found!")
                    print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}")
                    
                    # Show relevant tracking info
                    if isinstance(data, dict):
                        if 'events' in data:
                            print(f"Events count: {len(data['events'])}")
                            if data['events']:
                                latest = data['events'][0]
                                print(f"Latest event: {latest.get('status', 'N/A')} at {latest.get('location', 'N/A')}")
                        
                        if 'status' in data:
                            print(f"Overall status: {data['status']}")
                            
                        if 'location' in data:
                            print(f"Current location: {data['location']}")
                            
                        # Print first 300 chars of response for inspection
                        print(f"Response preview: {json.dumps(data, indent=2)[:300]}...")
                        
                except json.JSONDecodeError:
                    print(f"✓ SUCCESS - Non-JSON response: {response.text[:200]}...")
                    
            elif response.status_code == 404:
                print(f"⚠ Not found - PRO may not exist or no tracking data available")
                
            elif response.status_code == 401:
                print(f"✗ Authentication failed: {response.text[:100]}")
                
            elif response.status_code == 422:
                print(f"⚠ Validation error: {response.text[:200]}")
                
            else:
                print(f"Response ({response.status_code}): {response.text[:200]}")
                
        except Exception as e:
            print(f"ERROR: {str(e)}")

def main():
    print("Testing Tracking API with Correct brokerageKey")
    print("=" * 50)
    test_tracking_with_correct_brokerage()

if __name__ == "__main__":
    main()