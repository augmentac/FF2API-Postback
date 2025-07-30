#!/usr/bin/env python3
"""
Test single PRO number: 2221294463
"""

import requests
import json

# Bearer token from user
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFjVUZKbnV5TS1RbHVSNHdYUGZWViJ9.eyJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3JvbGVzIjpbImFkbWluIl0sImF1Z21lbnQtcHJvZHVjdGlvbi51cy5hdXRoMC5jb20vYnJva2VyYWdlS2V5IjoiYXVnbWVudC1icm9rZXJhZ2UiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3VzZXJJZCI6IjAxanoxMGZyZ2I1eTFuNGFmZ3p6bmFzaGp6IiwiYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS9vbmJvYXJkaW5nU3RhZ2UiOiJDT01QTEVURUQiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL2VtYWlsIjoiYW50aG9ueS5jYWZhcm9AZ29hdWdtZW50LmNvbSIsImlzcyI6Imh0dHBzOi8vYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNDk0MjA1NDY3Mjc0MzMxNDk4MiIsImF1ZCI6Imh0dHBzOi8vZ29hdWdtZW50LmNvbSIsImlhdCI6MTc1MzkwMDU2NywiZXhwIjoxNzUzOTA3NzY3LCJzY29wZSI6IiIsImF6cCI6IjNaOTBlTVBFZk5qUVlsak5TMzA4aXk5YWlIY3d3Y2dJIn0.jlx4Lfxs0ORVOdh_6iTvEnNx_f11PRSNUYN6EvPoIlsvpO5ok58Abst2a29wTYURQYr1iHCOjjCsuaNJrypTf3i9Xiu9WDzn83pCsBO8D62vJWKbAyk2P6VzjEZOeZouSJRanwoTDsUcjPrY2e1KWQb4Ek2tBjxiKZoIUv3KeUMf6l0Oicb8tO2kJqY4meEXdgyzsgoXIlDEa0Rm9NWRi0T7UTd8l8XtjLxI1a6tA9S6MA53IAkH_Rk0b-aeY6b_EqEMQkLndhwX0vKtB0jW9ZPR7VB_9CIVJG8hFwNudHloGNIl95HkowbUoxfxl5Z4xCT0NtHwyhBp6rgq0nCZcg"

def test_single_pro():
    """Test single PRO: 2221294463"""
    
    pro_number = "2221294463"
    
    print(f"TESTING PRO: {pro_number}")
    print("=" * 40)
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json',
        'User-Agent': 'FF2API-TrackingEnrichment/1.0'
    }
    
    params = {
        'brokerageKey': 'eshipping',
        'browserTask': 'ESTES'
    }
    
    url = f'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/{pro_number}'
    
    print("REQUEST:")
    print(f"  URL: {url}")
    print(f"  Params: {params}")
    print(f"  Auth: Bearer {BEARER_TOKEN[:20]}...")
    print()
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        print("RESPONSE:")
        print(f"  Status: {response.status_code}")
        print(f"  Headers: {dict(response.headers)}")
        print()
        print("BODY:")
        
        if response.status_code == 200:
            print("🎉 SUCCESS!")
            try:
                data = response.json()
                print(json.dumps(data, indent=2))
                
                # Extract key tracking info
                print("\n" + "="*30)
                print("TRACKING SUMMARY:")
                if isinstance(data, dict):
                    if 'events' in data and data['events']:
                        print(f"Events found: {len(data['events'])}")
                        latest = data['events'][0]
                        print(f"Latest status: {latest.get('status', 'N/A')}")
                        print(f"Latest location: {latest.get('location', 'N/A')}")
                        print(f"Latest date: {latest.get('date', 'N/A')}")
                    
                    if 'status' in data:
                        print(f"Overall status: {data['status']}")
                    
                    if 'location' in data:
                        print(f"Current location: {data['location']}")
                        
            except json.JSONDecodeError:
                print("Non-JSON response:")
                print(response.text)
                
        elif response.status_code == 404:
            print("❌ NOT FOUND")
            try:
                error_data = response.json()
                print(json.dumps(error_data, indent=2))
            except:
                print(response.text)
                
        elif response.status_code == 401:
            print("❌ AUTHENTICATION FAILED")
            print(response.text)
            
        else:
            print(f"❌ ERROR {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"❌ REQUEST FAILED: {e}")

def main():
    test_single_pro()

if __name__ == "__main__":
    main()