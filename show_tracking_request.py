#!/usr/bin/env python3
"""
Show the exact tracking request format with full details
"""

import requests
import json

# Bearer token from user
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFjVUZKbnV5TS1RbHVSNHdYUGZWViJ9.eyJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3JvbGVzIjpbImFkbWluIl0sImF1Z21lbnQtcHJvZHVjdGlvbi51cy5hdXRoMC5jb20vYnJva2VyYWdlS2V5IjoiYXVnbWVudC1icm9rZXJhZ2UiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3VzZXJJZCI6IjAxanoxMGZyZ2I1eTFuNGFmZ3p6bmFzaGp6IiwiYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS9vbmJvYXJkaW5nU3RhZ2UiOiJDT01QTEVURUQiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL2VtYWlsIjoiYW50aG9ueS5jYWZhcm9AZ29hdWdtZW50LmNvbSIsImlzcyI6Imh0dHBzOi8vYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNDk0MjA1NDY3Mjc0MzMxNDk4MiIsImF1ZCI6Imh0dHBzOi8vZ29hdWdtZW50LmNvbSIsImlhdCI6MTc1MzkwMDU2NywiZXhwIjoxNzUzOTA3NzY3LCJzY29wZSI6IiIsImF6cCI6IjNaOTBlTVBFZk5qUVlsak5TMzA4aXk5YWlIY3d3Y2dJIn0.jlx4Lfxs0ORVOdh_6iTvEnNx_f11PRSNUYN6EvPoIlsvpO5ok58Abst2a29wTYURQYr1iHCOjjCsuaNJrypTf3i9Xiu9WDzn83pCsBO8D62vJWKbAyk2P6VzjEZOeZouSJRanwoTDsUcjPrY2e1KWQb4Ek2tBjxiKZoIUv3KeUMf6l0Oicb8tO2kJqY4meEXdgyzsgoXIlDEa0Rm9NWRi0T7UTd8l8XtjLxI1a6tA9S6MA53IAkH_Rk0b-aeY6b_EqEMQkLndhwX0vKtB0jW9ZPR7VB_9CIVJG8hFwNudHloGNIl95HkowbUoxfxl5Z4xCT0NtHwyhBp6rgq0nCZcg"

def show_tracking_request_format():
    """Show the exact tracking request format"""
    
    pro_number = "0968391969"  # Example PRO
    
    # Headers
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'Content-Type': 'application/json',
        'User-Agent': 'FF2API-TrackingEnrichment/1.0'
    }
    
    # Parameters
    params = {
        'brokerageKey': 'eshipping',
        'browserTask': 'ESTES'
    }
    
    # URL
    url = f'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/{pro_number}'
    
    print("TRACKING REQUEST FORMAT")
    print("=" * 50)
    print()
    print("METHOD: GET")
    print()
    print("URL:")
    print(f"  {url}")
    print()
    print("HEADERS:")
    for key, value in headers.items():
        if key == 'Authorization':
            # Show token but truncate for security
            token_preview = value[:30] + "..." + value[-10:] if len(value) > 40 else value
            print(f"  {key}: {token_preview}")
        else:
            print(f"  {key}: {value}")
    print()
    print("QUERY PARAMETERS:")
    for key, value in params.items():
        print(f"  {key}: {value}")
    print()
    print("FULL REQUEST URL WITH PARAMS:")
    param_string = "&".join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{url}?{param_string}"
    print(f"  {full_url}")
    print()
    
    # Show equivalent curl command
    print("EQUIVALENT CURL COMMAND:")
    print("=" * 30)
    curl_headers = " ".join([f'-H "{k}: {v[:30] + "..." if k == "Authorization" else v}"' for k, v in headers.items()])
    curl_params = "&".join([f"{k}={v}" for k, v in params.items()])
    print(f'curl -X GET \\')
    print(f'  "{url}?{curl_params}" \\')
    for key, value in headers.items():
        if key == 'Authorization':
            print(f'  -H "{key}: Bearer [REDACTED_TOKEN]" \\')
        else:
            print(f'  -H "{key}: {value}" \\')
    print('  --timeout 15')
    print()
    
    # Actually make the request to show response
    print("ACTUAL REQUEST RESPONSE:")
    print("=" * 25)
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
        print()
        print(f"Response Body:")
        if response.text:
            try:
                # Try to parse as JSON for pretty printing
                data = response.json()
                print(json.dumps(data, indent=2))
            except:
                print(response.text)
        else:
            print("(empty response body)")
            
    except Exception as e:
        print(f"Request failed: {e}")

def main():
    show_tracking_request_format()

if __name__ == "__main__":
    main()