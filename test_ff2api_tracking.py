#!/usr/bin/env python3
"""
Test if tracking functionality is available through the main FF2API endpoint
"""

import requests

def test_ff2api_tracking():
    api_key = 'augment-brokerage|YOUR_API_KEY_HERE'

    print('=== Testing tracking through main FF2API endpoint ===')
    
    try:
        # Get FF2API access token
        token_response = requests.post(
            'https://api.prod.goaugment.com/token/refresh',
            headers={'Content-Type': 'application/json'},
            json={'refreshToken': api_key},
            timeout=10
        )
        
        if token_response.status_code == 200:
            token_data = token_response.json()
            access_token = token_data.get('accessToken')
            print(f'✓ Got FF2API access token: {access_token[:20]}...')
            
            headers = {'Authorization': f'Bearer {access_token}'}
            
            # Test different potential tracking endpoints on the main API
            tracking_endpoints = [
                'https://api.prod.goaugment.com/v2/tracking',
                'https://api.prod.goaugment.com/tracking',
                'https://api.prod.goaugment.com/v2/tracking/pro-number/0968391969',
                'https://api.prod.goaugment.com/tracking/pro-number/0968391969',
                'https://api.prod.goaugment.com/v2/loads/tracking/0968391969',
                'https://api.prod.goaugment.com/loads/tracking/0968391969',
                'https://api.prod.goaugment.com/unstable/tracking',
                'https://api.prod.goaugment.com/unstable/completed-browser-task',
            ]
            
            for endpoint in tracking_endpoints:
                try:
                    if 'pro-number' in endpoint or '0968391969' in endpoint:
                        # Direct PRO lookup
                        response = requests.get(endpoint, headers=headers, timeout=10)
                    else:
                        # Try with parameters
                        response = requests.get(
                            endpoint,
                            params={'pro': '0968391969', 'carrier': 'ESTES'},
                            headers=headers,
                            timeout=10
                        )
                    
                    print(f'{endpoint}: {response.status_code}')
                    if response.status_code not in [401, 403, 404]:
                        print(f'  ✓ POTENTIAL SUCCESS: {response.status_code}')
                        if response.status_code == 200:
                            print(f'  Response preview: {response.text[:200]}')
                        
                except Exception as e:
                    print(f'{endpoint}: ERROR - {str(e)[:50]}')
        else:
            print(f'Failed to get FF2API token: {token_response.status_code}')
            
    except Exception as e:
        print(f'Error: {str(e)}')

    # Also test if there are any available endpoints
    print('\n=== Testing API discovery ===')
    try:
        token_response = requests.post(
            'https://api.prod.goaugment.com/token/refresh',
            headers={'Content-Type': 'application/json'},
            json={'refreshToken': api_key},
            timeout=10
        )
        
        if token_response.status_code == 200:
            token_data = token_response.json()
            access_token = token_data.get('accessToken')
            headers = {'Authorization': f'Bearer {access_token}'}
            
            # Try some common API discovery endpoints
            discovery_endpoints = [
                'https://api.prod.goaugment.com/',
                'https://api.prod.goaugment.com/api',
                'https://api.prod.goaugment.com/docs',
                'https://api.prod.goaugment.com/swagger',
            ]
            
            for endpoint in discovery_endpoints:
                try:
                    response = requests.get(endpoint, headers=headers, timeout=5)
                    print(f'{endpoint}: {response.status_code}')
                    if response.status_code == 200:
                        print(f'  Content type: {response.headers.get("content-type", "unknown")}')
                except Exception:
                    pass
    except Exception as e:
        print(f'Discovery error: {str(e)}')

if __name__ == '__main__':
    test_ff2api_tracking()