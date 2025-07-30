#!/usr/bin/env python3
"""
Test tracking API authentication methods to identify correct approach
"""

import requests

def test_tracking_authentication():
    api_key = 'augment-brokerage|YOUR_API_KEY_HERE'

    # Test 1: Try token refresh on tracking endpoint
    print('=== Testing tracking endpoint token refresh ===')
    try:
        response = requests.post(
            'https://track-and-trace-agent.prod.goaugment.com/token/refresh',
            headers={'Content-Type': 'application/json'},
            json={'refreshToken': api_key},
            timeout=10
        )
        print(f'Tracking token refresh: {response.status_code}')
        if response.status_code == 200:
            print('✓ Tracking endpoint supports token refresh')
        else:
            print(f'Response: {response.text[:200]}')
    except Exception as e:
        print(f'Error: {str(e)[:100]}')

    # Test 2: Try using the FF2API access token on tracking endpoint
    print('\n=== Testing with FF2API access token ===')
    try:
        # Get token from FF2API
        ff2api_response = requests.post(
            'https://api.prod.goaugment.com/token/refresh',
            headers={'Content-Type': 'application/json'},
            json={'refreshToken': api_key},
            timeout=10
        )
        
        if ff2api_response.status_code == 200:
            token_data = ff2api_response.json()
            access_token = token_data.get('accessToken')
            print(f'✓ Got FF2API access token: {access_token[:20]}...')
            
            # Try using this token on tracking endpoint
            test_url = 'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/0968391969'
            tracking_response = requests.get(
                test_url,
                params={'brokerageKey': 'eshipping', 'browserTask': 'ESTES'},
                headers={'Authorization': f'Bearer {access_token}'},
                timeout=10
            )
            print(f'Tracking API with FF2API token: {tracking_response.status_code}')
            if tracking_response.status_code != 401:
                print('✓ FF2API token works on tracking endpoint!')
                if tracking_response.status_code == 200:
                    print(f'Response: {tracking_response.text[:200]}')
            else:
                print('✗ FF2API token rejected by tracking endpoint')
                print(f'Response: {tracking_response.text[:200]}')
        else:
            print('Failed to get FF2API token')
            
    except Exception as e:
        print(f'Error: {str(e)[:100]}')

    # Test 3: Try raw API key as different auth methods
    print('\n=== Testing raw API key authentication ===')
    test_methods = [
        ('Bearer {api_key}', f'Bearer {api_key}'),
        ('API-Key {api_key}', f'API-Key {api_key}'),
        ('X-API-Key header', api_key),
    ]
    
    test_url = 'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/0968391969'
    
    for method_name, header_value in test_methods:
        try:
            if method_name.startswith('X-API-Key'):
                headers = {'X-API-Key': header_value}
            else:
                headers = {'Authorization': header_value}
            
            response = requests.get(
                test_url,
                params={'brokerageKey': 'eshipping', 'browserTask': 'ESTES'},
                headers=headers,
                timeout=10
            )
            print(f'{method_name}: {response.status_code}')
            if response.status_code not in [401, 403]:
                print(f'  ✓ SUCCESS with {method_name}!')
                break
        except Exception as e:
            print(f'{method_name}: ERROR - {str(e)[:50]}')

if __name__ == '__main__':
    test_tracking_authentication()