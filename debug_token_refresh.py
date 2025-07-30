#!/usr/bin/env python3
"""
Debug the token refresh process to see exactly what's happening
"""

import sys
import os
import requests
import json
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_token_refresh():
    """Debug the token refresh process step by step"""
    print("=== Token Refresh Debug ===")
    
    try:
        from src.backend.api_client import LoadsAPIClient
        
        # Test with the test API key to see the refresh process
        base_url = "https://api.prod.goaugment.com"
        api_key = "test-api-key-123"
        
        print(f"🔗 Base URL: {base_url}")
        print(f"🔑 API Key: {api_key}")
        print(f"🔄 Token refresh endpoint: {base_url}/token/refresh")
        
        # Test the raw token refresh call
        print(f"\n--- Raw Token Refresh Test ---")
        try:
            refresh_payload = {'refreshToken': api_key}
            print(f"📦 Refresh payload: {refresh_payload}")
            
            response = requests.post(
                f"{base_url}/token/refresh",
                headers={'Content-Type': 'application/json'},
                json=refresh_payload,
                timeout=30
            )
            
            print(f"📊 Response Status: {response.status_code}")
            print(f"📋 Response Headers: {dict(response.headers)}")
            
            if response.content:
                try:
                    response_json = response.json()
                    print(f"📄 Response JSON: {json.dumps(response_json, indent=2)}")
                except:
                    print(f"📄 Response Text: {response.text}")
            else:
                print("📄 Empty response body")
                
        except Exception as e:
            print(f"❌ Raw token refresh failed: {e}")
        
        # Test the API client initialization (which calls _refresh_token)
        print(f"\n--- API Client Initialization Test ---")
        try:
            client = LoadsAPIClient(base_url, api_key=api_key, auth_type='api_key')
            print(f"✅ API Client created successfully")
            print(f"🎫 Bearer token set: {client.bearer_token is not None}")
            if client.bearer_token:
                print(f"🎫 Bearer token (first 20 chars): {client.bearer_token[:20]}...")
        except Exception as e:
            print(f"❌ API Client initialization failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error debugging token refresh: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_token_refresh()
    exit(0 if success else 1)