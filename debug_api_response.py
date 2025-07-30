#!/usr/bin/env python3
"""
Debug API response to see what the server is actually returning
"""

import sys
import os
import requests
import json
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_api_call():
    """Make a raw API call to see the exact response"""
    print("=== Raw API Response Debug ===")
    
    try:
        from credential_manager import credential_manager
        
        # Get credentials
        brokerage_key = 'augment-brokerage'
        brokerage_creds = credential_manager.get_brokerage_credentials(brokerage_key)
        
        if not brokerage_creds:
            print("❌ No brokerage credentials found")
            return False
            
        base_url = brokerage_creds.get('base_url')
        api_key = brokerage_creds.get('api_key')
        
        print(f"🔗 Base URL: {base_url}")
        print(f"🔑 API Key (first 10 chars): {api_key[:10]}...")
        
        # Test minimal API call to see what happens
        print("\n--- Testing with Bearer token format ---")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }
        
        # Try a simple GET request first (less likely to fail due to payload issues)
        try:
            print("Testing GET /v2/loads endpoint...")
            response = requests.get(f"{base_url}/v2/loads", headers=headers, timeout=30)
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            
            if response.content:
                try:
                    response_json = response.json()
                    print(f"Response JSON: {json.dumps(response_json, indent=2)}")
                except:
                    print(f"Response Text: {response.text}")
            else:
                print("Empty response body")
                
        except Exception as e:
            print(f"GET request failed: {e}")
        
        print("\n--- Testing with API Key format ---")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'ApiKey {api_key}'
        }
        
        try:
            print("Testing GET /v2/loads endpoint with ApiKey format...")
            response = requests.get(f"{base_url}/v2/loads", headers=headers, timeout=30)
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            
            if response.content:
                try:
                    response_json = response.json()
                    print(f"Response JSON: {json.dumps(response_json, indent=2)}")
                except:
                    print(f"Response Text: {response.text}")
            else:
                print("Empty response body")
                
        except Exception as e:
            print(f"GET request with ApiKey failed: {e}")
        
        print("\n--- Testing with X-API-Key header ---")
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': api_key
        }
        
        try:
            print("Testing GET /v2/loads endpoint with X-API-Key header...")
            response = requests.get(f"{base_url}/v2/loads", headers=headers, timeout=30)
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            
            if response.content:
                try:
                    response_json = response.json()
                    print(f"Response JSON: {json.dumps(response_json, indent=2)}")
                except:
                    print(f"Response Text: {response.text}")
            else:
                print("Empty response body")
                
        except Exception as e:
            print(f"GET request with X-API-Key failed: {e}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error debugging API: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_api_call()
    exit(0 if success else 1)