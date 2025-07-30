#!/usr/bin/env python3
"""
Test API connection to help diagnose authentication issues
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_api_connection():
    """Test the API connection with detailed debugging"""
    print("=== FF2API Connection Test ===")
    
    try:
        from credential_manager import credential_manager
        from src.backend.api_client import LoadsAPIClient
        
        # Get credentials
        brokerage_key = 'augment-brokerage'
        brokerage_creds = credential_manager.get_brokerage_credentials(brokerage_key)
        
        if not brokerage_creds:
            print("❌ No brokerage credentials found")
            return False
            
        print(f"📋 Found credentials for: {brokerage_key}")
        print(f"🔗 Base URL: {brokerage_creds.get('base_url', 'NOT SET')}")
        print(f"🔑 Has API key: {'Yes' if brokerage_creds.get('api_key') else 'No'}")
        print(f"🎫 Has bearer token: {'Yes' if brokerage_creds.get('bearer_token') else 'No'}")
        print(f"🔐 Auth type: {brokerage_creds.get('auth_type', 'NOT SET')}")
        
        # Test with API key if available
        if brokerage_creds.get('api_key'):
            print("\n--- Testing API Key Authentication ---")
            client = LoadsAPIClient(
                base_url=brokerage_creds.get('base_url'),
                api_key=brokerage_creds.get('api_key'),
                auth_type='api_key'
            )
            
            result = client.validate_connection()
            print(f"Connection result: {result}")
            
            if result.get('success'):
                print("✅ API Key authentication SUCCESSFUL")
                return True
            else:
                print(f"❌ API Key authentication FAILED: {result.get('message', result.get('error', 'Unknown error'))}")
        
        # Test with bearer token if available
        if brokerage_creds.get('bearer_token'):
            print("\n--- Testing Bearer Token Authentication ---")
            client = LoadsAPIClient(
                base_url=brokerage_creds.get('base_url'),
                bearer_token=brokerage_creds.get('bearer_token'),
                auth_type='bearer_token'
            )
            
            result = client.validate_connection()
            print(f"Connection result: {result}")
            
            if result.get('success'):
                print("✅ Bearer Token authentication SUCCESSFUL")
                return True
            else:
                print(f"❌ Bearer Token authentication FAILED: {result.get('message', result.get('error', 'Unknown error'))}")
        
        print("\n❌ No working authentication method found")
        return False
        
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_api_connection()
    exit(0 if success else 1)