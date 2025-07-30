#!/usr/bin/env python3
"""
Debug credential loading to see exactly what's stored vs what's retrieved
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_credentials():
    """Debug credential loading step by step"""
    print("=== Credential Loading Debug ===")
    
    try:
        import streamlit as st
        from credential_manager import credential_manager
        
        brokerage_key = 'augment-brokerage'
        normalized_key = brokerage_key.replace('-', '_')  # 'augment_brokerage'
        
        print(f"🔍 Looking for brokerage: {brokerage_key}")
        print(f"🔄 Normalized key: {normalized_key}")
        
        # Check what's actually in secrets
        try:
            api_secrets = st.secrets.get("api", {})
            print(f"📦 Available API secrets keys: {list(api_secrets.keys())}")
            
            if normalized_key in api_secrets:
                raw_value = api_secrets[normalized_key]
                print(f"🔑 Raw value for {normalized_key}: '{raw_value}'")
                print(f"🔍 Value type: {type(raw_value)}")
                print(f"🔍 Value length: {len(str(raw_value))}")
                
                # Check if it's a pipe-separated format
                if '|' in str(raw_value):
                    parts = str(raw_value).split('|')
                    print(f"🔀 Pipe-separated parts: {parts}")
                    print(f"🔀 Part count: {len(parts)}")
                    if len(parts) >= 2:
                        print(f"🔀 Potential API key part: '{parts[1]}'")
                
            else:
                print(f"❌ Key {normalized_key} not found in secrets")
                
        except Exception as e:
            print(f"❌ Error accessing secrets: {e}")
        
        # Test credential manager
        print(f"\n--- Testing Credential Manager ---")
        api_key = credential_manager.get_brokerage_api_key(brokerage_key)
        print(f"🔑 Credential manager returned: '{api_key}'")
        print(f"🔍 Returned type: {type(api_key)}")
        if api_key:
            print(f"🔍 Returned length: {len(api_key)}")
            print(f"🔍 First 20 chars: '{api_key[:20]}...'")
        
        # Test full credentials
        print(f"\n--- Testing Full Credentials ---")
        full_creds = credential_manager.get_brokerage_credentials(brokerage_key)
        print(f"📋 Full credentials: {full_creds}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error debugging credentials: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_credentials()
    exit(0 if success else 1)