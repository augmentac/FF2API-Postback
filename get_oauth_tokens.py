#!/usr/bin/env python3
"""
One-time script to generate Google Drive OAuth tokens for FF2API backup system.
Run this locally to get the access_token and refresh_token for your Streamlit secrets.
"""

import requests
import urllib.parse
import json

def get_google_drive_tokens():
    """Generate OAuth tokens for Google Drive access"""
    
    # You'll need to fill these in from your Google Cloud Console
    print("=== Google Drive OAuth Token Generator ===\n")
    
    client_id = input("Enter your Google Client ID: ").strip()
    client_secret = input("Enter your Google Client Secret: ").strip()
    
    # Step 1: Generate authorization URL
    params = {
        'client_id': client_id,
        'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
        'scope': 'https://www.googleapis.com/auth/drive.file',
        'response_type': 'code',
        'access_type': 'offline',
        'prompt': 'consent'
    }
    
    base_url = "https://accounts.google.com/o/oauth2/auth"
    auth_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    print(f"\n🔗 STEP 1: Open this URL in your browser:")
    print(f"{auth_url}\n")
    
    print("📋 STEP 2: Grant permissions and copy the authorization code")
    auth_code = input("Enter the authorization code: ").strip()
    
    # Step 2: Exchange code for tokens
    print("\n⏳ Exchanging code for tokens...")
    
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': auth_code,
        'grant_type': 'authorization_code',
        'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob'
    }
    
    try:
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        
        tokens = response.json()
        access_token = tokens.get('access_token')
        refresh_token = tokens.get('refresh_token')
        expires_in = tokens.get('expires_in', 3600)
        
        if not access_token or not refresh_token:
            print("❌ ERROR: Failed to get valid tokens from Google")
            return
        
        print("✅ SUCCESS! Here are your tokens:\n")
        
        # Display secrets configuration
        print("=== ADD THESE TO YOUR STREAMLIT SECRETS ===")
        print("""
[google]
client_id = "{}"
client_secret = "{}"
access_token = "{}"
refresh_token = "{}"
token_encryption_key = "{}"
""".format(
            client_id,
            client_secret, 
            access_token,
            refresh_token,
            generate_encryption_key()
        ))
        
        print("=== NEXT STEPS ===")
        print("1. Copy the above configuration to your Streamlit Cloud app secrets")
        print("2. Restart your Streamlit app")
        print("3. The backup system will automatically use these tokens and encrypt them in the database")
        print("4. Future token refreshes will be automatic and encrypted")
        
    except Exception as e:
        print(f"❌ ERROR: Failed to exchange code for tokens: {e}")

def generate_encryption_key():
    """Generate a secure encryption key"""
    try:
        from cryptography.fernet import Fernet
        return Fernet.generate_key().decode()
    except ImportError:
        print("⚠️  WARNING: cryptography package not found. Install with: pip install cryptography")
        return "GENERATE_KEY_MANUALLY"

if __name__ == "__main__":
    get_google_drive_tokens()