"""
Automated Google Drive OAuth flow for Streamlit.
Handles the complete OAuth process within the Streamlit app.
"""

import streamlit as st
import requests
import urllib.parse
import json
from typing import Optional, Dict, Any

class StreamlitGoogleDriveAuth:
    """Handles Google Drive OAuth flow within Streamlit app"""
    
    def __init__(self):
        self.client_id = st.secrets.get("google", {}).get("client_id")
        self.client_secret = st.secrets.get("google", {}).get("client_secret")
        self.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
        self.scope = "https://www.googleapis.com/auth/drive.file"
        
    def is_configured(self) -> bool:
        """Check if basic OAuth credentials are configured"""
        return bool(self.client_id and self.client_secret)
    
    def has_valid_tokens(self) -> bool:
        """Check if we have valid access/refresh tokens"""
        try:
            access_token = st.secrets["google"]["access_token"]
            refresh_token = st.secrets["google"]["refresh_token"]
            return bool(access_token and refresh_token)
        except KeyError:
            return False
    
    def get_auth_url(self) -> str:
        """Generate Google OAuth authorization URL"""
        params = {
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri,
            'scope': self.scope,
            'response_type': 'code',
            'access_type': 'offline',
            'prompt': 'consent'
        }
        
        base_url = "https://accounts.google.com/o/oauth2/auth"
        return f"{base_url}?{urllib.parse.urlencode(params)}"
    
    def exchange_code_for_tokens(self, auth_code: str) -> Optional[Dict[str, Any]]:
        """Exchange authorization code for access/refresh tokens"""
        try:
            token_url = "https://oauth2.googleapis.com/token"
            
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'code': auth_code,
                'grant_type': 'authorization_code',
                'redirect_uri': self.redirect_uri
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            
            tokens = response.json()
            
            return {
                'access_token': tokens.get('access_token'),
                'refresh_token': tokens.get('refresh_token'),
                'expires_in': tokens.get('expires_in'),
                'token_type': tokens.get('token_type')
            }
            
        except Exception as e:
            st.error(f"Failed to exchange code for tokens: {e}")
            return None
    
    def render_auth_interface(self):
        """Render the OAuth authentication interface in Streamlit"""
        st.subheader("🔐 Google Drive Authentication Setup")
        
        if not self.is_configured():
            st.error("❌ Google OAuth credentials not configured!")
            st.markdown("""
            **Required Streamlit Secrets:**
            ```toml
            [google]
            client_id = "your-client-id.apps.googleusercontent.com"
            client_secret = "your-client-secret"
            ```
            """)
            return False
        
        if self.has_valid_tokens():
            st.success("✅ Google Drive authentication is already configured!")
            
            # Show current token status
            with st.expander("Token Information"):
                st.json({
                    "client_id": self.client_id,
                    "has_access_token": bool(st.secrets.get("google", {}).get("access_token")),
                    "has_refresh_token": bool(st.secrets.get("google", {}).get("refresh_token"))
                })
            
            return True
        
        # Show authentication flow
        st.warning("⚠️ Google Drive authentication required for database backup")
        
        st.markdown("### Step 1: Get Authorization Code")
        auth_url = self.get_auth_url()
        
        st.markdown(f"""
        Click the link below to authorize the application:
        
        🔗 **[Authorize Google Drive Access]({auth_url})**
        
        This will:
        1. Open Google's authorization page
        2. Ask you to sign in with your Google account  
        3. Show permissions (access to create/modify files in Drive)
        4. Give you an authorization code to paste below
        """)
        
        st.markdown("### Step 2: Enter Authorization Code")
        auth_code = st.text_input(
            "Paste the authorization code here:",
            placeholder="4/0AdQt8qh...",
            help="Copy the code from Google's authorization page"
        )
        
        if st.button("🔑 Complete Authentication", disabled=not auth_code):
            with st.spinner("Exchanging code for tokens..."):
                tokens = self.exchange_code_for_tokens(auth_code)
                
                if tokens:
                    st.success("✅ Authentication successful!")
                    
                    # Display the secrets that need to be added
                    st.markdown("### Step 3: Update Streamlit Secrets")
                    st.markdown("Add these tokens to your Streamlit app secrets:")
                    
                    secrets_config = f"""
```toml
[google]
client_id = "{self.client_id}"
client_secret = "{self.client_secret}"
access_token = "{tokens['access_token']}"
refresh_token = "{tokens['refresh_token']}"
```
"""
                    st.code(secrets_config, language="toml")
                    
                    st.info("📝 Copy the above configuration and paste it into your Streamlit Cloud app secrets, then restart the app.")
                    
                    return True
                else:
                    st.error("❌ Failed to get tokens. Please try again.")
        
        return False

# Global instance for easy import
google_drive_auth = StreamlitGoogleDriveAuth()