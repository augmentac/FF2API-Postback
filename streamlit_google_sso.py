"""
Direct In-App Google SSO Authentication for Streamlit.

Provides seamless Google authentication directly within the Streamlit application
without external redirects, using streamlit-oauth component for native integration.
"""

import streamlit as st
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import json
import base64

logger = logging.getLogger(__name__)

class StreamlitGoogleSSO:
    """Direct in-app Google SSO authentication manager."""
    
    # Required scopes for email automation
    REQUIRED_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify', 
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile'
    ]
    
    def __init__(self):
        """Initialize Google SSO manager."""
        self._config = self._load_sso_config()
    
    def _load_sso_config(self) -> Optional[Dict[str, str]]:
        """Load universal Google SSO configuration from secrets."""
        try:
            # Debug: Check what secrets are available
            logger.info(f"Available secret keys: {list(st.secrets.keys())}")
            
            google_sso = st.secrets.get("google_sso", {})
            logger.info(f"Google SSO section found: {bool(google_sso)}")
            
            if google_sso:
                logger.info(f"Google SSO keys: {list(google_sso.keys())}")
            
            required_fields = ['client_id', 'client_secret']
            missing_fields = [field for field in required_fields if field not in google_sso]
            
            if missing_fields:
                logger.warning(f"Missing Google SSO config: {missing_fields}")
                return None
            
            logger.info("Google SSO configuration loaded successfully")
            return {
                'client_id': google_sso['client_id'],
                'client_secret': google_sso['client_secret']
            }
            
        except Exception as e:
            logger.error(f"Error loading Google SSO config: {e}")
            return None
    
    def is_configured(self) -> bool:
        """Check if Google SSO is configured."""
        return self._config is not None
    
    def render_google_auth_button(self, brokerage_key: str, button_text: str = "🔐 Connect Gmail") -> Dict[str, Any]:
        """
        Render direct in-app Google authentication button.
        
        Args:
            brokerage_key: Brokerage identifier for token storage
            button_text: Text for the authentication button
            
        Returns:
            Authentication result dictionary
        """
        try:
            if not self.is_configured():
                return self._render_config_error()
            
            # Check if user is already authenticated for this brokerage
            existing_auth = self._get_stored_auth(brokerage_key)
            if existing_auth:
                return self._render_authenticated_state(brokerage_key, existing_auth)
            
            # Render authentication interface
            return self._render_auth_interface(brokerage_key, button_text)
            
        except Exception as e:
            logger.error(f"Error in Google auth button: {e}")
            st.error(f"Authentication error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _render_config_error(self) -> Dict[str, Any]:
        """Render configuration error interface."""
        st.error("🔧 **Google SSO Not Configured**")
        
        # Debug information
        with st.expander("🔍 Debug Information"):
            try:
                st.write("**Available secret sections:**", list(st.secrets.keys()))
                google_sso = st.secrets.get("google_sso", {})
                st.write("**Google SSO section exists:**", bool(google_sso))
                if google_sso:
                    st.write("**Google SSO keys:**", list(google_sso.keys()))
                    # Show if values exist (but not the actual values)
                    for key in ['client_id', 'client_secret']:
                        if key in google_sso:
                            value = google_sso[key]
                            st.write(f"**{key}:**", f"{'✅ Set' if value else '❌ Empty'} ({len(str(value))} chars)")
                        else:
                            st.write(f"**{key}:**", "❌ Missing")
            except Exception as e:
                st.write("**Debug error:**", str(e))
        
        with st.expander("Admin Setup Required"):
            st.markdown("### Universal Google SSO Setup")
            st.info("Configure a single Google OAuth2 client for all users to enable direct in-app authentication.")
            
            st.markdown("**Setup Steps:**")
            st.markdown("""
            1. **Create Google Cloud Project** (if not exists)
            2. **Enable Gmail API** in the project
            3. **Create OAuth2 Client ID** (Web Application type)
            4. **Add authorized origins:**
               - `http://localhost:8501` (for local development)
               - `https://your-app.streamlit.app` (for production)
            5. **Add configuration to Streamlit secrets**
            """)
            
            st.markdown("**Streamlit Secrets Configuration:**")
            st.code("""
[google_sso]
client_id = "your-universal-client-id.googleusercontent.com"
client_secret = "your-universal-client-secret"
            """, language='toml')
            
            st.markdown("**Benefits:**")
            st.markdown("""
            - ✅ **One-time setup** - single configuration serves all users
            - ✅ **Direct authentication** - no external redirects needed  
            - ✅ **Multi-user support** - any user can authenticate any email
            - ✅ **Self-service** - users control their own Gmail access
            """)
        
        return {'success': False, 'config_required': True}
    
    def _render_authenticated_state(self, brokerage_key: str, auth_data: Dict[str, Any]) -> Dict[str, Any]:
        """Render interface for already authenticated user."""
        user_email = auth_data.get('email', 'Unknown')
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.success(f"✅ **Connected:** {user_email}")
        
        with col2:
            if st.button("🔓 Disconnect", key=f"disconnect_{brokerage_key}"):
                self._clear_stored_auth(brokerage_key)
                st.success("Disconnected from Gmail")
                st.rerun()
        
        # Additional controls
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔍 Test Connection", key=f"test_{brokerage_key}"):
                test_result = self._test_gmail_connection(brokerage_key, auth_data)
                if test_result['success']:
                    st.success(f"✅ {test_result['message']}")
                    if 'total_messages' in test_result:
                        st.info(f"📧 Total messages: {test_result['total_messages']}")
                else:
                    st.error(f"❌ {test_result['message']}")
        
        with col2:
            if st.button("🔄 Refresh Token", key=f"refresh_{brokerage_key}"):
                refresh_result = self._refresh_auth_token(brokerage_key, auth_data)
                if refresh_result['success']:
                    st.success("✅ Token refreshed")
                    st.rerun()
                else:
                    st.error(f"❌ {refresh_result['message']}")
        
        return {
            'success': True,
            'authenticated': True,
            'user_email': user_email,
            'brokerage_key': brokerage_key
        }
    
    def _render_auth_interface(self, brokerage_key: str, button_text: str) -> Dict[str, Any]:
        """Render authentication interface for unauthenticated user."""
        st.info("🔐 **Gmail Authentication Required**")
        st.markdown("Connect your Gmail account to enable automatic email processing for this brokerage.")
        
        # Email hint for better UX
        email_hint = st.text_input(
            "Your Gmail address (optional):",
            placeholder="your-email@gmail.com",
            help="This will pre-fill the Google Sign-In screen for faster authentication",
            key=f"email_hint_{brokerage_key}"
        )
        
        # Use manual OAuth flow for better reliability
        st.info("🔐 Using secure manual authentication flow")
        return self._render_manual_auth_fallback(brokerage_key, email_hint)
        
        return {'success': False, 'authenticated': False, 'awaiting_auth': True}
    
    def _render_manual_auth_fallback(self, brokerage_key: str, email_hint: str = None) -> Dict[str, Any]:
        """Render manual authentication fallback when streamlit-oauth is not available."""
        
        st.markdown("### Manual Authentication Flow")
        st.info("Since direct OAuth component is not available, please follow these steps:")
        
        # Generate authentication URL
        auth_url = self._generate_auth_url(brokerage_key, email_hint)
        
        if auth_url:
            st.markdown("**Step 1:** Click the link below to authenticate with Google:")
            st.markdown(f"""
            <a href="{auth_url}" target="_blank" style="
                display: inline-block;
                background-color: #4285f4;
                color: white;
                padding: 12px 24px;
                text-decoration: none;
                border-radius: 8px;
                font-weight: bold;
                margin: 10px 0;
            ">🔗 Open Google Authentication</a>
            """, unsafe_allow_html=True)
            
            st.markdown("**Step 2:** After authentication, check the URL for a 'code' parameter or enter it manually:")
            
            # Check if we got redirected back with a code
            try:
                url_params = st.query_params
                auto_code = url_params.get('code', '')
                if auto_code:
                    st.success("✅ Authorization code detected from redirect!")
            except:
                auto_code = ''
            
            auth_code = st.text_input(
                "Authorization code:",
                value=auto_code,
                placeholder="Code should auto-fill from redirect, or paste manually",
                key=f"manual_auth_code_{brokerage_key}"
            )
            
            if auth_code and st.button("Complete Authentication", key=f"complete_manual_{brokerage_key}"):
                with st.spinner("Processing authentication..."):
                    result = self._handle_manual_auth_code(brokerage_key, auth_code)
                    
                    if result['success']:
                        st.success(f"✅ Successfully authenticated as: {result['user_email']}")
                        st.rerun()
                        return result
                    else:
                        st.error(f"❌ Authentication failed: {result['message']}")
        
        return {'success': False, 'authenticated': False, 'manual_flow': True}
    
    def _generate_auth_url(self, brokerage_key: str, email_hint: str = None) -> Optional[str]:
        """Generate Google OAuth2 authentication URL."""
        try:
            from urllib.parse import urlencode
            
            # Create state parameter for security
            state_data = {
                'brokerage_key': brokerage_key,
                'timestamp': datetime.now().isoformat(),
                'type': 'gmail_sso'
            }
            state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()
            
            # Use the app URL for redirect, but we'll handle it manually
            app_url = st.secrets.get('app_url', 'http://localhost:8501')
            redirect_uri = f"{app_url}/"
            
            params = {
                'client_id': self._config['client_id'],
                'redirect_uri': redirect_uri,
                'scope': ' '.join(self.REQUIRED_SCOPES),
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent',
                'state': state
            }
            
            if email_hint:
                params['login_hint'] = email_hint
            
            return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
            
        except Exception as e:
            logger.error(f"Error generating auth URL: {e}")
            return None
    
    def _handle_manual_auth_code(self, brokerage_key: str, auth_code: str) -> Dict[str, Any]:
        """Handle manual authentication code exchange."""
        try:
            import requests
            
            # Exchange code for tokens  
            token_url = "https://oauth2.googleapis.com/token"
            app_url = st.secrets.get('app_url', 'http://localhost:8501')
            redirect_uri = f"{app_url}/"
            
            data = {
                'client_id': self._config['client_id'],
                'client_secret': self._config['client_secret'],
                'code': auth_code,
                'grant_type': 'authorization_code',
                'redirect_uri': redirect_uri
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Get user info
            headers = {'Authorization': f"Bearer {token_data['access_token']}"}
            user_response = requests.get(
                'https://www.googleapis.com/oauth2/v2/userinfo',
                headers=headers
            )
            user_response.raise_for_status()
            user_info = user_response.json()
            
            # Store authentication
            auth_data = {
                'access_token': token_data['access_token'],
                'refresh_token': token_data.get('refresh_token', ''),
                'token_expiry': (datetime.now() + timedelta(seconds=token_data.get('expires_in', 3600))).isoformat(),
                'email': user_info.get('email', ''),
                'name': user_info.get('name', ''),
                'picture': user_info.get('picture', ''),
                'authenticated_at': datetime.now().isoformat()
            }
            
            self._store_auth_data(brokerage_key, auth_data)
            
            return {
                'success': True,
                'user_email': user_info.get('email', ''),
                'brokerage_key': brokerage_key
            }
            
        except Exception as e:
            logger.error(f"Error handling manual auth code: {e}")
            return {
                'success': False,
                'message': str(e)
            }
    
    def _store_auth_result(self, brokerage_key: str, auth_result: Dict[str, Any]):
        """Store OAuth authentication result."""
        try:
            user_info = auth_result.get('userinfo', {})
            token_info = auth_result.get('token', {})
            
            auth_data = {
                'access_token': token_info.get('access_token', ''),
                'refresh_token': token_info.get('refresh_token', ''),
                'token_expiry': (datetime.now() + timedelta(seconds=token_info.get('expires_in', 3600))).isoformat(),
                'email': user_info.get('email', ''),
                'name': user_info.get('name', ''),
                'picture': user_info.get('picture', ''),
                'authenticated_at': datetime.now().isoformat()
            }
            
            self._store_auth_data(brokerage_key, auth_data)
            
        except Exception as e:
            logger.error(f"Error storing auth result: {e}")
    
    def _store_auth_data(self, brokerage_key: str, auth_data: Dict[str, Any]):
        """Store authentication data in session state."""
        try:
            if 'google_sso_auth' not in st.session_state:
                st.session_state.google_sso_auth = {}
            
            st.session_state.google_sso_auth[brokerage_key] = auth_data
            
            # Also integrate with existing credential manager
            from gmail_auth_service import GmailCredentials, gmail_auth_service
            
            credentials = GmailCredentials(
                access_token=auth_data['access_token'],
                refresh_token=auth_data['refresh_token'],
                token_expiry=datetime.fromisoformat(auth_data['token_expiry']),
                email=auth_data['email'],
                scopes=self.REQUIRED_SCOPES,
                client_id=self._config['client_id']
            )
            
            gmail_auth_service.store_credentials(brokerage_key, credentials)
            
        except Exception as e:
            logger.error(f"Error storing auth data: {e}")
    
    def _get_stored_auth(self, brokerage_key: str) -> Optional[Dict[str, Any]]:
        """Get stored authentication data."""
        try:
            # Check session state first
            if 'google_sso_auth' in st.session_state:
                auth_data = st.session_state.google_sso_auth.get(brokerage_key)
                if auth_data:
                    # Check if token is still valid
                    expiry = datetime.fromisoformat(auth_data['token_expiry'])
                    if datetime.now() < expiry:
                        return auth_data
            
            # Fallback to credential manager
            from gmail_auth_service import gmail_auth_service
            credentials = gmail_auth_service.get_credentials(brokerage_key)
            if credentials:
                return {
                    'access_token': credentials.access_token,
                    'email': credentials.email,
                    'token_expiry': credentials.token_expiry.isoformat()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting stored auth: {e}")
            return None
    
    def _clear_stored_auth(self, brokerage_key: str):
        """Clear stored authentication data."""
        try:
            # Clear from session state
            if 'google_sso_auth' in st.session_state:
                st.session_state.google_sso_auth.pop(brokerage_key, None)
            
            # Clear from credential manager
            from gmail_auth_service import gmail_auth_service
            gmail_auth_service.revoke_credentials(brokerage_key)
            
        except Exception as e:
            logger.error(f"Error clearing auth data: {e}")
    
    def _test_gmail_connection(self, brokerage_key: str, auth_data: Dict[str, Any]) -> Dict[str, Any]:
        """Test Gmail connection with stored credentials."""
        try:
            from gmail_auth_service import gmail_auth_service
            return gmail_auth_service.test_credentials(brokerage_key)
            
        except Exception as e:
            logger.error(f"Error testing connection: {e}")
            return {'success': False, 'message': str(e)}
    
    def _refresh_auth_token(self, brokerage_key: str, auth_data: Dict[str, Any]) -> Dict[str, Any]:
        """Refresh authentication token."""
        try:
            from gmail_auth_service import gmail_auth_service
            credentials = gmail_auth_service.get_credentials(brokerage_key)
            
            if credentials:
                refreshed = gmail_auth_service.refresh_credentials(brokerage_key, credentials)
                if refreshed:
                    # Update session state
                    self._store_auth_data(brokerage_key, {
                        'access_token': refreshed.access_token,
                        'refresh_token': refreshed.refresh_token,
                        'token_expiry': refreshed.token_expiry.isoformat(),
                        'email': refreshed.email,
                        'authenticated_at': datetime.now().isoformat()
                    })
                    return {'success': True, 'message': 'Token refreshed successfully'}
            
            return {'success': False, 'message': 'Unable to refresh token'}
            
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_user_email(self, brokerage_key: str) -> Optional[str]:
        """Get authenticated user email for brokerage."""
        auth_data = self._get_stored_auth(brokerage_key)
        return auth_data.get('email') if auth_data else None
    
    def is_authenticated(self, brokerage_key: str) -> bool:
        """Check if user is authenticated for brokerage."""
        return self._get_stored_auth(brokerage_key) is not None


# Global instance
streamlit_google_sso = StreamlitGoogleSSO()