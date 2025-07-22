"""
Universal Google Sign-In Authentication for Email Automation.

Enables any user to authenticate with their Google account to set up email automation,
eliminating the need for per-brokerage OAuth2 configuration by admins.

Features:
- Universal Google OAuth2 client configuration
- Direct user Google Sign-In flow
- Self-service email automation setup  
- Automatic user-brokerage association
- Streamlined authentication experience
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import streamlit as st
import requests
from urllib.parse import urlencode
import base64
from dataclasses import dataclass

from gmail_auth_service import GmailCredentials, gmail_auth_service

logger = logging.getLogger(__name__)

@dataclass 
class GoogleSignInConfig:
    """Universal Google Sign-In configuration."""
    client_id: str
    client_secret: str
    redirect_uri: str

class GoogleSignInAuth:
    """Universal Google Sign-In authentication service."""
    
    # Required scopes for email automation
    REQUIRED_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile'
    ]
    
    def __init__(self):
        """Initialize Google Sign-In authentication service."""
        self._config = self._load_universal_config()
    
    def _load_universal_config(self) -> Optional[GoogleSignInConfig]:
        """
        Load universal Google OAuth2 configuration.
        
        Returns:
            GoogleSignInConfig or None if not configured
        """
        try:
            # Check for universal Google OAuth2 configuration
            google_signin = st.secrets.get("google_signin", {})
            
            if not google_signin:
                logger.info("No universal Google Sign-In configuration found")
                return None
            
            required_fields = ['client_id', 'client_secret', 'redirect_uri']
            missing_fields = [field for field in required_fields if field not in google_signin]
            
            if missing_fields:
                logger.error(f"Missing Google Sign-In config fields: {missing_fields}")
                return None
            
            return GoogleSignInConfig(
                client_id=google_signin['client_id'],
                client_secret=google_signin['client_secret'], 
                redirect_uri=google_signin['redirect_uri']
            )
            
        except Exception as e:
            logger.error(f"Error loading Google Sign-In config: {e}")
            return None
    
    def is_configured(self) -> bool:
        """Check if universal Google Sign-In is configured."""
        return self._config is not None
    
    def generate_signin_url(self, brokerage_key: str, user_hint: str = None) -> Optional[str]:
        """
        Generate Google Sign-In URL for user authentication.
        
        Args:
            brokerage_key: Brokerage identifier for context
            user_hint: Optional email hint for better UX
            
        Returns:
            Google Sign-In URL or None if not configured
        """
        try:
            if not self._config:
                return None
            
            # Create state parameter with brokerage info for security
            state_data = {
                'brokerage_key': brokerage_key,
                'timestamp': datetime.now().isoformat(),
                'type': 'email_automation_setup'
            }
            state = base64.urlsafe_b64encode(json.dumps(state_data).encode()).decode()
            
            params = {
                'client_id': self._config.client_id,
                'redirect_uri': self._config.redirect_uri,
                'scope': ' '.join(self.REQUIRED_SCOPES),
                'response_type': 'code',
                'access_type': 'offline',  # Get refresh token
                'prompt': 'consent',  # Force consent screen
                'include_granted_scopes': 'true',
                'state': state
            }
            
            if user_hint:
                params['login_hint'] = user_hint
            
            signin_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
            logger.info(f"Generated Google Sign-In URL for {brokerage_key}")
            return signin_url
            
        except Exception as e:
            logger.error(f"Error generating Sign-In URL: {e}")
            return None
    
    def handle_signin_callback(self, authorization_code: str, state: str) -> Optional[Dict[str, Any]]:
        """
        Handle Google Sign-In callback and exchange code for tokens.
        
        Args:
            authorization_code: OAuth2 authorization code
            state: State parameter from original request
            
        Returns:
            Dictionary with user info and brokerage_key, or None if failed
        """
        try:
            if not self._config:
                return None
            
            # Decode and validate state
            try:
                state_data = json.loads(base64.urlsafe_b64decode(state.encode()).decode())
                brokerage_key = state_data['brokerage_key']
                
                # Basic state validation
                if state_data.get('type') != 'email_automation_setup':
                    logger.error("Invalid state type in callback")
                    return None
                    
            except Exception as e:
                logger.error(f"Invalid state parameter: {e}")
                return None
            
            # Exchange code for tokens
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                'client_id': self._config.client_id,
                'client_secret': self._config.client_secret,
                'code': authorization_code,
                'grant_type': 'authorization_code',
                'redirect_uri': self._config.redirect_uri
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Get user info
            user_info = self._get_user_info(token_data['access_token'])
            
            # Calculate token expiry
            expires_in = token_data.get('expires_in', 3600)
            token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # Create credentials
            credentials = GmailCredentials(
                access_token=token_data['access_token'],
                refresh_token=token_data.get('refresh_token', ''),
                token_expiry=token_expiry,
                email=user_info.get('email', ''),
                scopes=self.REQUIRED_SCOPES,
                client_id=self._config.client_id
            )
            
            # Store credentials for the brokerage
            gmail_auth_service.store_credentials(brokerage_key, credentials)
            
            return {
                'success': True,
                'brokerage_key': brokerage_key,
                'user_email': user_info.get('email', ''),
                'user_name': user_info.get('name', ''),
                'user_picture': user_info.get('picture', ''),
                'credentials': credentials
            }
            
        except Exception as e:
            logger.error(f"Error handling Sign-In callback: {e}")
            return None
    
    def authenticate_user_for_brokerage(self, brokerage_key: str, user_email: str = None) -> Dict[str, Any]:
        """
        Start authentication process for a user and brokerage.
        
        Args:
            brokerage_key: Brokerage identifier
            user_email: Optional user email hint
            
        Returns:
            Dictionary with authentication URL and instructions
        """
        try:
            if not self.is_configured():
                return {
                    'success': False,
                    'message': 'Universal Google Sign-In not configured',
                    'setup_required': True
                }
            
            # Check if user is already authenticated for this brokerage
            existing_creds = gmail_auth_service.get_credentials(brokerage_key)
            if existing_creds:
                return {
                    'success': True,
                    'already_authenticated': True,
                    'user_email': existing_creds.email,
                    'message': f'Already authenticated as {existing_creds.email}'
                }
            
            # Generate sign-in URL
            signin_url = self.generate_signin_url(brokerage_key, user_email)
            if not signin_url:
                return {
                    'success': False,
                    'message': 'Failed to generate authentication URL'
                }
            
            return {
                'success': True,
                'signin_url': signin_url,
                'message': 'Click the link below to sign in with Google',
                'brokerage_key': brokerage_key
            }
            
        except Exception as e:
            logger.error(f"Error starting authentication for {brokerage_key}: {e}")
            return {
                'success': False,
                'message': f'Authentication error: {str(e)}'
            }
    
    def complete_authentication(self, authorization_code: str, state: str) -> Dict[str, Any]:
        """
        Complete the authentication process with authorization code.
        
        Args:
            authorization_code: Code from Google OAuth2 callback
            state: State parameter for validation
            
        Returns:
            Authentication result dictionary
        """
        try:
            result = self.handle_signin_callback(authorization_code, state)
            
            if not result:
                return {
                    'success': False,
                    'message': 'Authentication failed'
                }
            
            return {
                'success': True,
                'message': f'Successfully authenticated as {result["user_email"]}',
                'brokerage_key': result['brokerage_key'],
                'user_email': result['user_email'],
                'user_name': result.get('user_name', ''),
                'ready_for_automation': True
            }
            
        except Exception as e:
            logger.error(f"Error completing authentication: {e}")
            return {
                'success': False,
                'message': f'Authentication completion failed: {str(e)}'
            }
    
    def get_user_email_for_brokerage(self, brokerage_key: str) -> Optional[str]:
        """
        Get authenticated user's email for a brokerage.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            User email or None if not authenticated
        """
        try:
            credentials = gmail_auth_service.get_credentials(brokerage_key)
            return credentials.email if credentials else None
            
        except Exception as e:
            logger.error(f"Error getting user email for {brokerage_key}: {e}")
            return None
    
    def disconnect_user_from_brokerage(self, brokerage_key: str) -> bool:
        """
        Disconnect user from brokerage email automation.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            True if disconnection successful
        """
        try:
            return gmail_auth_service.revoke_credentials(brokerage_key)
            
        except Exception as e:
            logger.error(f"Error disconnecting user from {brokerage_key}: {e}")
            return False
    
    def _get_user_info(self, access_token: str) -> Dict[str, Any]:
        """Get user information from Google API."""
        try:
            headers = {'Authorization': f'Bearer {access_token}'}
            response = requests.get(
                'https://www.googleapis.com/oauth2/v2/userinfo',
                headers=headers
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            return {}
    
    def get_setup_instructions(self) -> Dict[str, str]:
        """Get setup instructions for universal Google Sign-In."""
        return {
            'title': 'Universal Google Sign-In Setup',
            'description': 'Configure a single Google OAuth2 client for all users',
            'steps': [
                '1. Create a Google Cloud Project',
                '2. Enable Gmail API',  
                '3. Create OAuth2 Client ID (Web Application)',
                '4. Add authorized redirect URIs',
                '5. Add configuration to Streamlit secrets'
            ],
            'secrets_example': '''
[google_signin]
client_id = "your-universal-client-id.googleusercontent.com"
client_secret = "your-universal-client-secret"
redirect_uri = "https://your-app.streamlit.app/oauth/callback"
            ''',
            'benefits': [
                '✅ Users can self-authenticate with their own Google accounts',
                '✅ No per-brokerage OAuth2 setup required',
                '✅ Simplified admin configuration',
                '✅ Users control their own email access'
            ]
        }


# Global instance for application use
google_signin_auth = GoogleSignInAuth()