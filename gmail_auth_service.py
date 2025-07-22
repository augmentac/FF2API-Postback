"""
Gmail OAuth2 Authentication Service for Email Automation.

Provides secure Gmail API access through OAuth2 authentication flow:
- Google OAuth2 consent screen integration
- Secure token storage with encryption
- Token refresh and error handling
- Gmail API scopes management
"""

import os
import json
import base64
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from dataclasses import dataclass
import streamlit as st
import requests
from urllib.parse import urlencode, parse_qs

logger = logging.getLogger(__name__)

@dataclass
class GmailCredentials:
    """Gmail API credentials with OAuth2 tokens."""
    access_token: str
    refresh_token: str
    token_expiry: datetime
    email: str
    scopes: List[str]
    client_id: str

@dataclass
class GmailAuthConfig:
    """Gmail OAuth2 configuration."""
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: List[str]

class GmailAuthService:
    """Gmail OAuth2 authentication and token management service."""
    
    # Required Gmail API scopes for email automation
    REQUIRED_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.modify',
        'https://www.googleapis.com/auth/userinfo.email'
    ]
    
    def __init__(self):
        """Initialize Gmail authentication service."""
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher = Fernet(self.encryption_key)
        
    def get_auth_config(self, brokerage_key: str) -> Optional[GmailAuthConfig]:
        """
        Get Gmail OAuth2 configuration for a brokerage.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            GmailAuthConfig or None if not configured
        """
        try:
            # Check secrets for OAuth2 configuration
            gmail_oauth = st.secrets.get("gmail_oauth", {})
            brokerage_config = gmail_oauth.get(brokerage_key.replace('-', '_'), {})
            
            if not brokerage_config:
                logger.warning(f"No Gmail OAuth2 config found for {brokerage_key}")
                return None
            
            required_fields = ['client_id', 'client_secret', 'redirect_uri']
            missing_fields = [field for field in required_fields if field not in brokerage_config]
            
            if missing_fields:
                logger.error(f"Missing Gmail OAuth2 fields for {brokerage_key}: {missing_fields}")
                return None
            
            return GmailAuthConfig(
                client_id=brokerage_config['client_id'],
                client_secret=brokerage_config['client_secret'],
                redirect_uri=brokerage_config['redirect_uri'],
                scopes=brokerage_config.get('scopes', self.REQUIRED_SCOPES)
            )
            
        except Exception as e:
            logger.error(f"Error getting Gmail auth config for {brokerage_key}: {e}")
            return None
    
    def generate_auth_url(self, brokerage_key: str, state: str = None) -> Optional[str]:
        """
        Generate Gmail OAuth2 authorization URL.
        
        Args:
            brokerage_key: Brokerage identifier
            state: Optional state parameter for security
            
        Returns:
            Authorization URL or None if config missing
        """
        try:
            auth_config = self.get_auth_config(brokerage_key)
            if not auth_config:
                return None
            
            params = {
                'client_id': auth_config.client_id,
                'redirect_uri': auth_config.redirect_uri,
                'scope': ' '.join(auth_config.scopes),
                'response_type': 'code',
                'access_type': 'offline',  # Get refresh token
                'prompt': 'consent',  # Force consent screen
                'include_granted_scopes': 'true'
            }
            
            if state:
                params['state'] = state
            
            auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
            logger.info(f"Generated Gmail auth URL for {brokerage_key}")
            return auth_url
            
        except Exception as e:
            logger.error(f"Error generating auth URL for {brokerage_key}: {e}")
            return None
    
    def exchange_code_for_tokens(self, brokerage_key: str, authorization_code: str) -> Optional[GmailCredentials]:
        """
        Exchange authorization code for access tokens.
        
        Args:
            brokerage_key: Brokerage identifier
            authorization_code: OAuth2 authorization code from redirect
            
        Returns:
            GmailCredentials or None if exchange failed
        """
        try:
            auth_config = self.get_auth_config(brokerage_key)
            if not auth_config:
                return None
            
            # Exchange code for tokens
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                'client_id': auth_config.client_id,
                'client_secret': auth_config.client_secret,
                'code': authorization_code,
                'grant_type': 'authorization_code',
                'redirect_uri': auth_config.redirect_uri
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Get user info
            user_info = self._get_user_info(token_data['access_token'])
            
            # Calculate token expiry
            expires_in = token_data.get('expires_in', 3600)
            token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            credentials = GmailCredentials(
                access_token=token_data['access_token'],
                refresh_token=token_data.get('refresh_token', ''),
                token_expiry=token_expiry,
                email=user_info.get('email', ''),
                scopes=auth_config.scopes,
                client_id=auth_config.client_id
            )
            
            # Store credentials securely
            self.store_credentials(brokerage_key, credentials)
            
            logger.info(f"Successfully exchanged code for tokens for {brokerage_key}")
            return credentials
            
        except Exception as e:
            logger.error(f"Error exchanging code for tokens for {brokerage_key}: {e}")
            return None
    
    def get_credentials(self, brokerage_key: str) -> Optional[GmailCredentials]:
        """
        Get stored Gmail credentials for a brokerage.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            GmailCredentials or None if not found/invalid
        """
        try:
            # Try session state first
            if hasattr(st, 'session_state') and hasattr(st.session_state, 'gmail_credentials'):
                session_creds = st.session_state.gmail_credentials.get(brokerage_key)
                if session_creds:
                    # Check if token needs refresh
                    if datetime.now() >= session_creds.token_expiry:
                        refreshed = self.refresh_credentials(brokerage_key, session_creds)
                        if refreshed:
                            return refreshed
                    else:
                        return session_creds
            
            # Try encrypted storage
            stored_creds = self._load_encrypted_credentials(brokerage_key)
            if stored_creds:
                # Check if token needs refresh
                if datetime.now() >= stored_creds.token_expiry:
                    refreshed = self.refresh_credentials(brokerage_key, stored_creds)
                    if refreshed:
                        return refreshed
                else:
                    return stored_creds
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting credentials for {brokerage_key}: {e}")
            return None
    
    def store_credentials(self, brokerage_key: str, credentials: GmailCredentials):
        """
        Store Gmail credentials securely.
        
        Args:
            brokerage_key: Brokerage identifier
            credentials: GmailCredentials to store
        """
        try:
            # Store in session state for immediate use
            if not hasattr(st.session_state, 'gmail_credentials'):
                st.session_state.gmail_credentials = {}
            
            st.session_state.gmail_credentials[brokerage_key] = credentials
            
            # Store encrypted for persistence
            self._save_encrypted_credentials(brokerage_key, credentials)
            
            logger.info(f"Stored Gmail credentials for {brokerage_key}")
            
        except Exception as e:
            logger.error(f"Error storing credentials for {brokerage_key}: {e}")
    
    def refresh_credentials(self, brokerage_key: str, credentials: GmailCredentials) -> Optional[GmailCredentials]:
        """
        Refresh expired access token using refresh token.
        
        Args:
            brokerage_key: Brokerage identifier
            credentials: Existing credentials with refresh token
            
        Returns:
            Refreshed GmailCredentials or None if refresh failed
        """
        try:
            if not credentials.refresh_token:
                logger.warning(f"No refresh token available for {brokerage_key}")
                return None
            
            auth_config = self.get_auth_config(brokerage_key)
            if not auth_config:
                return None
            
            # Refresh token
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                'client_id': auth_config.client_id,
                'client_secret': auth_config.client_secret,
                'refresh_token': credentials.refresh_token,
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Calculate new expiry
            expires_in = token_data.get('expires_in', 3600)
            new_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # Update credentials
            refreshed_credentials = GmailCredentials(
                access_token=token_data['access_token'],
                refresh_token=credentials.refresh_token,  # Keep existing refresh token
                token_expiry=new_expiry,
                email=credentials.email,
                scopes=credentials.scopes,
                client_id=credentials.client_id
            )
            
            # Store refreshed credentials
            self.store_credentials(brokerage_key, refreshed_credentials)
            
            logger.info(f"Refreshed Gmail credentials for {brokerage_key}")
            return refreshed_credentials
            
        except Exception as e:
            logger.error(f"Error refreshing credentials for {brokerage_key}: {e}")
            return None
    
    def revoke_credentials(self, brokerage_key: str) -> bool:
        """
        Revoke Gmail credentials and remove stored tokens.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            True if revocation successful
        """
        try:
            credentials = self.get_credentials(brokerage_key)
            if credentials:
                # Revoke token with Google
                revoke_url = f"https://oauth2.googleapis.com/revoke?token={credentials.access_token}"
                requests.post(revoke_url)
            
            # Remove from session state
            if hasattr(st.session_state, 'gmail_credentials'):
                st.session_state.gmail_credentials.pop(brokerage_key, None)
            
            # Remove encrypted storage
            self._delete_encrypted_credentials(brokerage_key)
            
            logger.info(f"Revoked Gmail credentials for {brokerage_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error revoking credentials for {brokerage_key}: {e}")
            return False
    
    def test_credentials(self, brokerage_key: str) -> Dict[str, Any]:
        """
        Test Gmail credentials by making a simple API call.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            Test result dictionary
        """
        try:
            credentials = self.get_credentials(brokerage_key)
            if not credentials:
                return {'success': False, 'message': 'No credentials found'}
            
            # Test with Gmail API
            headers = {'Authorization': f'Bearer {credentials.access_token}'}
            response = requests.get(
                'https://gmail.googleapis.com/gmail/v1/users/me/profile',
                headers=headers
            )
            
            if response.status_code == 200:
                profile = response.json()
                return {
                    'success': True,
                    'message': 'Gmail connection successful',
                    'email': profile.get('emailAddress', ''),
                    'total_messages': profile.get('messagesTotal', 0)
                }
            else:
                return {'success': False, 'message': f'Gmail API error: {response.status_code}'}
                
        except Exception as e:
            logger.error(f"Error testing credentials for {brokerage_key}: {e}")
            return {'success': False, 'message': f'Test failed: {str(e)}'}
    
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
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for secure storage."""
        try:
            key_file = os.path.join('config', 'encryption.key')
            
            if os.path.exists(key_file):
                with open(key_file, 'rb') as f:
                    return f.read()
            else:
                # Create new key
                key = Fernet.generate_key()
                os.makedirs(os.path.dirname(key_file), exist_ok=True)
                with open(key_file, 'wb') as f:
                    f.write(key)
                return key
                
        except Exception as e:
            logger.error(f"Error with encryption key: {e}")
            # Fallback to session-only storage
            return Fernet.generate_key()
    
    def _save_encrypted_credentials(self, brokerage_key: str, credentials: GmailCredentials):
        """Save credentials with encryption."""
        try:
            creds_data = {
                'access_token': credentials.access_token,
                'refresh_token': credentials.refresh_token,
                'token_expiry': credentials.token_expiry.isoformat(),
                'email': credentials.email,
                'scopes': credentials.scopes,
                'client_id': credentials.client_id
            }
            
            # Encrypt and encode
            encrypted_data = self.cipher.encrypt(json.dumps(creds_data).encode())
            encoded_data = base64.b64encode(encrypted_data).decode()
            
            # Store in config directory
            config_dir = 'config'
            os.makedirs(config_dir, exist_ok=True)
            
            creds_file = os.path.join(config_dir, f'gmail_creds_{brokerage_key.replace("-", "_")}.enc')
            with open(creds_file, 'w') as f:
                f.write(encoded_data)
                
        except Exception as e:
            logger.error(f"Error saving encrypted credentials: {e}")
    
    def _load_encrypted_credentials(self, brokerage_key: str) -> Optional[GmailCredentials]:
        """Load credentials from encrypted storage."""
        try:
            creds_file = os.path.join('config', f'gmail_creds_{brokerage_key.replace("-", "_")}.enc')
            
            if not os.path.exists(creds_file):
                return None
            
            with open(creds_file, 'r') as f:
                encoded_data = f.read()
            
            # Decode and decrypt
            encrypted_data = base64.b64decode(encoded_data.encode())
            decrypted_data = self.cipher.decrypt(encrypted_data)
            creds_data = json.loads(decrypted_data.decode())
            
            return GmailCredentials(
                access_token=creds_data['access_token'],
                refresh_token=creds_data['refresh_token'],
                token_expiry=datetime.fromisoformat(creds_data['token_expiry']),
                email=creds_data['email'],
                scopes=creds_data['scopes'],
                client_id=creds_data['client_id']
            )
            
        except Exception as e:
            logger.error(f"Error loading encrypted credentials: {e}")
            return None
    
    def _delete_encrypted_credentials(self, brokerage_key: str):
        """Delete encrypted credentials file."""
        try:
            creds_file = os.path.join('config', f'gmail_creds_{brokerage_key.replace("-", "_")}.enc')
            if os.path.exists(creds_file):
                os.remove(creds_file)
                
        except Exception as e:
            logger.error(f"Error deleting encrypted credentials: {e}")


# Global instance for application use
gmail_auth_service = GmailAuthService()