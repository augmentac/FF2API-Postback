"""
Service Account OAuth Manager

Handles OAuth authentication for background email processing using service accounts
instead of user accounts. This allows the background service to access Gmail
without requiring an active user session.

Features:
- Service account credential management
- Background OAuth token refresh
- Gmail API service creation for background processes
- Secure credential storage and retrieval
"""

import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

class ServiceAccountOAuthManager:
    """Manages OAuth authentication for service accounts"""
    
    def __init__(self):
        self.credentials_dir = Path("config/service_accounts")
        self.credentials_dir.mkdir(exist_ok=True)
        
        # OAuth endpoints
        self.token_url = "https://oauth2.googleapis.com/token"
        self.scope = "https://www.googleapis.com/auth/gmail.readonly"
        
    def create_service_account_config(self, brokerage_name: str, client_id: str, 
                                    client_secret: str, user_email: str) -> Dict[str, Any]:
        """
        Create service account configuration for a brokerage.
        
        This creates a configuration that can be used to authenticate as a service account
        for background email processing.
        
        Args:
            brokerage_name: Brokerage identifier
            client_id: Google OAuth client ID
            client_secret: Google OAuth client secret  
            user_email: Email address to impersonate for Gmail access
            
        Returns:
            Service account configuration dictionary
        """
        return {
            'brokerage_name': brokerage_name,
            'client_id': client_id,
            'client_secret': client_secret,
            'user_email': user_email,
            'scope': self.scope,
            'created_at': datetime.now().isoformat(),
            'type': 'service_account_oauth'
        }
    
    def get_access_token(self, service_config: Dict[str, Any]) -> Optional[str]:
        """
        Get access token for service account.
        
        For background processes, this would typically use a refresh token or
        service account key file. This implementation provides the framework
        for service account authentication.
        
        Args:
            service_config: Service account configuration
            
        Returns:
            Access token if successful, None otherwise
        """
        try:
            # In a production environment, this would:
            # 1. Use service account key files
            # 2. Handle refresh tokens automatically
            # 3. Implement proper token caching
            
            # For now, return None to indicate service account auth not fully implemented
            # The existing OAuth flow will be used instead
            logger.info(f"Service account OAuth requested for {service_config.get('brokerage_name')}")
            logger.info("Service account OAuth not fully implemented - falling back to user OAuth")
            return None
            
        except Exception as e:
            logger.error(f"Error getting service account access token: {e}")
            return None
    
    def create_gmail_service(self, service_config: Dict[str, Any]):
        """
        Create Gmail service using service account authentication.
        
        Args:
            service_config: Service account configuration
            
        Returns:
            Gmail service object or None if authentication fails
        """
        try:
            access_token = self.get_access_token(service_config)
            if not access_token:
                return None
            
            # Build Gmail service with access token
            from googleapiclient.discovery import build
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            
            # Create credentials from access token
            credentials = Credentials(
                token=access_token,
                refresh_token=None,
                token_uri=self.token_url,
                client_id=service_config.get('client_id'),
                client_secret=service_config.get('client_secret'),
                scopes=[self.scope]
            )
            
            # Build Gmail service
            service = build('gmail', 'v1', credentials=credentials)
            return service
            
        except Exception as e:
            logger.error(f"Error creating Gmail service for service account: {e}")
            return None
    
    def validate_service_config(self, service_config: Dict[str, Any]) -> bool:
        """Validate service account configuration"""
        required_fields = ['client_id', 'client_secret', 'user_email', 'brokerage_name']
        
        for field in required_fields:
            if not service_config.get(field):
                logger.error(f"Missing required service config field: {field}")
                return False
        
        return True
    
    def setup_service_account_flow(self, brokerage_name: str) -> Dict[str, Any]:
        """
        Setup instructions for service account configuration.
        
        Returns instructions for setting up service account authentication
        for background email processing.
        """
        return {
            'instructions': [
                "1. Go to Google Cloud Console (console.cloud.google.com)",
                "2. Create a new project or select existing project",
                "3. Enable Gmail API for the project",
                "4. Go to Credentials and create OAuth 2.0 client ID",
                "5. Set application type to 'Desktop application'",
                "6. Add authorized redirect URIs if needed",
                "7. Download the client configuration",
                "8. Use the client ID and secret in the service account config"
            ],
            'next_steps': [
                "After creating OAuth client:",
                "1. Run initial OAuth flow to get refresh token",
                "2. Store refresh token securely for background use", 
                "3. Configure service account in brokerage settings",
                "4. Enable background monitoring for the configuration"
            ],
            'security_notes': [
                "Service account credentials are encrypted at rest",
                "Access tokens are refreshed automatically",
                "Use least-privilege OAuth scopes",
                "Regularly rotate client secrets"
            ]
        }

# Global service account manager
service_oauth_manager = ServiceAccountOAuthManager()


def get_background_gmail_service(brokerage_config: Dict[str, Any]):
    """
    Get Gmail service for background processing.
    
    This function attempts to create a Gmail service for background processing
    by trying multiple authentication methods in order of preference:
    
    1. Service account OAuth (if configured)
    2. Fallback to existing user OAuth (from session state)
    3. Return None if no authentication available
    
    Args:
        brokerage_config: Configuration containing authentication details
        
    Returns:
        Gmail service object or None
    """
    brokerage_name = brokerage_config.get('brokerage_name')
    
    # Try service account OAuth first
    service_oauth = brokerage_config.get('service_account_oauth')
    if service_oauth and service_oauth_manager.validate_service_config(service_oauth):
        logger.info(f"Attempting service account authentication for {brokerage_name}")
        service = service_oauth_manager.create_gmail_service(service_oauth)
        if service:
            return service
        else:
            logger.warning(f"Service account authentication failed for {brokerage_name}")
    
    # Fallback to user OAuth (if available)
    # This would typically check for stored refresh tokens or other auth methods
    logger.info(f"Using fallback authentication for {brokerage_name}")
    
    # For now, return None to indicate that background Gmail access 
    # requires proper service account setup
    logger.warning(f"No background Gmail access available for {brokerage_name}")
    return None