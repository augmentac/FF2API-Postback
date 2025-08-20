"""
Authentication State Synchronization Module

This module provides utilities to synchronize authentication state between
the backend storage systems and frontend UI state in Streamlit applications.

Fixes issues where authentication succeeds in backend but UI doesn't reflect
the authenticated state properly.
"""

import streamlit as st
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class AuthStateSync:
    """Authentication state synchronization manager."""
    
    def __init__(self):
        """Initialize auth state sync manager."""
        pass
    
    def validate_and_sync_auth_state(self, brokerage_key: str) -> Dict[str, Any]:
        """
        Validate and synchronize authentication state between backend and frontend.
        
        This function checks both backend storage (gmail_auth_service) and frontend
        session state, resolving any inconsistencies by syncing them.
        
        Args:
            brokerage_key: The brokerage identifier to check auth for
            
        Returns:
            Dict containing:
            - authenticated: bool - Whether user is authenticated
            - user_email: str - Authenticated user email (if authenticated)
            - sync_performed: bool - Whether sync was needed and performed
            - source: str - Where the valid auth data came from
            - issues_found: List[str] - Any issues that were discovered and fixed
        """
        result = {
            'authenticated': False,
            'user_email': None,
            'sync_performed': False,
            'source': None,
            'issues_found': []
        }
        
        try:
            logger.info(f"Validating auth state for brokerage: {brokerage_key}")
            
            # Check backend storage (gmail_auth_service)
            backend_auth = self._check_backend_auth(brokerage_key)
            
            # Check frontend session state (streamlit_google_sso)
            frontend_auth = self._check_frontend_auth(brokerage_key)
            
            # Check credential manager storage
            gmail_service_auth = self._check_gmail_service_auth(brokerage_key)
            
            logger.info(f"Auth state check - Backend: {bool(backend_auth)}, Frontend: {bool(frontend_auth)}, Gmail Service: {bool(gmail_service_auth)}")
            
            # Determine the best source of truth
            valid_auth_data = self._determine_auth_source(backend_auth, frontend_auth, gmail_service_auth)
            
            if valid_auth_data:
                result['authenticated'] = True
                result['user_email'] = valid_auth_data.get('email', 'authenticated')
                result['source'] = valid_auth_data['source']
                
                # Perform synchronization if needed
                sync_result = self._sync_auth_states(brokerage_key, valid_auth_data, backend_auth, frontend_auth, gmail_service_auth)
                result['sync_performed'] = sync_result['sync_performed']
                result['issues_found'] = sync_result['issues_found']
                
                logger.info(f"Auth validation successful for {brokerage_key}: {result['user_email']} (source: {result['source']})")
            else:
                result['issues_found'].append("No valid authentication found in any storage system")
                logger.info(f"No valid authentication found for {brokerage_key}")
            
        except Exception as e:
            logger.error(f"Error validating auth state for {brokerage_key}: {e}")
            result['issues_found'].append(f"Validation error: {str(e)}")
        
        return result
    
    def _check_backend_auth(self, brokerage_key: str) -> Optional[Dict[str, Any]]:
        """Check backend authentication storage."""
        try:
            from gmail_auth_service import gmail_auth_service
            
            credentials = gmail_auth_service.get_credentials(brokerage_key)
            if credentials:
                return {
                    'email': credentials.email,
                    'access_token': credentials.access_token,
                    'token_expiry': credentials.token_expiry,
                    'source': 'backend_gmail_service',
                    'valid': datetime.now() < credentials.token_expiry
                }
        except Exception as e:
            logger.debug(f"Backend auth check failed: {e}")
        
        return None
    
    def _check_frontend_auth(self, brokerage_key: str) -> Optional[Dict[str, Any]]:
        """Check frontend session state authentication."""
        try:
            from streamlit_google_sso import streamlit_google_sso
            
            auth_data = streamlit_google_sso._get_stored_auth(brokerage_key)
            if auth_data:
                token_expiry = datetime.fromisoformat(auth_data['token_expiry'])
                return {
                    'email': auth_data.get('email', ''),
                    'access_token': auth_data.get('access_token', ''),
                    'token_expiry': token_expiry,
                    'source': 'frontend_sso',
                    'valid': datetime.now() < token_expiry
                }
        except Exception as e:
            logger.debug(f"Frontend auth check failed: {e}")
        
        return None
    
    def _check_gmail_service_auth(self, brokerage_key: str) -> Optional[Dict[str, Any]]:
        """Check gmail service authentication in session state."""
        try:
            if 'gmail_credentials' in st.session_state:
                cred_data = st.session_state.gmail_credentials.get(brokerage_key)
                if cred_data and hasattr(cred_data, 'email'):
                    return {
                        'email': cred_data.email,
                        'access_token': cred_data.access_token,
                        'token_expiry': cred_data.token_expiry,
                        'source': 'session_gmail_credentials',
                        'valid': datetime.now() < cred_data.token_expiry
                    }
        except Exception as e:
            logger.debug(f"Gmail service auth check failed: {e}")
        
        return None
    
    def _determine_auth_source(self, backend_auth: Optional[Dict], frontend_auth: Optional[Dict], gmail_service_auth: Optional[Dict]) -> Optional[Dict[str, Any]]:
        """Determine which authentication source is most reliable."""
        
        # Priority order: backend_auth > gmail_service_auth > frontend_auth
        auth_sources = [
            ('backend', backend_auth),
            ('gmail_service', gmail_service_auth), 
            ('frontend', frontend_auth)
        ]
        
        # Find the first valid authentication source
        for source_name, auth_data in auth_sources:
            if auth_data and auth_data.get('valid', False) and auth_data.get('email'):
                logger.info(f"Using {source_name} as auth source for validation")
                return auth_data
        
        # Fallback: use any authentication source even if potentially expired
        for source_name, auth_data in auth_sources:
            if auth_data and auth_data.get('email'):
                logger.warning(f"Using potentially expired {source_name} as auth source")
                return auth_data
        
        return None
    
    def _sync_auth_states(self, brokerage_key: str, valid_auth: Dict[str, Any], backend_auth: Optional[Dict], frontend_auth: Optional[Dict], gmail_service_auth: Optional[Dict]) -> Dict[str, Any]:
        """Synchronize authentication states across all storage systems."""
        
        sync_result = {
            'sync_performed': False,
            'issues_found': []
        }
        
        try:
            # Sync frontend session state if missing or different
            if not frontend_auth or frontend_auth.get('email') != valid_auth.get('email'):
                self._sync_frontend_state(brokerage_key, valid_auth)
                sync_result['sync_performed'] = True
                sync_result['issues_found'].append("Frontend session state was out of sync - updated")
            
            # Sync gmail service session state if missing or different 
            if not gmail_service_auth or gmail_service_auth.get('email') != valid_auth.get('email'):
                self._sync_gmail_service_state(brokerage_key, valid_auth)
                sync_result['sync_performed'] = True
                sync_result['issues_found'].append("Gmail service session state was out of sync - updated")
            
            # Update UI state flags
            self._update_ui_auth_flags(brokerage_key, valid_auth)
            
            if sync_result['sync_performed']:
                logger.info(f"Successfully synchronized auth states for {brokerage_key}")
            
        except Exception as e:
            logger.error(f"Error during auth state sync: {e}")
            sync_result['issues_found'].append(f"Sync error: {str(e)}")
        
        return sync_result
    
    def _sync_frontend_state(self, brokerage_key: str, valid_auth: Dict[str, Any]):
        """Sync frontend session state with valid auth data."""
        try:
            if 'google_sso_auth' not in st.session_state:
                st.session_state.google_sso_auth = {}
            
            st.session_state.google_sso_auth[brokerage_key] = {
                'access_token': valid_auth['access_token'],
                'email': valid_auth['email'],
                'token_expiry': valid_auth['token_expiry'].isoformat() if isinstance(valid_auth['token_expiry'], datetime) else valid_auth['token_expiry'],
                'authenticated_at': datetime.now().isoformat(),
                'synced_from': valid_auth['source']
            }
            
            logger.debug(f"Updated frontend session state for {brokerage_key}")
        
        except Exception as e:
            logger.error(f"Error syncing frontend state: {e}")
    
    def _sync_gmail_service_state(self, brokerage_key: str, valid_auth: Dict[str, Any]):
        """Sync gmail service session state with valid auth data."""
        try:
            if 'gmail_credentials' not in st.session_state:
                st.session_state.gmail_credentials = {}
            
            # Create a mock credentials object if it doesn't exist
            if brokerage_key not in st.session_state.gmail_credentials:
                from gmail_auth_service import GmailCredentials
                from streamlit_google_sso import StreamlitGoogleSSO
                
                credentials = GmailCredentials(
                    access_token=valid_auth['access_token'],
                    refresh_token='',  # May not be available in sync
                    token_expiry=valid_auth['token_expiry'] if isinstance(valid_auth['token_expiry'], datetime) else datetime.fromisoformat(valid_auth['token_expiry']),
                    email=valid_auth['email'],
                    scopes=StreamlitGoogleSSO.REQUIRED_SCOPES,
                    client_id=''  # Will be filled by gmail_auth_service if needed
                )
                
                st.session_state.gmail_credentials[brokerage_key] = credentials
                logger.debug(f"Created gmail service credentials for {brokerage_key}")
        
        except Exception as e:
            logger.error(f"Error syncing gmail service state: {e}")
    
    def _update_ui_auth_flags(self, brokerage_key: str, valid_auth: Dict[str, Any]):
        """Update UI-specific authentication flags."""
        try:
            # Set email automation flags
            email_automation_key = f'email_automation_authenticated_{brokerage_key}'
            st.session_state[email_automation_key] = True
            
            # Set general authentication flag
            auth_flag_key = f'gmail_authenticated_{brokerage_key}'
            st.session_state[auth_flag_key] = True
            
            # Store user email for UI display
            user_email_key = f'gmail_user_email_{brokerage_key}'
            st.session_state[user_email_key] = valid_auth['email']
            
            logger.debug(f"Updated UI auth flags for {brokerage_key}")
            
        except Exception as e:
            logger.error(f"Error updating UI auth flags: {e}")
    
    def clear_all_auth_state(self, brokerage_key: str) -> Dict[str, Any]:
        """
        Completely clear all authentication state for a brokerage.
        
        This is the proper "reset and reconfigure" function that ensures
        no partial auth data remains.
        
        Args:
            brokerage_key: Brokerage to clear auth for
            
        Returns:
            Dict with results of the clear operation
        """
        result = {
            'cleared_sources': [],
            'errors': []
        }
        
        try:
            logger.info(f"Clearing all auth state for {brokerage_key}")
            
            # Clear backend storage
            try:
                from gmail_auth_service import gmail_auth_service
                gmail_auth_service.revoke_credentials(brokerage_key)
                result['cleared_sources'].append('backend_gmail_service')
            except Exception as e:
                result['errors'].append(f"Backend clear error: {str(e)}")
            
            # Clear frontend session state
            try:
                if 'google_sso_auth' in st.session_state:
                    st.session_state.google_sso_auth.pop(brokerage_key, None)
                result['cleared_sources'].append('frontend_sso')
            except Exception as e:
                result['errors'].append(f"Frontend clear error: {str(e)}")
            
            # Clear gmail service session state
            try:
                if 'gmail_credentials' in st.session_state:
                    st.session_state.gmail_credentials.pop(brokerage_key, None)
                result['cleared_sources'].append('session_gmail_credentials')
            except Exception as e:
                result['errors'].append(f"Gmail service clear error: {str(e)}")
            
            # Clear UI flags
            try:
                ui_keys_to_clear = [
                    f'email_automation_authenticated_{brokerage_key}',
                    f'gmail_authenticated_{brokerage_key}',
                    f'gmail_user_email_{brokerage_key}',
                    f'gmail_auth_success_{brokerage_key}',
                    f'processed_code_{brokerage_key}'
                ]
                
                for key in ui_keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                
                result['cleared_sources'].append('ui_flags')
            except Exception as e:
                result['errors'].append(f"UI flags clear error: {str(e)}")
            
            logger.info(f"Auth state clearing complete for {brokerage_key}. Cleared: {result['cleared_sources']}")
            
            if result['errors']:
                logger.warning(f"Some errors during auth clear: {result['errors']}")
            
        except Exception as e:
            logger.error(f"Error during complete auth clear: {e}")
            result['errors'].append(f"General clear error: {str(e)}")
        
        return result
    
    def get_auth_status_summary(self, brokerage_key: str) -> Dict[str, Any]:
        """Get a comprehensive summary of current authentication status."""
        summary = {
            'brokerage_key': brokerage_key,
            'timestamp': datetime.now().isoformat(),
            'backend_auth': None,
            'frontend_auth': None,
            'gmail_service_auth': None,
            'overall_authenticated': False,
            'primary_email': None,
            'issues': []
        }
        
        try:
            # Check all storage systems
            backend_auth = self._check_backend_auth(brokerage_key)
            frontend_auth = self._check_frontend_auth(brokerage_key)
            gmail_service_auth = self._check_gmail_service_auth(brokerage_key)
            
            summary['backend_auth'] = {
                'exists': bool(backend_auth),
                'email': backend_auth.get('email') if backend_auth else None,
                'valid': backend_auth.get('valid') if backend_auth else False
            }
            
            summary['frontend_auth'] = {
                'exists': bool(frontend_auth),
                'email': frontend_auth.get('email') if frontend_auth else None,
                'valid': frontend_auth.get('valid') if frontend_auth else False
            }
            
            summary['gmail_service_auth'] = {
                'exists': bool(gmail_service_auth),
                'email': gmail_service_auth.get('email') if gmail_service_auth else None,
                'valid': gmail_service_auth.get('valid') if gmail_service_auth else False
            }
            
            # Determine overall status
            if backend_auth or frontend_auth or gmail_service_auth:
                summary['overall_authenticated'] = True
                # Use the first available email
                for auth in [backend_auth, gmail_service_auth, frontend_auth]:
                    if auth and auth.get('email'):
                        summary['primary_email'] = auth['email']
                        break
                
                # Check for inconsistencies
                emails = [auth.get('email') for auth in [backend_auth, frontend_auth, gmail_service_auth] if auth and auth.get('email')]
                unique_emails = list(set(emails))
                if len(unique_emails) > 1:
                    summary['issues'].append(f"Inconsistent emails across storage systems: {unique_emails}")
            
        except Exception as e:
            logger.error(f"Error getting auth status summary: {e}")
            summary['issues'].append(f"Status check error: {str(e)}")
        
        return summary


# Global instance
auth_state_sync = AuthStateSync()

def validate_auth_state_for_ui(brokerage_key: str) -> Tuple[bool, Optional[str]]:
    """
    Simple helper function for UI components to validate auth state.
    
    Returns:
        Tuple of (is_authenticated, user_email)
    """
    try:
        result = auth_state_sync.validate_and_sync_auth_state(brokerage_key)
        return result['authenticated'], result['user_email']
    except Exception as e:
        logger.error(f"Error in UI auth validation: {e}")
        return False, None

def clear_auth_state_for_ui(brokerage_key: str) -> bool:
    """
    Simple helper function for UI reset operations.
    
    Returns:
        True if clearing was successful
    """
    try:
        result = auth_state_sync.clear_all_auth_state(brokerage_key)
        return len(result['errors']) == 0
    except Exception as e:
        logger.error(f"Error in UI auth clear: {e}")
        return False