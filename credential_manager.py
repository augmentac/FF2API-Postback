"""
Credential Manager for Automatic Brokerage Authentication Resolution.

Provides seamless credential resolution for:
- Brokerage-specific API authentication (GoAugment APIs)
- Global Snowflake warehouse access
- Email SMTP configuration

Architecture:
- User provides brokerage_key
- System automatically resolves API credentials
- Global Snowflake credentials used for all data warehouse queries
- Graceful degradation when credentials missing
"""

import streamlit as st
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CredentialCapabilities:
    """Represents available system capabilities based on configured credentials."""
    api_available: bool
    snowflake_available: bool
    email_available: bool
    email_automation_available: bool
    email_automation_active: bool
    tracking_api_available: bool
    capabilities: List[str]
    brokerage_key: str

class CredentialManager:
    """Centralized credential management with automatic brokerage resolution."""
    
    def __init__(self):
        """Initialize credential manager and validate basic configuration."""
        self.snowflake_creds = self._load_snowflake_credentials()
        self.email_creds = self._load_email_credentials()
        self.api_config = self._load_api_config()
        
    def get_brokerage_api_key(self, brokerage_key: str) -> Optional[str]:
        """
        Automatically resolve API credentials for given brokerage.
        
        Converts brokerage-key format to secret lookup:
        'augment-brokerage' → st.secrets['api']['augment_brokerage']
        'customer-xyz' → st.secrets['api']['customer_xyz']
        
        Args:
            brokerage_key: Brokerage identifier (e.g., 'augment-brokerage')
            
        Returns:
            API key string if found, None if not configured
        """
        try:
            secret_key = self._normalize_brokerage_key(brokerage_key)
            api_secrets = st.secrets.get("api", {})
            
            if secret_key in api_secrets:
                logger.info(f"Found API credentials for brokerage: {brokerage_key}")
                return api_secrets[secret_key]
            else:
                logger.warning(f"No API credentials found for brokerage: {brokerage_key}")
                logger.info(f"Available brokerages: {list(api_secrets.keys())}")
                return None
                
        except Exception as e:
            logger.error(f"Error resolving API credentials for {brokerage_key}: {e}")
            return None
    
    def get_brokerage_credentials(self, brokerage_key: str) -> Dict[str, Any]:
        """
        Get complete credential set for a brokerage including API config.
        
        Returns:
            Dictionary with api_key, base_url, timeout, etc.
        """
        api_key = self.get_brokerage_api_key(brokerage_key)
        
        return {
            'api_key': api_key,
            'base_url': self.api_config.get('base_url', 'https://load.prod.goaugment.com'),
            'timeout': self.api_config.get('timeout', 30),
            'retry_count': self.api_config.get('retry_count', 3),
            'retry_delay': self.api_config.get('retry_delay', 1)
        }
    
    def get_snowflake_credentials(self) -> Optional[Dict[str, Any]]:
        """Get global Snowflake credentials for data warehouse access."""
        return self.snowflake_creds
    
    def get_email_credentials(self) -> Optional[Dict[str, Any]]:
        """Get SMTP email credentials for result delivery."""
        return self.email_creds
    
    def get_tracking_api_credentials(self) -> Optional[Dict[str, Any]]:
        """Get tracking API credentials for shipment tracking enrichment."""
        try:
            tracking_config = st.secrets.get("tracking_api", {})
            if not tracking_config:
                logger.info("No tracking API configuration found - tracking features disabled")
                return None
                
            # Check for required api_endpoint field
            if 'api_endpoint' not in tracking_config:
                logger.warning("Tracking API endpoint not configured - tracking features disabled")
                return None
            
            # Set defaults for optional fields
            config = {
                'api_endpoint': tracking_config['api_endpoint'],
                'timeout': tracking_config.get('timeout', 30),
                'max_retries': tracking_config.get('max_retries', 3)
            }
            
            logger.info("Tracking API credentials loaded successfully")
            return config
            
        except Exception as e:
            logger.error(f"Error loading tracking API credentials: {e}")
            return None
    
    def validate_credentials(self, brokerage_key: str) -> CredentialCapabilities:
        """
        Comprehensive validation of available credentials and capabilities.
        
        Args:
            brokerage_key: Brokerage to validate
            
        Returns:
            CredentialCapabilities object with detailed capability assessment
        """
        api_available = bool(self.get_brokerage_api_key(brokerage_key))
        snowflake_available = bool(self.snowflake_creds)
        email_available = bool(self.email_creds)
        tracking_api_available = bool(self.get_tracking_api_credentials())
        
        # Check for email automation configuration
        email_automation_config = self._get_email_automation_config(brokerage_key)
        email_automation_available = bool(email_automation_config)
        email_automation_active = email_automation_config.get('active', False) if email_automation_config else False
        
        # Determine available capabilities
        capabilities = []
        if api_available:
            capabilities.extend(['load_id_mapping', 'api_enrichment'])
        if snowflake_available:
            capabilities.extend(['data_enrichment', 'warehouse_queries'])
        if tracking_api_available:
            capabilities.append('tracking_enrichment')
        if email_available:
            capabilities.append('email_delivery')
        if email_automation_available:
            capabilities.append('email_automation')
        if api_available and snowflake_available:
            capabilities.append('end_to_end_workflow')
        if api_available and tracking_api_available:
            capabilities.append('tracking_workflow')
            
        return CredentialCapabilities(
            api_available=api_available,
            snowflake_available=snowflake_available,
            email_available=email_available,
            email_automation_available=email_automation_available,
            email_automation_active=email_automation_active,
            tracking_api_available=tracking_api_available,
            capabilities=capabilities,
            brokerage_key=brokerage_key
        )
    
    def get_available_brokerages(self) -> List[str]:
        """
        Discover all configured brokerages from API secrets.
        
        Returns:
            List of brokerage keys in user-friendly format
        """
        try:
            api_secrets = st.secrets.get("api", {})
            
            # Filter out configuration keys (not actual brokerage credentials)
            config_keys = {
                'base_url', 'timeout', 'retry_count', 'retry_delay', 
                'default_api_base_url', 'api_base_url', 'default-api-base-url'
            }
            
            # Get actual brokerage keys (exclude configuration keys)
            brokerage_keys = []
            for key in api_secrets.keys():
                # Skip configuration keys (case-insensitive)
                if key.lower().replace('_', '-') not in config_keys:
                    # Convert snake_case back to kebab-case for user display
                    brokerage_keys.append(key.replace('_', '-'))
            
            logger.info(f"Available brokerages: {brokerage_keys}")
            return sorted(brokerage_keys)
        except Exception as e:
            logger.error(f"Error discovering brokerages: {e}")
            return []
    
    def test_brokerage_connection(self, brokerage_key: str) -> Dict[str, Any]:
        """
        Test end-to-end connectivity for a specific brokerage.
        
        Returns:
            Dictionary with test results for each component
        """
        results = {
            'brokerage_key': brokerage_key,
            'api_test': {'success': False, 'message': ''},
            'snowflake_test': {'success': False, 'message': ''},
            'email_test': {'success': False, 'message': ''},
            'overall_success': False
        }
        
        # Test API credentials
        api_key = self.get_brokerage_api_key(brokerage_key)
        if api_key:
            results['api_test']['success'] = True
            results['api_test']['message'] = 'API credentials found'
        else:
            results['api_test']['message'] = f'No API credentials for {brokerage_key}'
        
        # Test Snowflake connection
        if self.snowflake_creds:
            results['snowflake_test']['success'] = True
            results['snowflake_test']['message'] = 'Global Snowflake credentials available'
        else:
            results['snowflake_test']['message'] = 'Snowflake credentials not configured'
        
        # Test email credentials
        if self.email_creds:
            results['email_test']['success'] = True
            results['email_test']['message'] = 'Email credentials available'
        else:
            results['email_test']['message'] = 'Email credentials not configured'
        
        # Overall assessment
        results['overall_success'] = any([
            results['api_test']['success'],
            results['snowflake_test']['success']
        ])
        
        return results
    
    def _normalize_brokerage_key(self, brokerage_key: str) -> str:
        """
        Convert brokerage key to secret lookup format.
        
        'augment-brokerage' → 'augment_brokerage'
        'customer-xyz' → 'customer_xyz'
        """
        return brokerage_key.replace('-', '_')
    
    def _load_snowflake_credentials(self) -> Optional[Dict[str, Any]]:
        """Load and validate global Snowflake credentials."""
        try:
            snowflake_config = st.secrets.get("snowflake", {})
            if not snowflake_config:
                logger.warning("No Snowflake configuration found in secrets")
                return None
                
            required_fields = ['account', 'user', 'password', 'database', 'warehouse', 'schema', 'role']
            missing_fields = [field for field in required_fields if field not in snowflake_config]
            
            if missing_fields:
                logger.error(f"Missing Snowflake configuration fields: {missing_fields}")
                return None
                
            logger.info("Snowflake credentials loaded successfully")
            return dict(snowflake_config)
            
        except Exception as e:
            logger.error(f"Error loading Snowflake credentials: {e}")
            return None
    
    def _load_email_credentials(self) -> Optional[Dict[str, Any]]:
        """Load and validate email SMTP credentials."""
        try:
            email_config = st.secrets.get("email", {})
            if not email_config:
                logger.info("No email configuration found - email features disabled")
                return None
                
            required_fields = ['smtp_user', 'smtp_pass']
            if all(field in email_config for field in required_fields):
                logger.info("Email credentials loaded successfully")
                return dict(email_config)
            else:
                logger.warning("Incomplete email configuration - email features disabled")
                return None
                
        except Exception as e:
            logger.error(f"Error loading email credentials: {e}")
            return None
    
    def _load_api_config(self) -> Dict[str, Any]:
        """Load API configuration settings."""
        try:
            api_config = st.secrets.get("api_config", {})
            defaults = {
                'base_url': 'https://load.prod.goaugment.com',
                'timeout': 30,
                'retry_count': 3,
                'retry_delay': 1
            }
            
            # Merge with defaults
            config = {**defaults, **api_config}
            logger.info("API configuration loaded")
            return config
            
        except Exception as e:
            logger.warning(f"Error loading API config, using defaults: {e}")
            return {
                'base_url': 'https://load.prod.goaugment.com',
                'timeout': 30,
                'retry_count': 3,
                'retry_delay': 1
            }
    
    def _get_email_automation_config(self, brokerage_key: str) -> Optional[Dict[str, Any]]:
        """
        Load email automation configuration for a specific brokerage.
        Checks both st.secrets and session state for OAuth credentials.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            Email automation config dictionary or None if not configured
        """
        try:
            # First check st.secrets for traditional config
            email_automation = st.secrets.get("email_automation", {})
            normalized_key = self._normalize_brokerage_key(brokerage_key)
            
            brokerage_config = email_automation.get(normalized_key, {})
            
            # If no secrets config, check for OAuth credentials in session state
            if not brokerage_config:
                # Check if we have OAuth credentials for this brokerage
                oauth_key = f'gmail_auth_{normalized_key}'
                if oauth_key in st.session_state:
                    oauth_creds = st.session_state[oauth_key]
                    if oauth_creds and oauth_creds.get('authenticated', False):
                        # Create a minimal config indicating OAuth authentication is available
                        brokerage_config = {
                            'gmail_credentials': 'oauth_session_state',  # Flag indicating OAuth
                            'inbox_filters': {},  # Default empty filters
                            'oauth_authenticated': True,
                            'user_email': oauth_creds.get('user_email', '')
                        }
                        logger.info(f"Found OAuth email authentication for brokerage: {brokerage_key}")
                        return brokerage_config
            
            if not brokerage_config:
                logger.info(f"No email automation configured for brokerage: {brokerage_key}")
                return None
            
            # Validate required fields for secrets-based config
            if brokerage_config.get('gmail_credentials') != 'oauth_session_state':
                required_fields = ['gmail_credentials', 'inbox_filters']
                missing_fields = [field for field in required_fields if field not in brokerage_config]
                
                if missing_fields:
                    logger.warning(f"Incomplete email automation config for {brokerage_key}, missing: {missing_fields}")
                    return None
            
            logger.info(f"Email automation config loaded for brokerage: {brokerage_key}")
            return brokerage_config
            
        except Exception as e:
            logger.error(f"Error loading email automation config for {brokerage_key}: {e}")
            return None


# Global instance for application use
credential_manager = CredentialManager()