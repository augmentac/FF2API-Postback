"""
Tracking API Enrichment Integration
Auto-inherits authentication from existing brokerage configuration
Provides real-time carrier tracking data via browser automation API
"""

import requests
import logging
import time
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from .base import EnrichmentSource

logger = logging.getLogger(__name__)


class TrackingAPIEnricher(EnrichmentSource):
    """
    Real-time tracking enrichment using browser automation API.
    Automatically inherits authentication from existing FF2API configuration.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize tracking API enrichment with hardcoded authentication from secrets.
        
        Args:
            config: Configuration containing brokerage settings and field mappings
        """
        super().__init__(config)
        
        # Brokerage info for context
        self.brokerage_key = config.get('brokerage_key', '')
        
        # CSV field mappings
        self.pro_column = config.get('pro_column', 'PRO')
        self.carrier_column = config.get('carrier_column', 'carrier')
        
        # Tracking endpoint
        self.tracking_base_url = self._derive_tracking_endpoint()
        
        # Standard settings
        self.timeout = config.get('timeout', 30)
        self.retry_count = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)
        
        # Initialize session for persistent connection
        self.session = requests.Session()
        
        # Use hardcoded authentication from secrets (managed separately)
        self._setup_hardcoded_auth()
        
        # Cache for API results to avoid duplicate calls
        self._tracking_cache = {}
        
        logger.info(f"Tracking API initialized for brokerage: {self.brokerage_key}")
        logger.info(f"Tracking endpoint: {self.tracking_base_url}")
        logger.info(f"Column mapping - PRO: {self.pro_column}, Carrier: {self.carrier_column}")
        logger.info("Using hardcoded authentication for tracking API")
    
    def _setup_hardcoded_auth(self):
        """
        Setup authentication with comprehensive debugging for tracking_api secrets.
        """
        # COMPREHENSIVE DEBUG LOGGING
        logger.info("🔍 DEBUG: Starting Tracking API authentication analysis")
        
        try:
            import streamlit as st
            
            # Step 1: Test Streamlit import and basic access
            logger.info(f"🔍 DEBUG: Streamlit module available: {st is not None}")
            logger.info(f"🔍 DEBUG: hasattr(st, 'secrets'): {hasattr(st, 'secrets')}")
            
            if not hasattr(st, 'secrets'):
                logger.error("❌ DEBUG: st.secrets attribute not available")
                raise Exception("DEBUG: Streamlit secrets not available - check cloud deployment configuration")
            
            # Step 2: Test secrets object accessibility
            try:
                secrets_obj = st.secrets
                logger.info(f"🔍 DEBUG: st.secrets object type: {type(secrets_obj)}")
                logger.info(f"🔍 DEBUG: st.secrets object exists: {secrets_obj is not None}")
            except Exception as secrets_error:
                logger.error(f"❌ DEBUG: Error accessing st.secrets object: {secrets_error}")
                raise Exception(f"DEBUG: Cannot access st.secrets object: {secrets_error}")
            
            # Step 3: Test secrets conversion to dict
            try:
                secrets_dict = dict(st.secrets)
                available_sections = list(secrets_dict.keys())
                logger.info(f"🔍 DEBUG: Available secrets sections: {available_sections}")
                logger.info(f"🔍 DEBUG: Total sections found: {len(available_sections)}")
            except Exception as dict_error:
                logger.error(f"❌ DEBUG: Error converting secrets to dict: {dict_error}")
                available_sections = ["DICT_CONVERSION_FAILED"]
            
            # Step 4: Test tracking_api section access
            logger.info("🔍 DEBUG: Testing tracking_api section access...")
            
            # Method 1: Dictionary membership test
            try:
                tracking_api_in_dict = 'tracking_api' in st.secrets
                logger.info(f"🔍 DEBUG: 'tracking_api' in st.secrets (dict method): {tracking_api_in_dict}")
            except Exception as dict_test_error:
                logger.error(f"❌ DEBUG: Dictionary membership test failed: {dict_test_error}")
                tracking_api_in_dict = False
            
            # Method 2: hasattr test
            try:
                tracking_api_hasattr = hasattr(st.secrets, 'tracking_api')
                logger.info(f"🔍 DEBUG: hasattr(st.secrets, 'tracking_api'): {tracking_api_hasattr}")
            except Exception as hasattr_error:
                logger.error(f"❌ DEBUG: hasattr test failed: {hasattr_error}")
                tracking_api_hasattr = False
            
            # Step 5: If section missing, provide detailed diagnosis
            if not tracking_api_in_dict and not tracking_api_hasattr:
                logger.error(f"❌ DEBUG: tracking_api section not found")
                logger.error(f"❌ DEBUG: Available sections: {available_sections}")
                
                # Check for similar section names
                similar_sections = [s for s in available_sections if 'tracking' in s.lower() or 'api' in s.lower()]
                if similar_sections:
                    logger.error(f"❌ DEBUG: Similar sections found: {similar_sections}")
                
                raise Exception(f"DEBUG: Missing [tracking_api] section. Available sections: {available_sections}")
            
            # Step 6: Access tracking_api section
            logger.info("🔍 DEBUG: Accessing tracking_api section...")
            try:
                tracking_secrets = st.secrets.tracking_api
                logger.info(f"🔍 DEBUG: tracking_secrets object type: {type(tracking_secrets)}")
                logger.info(f"🔍 DEBUG: tracking_secrets object exists: {tracking_secrets is not None}")
            except Exception as section_error:
                logger.error(f"❌ DEBUG: Error accessing tracking_api section: {section_error}")
                raise Exception(f"DEBUG: Cannot access tracking_api section: {section_error}")
            
            # Step 7: Test bearer_token access
            logger.info("🔍 DEBUG: Testing bearer_token access...")
            try:
                has_bearer_token = hasattr(tracking_secrets, 'bearer_token')
                logger.info(f"🔍 DEBUG: hasattr(tracking_secrets, 'bearer_token'): {has_bearer_token}")
                
                if has_bearer_token:
                    bearer_token_raw = tracking_secrets.bearer_token
                    logger.info(f"🔍 DEBUG: bearer_token raw type: {type(bearer_token_raw)}")
                    logger.info(f"🔍 DEBUG: bearer_token raw value exists: {bearer_token_raw is not None}")
                    logger.info(f"🔍 DEBUG: bearer_token raw length: {len(str(bearer_token_raw)) if bearer_token_raw else 0}")
                    
                    if bearer_token_raw:
                        bearer_token = str(bearer_token_raw).strip()
                        logger.info(f"🔍 DEBUG: bearer_token after processing length: {len(bearer_token)}")
                        logger.info(f"🔍 DEBUG: bearer_token preview: {bearer_token[:10]}...{bearer_token[-4:] if len(bearer_token) > 14 else bearer_token}")
                        
                        if bearer_token:
                            self.session.headers.update({
                                'Authorization': f'Bearer {bearer_token}',
                                'Content-Type': 'application/json',
                                'User-Agent': 'FF2API-TrackingEnrichment/1.0'
                            })
                            logger.info("✅ DEBUG: Successfully set session Authorization header with bearer_token")
                            logger.info(f"🔍 DEBUG: Session headers now: {dict(self.session.headers)}")
                            return
                        else:
                            logger.error("❌ DEBUG: bearer_token is empty after processing")
                    else:
                        logger.error("❌ DEBUG: bearer_token raw value is None/empty")
            except Exception as bearer_error:
                logger.error(f"❌ DEBUG: Error accessing bearer_token: {bearer_error}")
            
            # Step 8: Test api_key access
            logger.info("🔍 DEBUG: Testing api_key access...")
            try:
                has_api_key = hasattr(tracking_secrets, 'api_key')
                logger.info(f"🔍 DEBUG: hasattr(tracking_secrets, 'api_key'): {has_api_key}")
                
                if has_api_key:
                    api_key_raw = tracking_secrets.api_key
                    logger.info(f"🔍 DEBUG: api_key raw type: {type(api_key_raw)}")
                    logger.info(f"🔍 DEBUG: api_key raw value exists: {api_key_raw is not None}")
                    logger.info(f"🔍 DEBUG: api_key raw length: {len(str(api_key_raw)) if api_key_raw else 0}")
                    
                    if api_key_raw:
                        api_key = str(api_key_raw).strip()
                        logger.info(f"🔍 DEBUG: api_key after processing length: {len(api_key)}")
                        logger.info(f"🔍 DEBUG: api_key preview: {api_key[:10]}...{api_key[-4:] if len(api_key) > 14 else api_key}")
                        
                        if api_key:
                            self.session.headers.update({
                                'Authorization': f'Bearer {api_key}',
                                'Content-Type': 'application/json',
                                'User-Agent': 'FF2API-TrackingEnrichment/1.0'
                            })
                            logger.info("✅ DEBUG: Successfully set session Authorization header with api_key")
                            logger.info(f"🔍 DEBUG: Session headers now: {dict(self.session.headers)}")
                            return
                        else:
                            logger.error("❌ DEBUG: api_key is empty after processing")
                    else:
                        logger.error("❌ DEBUG: api_key raw value is None/empty")
            except Exception as api_key_error:
                logger.error(f"❌ DEBUG: Error accessing api_key: {api_key_error}")
            
            # Step 9: Final failure analysis
            logger.error("❌ DEBUG: No valid authentication credentials found")
            available_keys = []
            try:
                if hasattr(tracking_secrets, 'bearer_token'):
                    available_keys.append('bearer_token')
                if hasattr(tracking_secrets, 'api_key'):
                    available_keys.append('api_key')
                logger.error(f"❌ DEBUG: Available keys in tracking_api section: {available_keys}")
            except:
                logger.error("❌ DEBUG: Cannot enumerate keys in tracking_api section")
            
            raise Exception(f"DEBUG: No valid credentials in tracking_api section. Available keys: {available_keys}")
                
        except Exception as e:
            logger.error(f"❌ DEBUG: Tracking API authentication failed with: {type(e).__name__}: {e}")
            logger.error("❌ DEBUG: This detailed error information should help identify the root cause")
            raise Exception(f"Tracking API authentication debug error: {e}")
    
    # Debug version with comprehensive logging - default headers method removed for debugging

    def _derive_tracking_endpoint(self) -> str:
        """
        Auto-derive tracking API endpoint from existing FF2API base URL.
        
        Returns:
            Tracking API base URL
        """
        # Use production tracking endpoint
        return "https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task"
    
    
    def validate_config(self) -> bool:
        """
        Check if tracking API configuration is valid.
        Also performs a test call to verify tracking API access.
        
        Returns:
            True if configuration is valid and tracking API is accessible
        """
        if not self.brokerage_key:
            logger.error("No brokerage key configured for tracking API")
            return False
        
        if not self.tracking_base_url:
            logger.error("No tracking API endpoint configured")
            return False
        
        # Check if authentication headers are set
        auth_header = self.session.headers.get('Authorization')
        if not auth_header:
            logger.warning("No authentication configured in secrets for tracking API")
            logger.warning("Add tracking_api.bearer_token or tracking_api.api_key to Streamlit secrets")
            return False
        
        # Test tracking API access
        logger.info("Testing tracking API accessibility...")
        test_result = self._test_tracking_access()
        if not test_result:
            logger.warning("Tracking API is not accessible with current credentials")
            logger.warning("Enhanced workflow will continue without tracking enrichment")
            return False
        
        return True
    
    def _test_tracking_access(self) -> bool:
        """
        Test if tracking API is accessible with current credentials.
        
        Returns:
            True if tracking API responds (even with 404 for specific PRO)
        """
        try:
            # Test with a dummy PRO number using correct brokerage key and valid carrier
            test_url = f"{self.tracking_base_url}/pro-number/TEST123"
            params = {
                'brokerageKey': self.brokerage_key,  # Use actual brokerage key
                'browserTask': 'ESTES'  # Use valid carrier name that API accepts
            }
            
            logger.info(f"🔍 DEBUG: Testing tracking API with URL: {test_url}")
            logger.info(f"🔍 DEBUG: Test params: {params}")
            logger.info(f"🔍 DEBUG: Session headers: {dict(self.session.headers)}")
            
            response = self.session.get(
                test_url,
                params=params,
                timeout=10
            )
            
            logger.info(f"🔍 DEBUG: Response status: {response.status_code}")
            logger.info(f"🔍 DEBUG: Response headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                try:
                    response_text = response.text[:500]  # First 500 chars
                    logger.info(f"🔍 DEBUG: Response body: {response_text}")
                except:
                    logger.info("🔍 DEBUG: Could not read response body")
            
            # Consider 404 as "accessible but no data" (good)
            # Consider 401/403 as "not authorized" (bad)  
            # Consider 422 as "bad request format" (fixable)
            if response.status_code in [200, 404]:
                logger.info("✓ Tracking API is accessible")
                return True
            elif response.status_code in [401, 403]:
                logger.warning(f"✗ Tracking API authentication failed: {response.status_code}")
                logger.warning("This brokerage may not have tracking API access enabled")
                return False
            elif response.status_code == 422:
                logger.warning(f"⚠️ Tracking API returned 422 (Unprocessable Entity) - request format issue")
                logger.warning("Authentication is working but API request format needs adjustment")
                # For now, consider this a partial success since auth is working
                return True
            else:
                logger.warning(f"✗ Tracking API returned unexpected status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"✗ Tracking API test failed: {str(e)}")
            return False
    
    def is_applicable(self, row: Dict[str, Any]) -> bool:
        """
        Check if tracking enrichment is applicable to this row.
        
        Args:
            row: Data row to check
            
        Returns:
            True if row has required PRO and carrier fields
        """
        pro_number, carrier = self._extract_row_data(row)
        return bool(pro_number and carrier)
    
    def _extract_row_data(self, row_data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        """
        Validate and extract required tracking data from CSV row.
        
        Args:
            row_data: Dictionary containing CSV row data
            
        Returns:
            Tuple of (pro_number, carrier) or (None, None) if invalid
        """
        # Extract PRO number - prioritize FF2API-sourced data over CSV fields
        pro_number = None
        # First priority: PRO numbers from FF2API load details
        ff2api_pro_fields = ['ff2api_pro_number', 'PRO']  # PRO is set by workflow from FF2API data
        csv_pro_fields = [self.pro_column, 'pro_number', 'ProNumber', 'tracking_number']
        
        # Try FF2API fields first
        for pro_field in ff2api_pro_fields:
            if pro_field in row_data and row_data[pro_field]:
                pro_number = str(row_data[pro_field]).strip()
                logger.debug(f"Using FF2API PRO number from field '{pro_field}': {pro_number}")
                break
        
        # Fallback to CSV fields if no FF2API data
        if not pro_number:
            for pro_field in csv_pro_fields:
                if pro_field in row_data and row_data[pro_field]:
                    pro_number = str(row_data[pro_field]).strip() 
                    logger.debug(f"Using CSV PRO number from field '{pro_field}': {pro_number}")
                    break
        
        if not pro_number:
            logger.debug(f"No PRO number found in row data. Checked FF2API fields: {ff2api_pro_fields}, CSV fields: {csv_pro_fields}")
            return None, None
        
        # Extract carrier - prioritize FF2API-sourced data over CSV fields
        carrier = None
        # First priority: Carrier names from FF2API load details
        ff2api_carrier_fields = ['ff2api_carrier_name', 'carrier']  # carrier is set by workflow from FF2API data
        csv_carrier_fields = [self.carrier_column, 'Carrier Name', 'carrier_name', 'scac_code']
        
        # Try FF2API fields first
        for carrier_field in ff2api_carrier_fields:
            if carrier_field in row_data and row_data[carrier_field]:
                carrier = str(row_data[carrier_field]).strip().upper()
                logger.debug(f"Using FF2API carrier from field '{carrier_field}': {carrier}")
                break
        
        # Fallback to CSV fields if no FF2API data
        if not carrier:
            for carrier_field in csv_carrier_fields:
                if carrier_field in row_data and row_data[carrier_field]:
                    carrier = str(row_data[carrier_field]).strip().upper()
                    logger.debug(f"Using CSV carrier from field '{carrier_field}': {carrier}")
                    break
        
        if not carrier:
            logger.debug(f"No carrier found in row data. Checked FF2API fields: {ff2api_carrier_fields}, CSV fields: {csv_carrier_fields}")
            return None, None
        
        return pro_number, carrier
    
    def _call_tracking_api(self, pro_number: str, carrier: str) -> Optional[Dict[str, Any]]:
        """
        Make tracking API call with automatic retries.
        
        Args:
            pro_number: PRO/tracking number
            carrier: Carrier name for browser task
            
        Returns:
            Tracking data dictionary or None if failed
        """
        # Create cache key
        cache_key = f"{carrier}:{pro_number}"
        
        # Check cache first
        if cache_key in self._tracking_cache:
            logger.debug(f"Using cached tracking data for {carrier} PRO {pro_number}")
            return self._tracking_cache[cache_key]
        
        url = f"{self.tracking_base_url}/pro-number/{pro_number}"
        params = {
            'brokerageKey': self.brokerage_key,  # Use actual brokerage key
            'browserTask': carrier
        }
        
        logger.debug(f"🔍 DEBUG: Tracking API call - URL: {url}")
        logger.debug(f"🔍 DEBUG: Tracking API call - Params: {params}")
        logger.debug(f"🔍 DEBUG: Tracking API call - Headers: {dict(self.session.headers)}")
        
        for attempt in range(self.retry_count):
            try:
                logger.debug(f"Tracking API call attempt {attempt + 1}: {url}")
                logger.debug(f"Parameters: {params}")
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                
                logger.debug(f"🔍 DEBUG: Tracking API response status: {response.status_code}")
                if response.status_code != 200:
                    logger.debug(f"🔍 DEBUG: Tracking API response body: {response.text[:300]}")
                
                if response.status_code == 200:
                    tracking_data = response.json()
                    logger.debug(f"Tracking API success for PRO {pro_number}: {tracking_data.get('result', {}).get('status', 'Unknown')}")
                    
                    # Cache the successful result
                    self._tracking_cache[cache_key] = tracking_data
                    return tracking_data
                
                elif response.status_code == 404:
                    logger.info(f"Tracking not found for PRO {pro_number} with carrier {carrier}")
                    # Cache the failure to avoid repeated attempts
                    self._tracking_cache[cache_key] = None
                    return None
                
                elif response.status_code == 429:
                    logger.warning(f"Rate limited on tracking API. Attempt {attempt + 1}")
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                        continue
                
                elif response.status_code in [401, 403]:
                    logger.error(f"Authentication failed for tracking API: {response.status_code}")
                    logger.error("Check hardcoded tracking API credentials in secrets")
                    logger.error("Update tracking_api.bearer_token or tracking_api.api_key in Streamlit secrets")
                    # Cache the auth failure to avoid repeated attempts
                    self._tracking_cache[cache_key] = None
                    return None
                
                else:
                    logger.warning(f"Tracking API returned {response.status_code} for PRO {pro_number}")
                    if attempt < self.retry_count - 1:
                        time.sleep(self.retry_delay)
                        continue
                
            except requests.exceptions.Timeout:
                logger.warning(f"Tracking API timeout for PRO {pro_number}. Attempt {attempt + 1}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                    continue
            
            except requests.exceptions.ConnectionError:
                logger.warning(f"Tracking API connection error for PRO {pro_number}. Attempt {attempt + 1}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
                    continue
            
            except Exception as e:
                logger.error(f"Unexpected error in tracking API call: {e}")
                break
        
        logger.warning(f"Tracking API failed after {self.retry_count} attempts for PRO {pro_number}")
        # Cache the failure to avoid repeated attempts
        self._tracking_cache[cache_key] = None
        return None
    
    def _extract_tracking_fields(self, tracking_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and normalize tracking fields from API response.
        
        Args:
            tracking_data: Raw tracking API response
            
        Returns:
            Normalized tracking fields
        """
        result = tracking_data.get('result', {})
        
        # Extract tracking information
        tracking_fields = {
            'tracking_status': result.get('status', ''),
            'tracking_detailed_status': result.get('detailedStatus', ''),
            'tracking_city': result.get('city', ''),
            'tracking_state': result.get('state', ''),
            'tracking_country': result.get('country', ''),
            'tracking_date': result.get('data', ''),
            'tracking_location': '',
            'tracking_updated_at': tracking_data.get('updatedAt', '')
        }
        
        # Combine location fields
        location_parts = []
        if tracking_fields['tracking_city']:
            location_parts.append(tracking_fields['tracking_city'])
        if tracking_fields['tracking_state']:
            location_parts.append(tracking_fields['tracking_state'])
        if tracking_fields['tracking_country'] and tracking_fields['tracking_country'] != 'US':
            location_parts.append(tracking_fields['tracking_country'])
        
        tracking_fields['tracking_location'] = ', '.join(location_parts)
        
        # Clean up empty fields
        return {k: v for k, v in tracking_fields.items() if v}
    
    def enrich(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single CSV row with real-time tracking data.
        
        Args:
            row_data: Dictionary containing CSV row data
            
        Returns:
            Row data enriched with tracking information
        """
        if not self.validate_config():
            logger.error("Tracking API configuration is invalid")
            return row_data
        
        # Initialize tracking fields
        enriched_row = row_data.copy()
        enriched_row.update({
            'tracking_status': None,
            'tracking_location': None,
            'tracking_date': None
        })
        
        # Validate and extract required fields
        pro_number, carrier = self._extract_row_data(row_data)
        if not pro_number or not carrier:
            logger.debug("Missing PRO number or carrier for tracking enrichment")
            return enriched_row
        
        # Make tracking API call
        tracking_data = self._call_tracking_api(pro_number, carrier)
        if not tracking_data:
            logger.debug(f"No tracking data available for PRO {pro_number}")
            return enriched_row
        
        # Extract and merge tracking fields
        tracking_fields = self._extract_tracking_fields(tracking_data)
        
        # Merge with original row data
        enriched_row.update(tracking_fields)
        
        logger.debug(f"Successfully enriched PRO {pro_number} with tracking data")
        return enriched_row