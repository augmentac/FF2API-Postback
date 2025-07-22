"""Load ID mapper for retrieving internal load IDs from GoAugment API."""

import requests
import streamlit as st
from typing import Dict, Any, List, Optional
import logging
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)


@dataclass
class LoadProcessingResult:
    """Result of FF2API load processing."""
    csv_row_index: int
    load_number: Optional[str]
    success: bool
    error_message: Optional[str] = None
    response_data: Optional[Dict] = None


@dataclass
class LoadIDMapping:
    """Mapping between CSV row and internal load ID."""
    csv_row_index: int
    load_number: str
    internal_load_id: Optional[str]
    api_status: str  # 'success', 'failed', 'not_found'
    error_message: Optional[str] = None


class LoadIDMapper:
    """Maps CSV load numbers to internal system Load IDs via GoAugment API."""
    
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config.get('load_api_url', 'https://load.prod.goaugment.com/unstable/loads')
        self.brokerage_key = config.get('brokerage_key', 'augment-brokerage')
        self.timeout = config.get('api_timeout', 30)
        self.retry_count = config.get('retry_count', 3)
        self.retry_delay = config.get('retry_delay', 1)
        
        # Get auth configuration - reuse FF2API auth
        self.auth_config = config.get('auth', {})
        
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API calls."""
        headers = {'Content-Type': 'application/json'}
        
        # Try to get auth from secrets or config
        try:
            if hasattr(st, 'secrets') and 'api' in st.secrets:
                api_key = st.secrets.api.get('API_KEY')
                if api_key:
                    headers['Authorization'] = f'Bearer {api_key}'
            elif self.auth_config.get('api_key'):
                headers['Authorization'] = f'Bearer {self.auth_config["api_key"]}'
            else:
                logger.warning("No API authentication configured")
                
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            
        return headers
    
    def map_load_ids(self, processing_results: List[LoadProcessingResult]) -> List[LoadIDMapping]:
        """
        Map CSV load numbers to internal load IDs via API calls.
        
        Args:
            processing_results: Results from FF2API load processing
            
        Returns:
            List of LoadIDMapping objects with internal load IDs
        """
        mappings = []
        
        for result in processing_results:
            if result.success and result.load_number:
                internal_id, status, error = self._fetch_internal_load_id(result.load_number)
                
                mapping = LoadIDMapping(
                    csv_row_index=result.csv_row_index,
                    load_number=result.load_number,
                    internal_load_id=internal_id,
                    api_status=status,
                    error_message=error
                )
                mappings.append(mapping)
                
            else:
                # Load processing failed, create mapping without internal ID
                mapping = LoadIDMapping(
                    csv_row_index=result.csv_row_index,
                    load_number=result.load_number,
                    internal_load_id=None,
                    api_status='load_processing_failed',
                    error_message=result.error_message
                )
                mappings.append(mapping)
        
        return mappings
    
    def _fetch_internal_load_id(self, load_number: str) -> tuple[Optional[str], str, Optional[str]]:
        """
        Fetch internal load ID for a given load number.
        
        Args:
            load_number: The brokerage load number (e.g., CSVTEST75279)
            
        Returns:
            Tuple of (internal_load_id, status, error_message)
        """
        url = f"{self.base_url}/brokerage-key/{self.brokerage_key}/brokerage-load-id/{load_number}"
        headers = self.get_auth_headers()
        
        for attempt in range(self.retry_count):
            try:
                logger.info(f"Fetching load ID for {load_number} (attempt {attempt + 1})")
                
                response = requests.get(
                    url, 
                    headers=headers, 
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract internal load ID from response
                    # Adjust field name based on actual API response structure
                    internal_id = data.get('load_id') or data.get('id') or data.get('internal_load_id')
                    
                    if internal_id:
                        logger.info(f"Successfully retrieved load ID {internal_id} for {load_number}")
                        return internal_id, 'success', None
                    else:
                        logger.warning(f"No load ID found in response for {load_number}")
                        return None, 'no_id_in_response', 'Load ID not found in API response'
                        
                elif response.status_code == 404:
                    logger.warning(f"Load {load_number} not found in system")
                    return None, 'not_found', f'Load {load_number} not found'
                    
                elif response.status_code == 401:
                    logger.error("API authentication failed")
                    return None, 'auth_failed', 'API authentication failed'
                    
                elif response.status_code == 403:
                    logger.error("API access forbidden")
                    return None, 'access_forbidden', 'Access forbidden'
                    
                else:
                    logger.warning(f"API returned status {response.status_code} for {load_number}: {response.text}")
                    error_msg = f"API error: {response.status_code}"
                    
                    # Don't retry on client errors (4xx)
                    if 400 <= response.status_code < 500:
                        return None, 'client_error', error_msg
                        
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1} for {load_number}")
                if attempt == self.retry_count - 1:
                    return None, 'timeout', f'API timeout after {self.retry_count} attempts'
                    
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error on attempt {attempt + 1} for {load_number}")
                if attempt == self.retry_count - 1:
                    return None, 'connection_error', f'Connection failed after {self.retry_count} attempts'
                    
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1} for {load_number}: {e}")
                if attempt == self.retry_count - 1:
                    return None, 'error', str(e)
            
            # Wait before retry
            if attempt < self.retry_count - 1:
                time.sleep(self.retry_delay)
                
        return None, 'failed', f'All {self.retry_count} attempts failed'
    
    def get_mapping_summary(self, mappings: List[LoadIDMapping]) -> Dict[str, int]:
        """Get summary statistics for load ID mappings."""
        summary = {
            'total': len(mappings),
            'success': 0,
            'failed': 0,
            'not_found': 0,
            'load_processing_failed': 0,
            'auth_failed': 0,
            'timeout': 0,
            'connection_error': 0,
            'other_error': 0
        }
        
        for mapping in mappings:
            if mapping.api_status == 'success':
                summary['success'] += 1
            elif mapping.api_status == 'not_found':
                summary['not_found'] += 1
            elif mapping.api_status == 'load_processing_failed':
                summary['load_processing_failed'] += 1
            elif mapping.api_status == 'auth_failed':
                summary['auth_failed'] += 1
            elif mapping.api_status == 'timeout':
                summary['timeout'] += 1
            elif mapping.api_status == 'connection_error':
                summary['connection_error'] += 1
            else:
                summary['other_error'] += 1
                
        summary['failed'] = summary['total'] - summary['success']
        return summary