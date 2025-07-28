"""
Tracking API Enrichment Source.

Provides tracking data enrichment by calling a single API endpoint
with dynamic carrier (browserTask) and PRO number parameters.
"""

import requests
import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from .base import EnrichmentSource

logger = logging.getLogger(__name__)

class TrackingAPIEnricher(EnrichmentSource):
    """Enrichment source that fetches tracking data from a single API endpoint."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize tracking API enricher.
        
        Args:
            config: Configuration dictionary containing:
                - api_endpoint: The tracking API endpoint URL
                - timeout: Request timeout in seconds (default: 30)
                - max_retries: Maximum retry attempts (default: 3)
        """
        super().__init__(config)
        self.api_endpoint = config.get('api_endpoint')
        self.timeout = config.get('timeout', 30)
        self.max_retries = config.get('max_retries', 3)
        
        # Column mapping - will be set by configuration
        self.pro_column = config.get('pro_column', 'PRO')
        self.carrier_column = config.get('carrier_column', 'carrier')
        
        if not self.api_endpoint:
            raise ValueError("api_endpoint is required for tracking API enricher")
        
        # Cache for API results to avoid duplicate calls
        self._tracking_cache = {}
        
        logger.info(f"Initialized tracking API enricher with endpoint: {self.api_endpoint}")
        logger.info(f"Column mapping - PRO: {self.pro_column}, Carrier: {self.carrier_column}")
        
    def enrich(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single data row with tracking information.
        
        Args:
            row: Original data row dictionary
            
        Returns:
            Enriched data row dictionary with tracking columns added
        """
        enriched_row = row.copy()
        
        # Get PRO and carrier from the row
        pro_number = row.get(self.pro_column)
        carrier = row.get(self.carrier_column)
        
        # Initialize tracking columns
        enriched_row['tracking_status'] = None
        enriched_row['tracking_location'] = None
        enriched_row['tracking_date'] = None
        
        if not pro_number or not carrier:
            logger.debug(f"Skipping tracking enrichment: missing PRO ({pro_number}) or carrier ({carrier})")
            return enriched_row
        
        # Convert to strings and get tracking data
        pro_str = str(pro_number).strip()
        carrier_str = str(carrier).strip()
        
        if not pro_str or not carrier_str:
            logger.debug(f"Skipping tracking enrichment: empty PRO or carrier after conversion")
            return enriched_row
        
        tracking_data = self._get_tracking_data(carrier_str, pro_str)
        
        if tracking_data:
            enriched_row['tracking_status'] = tracking_data.get('status')
            enriched_row['tracking_location'] = tracking_data.get('location')
            enriched_row['tracking_date'] = tracking_data.get('date')
            logger.debug(f"Successfully enriched tracking data for {carrier_str} PRO {pro_str}")
        else:
            logger.debug(f"Failed to get tracking data for {carrier_str} PRO {pro_str}")
        
        return enriched_row
    
    def is_applicable(self, row: Dict[str, Any]) -> bool:
        """Check if tracking enrichment is applicable to this row.
        
        Args:
            row: Data row to check
            
        Returns:
            True if row has required PRO and carrier fields
        """
        pro_number = row.get(self.pro_column)
        carrier = row.get(self.carrier_column)
        
        return bool(pro_number and carrier)
    
    def validate_config(self) -> bool:
        """Validate tracking API configuration.
        
        Returns:
            True if configuration is valid
        """
        if not self.api_endpoint:
            logger.error("Tracking API endpoint is required")
            return False
        
        if not isinstance(self.timeout, (int, float)) or self.timeout <= 0:
            logger.error("Tracking API timeout must be a positive number")
            return False
        
        if not isinstance(self.max_retries, int) or self.max_retries < 0:
            logger.error("Tracking API max_retries must be a non-negative integer")
            return False
        
        return True
    
    def _get_tracking_data(self, carrier: str, pro_number: str) -> Optional[Dict[str, Any]]:
        """Get tracking data for a specific carrier and PRO number.
        
        Args:
            carrier: Carrier name (used as browserTask)
            pro_number: PRO number to track
            
        Returns:
            Dictionary with tracking data or None if failed
        """
        # Create cache key
        cache_key = f"{carrier}:{pro_number}"
        
        # Check cache first
        if cache_key in self._tracking_cache:
            logger.debug(f"Using cached tracking data for {carrier} PRO {pro_number}")
            return self._tracking_cache[cache_key]
        
        payload = {
            "browserTask": carrier,
            "params": {
                "proNumber": pro_number
            }
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Tracking API request attempt {attempt + 1} for {carrier} PRO {pro_number}")
                
                response = requests.post(
                    self.api_endpoint,
                    json=payload,
                    timeout=self.timeout,
                    headers={'Content-Type': 'application/json'}
                )
                response.raise_for_status()
                
                data = response.json()
                result = data.get('completedBrowserTask', {}).get('result', {})
                
                if not result:
                    logger.warning(f"Empty result from tracking API for {carrier} PRO {pro_number}")
                    # Cache the failure to avoid repeated attempts
                    self._tracking_cache[cache_key] = None
                    return None
                
                tracking_data = {
                    'status': result.get('detailedStatus', result.get('status')),
                    'location': self._format_location(result),
                    'date': result.get('date')
                }
                
                # Cache the successful result
                self._tracking_cache[cache_key] = tracking_data
                
                logger.debug(f"Successfully retrieved tracking data for {carrier} PRO {pro_number}: {tracking_data}")
                return tracking_data
                
            except requests.exceptions.Timeout:
                logger.warning(f"Tracking API timeout (attempt {attempt + 1}) for {carrier} PRO {pro_number}")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"Tracking API HTTP error (attempt {attempt + 1}) for {carrier} PRO {pro_number}: {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Tracking API request error (attempt {attempt + 1}) for {carrier} PRO {pro_number}: {e}")
            except ValueError as e:
                logger.warning(f"Tracking API JSON decode error (attempt {attempt + 1}) for {carrier} PRO {pro_number}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error (attempt {attempt + 1}) for {carrier} PRO {pro_number}: {e}")
                
            # If this was the last attempt, log final failure and cache it
            if attempt == self.max_retries - 1:
                logger.error(f"All {self.max_retries} attempts failed for {carrier} PRO {pro_number}")
                self._tracking_cache[cache_key] = None
                    
        return None
    
    def _format_location(self, result: Dict[str, Any]) -> Optional[str]:
        """Format location from API result into a single string.
        
        Args:
            result: API result dictionary containing location fields
            
        Returns:
            Formatted location string or None if no location data
        """
        city = result.get('city', '').strip()
        state = result.get('state', '').strip()
        country = result.get('country', '').strip()
        
        location_parts = [part for part in [city, state, country] if part]
        
        if location_parts:
            formatted_location = ', '.join(location_parts)
            logger.debug(f"Formatted location: {formatted_location}")
            return formatted_location
        
        return None