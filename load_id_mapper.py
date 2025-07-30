"""Load ID mapper for retrieving internal load IDs from GoAugment API."""

import requests
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple
import logging
from dataclasses import dataclass
import time
import re

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
    # Additional load data from FF2API
    pro_number: Optional[str] = None
    carrier_name: Optional[str] = None
    load_details: Optional[Dict[str, Any]] = None
    # Enhanced fields for agent events workflow
    agent_events_data: Optional[List[Dict]] = None
    pro_source_type: Optional[str] = None  # 'csv', 'reference_numbers', 'email', 'call', 'text', 'system'
    pro_confidence: Optional[str] = None   # 'high', 'medium', 'low'
    pro_context: Optional[str] = None      # Description of where PRO was found
    workflow_path: Optional[str] = None    # 'direct_tracking', 'full_workflow'


class LoadIDMapper:
    """Maps CSV load numbers to internal system Load IDs via GoAugment API."""
    
    def __init__(self, brokerage_key: str, credentials: Dict[str, Any]):
        """
        Initialize with automatic credential resolution.
        
        Args:
            brokerage_key: Brokerage identifier for API context
            credentials: Resolved credentials from credential manager
        """
        self.brokerage_key = brokerage_key
        self.api_key = credentials.get('api_key')
        self.base_url = credentials.get('base_url', 'https://load.prod.goaugment.com/unstable/loads')
        self.timeout = credentials.get('timeout', 30)
        self.retry_count = credentials.get('retry_count', 3)
        self.retry_delay = credentials.get('retry_delay', 1)
        
        if not self.api_key:
            logger.warning(f"No API credentials available for brokerage: {brokerage_key}")
    
    # Keep legacy constructor for backward compatibility
    @classmethod
    def from_config(cls, config: Dict[str, Any]):
        """Legacy constructor for backward compatibility."""
        brokerage_key = config.get('brokerage_key', 'augment-brokerage')
        credentials = {
            'api_key': config.get('auth', {}).get('api_key'),
            'base_url': config.get('load_api_url', 'https://load.prod.goaugment.com/unstable/loads'),
            'timeout': config.get('api_timeout', 30),
            'retry_count': config.get('retry_count', 3),
            'retry_delay': config.get('retry_delay', 1)
        }
        return cls(brokerage_key, credentials)
        
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API calls using resolved credentials."""
        headers = {'Content-Type': 'application/json'}
        
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
            logger.debug(f"Added API authentication for brokerage: {self.brokerage_key}")
        else:
            logger.warning(f"No API key available for brokerage: {self.brokerage_key}")
            
        return headers
    
    def map_load_ids(self, processing_results: List[LoadProcessingResult], csv_rows: List[Dict[str, Any]] = None) -> List[LoadIDMapping]:
        """
        Map CSV load numbers to internal load IDs via API calls with enhanced PRO extraction.
        
        Args:
            processing_results: Results from FF2API load processing
            csv_rows: Original CSV row data for PRO workflow determination
            
        Returns:
            List of LoadIDMapping objects with enhanced workflow data
        """
        mappings = []
        csv_lookup = {}
        
        # Create lookup for CSV rows by index
        if csv_rows:
            for i, row in enumerate(csv_rows):
                csv_lookup[i] = row
        
        for result in processing_results:
            if result.success and result.load_number:
                # Get corresponding CSV row for this result
                csv_row = csv_lookup.get(result.csv_row_index, {})
                csv_row['_row_index'] = result.csv_row_index  # Add index for tracking
                
                # Use enhanced workflow
                mapping = self._fetch_internal_load_id_enhanced(result.load_number, csv_row)
                mappings.append(mapping)
                
            else:
                # Load processing failed, create mapping without internal ID
                mapping = LoadIDMapping(
                    csv_row_index=result.csv_row_index,
                    load_number=result.load_number,
                    internal_load_id=None,
                    api_status='load_processing_failed',
                    error_message=result.error_message,
                    workflow_path='load_processing_failed'
                )
                mappings.append(mapping)
        
        return mappings
    
    def _fetch_internal_load_id(self, load_number: str, csv_row: Dict[str, Any] = None) -> tuple[Optional[str], str, Optional[str], Optional[str], Optional[str], Optional[Dict]]:
        """
        Enhanced fetch internal load ID with conditional PRO extraction workflow.
        
        Args:
            load_number: The brokerage load number (e.g., CSVTEST75279)
            csv_row: Original CSV row data for PRO workflow determination
            
        Returns:
            Tuple of (internal_load_id, status, error_message, pro_number, carrier_name, load_details)
        """
        # Check if we have API credentials
        if not self.api_key:
            logger.error(f"No API credentials for brokerage {self.brokerage_key}")
            return None, 'no_credentials', f'No API credentials configured for {self.brokerage_key}', None, None, None
        
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
                    
                    # Extract PRO number from various possible fields
                    pro_number = (data.get('pro_number') or 
                                data.get('PRO') or 
                                data.get('proNumber') or
                                data.get('tracking_number') or
                                data.get('carrier_pro') or
                                data.get('load', {}).get('pro_number') or
                                data.get('load', {}).get('PRO'))
                    
                    # Extract carrier name from various possible fields  
                    carrier_name = (data.get('carrier_name') or
                                  data.get('carrier') or
                                  data.get('load', {}).get('carrier_name') or
                                  data.get('load', {}).get('carrier') or
                                  data.get('carrier_company_name'))
                    
                    if internal_id:
                        logger.info(f"Successfully retrieved load data for {load_number}: ID={internal_id}, PRO={pro_number}, Carrier={carrier_name}")
                        return internal_id, 'success', None, pro_number, carrier_name, data
                    else:
                        logger.warning(f"No load ID found in response for {load_number}")
                        return None, 'no_id_in_response', 'Load ID not found in API response', pro_number, carrier_name, data
                        
                elif response.status_code == 404:
                    logger.warning(f"Load {load_number} not found in system")
                    return None, 'not_found', f'Load {load_number} not found', None, None, None
                    
                elif response.status_code == 401:
                    logger.error("API authentication failed")
                    return None, 'auth_failed', 'API authentication failed', None, None, None
                    
                elif response.status_code == 403:
                    logger.error("API access forbidden")
                    return None, 'access_forbidden', 'Access forbidden', None, None, None
                    
                else:
                    logger.warning(f"API returned status {response.status_code} for {load_number}: {response.text}")
                    error_msg = f"API error: {response.status_code}"
                    
                    # Don't retry on client errors (4xx)
                    if 400 <= response.status_code < 500:
                        return None, 'client_error', error_msg, None, None, None
                        
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1} for {load_number}")
                if attempt == self.retry_count - 1:
                    return None, 'timeout', f'API timeout after {self.retry_count} attempts', None, None, None
                    
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error on attempt {attempt + 1} for {load_number}")
                if attempt == self.retry_count - 1:
                    return None, 'connection_error', f'Connection failed after {self.retry_count} attempts', None, None, None
                    
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1} for {load_number}: {e}")
                if attempt == self.retry_count - 1:
                    return None, 'error', str(e), None, None, None
            
            # Wait before retry
            if attempt < self.retry_count - 1:
                time.sleep(self.retry_delay)
                
        return None, 'failed', f'All {self.retry_count} attempts failed', None, None, None
    
    def _fetch_internal_load_id_enhanced(self, load_number: str, csv_row: Dict[str, Any] = None) -> LoadIDMapping:
        """
        Enhanced load ID fetching with conditional PRO extraction workflow.
        
        Args:
            load_number: The brokerage load number
            csv_row: Original CSV row data for PRO workflow determination
            
        Returns:
            LoadIDMapping object with enhanced fields populated
        """
        # Initialize base mapping
        mapping = LoadIDMapping(
            csv_row_index=csv_row.get('_row_index', 0) if csv_row else 0,
            load_number=load_number,
            internal_load_id=None,
            api_status='pending',
            workflow_path=None
        )
        
        # Step 1: Fetch internal load ID and basic load details
        internal_id, status, error, pro_number, carrier_name, load_details = self._fetch_internal_load_id(load_number, csv_row)
        
        # Update mapping with basic results
        mapping.internal_load_id = internal_id
        mapping.api_status = status
        mapping.error_message = error
        mapping.pro_number = pro_number
        mapping.carrier_name = carrier_name
        mapping.load_details = load_details
        
        # If API call failed, return early
        if status != 'success' or not internal_id:
            mapping.workflow_path = 'api_failed'
            logger.warning(f"API call failed for {load_number}: {status}")
            return mapping
        
        # Step 2: Determine PRO workflow path
        workflow_path, final_pro, source_type, context = self._determine_pro_workflow_path(
            csv_row or {}, load_details
        )
        mapping.workflow_path = workflow_path
        
        if workflow_path == 'direct_tracking':
            # PRO found in CSV or load details - use it directly
            mapping.pro_number = final_pro
            mapping.pro_source_type = source_type
            mapping.pro_confidence = 'high'
            mapping.pro_context = context
            logger.info(f"Direct tracking workflow for {load_number}: PRO={final_pro}")
            return mapping
        
        # Step 3: Full workflow - extract PRO from agent events
        logger.info(f"Executing full workflow for {load_number} - fetching agent events")
        
        # Fetch agent events
        agent_events = self._get_agent_events(internal_id)
        mapping.agent_events_data = agent_events
        
        if not agent_events:
            logger.warning(f"No agent events found for {load_number}")
            mapping.pro_source_type = 'none'
            mapping.pro_confidence = 'none'
            mapping.pro_context = 'No agent events available'
            return mapping
        
        # Extract PRO from events
        pro_extraction_result = self._extract_pro_from_events(agent_events, load_number)
        
        if pro_extraction_result:
            extracted_pro, source_type, confidence, context = pro_extraction_result
            mapping.pro_number = extracted_pro
            mapping.pro_source_type = source_type
            mapping.pro_confidence = confidence
            mapping.pro_context = context
            logger.info(f"Successfully extracted PRO {extracted_pro} from {source_type} for {load_number}")
        else:
            logger.warning(f"No PRO number extracted from agent events for {load_number}")
            mapping.pro_source_type = 'none'
            mapping.pro_confidence = 'none'
            mapping.pro_context = 'PRO extraction failed from agent events'
        
        return mapping
    
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

    def _determine_pro_workflow_path(self, csv_row: Dict[str, Any], load_details: Dict[str, Any] = None) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
        """
        Determine which workflow path to take based on PRO availability.
        
        Args:
            csv_row: Original CSV row data
            load_details: Load details from FF2API (if available)
            
        Returns:
            Tuple of (workflow_path, pro_number, source_type, context)
        """
        # Priority 1: Check CSV PRO field first
        csv_pro_fields = ['PRO', 'pro_number', 'ProNumber', 'tracking_number', 'carrier_pro']
        for field in csv_pro_fields:
            pro_value = csv_row.get(field)
            if pro_value and str(pro_value).strip():
                pro_number = str(pro_value).strip()
                logger.info(f"Found PRO number in CSV field '{field}': {pro_number}")
                return 'direct_tracking', pro_number, 'csv', f"Found in CSV field '{field}'"
        
        # Priority 2: Check load details reference numbers
        if load_details:
            pro_from_refs = self._extract_pro_from_reference_numbers(load_details)
            if pro_from_refs:
                pro_number, context = pro_from_refs
                logger.info(f"Found PRO number in load details reference numbers: {pro_number}")
                return 'direct_tracking', pro_number, 'reference_numbers', context
        
        # No PRO found - need full workflow
        logger.info("No PRO number found in CSV or load details - will execute full workflow")
        return 'full_workflow', None, None, None

    def _extract_pro_from_reference_numbers(self, load_details: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """
        Extract PRO number from load details reference numbers.
        
        Args:
            load_details: Load details response from FF2API
            
        Returns:
            Tuple of (pro_number, context) or None if not found
        """
        reference_numbers = load_details.get('referenceNumbers', [])
        if not reference_numbers:
            return None
        
        # Look for PRO-related reference names
        pro_ref_names = ['pro_number', 'pro', 'PRO', 'tracking_number', 'carrier_pro', 'pro_num']
        
        for ref in reference_numbers:
            ref_name = ref.get('name', '').lower()
            ref_value = ref.get('value', '')
            
            if any(pro_name.lower() in ref_name for pro_name in pro_ref_names) and ref_value:
                pro_number = str(ref_value).strip()
                if self._validate_pro_format(pro_number):
                    context = f"Found in reference number '{ref.get('name')}'"
                    return pro_number, context
        
        return None

    def _get_agent_events(self, internal_load_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve agent events for a load to extract PRO numbers.
        
        Args:
            internal_load_id: Internal load ID from FF2API
            
        Returns:
            List of events or None if failed
        """
        if not internal_load_id:
            return None
        
        url = f"https://agent-orchestrator.prod.goaugment.com/unstable/events"
        params = {
            'loadId': internal_load_id,
            'limit': 1000
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        try:
            logger.info(f"Fetching agent events for load ID: {internal_load_id}")
            response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            
            if response.status_code == 200:
                events_data = response.json()
                events = events_data.get('records', [])
                logger.info(f"Retrieved {len(events)} agent events for load {internal_load_id}")
                return events
            else:
                logger.warning(f"Agent events API returned {response.status_code} for load {internal_load_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching agent events for load {internal_load_id}: {e}")
            return None

    def _extract_pro_from_events(self, events: List[Dict[str, Any]], original_load_number: str) -> Optional[Tuple[str, str, str, str]]:
        """
        Extract PRO number from agent events using multi-strategy approach.
        
        Args:
            events: List of agent events
            original_load_number: Original load number to exclude from results
            
        Returns:
            Tuple of (pro_number, source_type, confidence, context) or None
        """
        if not events:
            return None
        
        # PRO regex patterns
        pro_patterns = [
            r'[Pp][Rr][Oo][\s#]*(\d{10,12})',                    # "Pro 30112031668", "PRO# 123456789"
            r'[Pp][Rr][Oo][\s]*[Nn]umber[\s]*:?[\s]*(\d{10,12})', # "Pro number: 123456789"
            r'picked up under [Pp][Rr][Oo][\s]*(\d{10,12})',      # "picked up under Pro 30112031668"
            r'tracking[\s]*[#]?[\s]*(\d{10,12})',                 # "tracking 123456789"
            r'shipment[\s]*[#]?[\s]*(\d{10,12})',                 # "shipment 123456789"
        ]
        
        # Sort events by priority and recency
        prioritized_events = self._prioritize_events_for_pro_extraction(events)
        
        for event in prioritized_events:
            event_code = event.get('code', '')
            data = event.get('data', {})
            
            # Determine event type and confidence
            source_type, confidence = self._determine_event_source_type(event_code)
            
            # Extract text to search based on event type
            search_texts = self._extract_searchable_text_from_event(event, event_code, data)
            
            # Search for PRO numbers in extracted text
            for search_text, text_source in search_texts:
                if not search_text:
                    continue
                    
                for pattern in pro_patterns:
                    matches = re.findall(pattern, search_text, re.IGNORECASE)
                    for match in matches:
                        pro_candidate = match.strip()
                        
                        # Validate and filter out internal load numbers
                        if (self._validate_pro_format(pro_candidate) and 
                            pro_candidate != original_load_number and
                            not self._is_internal_load_number(pro_candidate, original_load_number)):
                            
                            context = f"Found in {source_type} event ({text_source}): {event.get('id', 'unknown')}"
                            logger.info(f"Extracted PRO {pro_candidate} from {source_type} event")
                            return pro_candidate, source_type, confidence, context
        
        logger.info("No PRO number found in agent events")
        return None

    def _prioritize_events_for_pro_extraction(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioritize events for PRO extraction based on type and recency."""
        # Event priority mapping (higher number = higher priority)
        event_priorities = {
            'NEW_EMAIL': 5,
            'SENT_EMAIL': 4, 
            'COMPLETED_CALL': 4,
            'WORKFLOW_STATUS_UPDATE': 5,
            'SCHEDULE_EVENT': 2,
            'SEND_CALL': 1
        }
        
        def event_sort_key(event):
            code = event.get('code', '')
            priority = event_priorities.get(code, 0)
            created_at = event.get('createdAt', '')
            return (priority, created_at)
        
        return sorted(events, key=event_sort_key, reverse=True)

    def _determine_event_source_type(self, event_code: str) -> Tuple[str, str]:
        """Determine source type and confidence from event code."""
        if event_code in ['NEW_EMAIL', 'SENT_EMAIL']:
            return 'email', 'high'
        elif event_code == 'COMPLETED_CALL':
            return 'call', 'medium'
        elif event_code == 'WORKFLOW_STATUS_UPDATE':
            return 'system', 'high'
        elif 'SMS' in event_code or 'TEXT' in event_code:
            return 'text', 'high'
        else:
            return 'other', 'low'

    def _extract_searchable_text_from_event(self, event: Dict, event_code: str, data: Dict) -> List[Tuple[str, str]]:
        """Extract searchable text from event based on type."""
        search_texts = []
        
        if event_code in ['NEW_EMAIL', 'SENT_EMAIL']:
            # Email events
            if 'body' in data:
                body = data['body']
                if isinstance(body, dict):
                    if 'content' in body:
                        search_texts.append((body['content'], 'email body content'))
                    if 'preview' in body:
                        search_texts.append((body['preview'], 'email body preview'))
                elif isinstance(body, str):
                    search_texts.append((body, 'email body'))
            
            if 'subject' in data:
                search_texts.append((data['subject'], 'email subject'))
            
            if 'emailAnalysisResult' in data:
                analysis = data['emailAnalysisResult']
                if 'summary' in analysis:
                    search_texts.append((analysis['summary'], 'email analysis summary'))
                if 'emailText' in analysis:
                    search_texts.append((analysis['emailText'], 'email analysis text'))
        
        elif event_code == 'COMPLETED_CALL':
            # Call events
            if 'analysisResult' in data:
                analysis = data['analysisResult']
                if 'summary' in analysis:
                    search_texts.append((analysis['summary'], 'call analysis summary'))
            
            if 'summary' in data:
                search_texts.append((data['summary'], 'call summary'))
        
        elif event_code == 'WORKFLOW_STATUS_UPDATE':
            # System events
            if 'context' in data:
                search_texts.append((data['context'], 'workflow context'))
            if 'status' in data:
                search_texts.append((str(data['status']), 'workflow status'))
        
        # Generic data search for any event
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and len(value) > 10:  # Skip short values
                    search_texts.append((value, f'data.{key}'))
        
        return search_texts

    def _validate_pro_format(self, pro_candidate: str) -> bool:
        """Validate PRO number format (typically 10-12 digits)."""
        if not pro_candidate:
            return False
        
        # Remove any non-digit characters for validation
        digits_only = re.sub(r'\D', '', pro_candidate)
        
        # PRO numbers are typically 10-12 digits
        return 10 <= len(digits_only) <= 12

    def _is_internal_load_number(self, pro_candidate: str, original_load_number: str) -> bool:
        """Check if the candidate is an internal load number to exclude."""
        if not pro_candidate or not original_load_number:
            return False
        
        # Exclude exact matches
        if pro_candidate == original_load_number:
            return True
            
        # Exclude if it contains the original load number
        if original_load_number in pro_candidate or pro_candidate in original_load_number:
            return True
            
        return False