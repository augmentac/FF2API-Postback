"""End-to-end workflow processor for CSV → FF2API → Load IDs → Snowflake → Postback."""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
import logging
import json
from dataclasses import dataclass
from datetime import datetime

# Import existing components
from load_id_mapper import LoadIDMapper, LoadProcessingResult, LoadIDMapping
from enrichment.manager import EnrichmentManager
from postback.router import PostbackRouter
from credential_manager import credential_manager
from src.backend.api_client import LoadsAPIClient

logger = logging.getLogger(__name__)


@dataclass
class WorkflowStep:
    """Represents a step in the workflow process."""
    name: str
    status: str  # 'pending', 'in_progress', 'completed', 'failed'
    message: str = ""
    progress: float = 0.0
    details: Optional[Dict] = None


@dataclass
class WorkflowResults:
    """Complete results from end-to-end workflow processing."""
    csv_data: List[Dict[str, Any]]
    ff2api_results: List[LoadProcessingResult]
    load_id_mappings: List[LoadIDMapping]
    enriched_data: List[Dict[str, Any]]
    postback_results: Dict[str, bool]
    summary: Dict[str, Any]
    errors: List[str]


class EndToEndWorkflowProcessor:
    """Orchestrates the complete end-to-end load processing workflow."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.brokerage_key = config.get('brokerage_key', 'augment-brokerage')
        self.workflow_type = config.get('workflow_type', 'endtoend')  # 'endtoend' or 'postback'
        
        # Get automatic credential resolution
        self.credentials = credential_manager.validate_credentials(self.brokerage_key)
        brokerage_creds = credential_manager.get_brokerage_credentials(self.brokerage_key)
        
        # Initialize load ID mapper with resolved credentials
        self.load_id_mapper = LoadIDMapper(self.brokerage_key, brokerage_creds)
        
        # Initialize enrichment with global credentials and brokerage context
        enrichment_config = self._build_enrichment_config(config.get('enrichment', {}).get('sources', []))
        
        # Build brokerage configuration for auto-authentication
        brokerage_config = {
            'brokerage_key': self.brokerage_key,
            'api_base_url': config.get('load_api_url', ''),
            'api_key': brokerage_creds.get('api_key', ''),
            'bearer_token': brokerage_creds.get('bearer_token', ''),
            'auth_type': brokerage_creds.get('auth_type', 'api_key')
        }
        
        self.enrichment_manager = EnrichmentManager(enrichment_config, brokerage_config)
        
        # Initialize postback router
        postback_config = config.get('postback', {}).get('handlers', [])
        self.postback_router = PostbackRouter(postback_config)
        
        # Workflow state
        self.steps = [
            WorkflowStep('upload', 'pending', 'Upload and validate CSV data'),
            WorkflowStep('ff2api', 'pending', 'Process loads through FF2API'),
            WorkflowStep('mapping', 'pending', 'Retrieve internal load IDs'),
            WorkflowStep('enrichment', 'pending', 'Enrich data with warehouse information'),
            WorkflowStep('postback', 'pending', 'Send results via configured handlers')
        ]
    
    def get_step(self, step_name: str) -> Optional[WorkflowStep]:
        """Get a specific workflow step by name."""
        for step in self.steps:
            if step.name == step_name:
                return step
        return None
    
    def update_step(self, step_name: str, status: str, message: str = "", progress: float = 0.0, details: Dict = None):
        """Update the status of a workflow step."""
        step = self.get_step(step_name)
        if step:
            step.status = status
            step.message = message
            step.progress = progress
            step.details = details or {}
    
    def process_workflow(self, csv_data: pd.DataFrame, progress_callback=None) -> WorkflowResults:
        """
        Execute the complete end-to-end workflow.
        
        Args:
            csv_data: Uploaded CSV data as DataFrame
            progress_callback: Optional callback function for progress updates
            
        Returns:
            WorkflowResults object with complete processing results
        """
        results = WorkflowResults(
            csv_data=[],
            ff2api_results=[],
            load_id_mappings=[],
            enriched_data=[],
            postback_results={},
            summary={},
            errors=[]
        )
        
        try:
            # Step 1: Upload and validate CSV
            self.update_step('upload', 'in_progress', 'Validating CSV data...')
            if progress_callback:
                progress_callback(0.1, "Validating CSV data...")
            
            results.csv_data = csv_data.to_dict('records')
            validation_errors = self._validate_csv_data(results.csv_data)
            
            if validation_errors:
                results.errors.extend(validation_errors)
                self.update_step('upload', 'failed', f'{len(validation_errors)} validation errors')
                return results
            
            self.update_step('upload', 'completed', f'Validated {len(results.csv_data)} rows')
            
            # Step 2: Process loads through FF2API (simulated for now)
            self.update_step('ff2api', 'in_progress', 'Processing loads through FF2API...')
            if progress_callback:
                progress_callback(0.3, "Processing loads through FF2API...")
                
            results.ff2api_results = self._process_loads_via_ff2api(results.csv_data)
            
            successful_loads = len([r for r in results.ff2api_results if r.success])
            self.update_step('ff2api', 'completed', 
                           f'{successful_loads}/{len(results.ff2api_results)} loads processed successfully')
            
            # Step 3: Retrieve internal load IDs
            self.update_step('mapping', 'in_progress', 'Retrieving internal load IDs...')
            if progress_callback:
                progress_callback(0.5, "Retrieving internal load IDs...")
            
            results.load_id_mappings = self.load_id_mapper.map_load_ids(results.ff2api_results, results.csv_data)
            mapping_summary = self.load_id_mapper.get_mapping_summary(results.load_id_mappings)
            
            self.update_step('mapping', 'completed', 
                           f'{mapping_summary["success"]}/{mapping_summary["total"]} load IDs retrieved',
                           details=mapping_summary)
            
            # Step 4: Enrich data with Snowflake
            self.update_step('enrichment', 'in_progress', 'Enriching data with warehouse information...')
            if progress_callback:
                progress_callback(0.7, "Enriching data with warehouse information...")
            
            results.enriched_data = self._enrich_data_with_load_ids(
                results.csv_data, 
                results.load_id_mappings
            )
            
            enriched_count = len([row for row in results.enriched_data if 'sf_enrichment_timestamp' in row])
            self.update_step('enrichment', 'completed', f'{enriched_count} rows enriched')
            
            # Step 5: Send results via postback handlers
            self.update_step('postback', 'in_progress', 'Sending results via configured handlers...')
            if progress_callback:
                progress_callback(0.9, "Sending results via configured handlers...")
            
            if results.enriched_data:
                results.postback_results = self.postback_router.post_all(results.enriched_data)
            
            successful_handlers = len([success for success in results.postback_results.values() if success])
            total_handlers = len(results.postback_results)
            
            self.update_step('postback', 'completed', 
                           f'{successful_handlers}/{total_handlers} handlers succeeded')
            
            # Generate summary
            results.summary = self._generate_workflow_summary(results)
            
            if progress_callback:
                progress_callback(1.0, "Workflow completed!")
                
        except Exception as e:
            error_msg = f"Workflow error: {str(e)}"
            results.errors.append(error_msg)
            logger.error(error_msg)
            
            # Mark current step as failed
            for step in self.steps:
                if step.status == 'in_progress':
                    step.status = 'failed'
                    step.message = error_msg
                    break
        
        return results
    
    def _validate_csv_data(self, csv_data: List[Dict[str, Any]]) -> List[str]:
        """Validate CSV data for required fields and format based on workflow type."""
        errors = []
        
        if not csv_data:
            errors.append("CSV file is empty")
            return errors
        
        first_row = csv_data[0]
        
        if self.workflow_type == 'endtoend':
            # Strict validation for end-to-end workflow (new load creation)
            # Accept either load_id or load_number for load identification
            load_id_fields = ['load_id', 'load_number', 'loadNumber', 'load_num', 'LoadNumber']
            has_load_identifier = any(field in first_row for field in load_id_fields)
            
            if not has_load_identifier:
                errors.append(f"Required load identifier field is missing. Expected one of: {load_id_fields}")
            
            recommended_fields = ['carrier', 'PRO', 'customer_code', 'origin_zip', 'dest_zip']
                    
            missing_recommended = [field for field in recommended_fields if field not in first_row]
            if missing_recommended:
                logger.warning(f"Recommended fields missing: {missing_recommended}")
                
        elif self.workflow_type == 'postback':
            # Flexible validation for postback workflow (existing loads)
            # Just need at least one identifier field to lookup existing loads
            identifier_fields = ['load_id', 'BOL #', 'Carrier Pro#', 'carrier_pro', 'PRO', 'customer_code', 'bol_number']
            has_identifier = any(field in first_row for field in identifier_fields)
            
            if not has_identifier:
                errors.append(f"At least one identifier field required: {identifier_fields[:3]}... (or similar)")
            else:
                logger.info(f"Found identifier fields: {[f for f in identifier_fields if f in first_row]}")
        
        return errors
    
    def _process_loads_via_ff2api(self, csv_data: List[Dict[str, Any]]) -> List[LoadProcessingResult]:
        """
        Process loads through real FF2API system.
        
        Args:
            csv_data: List of CSV row dictionaries
            
        Returns:
            List of LoadProcessingResult objects with FF2API response data
        """
        results = []
        
        # Initialize FF2API client with brokerage credentials
        try:
            api_base_url = self.config.get('load_api_url', 'https://load.prod.goaugment.com')
            # Remove the /unstable/loads suffix if present, we need base URL
            if '/unstable/loads' in api_base_url:
                api_base_url = api_base_url.replace('/unstable/loads', '')
            
            client = LoadsAPIClient(
                base_url=api_base_url,
                api_key=self.credentials.api_available and credential_manager.get_brokerage_api_key(self.brokerage_key),
                auth_type='api_key'
            )
            
            logger.info(f"Initialized FF2API client for {self.brokerage_key} at {api_base_url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize FF2API client: {e}")
            # Return all failed results
            return [
                LoadProcessingResult(
                    csv_row_index=i,
                    load_number=None,
                    success=False,
                    error_message=f"API client initialization failed: {e}"
                ) for i, _ in enumerate(csv_data)
            ]
        
        # Process each load
        for i, row in enumerate(csv_data):
            try:
                # Use the original load_number from CSV as the load identifier
                # This is what will be used later for load details retrieval
                original_load_number = row.get('load_number', f'LOAD{i:03d}')
                logger.info(f"Processing load {i}: {original_load_number}")
                
                # Prepare load data for FF2API (this would need proper field mapping)
                load_payload = self._prepare_load_payload(row)
                logger.info(f"Generated load payload for {original_load_number}: {json.dumps(load_payload, indent=2)}")
                
                # Submit to FF2API
                logger.info(f"Submitting load to FF2API: {original_load_number}")
                api_result = client.create_load(load_payload)
                logger.info(f"FF2API response for {original_load_number}: {api_result}")
                
                if api_result['success']:
                    # Use original load_number for later retrieval via load_id_mapper
                    result = LoadProcessingResult(
                        csv_row_index=i,
                        load_number=original_load_number,  # Use CSV load_number for consistency
                        success=True,
                        error_message=None,
                        response_data=api_result.get('data', {})
                    )
                    logger.info(f"Successfully processed load {original_load_number}")
                else:
                    result = LoadProcessingResult(
                        csv_row_index=i,
                        load_number=original_load_number,
                        success=False,
                        error_message=api_result.get('error', 'Unknown API error'),
                        response_data=api_result
                    )
                    logger.error(f"Failed to process load {original_load_number}: {api_result.get('error')}")
                
            except Exception as e:
                logger.error(f"Exception processing load {i}: {e}")
                result = LoadProcessingResult(
                    csv_row_index=i,
                    load_number=row.get('load_number', f'LOAD{i:03d}'),
                    success=False,
                    error_message=f"Processing exception: {str(e)}"
                )
            
            results.append(result)
        
        logger.info(f"FF2API processing complete: {len([r for r in results if r.success])}/{len(results)} successful")
        return results
    
    def _prepare_load_payload(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare CSV row data for FF2API load creation with proper nested structure.
        
        Uses the same nested structure logic as DataProcessor.format_for_api()
        to ensure compatibility with FF2API validation requirements.
        """
        # Required nested structure for FF2API
        payload = {
            'load': {
                'loadNumber': row.get('load_number'),
                'mode': row.get('mode', 'FTL'),
                'rateType': row.get('rate_type', 'SPOT'),
                'status': 'DRAFT',  # Fixed: use valid status instead of CSV data
                'equipment': {
                    'equipmentType': 'DRY_VAN'
                },
                'items': [],
                'referenceNumbers': [],  # Initialize for reference numbers
                'route': [
                    {
                        'sequence': 1,
                        'stopActivity': 'PICKUP',
                        'address': {
                            'street1': '123 Test St',
                            'city': 'Test City',
                            'stateOrProvince': 'CA',
                            'country': 'US',
                            'postalCode': '90210'
                        },
                        'expectedArrivalWindowStart': '2024-01-01T08:00:00Z',
                        'expectedArrivalWindowEnd': '2024-01-01T17:00:00Z'
                    },
                    {
                        'sequence': 2,
                        'stopActivity': 'DELIVERY',
                        'address': {
                            'street1': '456 Test Ave',
                            'city': 'Test City 2',
                            'stateOrProvince': 'NY',
                            'country': 'US',
                            'postalCode': '10001'
                        },
                        'expectedArrivalWindowStart': '2024-01-02T08:00:00Z',
                        'expectedArrivalWindowEnd': '2024-01-02T17:00:00Z'
                    }
                ]
            },
            'customer': {
                'name': 'Test Customer'
            },
            'brokerage': {
                'contacts': [
                    {
                        'name': 'Test Broker',
                        'email': 'test@example.com',
                        'phone': '555-123-4567',
                        'role': 'ACCOUNT_MANAGER'
                    }
                ]
            }
        }
        
        # Add reference numbers if available in row data
        # Check for common PRO number fields
        pro_number = None
        pro_fields = ['PRO', 'pro_number', 'Carrier Pro#', 'pro', 'pro_num', 'tracking_number']
        
        for field in pro_fields:
            if field in row and row[field]:
                pro_number = str(row[field]).strip()
                break
        
        if pro_number:
            payload['load']['referenceNumbers'].append({
                'name': 'PRO_NUMBER',
                'value': pro_number
            })
        
        # Check for other reference numbers
        po_number = None
        po_fields = ['PO#', 'po_number', 'purchase_order', 'po', 'PO Number']
        
        for field in po_fields:
            if field in row and row[field]:
                po_number = str(row[field]).strip()
                break
        
        if po_number:
            payload['load']['referenceNumbers'].append({
                'name': 'PO_NUMBER', 
                'value': po_number
            })
        
        return payload
    
    def _enrich_data_with_load_ids(self, csv_data: List[Dict[str, Any]], 
                                  mappings: List[LoadIDMapping]) -> List[Dict[str, Any]]:
        """Enrich CSV data using load ID mappings and Snowflake enrichment."""
        enriched_data = []
        
        # Debug logging
        logger.info(f"Starting enrichment for {len(csv_data)} rows with {len(mappings)} load ID mappings")
        for mapping in mappings[:3]:  # Log first 3 mappings for debugging
            logger.info(f"Mapping {mapping.csv_row_index}: load_number={mapping.load_number}, internal_load_id={mapping.internal_load_id}, status={mapping.api_status}")
        
        # Create mapping lookup for efficiency
        mapping_lookup = {m.csv_row_index: m for m in mappings}
        
        for i, row in enumerate(csv_data):
            enriched_row = row.copy()
            
            # Add load mapping information
            mapping = mapping_lookup.get(i)
            if mapping:
                enriched_row['load_number'] = mapping.load_number
                enriched_row['internal_load_id'] = mapping.internal_load_id
                enriched_row['load_id_status'] = mapping.api_status
                
                # Add PRO number and carrier from FF2API load details (if available)
                if mapping.pro_number:
                    enriched_row['ff2api_pro_number'] = mapping.pro_number
                    # Also set standard PRO field for tracking integration
                    enriched_row['PRO'] = mapping.pro_number
                    
                if mapping.carrier_name:
                    enriched_row['ff2api_carrier_name'] = mapping.carrier_name
                    # Also set standard carrier field for tracking integration
                    enriched_row['carrier'] = mapping.carrier_name
                
                # Add enhanced workflow fields
                if mapping.workflow_path:
                    enriched_row['workflow_path'] = mapping.workflow_path
                    
                if mapping.pro_source_type:
                    enriched_row['pro_source_type'] = mapping.pro_source_type
                    
                if mapping.pro_confidence:
                    enriched_row['pro_confidence'] = mapping.pro_confidence
                    
                if mapping.pro_context:
                    enriched_row['pro_context'] = mapping.pro_context
                    
                # Add agent events summary (count only, not full data for CSV output)
                if mapping.agent_events_data:
                    enriched_row['agent_events_count'] = len(mapping.agent_events_data)
                else:
                    enriched_row['agent_events_count'] = 0
                
                if mapping.error_message:
                    enriched_row['load_id_error'] = mapping.error_message
            
            # Apply enrichment using existing enrichment manager
            # The Snowflake enrichment will use internal_load_id if available
            pre_columns = set(enriched_row.keys())
            enriched_row = self.enrichment_manager.enrich_row(enriched_row)
            post_columns = set(enriched_row.keys())
            new_columns = post_columns - pre_columns
            
            if new_columns:
                logger.info(f"Row {i}: Enrichment added columns: {new_columns}")
            else:
                logger.debug(f"Row {i}: No new columns added by enrichment")
            
            enriched_data.append(enriched_row)
        
        return enriched_data
    
    def _generate_workflow_summary(self, results: WorkflowResults) -> Dict[str, Any]:
        """Generate summary statistics for the workflow."""
        summary = {
            'total_rows': len(results.csv_data),
            'ff2api_success': len([r for r in results.ff2api_results if r.success]),
            'load_ids_retrieved': len([m for m in results.load_id_mappings if m.api_status == 'success']),
            'rows_enriched': len([row for row in results.enriched_data if 'sf_enrichment_timestamp' in row]),
            'postback_handlers_success': len([success for success in results.postback_results.values() if success]),
            'postback_handlers_total': len(results.postback_results),
            'errors_count': len(results.errors),
            'processing_time': datetime.now().isoformat()
        }
        
        # Add step completion status
        summary['steps_completed'] = len([s for s in self.steps if s.status == 'completed'])
        summary['steps_failed'] = len([s for s in self.steps if s.status == 'failed'])
        
        return summary
    
    def _build_enrichment_config(self, enrichment_sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build enrichment configuration with automatic credential resolution."""
        enrichment_config = []
        
        # Always try to add tracking API enrichment if credentials are available
        tracking_creds = credential_manager.get_tracking_api_credentials()
        brokerage_creds = credential_manager.get_brokerage_credentials(self.brokerage_key)
        
        if tracking_creds and brokerage_creds.get('api_key'):
            tracking_config = {
                'type': 'tracking_api',
                'pro_column': 'PRO',  
                'carrier_column': 'carrier',
                'brokerage_key': self.brokerage_key,
                **tracking_creds
            }
            enrichment_config.append(tracking_config)
            logger.info("Added tracking API enrichment to workflow configuration")
        else:
            logger.warning("Tracking API credentials not available - skipping tracking API enrichment")
            if not tracking_creds:
                logger.warning("tracking_creds is None")
            if not brokerage_creds.get('api_key'):
                logger.warning("brokerage API key not available")
        
        # Process additional enrichment sources from configuration
        for source in enrichment_sources:
            if source.get('type') == 'snowflake_augment':
                # Add global Snowflake credentials and brokerage context
                snowflake_creds = credential_manager.get_snowflake_credentials()
                if snowflake_creds:
                    enriched_source = {
                        **source,
                        **snowflake_creds,  # Add global credentials
                        'brokerage_key': self.brokerage_key  # Add brokerage context for filtering
                    }
                    enrichment_config.append(enriched_source)
                else:
                    logger.warning("Snowflake credentials not available - skipping Snowflake enrichment")
            elif source.get('type') == 'tracking_api':
                # Skip - already added above automatically
                logger.info("Skipping duplicate tracking_api source from configuration")
            else:
                # Other enrichment sources pass through unchanged
                enrichment_config.append(source)
        
        logger.info(f"Built enrichment configuration with {len(enrichment_config)} sources")
        return enrichment_config
    
    def get_credential_status(self) -> Dict[str, Any]:
        """Get current credential validation status for UI display."""
        return {
            'brokerage_key': self.brokerage_key,
            'api_available': self.credentials.api_available,
            'snowflake_available': self.credentials.snowflake_available,
            'email_available': self.credentials.email_available,
            'capabilities': self.credentials.capabilities,
            'available_brokerages': credential_manager.get_available_brokerages()
        }