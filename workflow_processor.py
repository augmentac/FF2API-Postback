"""End-to-end workflow processor for CSV → FF2API → Load IDs → Snowflake → Postback."""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
import logging
from dataclasses import dataclass
from datetime import datetime

# Import existing components
from load_id_mapper import LoadIDMapper, LoadProcessingResult, LoadIDMapping
from enrichment.manager import EnrichmentManager
from postback.router import PostbackRouter
from credential_manager import credential_manager

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
        self.enrichment_manager = EnrichmentManager(enrichment_config)
        
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
                
            results.ff2api_results = self._simulate_ff2api_processing(results.csv_data)
            
            successful_loads = len([r for r in results.ff2api_results if r.success])
            self.update_step('ff2api', 'completed', 
                           f'{successful_loads}/{len(results.ff2api_results)} loads processed successfully')
            
            # Step 3: Retrieve internal load IDs
            self.update_step('mapping', 'in_progress', 'Retrieving internal load IDs...')
            if progress_callback:
                progress_callback(0.5, "Retrieving internal load IDs...")
            
            results.load_id_mappings = self.load_id_mapper.map_load_ids(results.ff2api_results)
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
            required_fields = ['load_id']  # Required for FF2API processing
            recommended_fields = ['carrier', 'PRO', 'customer_code', 'origin_zip', 'dest_zip']
            
            for field in required_fields:
                if field not in first_row:
                    errors.append(f"Required field '{field}' is missing for end-to-end processing")
                    
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
    
    def _simulate_ff2api_processing(self, csv_data: List[Dict[str, Any]]) -> List[LoadProcessingResult]:
        """
        Simulate FF2API processing results.
        In actual implementation, this would call the real FF2API system.
        """
        results = []
        
        for i, row in enumerate(csv_data):
            # Simulate processing with some realistic success/failure patterns
            success = True  # For demo, assume most succeed
            load_number = None
            error_message = None
            
            if success:
                # Generate load number based on load_id
                base_load_id = row.get('load_id', f'LOAD{i:03d}')
                load_number = f"CSV{base_load_id}{i:05d}"  # e.g., CSVLOAD00175279
            else:
                error_message = "Simulated processing error"
            
            result = LoadProcessingResult(
                csv_row_index=i,
                load_number=load_number,
                success=success,
                error_message=error_message,
                response_data={'processed_at': datetime.now().isoformat()}
            )
            results.append(result)
        
        return results
    
    def _enrich_data_with_load_ids(self, csv_data: List[Dict[str, Any]], 
                                  mappings: List[LoadIDMapping]) -> List[Dict[str, Any]]:
        """Enrich CSV data using load ID mappings and Snowflake enrichment."""
        enriched_data = []
        
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
                
                if mapping.error_message:
                    enriched_row['load_id_error'] = mapping.error_message
            
            # Apply enrichment using existing enrichment manager
            # The Snowflake enrichment will use internal_load_id if available
            enriched_row = self.enrichment_manager.enrich_row(enriched_row)
            
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
                # Add tracking API credentials and configuration
                tracking_creds = credential_manager.get_tracking_api_credentials()
                if tracking_creds:
                    enriched_source = {
                        **source,
                        **tracking_creds,  # Add API endpoint and credentials
                        'brokerage_key': self.brokerage_key  # Add brokerage context
                    }
                    enrichment_config.append(enriched_source)
                else:
                    logger.warning("Tracking API credentials not available - skipping tracking API enrichment")
            else:
                # Other enrichment sources pass through unchanged
                enrichment_config.append(source)
        
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