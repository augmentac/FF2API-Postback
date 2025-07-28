"""
Unified Load Processor - Consolidates FF2API and End-to-End workflows.

This module provides a unified interface that combines:
- FF2API load processing with field mapping and validation
- Load ID mapping and retrieval
- Data enrichment capabilities
- Postback and output handling
- Email automation support
"""

import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import streamlit as st

# Import existing components  
from .api_client import LoadsAPIClient
from .data_processor import DataProcessor
from .database import DatabaseManager

# Import UI components with fallback
try:
    from ..frontend.ui_components import get_full_api_schema
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'frontend'))
    from ui_components import get_full_api_schema

# Import end-to-end components
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from load_id_mapper import LoadIDMapper, LoadProcessingResult, LoadIDMapping
from enrichment.manager import EnrichmentManager
from postback.router import PostbackRouter
from credential_manager import credential_manager

logger = logging.getLogger(__name__)


@dataclass
class ProcessingMode:
    """Defines the processing mode configuration."""
    name: str
    description: str
    show_field_mapping: bool = True
    show_validation: bool = True
    show_enrichment: bool = False
    show_postback: bool = False
    auto_process: bool = False


@dataclass
class UnifiedProcessingResult:
    """Complete results from unified processing workflow."""
    csv_data: List[Dict[str, Any]]
    ff2api_results: List[Dict[str, Any]]
    load_id_mappings: Optional[List[LoadIDMapping]] = None
    enriched_data: Optional[List[Dict[str, Any]]] = None
    postback_results: Optional[Dict[str, bool]] = None
    summary: Dict[str, Any] = None
    errors: List[str] = None
    processing_mode: str = "manual"


class UnifiedLoadProcessor:
    """
    Unified processor that combines FF2API capabilities with end-to-end workflow features.
    
    Supports multiple processing modes:
    - Manual: Full control with step-by-step processing
    - Automated: Single-click processing using saved configurations
    - Hybrid: Quick setup with advanced options available
    """
    
    # Available processing modes
    PROCESSING_MODES = {
        'manual': ProcessingMode(
            name="Manual Processing",
            description="Full control with step-by-step field mapping and validation",
            show_field_mapping=True,
            show_validation=True,
            show_enrichment=False,
            show_postback=False,
            auto_process=False
        ),
        'automated': ProcessingMode(
            name="Automated Processing",
            description="Single-click processing using saved configuration",
            show_field_mapping=False,
            show_validation=False,
            show_enrichment=True,
            show_postback=True,
            auto_process=True
        ),
        'endtoend': ProcessingMode(
            name="End-to-End Pipeline",
            description="Complete workflow with load mapping, enrichment, and postback",
            show_field_mapping=True,
            show_validation=True,
            show_enrichment=True,
            show_postback=True,
            auto_process=False
        )
    }
    
    def __init__(self, config: Dict[str, Any], processing_mode: str = 'manual'):
        """
        Initialize unified processor.
        
        Args:
            config: Processing configuration including credentials and settings
            processing_mode: Processing mode ('manual', 'automated', 'endtoend')
        """
        self.config = config
        self.processing_mode = processing_mode
        self.mode_config = self.PROCESSING_MODES.get(processing_mode, self.PROCESSING_MODES['manual'])
        
        # Initialize core components
        self.db_manager = DatabaseManager()
        # Lazy initialize API client - will be configured when needed
        self.api_client = None
        self.data_processor = DataProcessor()
        
        # Initialize brokerage context
        self.brokerage_key = config.get('brokerage_key', 'augment-brokerage')
        self.credentials = credential_manager.validate_credentials(self.brokerage_key)
        
        # Initialize end-to-end components if needed
        if self.mode_config.show_enrichment or self.mode_config.show_postback:
            self._initialize_endtoend_components()
        
        logger.info(f"Initialized unified processor in {processing_mode} mode for brokerage {self.brokerage_key}")
    
    def _initialize_endtoend_components(self):
        """Initialize components needed for end-to-end processing."""
        try:
            # Initialize load ID mapper
            brokerage_creds = credential_manager.get_brokerage_credentials(self.brokerage_key)
            self.load_id_mapper = LoadIDMapper(self.brokerage_key, brokerage_creds)
            
            # Initialize enrichment manager
            enrichment_config = self._build_enrichment_config()
            self.enrichment_manager = EnrichmentManager(enrichment_config)
            
            # Initialize postback router
            postback_config = self.config.get('postback', {}).get('handlers', [])
            self.postback_router = PostbackRouter(postback_config)
            
            logger.info("End-to-end components initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing end-to-end components: {e}")
            self.load_id_mapper = None
            self.enrichment_manager = None
            self.postback_router = None
    
    def _build_enrichment_config(self) -> List[Dict[str, Any]]:
        """Build enrichment configuration from global credentials."""
        enrichment_config = []
        
        # Get enrichment sources from config
        enrichment_sources = self.config.get('enrichment', {}).get('sources', [])
        
        for source in enrichment_sources:
            if source.get('type') == 'snowflake_augment':
                # Add Snowflake credentials if available
                snowflake_creds = credential_manager.get_snowflake_credentials()
                if snowflake_creds:
                    enriched_source = {**source, **snowflake_creds, 'brokerage_key': self.brokerage_key}
                    enrichment_config.append(enriched_source)
                else:
                    logger.warning("Snowflake credentials not available - skipping Snowflake enrichment")
            
            elif source.get('type') == 'tracking_api':
                # Add tracking API credentials if available
                tracking_creds = credential_manager.get_tracking_api_credentials()
                if tracking_creds:
                    enriched_source = {**source, **tracking_creds, 'brokerage_key': self.brokerage_key}
                    enrichment_config.append(enriched_source)
                else:
                    logger.warning("Tracking API credentials not available - skipping tracking API enrichment")
            
            else:
                # Other enrichment sources pass through unchanged
                enrichment_config.append(source)
        
        return enrichment_config
    
    def get_available_configurations(self, brokerage_name: str) -> List[Dict[str, Any]]:
        """Get saved configurations for a brokerage."""
        try:
            return self.db_manager.get_brokerage_configurations(brokerage_name)
        except Exception as e:
            logger.error(f"Error retrieving configurations for {brokerage_name}: {e}")
            return []
    
    def save_configuration(self, brokerage_name: str, config_name: str, field_mapping: Dict[str, str], 
                          api_credentials: Dict[str, Any], additional_config: Dict[str, Any] = None) -> bool:
        """Save a processing configuration for reuse."""
        try:
            # Extend configuration with end-to-end settings if applicable
            enrichment_config = {
                'enabled': self.mode_config.show_enrichment,
                'sources': self.config.get('enrichment', {}).get('sources', [])
            } if self.mode_config.show_enrichment else None
            
            postback_config = {
                'enabled': self.mode_config.show_postback,
                'handlers': self.config.get('postback', {}).get('handlers', [])
            } if self.mode_config.show_postback else None
            
            # Build configuration description
            description = additional_config.get('description', '') if additional_config else ''
            auth_type = additional_config.get('auth_type', 'api_key') if additional_config else 'api_key'
            bearer_token = additional_config.get('bearer_token') if additional_config else None
            
            config_id = self.db_manager.save_brokerage_configuration(
                brokerage_name=brokerage_name,
                configuration_name=config_name,
                field_mappings=field_mapping,
                api_credentials=api_credentials,
                description=description,
                auth_type=auth_type,
                bearer_token=bearer_token,
                processing_mode=self.processing_mode,
                enrichment_config=enrichment_config,
                postback_config=postback_config
            )
            
            return config_id is not None
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            return False
    
    def validate_and_map_data(self, df: pd.DataFrame, field_mapping: Dict[str, str]) -> Tuple[pd.DataFrame, List[str]]:
        """Validate and map CSV data using field mapping configuration."""
        try:
            # Use existing data processor for validation and mapping
            mapped_df = self.data_processor.map_fields(df, field_mapping)
            
            # Validate against FF2API schema
            api_schema = get_full_api_schema()
            validation_errors = self.data_processor.validate_data(mapped_df, api_schema)
            
            return mapped_df, validation_errors
            
        except Exception as e:
            logger.error(f"Error in data validation and mapping: {e}")
            return df, [f"Mapping error: {str(e)}"]
    
    def process_ff2api(self, df: pd.DataFrame, api_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process data through FF2API."""
        try:
            # Initialize and configure API client
            if self.api_client is None:
                # Only initialize if we have credentials
                api_key = api_config.get('api_key')
                bearer_token = api_config.get('bearer_token')
                auth_type = api_config.get('auth_type', 'api_key')
                
                if auth_type == 'api_key' and not api_key:
                    raise ValueError("API key is required for api_key authentication")
                elif auth_type == 'bearer_token' and not bearer_token:
                    raise ValueError("Bearer token is required for bearer_token authentication")
                
                self.api_client = LoadsAPIClient(
                    base_url=api_config.get('base_url', 'https://api.prod.goaugment.com'),
                    api_key=api_key,
                    bearer_token=bearer_token,
                    auth_type=auth_type
                )
            else:
                # Reconfigure existing client if needed
                self.api_client.configure(
                    base_url=api_config.get('base_url'),
                    api_key=api_config.get('api_key'),
                    bearer_token=api_config.get('bearer_token')
                )
            
            # Process data through FF2API
            results = []
            for index, row in df.iterrows():
                try:
                    result = self.api_client.create_load(row.to_dict())
                    results.append({
                        'row_index': index,
                        'success': True,
                        'data': result,
                        'error': None
                    })
                except Exception as e:
                    results.append({
                        'row_index': index,
                        'success': False,
                        'data': None,
                        'error': str(e)
                    })
                    logger.error(f"FF2API processing error for row {index}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"FF2API processing error: {e}")
            return [{'row_index': 0, 'success': False, 'data': None, 'error': str(e)}]
    
    def process_load_id_mapping(self, ff2api_results: List[Dict[str, Any]]) -> List[LoadIDMapping]:
        """Retrieve load IDs for successful FF2API results."""
        if not self.load_id_mapper:
            logger.warning("Load ID mapper not available")
            return []
        
        try:
            # Extract successful results for load ID mapping
            successful_results = [r for r in ff2api_results if r.get('success')]
            
            if not successful_results:
                logger.info("No successful FF2API results for load ID mapping")
                return []
            
            # Create load processing results for mapping
            load_results = []
            for result in successful_results:
                data = result.get('data', {})
                load_results.append(LoadProcessingResult(
                    load_number=data.get('load_number', ''),
                    success=True,
                    api_response=data,
                    error_message=None
                ))
            
            # Process load ID mapping
            mappings = self.load_id_mapper.process_load_results(load_results)
            return mappings
            
        except Exception as e:
            logger.error(f"Load ID mapping error: {e}")
            return []
    
    def process_enrichment(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply data enrichment using configured sources."""
        if not self.enrichment_manager:
            logger.warning("Enrichment manager not available")
            return data
        
        try:
            enriched_data = []
            for row in data:
                enriched_row = self.enrichment_manager.enrich_data(row)
                enriched_data.append(enriched_row)
            
            logger.info(f"Enriched {len(enriched_data)} rows")
            return enriched_data
            
        except Exception as e:
            logger.error(f"Enrichment processing error: {e}")
            return data
    
    def process_postback(self, data: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Send results via configured postback handlers."""
        if not self.postback_router:
            logger.warning("Postback router not available")
            return {}
        
        try:
            results = self.postback_router.route_data(data)
            logger.info(f"Postback processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Postback processing error: {e}")
            return {'error': False}
    
    def process_unified_workflow(self, df: pd.DataFrame, field_mapping: Dict[str, str], 
                                api_config: Dict[str, Any]) -> UnifiedProcessingResult:
        """
        Execute the complete unified workflow based on processing mode.
        
        Args:
            df: Input DataFrame
            field_mapping: Field mapping configuration
            api_config: FF2API configuration
            
        Returns:
            UnifiedProcessingResult with complete processing results
        """
        logger.info(f"Starting unified workflow in {self.processing_mode} mode")
        
        result = UnifiedProcessingResult(
            csv_data=df.to_dict('records'),
            ff2api_results=[],
            errors=[],
            processing_mode=self.processing_mode
        )
        
        try:
            # Step 1: Validate and map data
            mapped_df, validation_errors = self.validate_and_map_data(df, field_mapping)
            if validation_errors:
                result.errors.extend(validation_errors)
                if not self.mode_config.auto_process:
                    # In manual mode, stop on validation errors
                    return result
            
            # Step 2: Process through FF2API
            ff2api_results = self.process_ff2api(mapped_df, api_config)
            result.ff2api_results = ff2api_results
            
            # Count successful results
            successful_ff2api = [r for r in ff2api_results if r.get('success')]
            
            # Step 3: Load ID mapping (if end-to-end mode)
            if self.mode_config.show_enrichment or self.mode_config.show_postback:
                load_mappings = self.process_load_id_mapping(ff2api_results)
                result.load_id_mappings = load_mappings
                
                # Prepare data for enrichment (combine FF2API results with load mappings)
                enrichment_data = []
                for ff2_result in successful_ff2api:
                    data = ff2_result.get('data', {})
                    # Find corresponding load mapping
                    load_mapping = next((lm for lm in load_mappings 
                                       if lm.load_number == data.get('load_number')), None)
                    if load_mapping:
                        data['load_id'] = load_mapping.load_id
                    enrichment_data.append(data)
            else:
                enrichment_data = [r.get('data', {}) for r in successful_ff2api]
            
            # Step 4: Data enrichment (if enabled)
            if self.mode_config.show_enrichment and enrichment_data:
                enriched_data = self.process_enrichment(enrichment_data)
                result.enriched_data = enriched_data
            else:
                result.enriched_data = enrichment_data
            
            # Step 5: Postback processing (if enabled)
            if self.mode_config.show_postback and result.enriched_data:
                postback_results = self.process_postback(result.enriched_data)
                result.postback_results = postback_results
            
            # Generate summary
            result.summary = {
                'total_rows': len(df),
                'ff2api_success': len(successful_ff2api),
                'ff2api_errors': len(ff2api_results) - len(successful_ff2api),
                'validation_errors': len(validation_errors),
                'load_ids_retrieved': len(result.load_id_mappings) if result.load_id_mappings else 0,
                'rows_enriched': len(result.enriched_data) if result.enriched_data else 0,
                'postback_handlers': len(result.postback_results) if result.postback_results else 0
            }
            
            logger.info(f"Unified workflow completed successfully: {result.summary}")
            
        except Exception as e:
            logger.error(f"Unified workflow error: {e}")
            result.errors.append(f"Workflow error: {str(e)}")
        
        return result
    
    def get_suggested_field_mapping(self, csv_columns: List[str]) -> Dict[str, str]:
        """Get AI-suggested field mapping based on column names."""
        try:
            # Use existing data processor's suggestion logic
            return self.data_processor.suggest_field_mapping(csv_columns)
        except Exception as e:
            logger.error(f"Error generating field mapping suggestions: {e}")
            return {}
    
    def get_processing_mode_config(self) -> ProcessingMode:
        """Get current processing mode configuration."""
        return self.mode_config
    
    def switch_processing_mode(self, new_mode: str):
        """Switch to a different processing mode."""
        if new_mode in self.PROCESSING_MODES:
            self.processing_mode = new_mode
            self.mode_config = self.PROCESSING_MODES[new_mode]
            
            # Re-initialize end-to-end components if needed
            if self.mode_config.show_enrichment or self.mode_config.show_postback:
                self._initialize_endtoend_components()
            
            logger.info(f"Switched to {new_mode} processing mode")
        else:
            raise ValueError(f"Invalid processing mode: {new_mode}")