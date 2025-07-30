#!/usr/bin/env python3
"""
Test script for enhanced tracking workflow with production endpoints.
Tests direct tracking path with provided PRO numbers.
"""

import os
import sys
import pandas as pd
import logging
from typing import Dict, Any, List
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Mock streamlit secrets for testing
class MockSecrets:
    def __init__(self):
        self.data = {
            "api": {
                "augment_brokerage": "augment-brokerage|YOUR_API_KEY_HERE"
            },
            "api_config": {
                "base_url": "https://api.prod.goaugment.com",
                "timeout": 30,
                "retry_count": 3,
                "retry_delay": 1
            }
        }
    
    def get(self, key, default=None):
        return self.data.get(key, default)

# Mock streamlit module
class MockStreamlit:
    def __init__(self):
        self.secrets = MockSecrets()
        self.session_state = {}

sys.modules['streamlit'] = MockStreamlit()

# Now import our modules
from workflow_processor import EndToEndWorkflowProcessor
from credential_manager import credential_manager

def create_test_csv_data() -> pd.DataFrame:
    """Create test CSV data with 5 sample loads and PRO numbers."""
    test_data = [
        {
            "load_number": "TEST001",
            "PRO": "0968391969",
            "carrier": "ESTES",
            "mode": "FTL",
            "rate_type": "SPOT",
            "status": "ACTIVE"
        },
        {
            "load_number": "TEST002", 
            "PRO": "1400266820",
            "carrier": "ESTES",
            "mode": "FTL",
            "rate_type": "SPOT",
            "status": "ACTIVE"
        },
        {
            "load_number": "TEST003",
            "PRO": "2121130165", 
            "carrier": "ESTES",
            "mode": "FTL",
            "rate_type": "SPOT",
            "status": "ACTIVE"
        },
        {
            "load_number": "TEST004",
            "PRO": "2121130168",
            "carrier": "ESTES", 
            "mode": "FTL",
            "rate_type": "SPOT",
            "status": "ACTIVE"
        },
        {
            "load_number": "TEST005",
            "PRO": "2121130170",
            "carrier": "ESTES",
            "mode": "FTL", 
            "rate_type": "SPOT",
            "status": "ACTIVE"
        }
    ]
    
    return pd.DataFrame(test_data)

def test_credential_validation():
    """Test credential validation for augment-brokerage."""
    logger.info("=== Testing Credential Validation ===")
    
    brokerage_key = "augment-brokerage"
    capabilities = credential_manager.validate_credentials(brokerage_key)
    
    logger.info(f"Brokerage: {capabilities.brokerage_key}")
    logger.info(f"API Available: {capabilities.api_available}")
    logger.info(f"Tracking API Available: {capabilities.tracking_api_available}")
    logger.info(f"Capabilities: {capabilities.capabilities}")
    
    return capabilities.api_available

def test_workflow_processor():
    """Test the complete enhanced workflow processor."""
    logger.info("=== Testing Enhanced Workflow Processor ===")
    
    # Create test configuration
    config = {
        'brokerage_key': 'augment-brokerage',
        'workflow_type': 'endtoend',
        'load_api_url': 'https://api.prod.goaugment.com',
        'enrichment': {
            'sources': [
                {
                    'type': 'tracking_api',
                    'config': {
                        'pro_column': 'PRO',
                        'carrier_column': 'carrier'
                    }
                }
            ]
        },
        'postback': {
            'handlers': [
                {
                    'type': 'csv',
                    'config': {
                        'output_path': './test_output/enhanced_workflow_results.csv'
                    }
                }
            ]
        }
    }
    
    # Initialize workflow processor
    processor = EndToEndWorkflowProcessor(config)
    
    # Create test CSV data
    csv_data = create_test_csv_data()
    logger.info(f"Created test CSV with {len(csv_data)} rows")
    
    # Process workflow with progress tracking
    def progress_callback(progress: float, message: str):
        logger.info(f"Progress: {progress:.1%} - {message}")
    
    try:
        results = processor.process_workflow(csv_data, progress_callback)
        
        logger.info("=== Workflow Results Summary ===")
        logger.info(f"Total CSV rows: {len(results.csv_data)}")
        logger.info(f"FF2API success: {len([r for r in results.ff2api_results if r.success])}/{len(results.ff2api_results)}")
        logger.info(f"Load IDs retrieved: {len([m for m in results.load_id_mappings if m.api_status == 'success'])}/{len(results.load_id_mappings)}")
        logger.info(f"Rows enriched: {len(results.enriched_data)}")
        logger.info(f"Errors: {len(results.errors)}")
        
        if results.errors:
            logger.error("Workflow errors:")
            for error in results.errors:
                logger.error(f"  - {error}")
        
        # Analyze workflow paths and PRO sources
        logger.info("=== Workflow Path Analysis ===")
        workflow_paths = {}
        pro_sources = {}
        
        for mapping in results.load_id_mappings:
            path = mapping.workflow_path or 'unknown'
            source = mapping.pro_source_type or 'unknown'
            
            workflow_paths[path] = workflow_paths.get(path, 0) + 1
            pro_sources[source] = pro_sources.get(source, 0) + 1
        
        logger.info(f"Workflow paths: {workflow_paths}")
        logger.info(f"PRO sources: {pro_sources}")
        
        # Check tracking enrichment results
        logger.info("=== Tracking Enrichment Analysis ===")
        tracking_success = 0
        tracking_total = 0
        
        for row in results.enriched_data:
            if 'PRO' in row and row['PRO']:
                tracking_total += 1
                if row.get('tracking_status'):
                    tracking_success += 1
        
        logger.info(f"Tracking enrichment: {tracking_success}/{tracking_total} successful")
        
        # Display sample enriched data
        if results.enriched_data:
            logger.info("=== Sample Enriched Data ===")
            sample_row = results.enriched_data[0]
            interesting_fields = [
                'load_number', 'PRO', 'carrier', 'workflow_path', 'pro_source_type', 
                'pro_confidence', 'tracking_status', 'tracking_location', 'agent_events_count'
            ]
            
            for field in interesting_fields:
                if field in sample_row:
                    logger.info(f"  {field}: {sample_row[field]}")
        
        return results
        
    except Exception as e:
        logger.error(f"Workflow processing failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Run the complete enhanced workflow test."""
    logger.info("Starting Enhanced Workflow End-to-End Test")
    logger.info("=" * 60)
    
    # Test 1: Validate credentials
    if not test_credential_validation():
        logger.error("Credential validation failed - cannot proceed")
        return False
    
    logger.info("")
    
    # Test 2: Run complete workflow
    results = test_workflow_processor()
    
    if results:
        logger.info("")
        logger.info("=== Test Completed Successfully ===")
        logger.info(f"Final summary: {results.summary}")
        return True
    else:
        logger.error("=== Test Failed ===")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)