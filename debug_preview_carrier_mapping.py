#!/usr/bin/env python3
"""
Debug script to investigate why JSON API Preview is missing auto-populated carrier fields
"""

import sys
import os
import pandas as pd
import logging

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.dirname(__file__))

from src.backend.data_processor import DataProcessor
from src.backend.database import DatabaseManager
from src.frontend.ui_components import generate_sample_api_preview

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_carrier_auto_mapping_in_preview():
    """Test if carrier auto-mapping is working correctly in the API preview"""
    
    print("=== Testing Carrier Auto-Mapping in JSON API Preview ===\n")
    
    # Initialize components
    data_processor = DataProcessor()
    db_manager = DatabaseManager()
    brokerage_name = "test_brokerage"
    
    # Create test data with Estes Express carrier
    test_data = {
        'load_number': ['TEST001'],
        'carrier_name': ['Estes Express'],  # This should trigger auto-mapping
        'load_mode': ['FTL'],
        'customer_name': ['Test Customer'],
        'pickup_city': ['Atlanta'],
        'pickup_state': ['GA'],
        'delivery_city': ['Miami'],
        'delivery_state': ['FL']
    }
    
    df = pd.DataFrame(test_data)
    print(f"Test DataFrame:\n{df}\n")
    
    # Create field mappings that include carrier name
    field_mappings = {
        'load.loadNumber': 'load_number',
        'carrier.name': 'carrier_name',
        'load.mode': 'MANUAL_VALUE:FTL',
        'customer.name': 'customer_name',
        'load.route.0.address.city': 'pickup_city',
        'load.route.0.address.stateOrProvince': 'pickup_state',
        'load.route.1.address.city': 'delivery_city',
        'load.route.1.address.stateOrProvince': 'delivery_state'
    }
    
    print(f"Field Mappings:\n{field_mappings}\n")
    
    # Check carrier auto-mapping configuration
    print("=== Checking Carrier Auto-Mapping Configuration ===")
    config = db_manager.get_carrier_mapping_config(brokerage_name)
    print(f"Auto-mapping enabled: {config.get('enable_auto_carrier_mapping', False)}")
    
    if not config.get('enable_auto_carrier_mapping', False):
        print("Enabling auto-mapping for test...")
        db_manager.set_carrier_mapping_config(brokerage_name, True)
        
        # Import and populate carrier mappings
        try:
            from carrier_config_parser import CARRIER_DETAILS
            
            # Convert carrier details to the format expected by the database
            for carrier_name, details in CARRIER_DETAILS.items():
                carrier_mapping = {
                    'carrier.name': carrier_name,
                    'carrier.dotNumber': details.get('dotNumber', ''),
                    'carrier.mcNumber': details.get('mcNumber', ''),
                    'carrier.scac': details.get('scac', ''),
                    'carrier.email': details.get('email', ''),
                    'carrier.phone': details.get('phone', ''),
                    'carrier.contacts.0.name': 'Customer Service',
                    'carrier.contacts.0.email': details.get('email', ''),
                    'carrier.contacts.0.phone': details.get('phone', ''),
                    'carrier.contacts.0.role': 'DISPATCHER'
                }
                
                db_manager.save_carrier_mapping(brokerage_name, carrier_name, carrier_mapping)
            
            print("Carrier mappings populated in database")
            
        except ImportError as e:
            print(f"Could not import carrier details: {e}")
    
    # Check if Estes Express mapping exists
    carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
    if 'Estes Express' in carrier_mappings:
        print("✅ Estes Express mapping found in database:")
        estes_mapping = carrier_mappings['Estes Express']
        for key, value in estes_mapping.items():
            print(f"  {key}: {value}")
    else:
        print("❌ Estes Express mapping NOT found in database")
    
    print("\n=== Testing Data Processing Steps ===")
    
    # Step 1: Apply field mappings
    print("Step 1: Applying field mappings...")
    mapped_df, mapping_errors = data_processor.apply_mapping(df, field_mappings, preview_mode=True)
    print(f"Mapped DataFrame columns: {list(mapped_df.columns)}")
    if len(mapped_df) > 0:
        print(f"First row after mapping:\n{mapped_df.iloc[0].to_dict()}\n")
    
    # Step 2: Apply carrier auto-mapping
    print("Step 2: Applying carrier auto-mapping...")
    carrier_mapped_df = data_processor.apply_carrier_mapping(mapped_df, brokerage_name, db_manager)
    print(f"Carrier-mapped DataFrame columns: {list(carrier_mapped_df.columns)}")
    if len(carrier_mapped_df) > 0:
        print("First row after carrier mapping:")
        first_row = carrier_mapped_df.iloc[0].to_dict()
        for key, value in first_row.items():
            if 'carrier' in key.lower():
                print(f"  {key}: {value}")
    
    # Step 3: Format for API
    print("\nStep 3: Formatting for API...")
    api_payloads = data_processor.format_for_api(carrier_mapped_df, preview_mode=True)
    
    if api_payloads:
        api_payload = api_payloads[0]
        print("Generated API payload:")
        
        if 'carrier' in api_payload:
            print("Carrier section in API payload:")
            import json
            print(json.dumps(api_payload['carrier'], indent=2))
        else:
            print("❌ No carrier section found in API payload")
            print(f"API payload keys: {list(api_payload.keys())}")
    
    # Step 4: Test the generate_sample_api_preview function directly
    print("\n=== Testing generate_sample_api_preview Function ===")
    preview_result = generate_sample_api_preview(df, field_mappings, data_processor, db_manager, brokerage_name)
    
    print("Preview result message:", preview_result.get('message', 'No message'))
    
    if 'preview' in preview_result and 'carrier' in preview_result['preview']:
        print("Carrier section in preview:")
        import json
        print(json.dumps(preview_result['preview']['carrier'], indent=2))
    else:
        print("❌ No carrier section found in preview")
        if 'preview' in preview_result:
            print(f"Preview keys: {list(preview_result['preview'].keys())}")
    
    print("\n=== Investigation Complete ===")

if __name__ == "__main__":
    test_carrier_auto_mapping_in_preview()