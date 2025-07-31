#!/usr/bin/env python3
"""
Debug the generate_sample_api_preview function specifically to see why carrier mapping is not applied
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

# Configure logging to capture all levels
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_preview_function():
    """Debug the generate_sample_api_preview function step by step"""
    
    print("=== Debug: generate_sample_api_preview Function ===\n")
    
    # Initialize components
    data_processor = DataProcessor()
    db_manager = DatabaseManager()
    brokerage_name = "test_brokerage"
    
    # Create test data
    test_data = {
        'load_number': ['TEST001'],
        'carrier_name': ['Estes Express'],
        'load_mode': ['FTL'],
        'customer_name': ['Test Customer']
    }
    
    df = pd.DataFrame(test_data)
    print(f"Input DataFrame:\n{df}\n")
    
    # Field mappings that will be used
    field_mappings = {
        'load.loadNumber': 'load_number',
        'carrier.name': 'carrier_name',
        'load.mode': 'MANUAL_VALUE:FTL',
        'customer.name': 'customer_name'
    }
    
    print(f"Field Mappings:\n{field_mappings}\n")
    
    # Set up carrier mapping configuration
    print("=== Setting up carrier mapping ===")
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    try:
        from carrier_config_parser import CARRIER_DETAILS
        
        # Add Estes Express mapping
        estes_details = CARRIER_DETAILS['Estes Express']
        carrier_mapping = {
            'carrier.name': 'Estes Express',
            'carrier.dotNumber': estes_details.get('dotNumber', ''),
            'carrier.mcNumber': estes_details.get('mcNumber', ''),
            'carrier.scac': estes_details.get('scac', ''),
            'carrier.email': estes_details.get('email', ''),
            'carrier.phone': estes_details.get('phone', ''),
            'carrier.contacts.0.name': 'Customer Service',
            'carrier.contacts.0.email': estes_details.get('email', ''),
            'carrier.contacts.0.phone': estes_details.get('phone', ''),
            'carrier.contacts.0.role': 'DISPATCHER'
        }
        
        db_manager.save_carrier_mapping(brokerage_name, 'Estes Express', carrier_mapping)
        print("✅ Saved Estes Express mapping to database")
        
    except ImportError as e:
        print(f"❌ Could not import carrier details: {e}")
        return
    
    # Now manually step through the generate_sample_api_preview function
    print("\n=== Manual step-through of generate_sample_api_preview ===")
    
    # Check if mappings exist
    if not field_mappings:
        print("❌ No field mappings")
        return
    
    # Check if DataFrame is empty
    if df.empty:
        print("❌ DataFrame is empty")
        return
    
    print("✅ Initial checks passed")
    
    # Get the first row for preview
    first_row_df = df.head(1).copy()
    print(f"First row DataFrame:\n{first_row_df}\n")
    
    # Step 1: Apply field mappings (with preview_mode=True)
    print("Step 1: Applying field mappings...")
    mapped_df, mapping_errors = data_processor.apply_mapping(first_row_df, field_mappings, preview_mode=True)
    print(f"After apply_mapping:")
    print(f"  Columns: {mapped_df.columns.tolist()}")
    print(f"  Data:\n{mapped_df}\n")
    print(f"  Mapping errors: {mapping_errors}")
    
    # Step 2: Apply carrier auto-mapping (key step!)
    print("Step 2: Applying carrier auto-mapping...")
    print(f"  brokerage_name: {brokerage_name}")
    print(f"  db_manager: {db_manager}")
    
    if db_manager and brokerage_name:
        print("  ✅ Database manager and brokerage name provided")
        
        try:
            # Capture original carrier fields for comparison
            original_carrier_fields = {}
            for col in mapped_df.columns:
                if col.startswith('carrier.'):
                    original_carrier_fields[col] = mapped_df[col].iloc[0] if not mapped_df[col].empty else None
            
            print(f"  Original carrier fields: {original_carrier_fields}")
            
            # Apply carrier auto-mapping
            print("  Calling apply_carrier_mapping...")
            mapped_df = data_processor.apply_carrier_mapping(mapped_df, brokerage_name, db_manager)
            
            print(f"  After apply_carrier_mapping:")
            print(f"    Columns: {mapped_df.columns.tolist()}")
            print(f"    Data:\n{mapped_df}\n")
            
            # Check if any carrier fields were auto-populated
            carrier_auto_mapped = False
            new_carrier_fields = {}
            for col in mapped_df.columns:
                if col.startswith('carrier.'):
                    new_value = mapped_df[col].iloc[0] if not mapped_df[col].empty else None
                    new_carrier_fields[col] = new_value
                    original_value = original_carrier_fields.get(col)
                    if new_value and new_value != original_value:
                        carrier_auto_mapped = True
                        print(f"    ✅ Auto-populated: {col} = {new_value}")
            
            print(f"  New carrier fields: {new_carrier_fields}")
            print(f"  Carrier auto-mapped: {carrier_auto_mapped}")
            
        except Exception as e:
            print(f"  ❌ Carrier auto-mapping failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("  ❌ Missing database manager or brokerage name")
    
    # Step 3: Format for API (with preview_mode=True)
    print("Step 3: Formatting for API...")
    api_preview_list = data_processor.format_for_api(mapped_df, preview_mode=True)
    
    if api_preview_list:
        api_preview = api_preview_list[0]
        print(f"  API preview generated successfully")
        print(f"  Top-level keys: {list(api_preview.keys())}")
        
        if 'carrier' in api_preview:
            print(f"  Carrier section:")
            import json
            print(json.dumps(api_preview['carrier'], indent=4))
        else:
            print(f"  ❌ No carrier section found")
    else:
        print(f"  ❌ No API preview generated")
    
    # Test the actual function
    print("\n=== Testing actual generate_sample_api_preview function ===")
    result = generate_sample_api_preview(df, field_mappings, data_processor, db_manager, brokerage_name)
    
    print(f"Result message: {result.get('message', 'No message')}")
    
    if 'preview' in result and 'carrier' in result['preview']:
        print("Carrier section in result:")
        import json
        print(json.dumps(result['preview']['carrier'], indent=2))
    else:
        print("❌ No carrier section in result")
        if 'preview' in result:
            print(f"Available sections: {list(result['preview'].keys())}")

if __name__ == "__main__":
    debug_preview_function()