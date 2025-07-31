#!/usr/bin/env python3
"""
Deep debug script to investigate why apply_carrier_mapping is not working
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

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_carrier_mapping():
    """Debug the carrier mapping process step by step"""
    
    print("=== Deep Debug: Carrier Auto-Mapping Process ===\n")
    
    # Initialize components
    data_processor = DataProcessor()
    db_manager = DatabaseManager()
    brokerage_name = "test_brokerage"
    
    # Create test data
    test_data = {
        'carrier_name': ['Estes Express'],
        'other_col': ['test']
    }
    
    df = pd.DataFrame(test_data)
    print(f"Input DataFrame:\n{df}\n")
    
    # First, ensure auto-mapping is enabled and database is populated
    print("=== Setting up carrier mapping configuration ===")
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    try:
        from carrier_config_parser import CARRIER_DETAILS
        
        # Clear existing mappings first
        existing_mappings = db_manager.get_carrier_mappings(brokerage_name)
        for carrier_name in existing_mappings.keys():
            # Note: there might not be a delete method, so we'll just overwrite
            pass
        
        # Add just Estes Express for testing
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
    
    # Verify database state
    print("\n=== Verifying database state ===")
    config = db_manager.get_carrier_mapping_config(brokerage_name)
    print(f"Auto-mapping enabled: {config.get('enable_auto_carrier_mapping', False)}")
    
    carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
    print(f"Number of carrier mappings: {len(carrier_mappings)}")
    print(f"Carrier mapping keys: {list(carrier_mappings.keys())}")
    
    if 'Estes Express' in carrier_mappings:
        print("✅ Estes Express found in database")
        estes_data = carrier_mappings['Estes Express']
        print("Estes Express mapping data:")
        for key, value in estes_data.items():
            print(f"  {key}: {value}")
    else:
        print("❌ Estes Express NOT found in database")
        return
    
    # Now test the carrier mapping function with detailed logging
    print("\n=== Testing apply_carrier_mapping function ===")
    
    # Convert original dataframe to have proper column names
    mapped_df = df.copy()
    mapped_df['carrier.name'] = mapped_df['carrier_name']  # Simulate field mapping result
    print(f"Input to apply_carrier_mapping:\n{mapped_df}\n")
    
    # Manually step through the apply_carrier_mapping logic
    print("=== Manual step-through of apply_carrier_mapping logic ===")
    
    # Check if auto-mapping is enabled
    config = db_manager.get_carrier_mapping_config(brokerage_name)
    print(f"Config check - auto-mapping enabled: {config.get('enable_auto_carrier_mapping', False)}")
    
    if not config.get('enable_auto_carrier_mapping', False):
        print("❌ Auto-mapping is disabled, exiting")
        return
    
    # Get carrier mappings
    carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
    print(f"Mappings check - found {len(carrier_mappings)} mappings")
    
    if not carrier_mappings:
        print("❌ No carrier mappings found, exiting")
        return
    
    # Check carrier_config_parser import
    try:
        from carrier_config_parser import carrier_config_parser
        print("✅ carrier_config_parser imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import carrier_config_parser: {e}")
        return
    
    # Create a copy to avoid modifying original
    df_copy = mapped_df.copy()
    print(f"Working with DataFrame copy: {df_copy.columns.tolist()}")
    
    # Track auto-mapped carriers for logging
    auto_mapped_count = 0
    
    for index, row in df_copy.iterrows():
        print(f"\n--- Processing row {index} ---")
        carrier_match = None
        
        # Look for carrier identifier in various columns
        potential_carrier_columns = [
            'carrier_name', 'carrier', 'scac', 'carrier_scac', 
            'Carrier', 'Carrier Name', 'SCAC', 'Carrier SCAC',
            'carrier.name'  # Add this since we have mapped columns
        ]
        
        print(f"Looking for carrier in columns: {potential_carrier_columns}")
        print(f"Available columns: {df_copy.columns.tolist()}")
        
        for col in potential_carrier_columns:
            if col in df_copy.columns:
                value = row.get(col)
                print(f"Checking column '{col}': value = '{value}' (type: {type(value)})")
                
                if pd.notna(value):
                    carrier_value = str(value).strip()
                    print(f"Cleaned carrier value: '{carrier_value}'")
                    
                    if carrier_value:
                        # Use fuzzy matching to find best carrier match
                        print(f"Searching for '{carrier_value}' in mapping keys: {list(carrier_mappings.keys())}")
                        
                        carrier_match = carrier_config_parser.find_best_carrier_match(
                            carrier_value, 
                            list(carrier_mappings.keys())
                        )
                        print(f"Fuzzy match result: '{carrier_match}'")
                        
                        if carrier_match:
                            print(f"✅ Found carrier match: '{carrier_match}'")
                            break
                        else:
                            print(f"❌ No match found for '{carrier_value}'")
                else:
                    print(f"Column '{col}' has NaN value")
            else:
                print(f"Column '{col}' not found in DataFrame")
        
        # Apply carrier mapping if match found
        if carrier_match and carrier_match in carrier_mappings:
            print(f"\n✅ Applying mapping for carrier: {carrier_match}")
            carrier_data = carrier_mappings[carrier_match]
            
            # Apply all carrier fields to this row
            for api_field, value in carrier_data.items():
                if value:  # Only apply non-empty values
                    print(f"  Setting {api_field} = {value}")
                    df_copy.loc[index, api_field] = value
                else:
                    print(f"  Skipping empty field {api_field}")
            
            auto_mapped_count += 1
            print(f"✅ Auto-mapped carrier '{carrier_match}' for row {index}")
        else:
            print(f"❌ No carrier mapping applied for row {index}")
    
    print(f"\n=== Final Results ===")
    print(f"Auto-mapped {auto_mapped_count} rows")
    print(f"Final DataFrame columns: {df_copy.columns.tolist()}")
    print(f"Final DataFrame:\n{df_copy}")
    
    # Test with the actual function
    print("\n=== Testing actual apply_carrier_mapping function ===")
    result_df = data_processor.apply_carrier_mapping(mapped_df, brokerage_name, db_manager)
    print(f"Function result columns: {result_df.columns.tolist()}")
    print(f"Function result:\n{result_df}")

if __name__ == "__main__":
    debug_carrier_mapping()