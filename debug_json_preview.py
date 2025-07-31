#!/usr/bin/env python3
"""
Debug the JSON API Preview to understand why carrier fields are missing
"""

import sys
import os
import pandas as pd

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'frontend'))
sys.path.append(os.path.dirname(__file__))

from src.backend.database import DatabaseManager
from src.backend.data_processor import DataProcessor
from carrier_config_parser import carrier_config_parser

def debug_json_preview():
    """Debug the complete JSON preview generation process"""
    print("=== DEBUGGING JSON API PREVIEW ===\n")
    
    # 1. Setup test scenario matching user's data
    brokerage_name = "TestBrokerage"
    
    # Create test data that matches the user's scenario
    test_df = pd.DataFrame({
        'load_number': ['1008014522'],
        'carrier_name': ['Estes Express'],  # This gets mapped to carrier.name
        'pickup_city': ['Nampa'],
        'delivery_city': ['Fairgrove'],
        'mode': ['LTL'],
        'rate_type': ['CONTRACT'],
        'status': ['IN_TRANSIT'],
        'pro_number': ['2221294463']
    })
    
    print("1. INITIAL TEST DATA")
    print("-" * 50)
    print(f"DataFrame shape: {test_df.shape}")
    print(f"Columns: {list(test_df.columns)}")
    print(f"Sample row: {test_df.iloc[0].to_dict()}")
    
    # 2. Setup database and carrier mappings
    print(f"\n2. SETTING UP CARRIER MAPPINGS")
    print("-" * 50)
    
    db_manager = DatabaseManager()
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    # Clear and re-import carrier template
    import sqlite3
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM brokerage_carrier_mappings WHERE brokerage_name = ?", (brokerage_name,))
    conn.commit()
    conn.close()
    
    template = carrier_config_parser.get_brokerage_template()
    db_manager.import_carrier_template(brokerage_name, template)
    print(f"Imported {len(template)} carriers")
    
    # Verify Estes mapping
    mappings = db_manager.get_carrier_mappings(brokerage_name)
    estes_mapping = None
    for carrier_id, mapping in mappings.items():
        if 'Estes' in mapping.get('carrier.name', ''):
            estes_mapping = mapping
            break
    
    if estes_mapping:
        print("✅ Estes Express mapping confirmed:")
        print(f"  carrier.name: {estes_mapping.get('carrier.name')}")
        print(f"  carrier.dotNumber: {estes_mapping.get('carrier.dotNumber')}")
        print(f"  carrier.mcNumber: {estes_mapping.get('carrier.mcNumber')}")
        print(f"  carrier.scac: {estes_mapping.get('carrier.scac')}")
        print(f"  carrier.contacts.0.name: {estes_mapping.get('carrier.contacts.0.name')}")
        print(f"  carrier.contacts.0.role: {estes_mapping.get('carrier.contacts.0.role')}")
    
    # 3. Simulate field mapping (what happens in the UI)
    print(f"\n3. SIMULATING FIELD MAPPING")
    print("-" * 50)
    
    # Apply field mappings to create API field structure
    mapped_df = test_df.copy()
    
    # Map carrier_name to carrier.name (what field mapping does)
    mapped_df['carrier.name'] = mapped_df['carrier_name']
    
    print(f"After field mapping:")
    print(f"  Columns: {list(mapped_df.columns)}")
    print(f"  carrier.name value: {mapped_df['carrier.name'].iloc[0]}")
    
    # 4. Apply carrier auto-mapping
    print(f"\n4. TESTING CARRIER AUTO-MAPPING")
    print("-" * 50)
    
    data_processor = DataProcessor()
    auto_mapped_df = data_processor.apply_carrier_mapping(mapped_df, brokerage_name, db_manager)
    
    print(f"After auto-mapping:")
    print(f"  Columns: {list(auto_mapped_df.columns)}")
    
    # Check if carrier fields were added
    carrier_fields = [col for col in auto_mapped_df.columns if col.startswith('carrier.')]
    print(f"  Carrier fields: {carrier_fields}")
    
    if carrier_fields:
        print("  Carrier field values:")
        for field in carrier_fields:
            value = auto_mapped_df[field].iloc[0]
            print(f"    {field}: {value}")
    else:
        print("  ❌ No carrier fields were auto-mapped!")
        
        # Debug why auto-mapping failed
        print("\n  DEBUGGING AUTO-MAPPING FAILURE:")
        potential_carrier_columns = [
            'carrier_name', 'carrier', 'scac', 'carrier_scac', 
            'Carrier', 'Carrier Name', 'SCAC', 'Carrier SCAC',
            'carrier.name'
        ]
        
        found_columns = [col for col in potential_carrier_columns if col in auto_mapped_df.columns]
        print(f"    Available carrier columns: {found_columns}")
        
        for col in found_columns:
            value = auto_mapped_df[col].iloc[0]
            print(f"    {col}: '{value}'")
    
    # 5. Test JSON API formatting
    print(f"\n5. TESTING JSON API FORMATTING")
    print("-" * 50)
    
    try:
        formatted_payloads = data_processor.format_for_api(auto_mapped_df, preview_mode=True)
        
        if formatted_payloads:
            payload = formatted_payloads[0]
            
            print("Generated JSON structure:")
            print(f"  Top-level keys: {list(payload.keys())}")
            
            if 'carrier' in payload:
                carrier = payload['carrier']
                print(f"  Carrier keys: {list(carrier.keys())}")
                
                print("  Carrier content:")
                for key, value in carrier.items():
                    if key == 'contacts' and isinstance(value, list) and value:
                        print(f"    {key}: [")
                        for i, contact in enumerate(value):
                            print(f"      {i}: {contact}")
                        print("    ]")
                    else:
                        print(f"    {key}: {value}")
                        
                # Check what's missing
                expected_fields = ['name', 'dotNumber', 'mcNumber', 'scac', 'email', 'phone']
                missing_fields = [f for f in expected_fields if f not in carrier]
                if missing_fields:
                    print(f"  ❌ Missing carrier fields: {missing_fields}")
                else:
                    print("  ✅ All expected carrier fields present")
                    
            else:
                print("  ❌ No carrier object in payload!")
                
        else:
            print("  ❌ No formatted payload generated!")
            
    except Exception as e:
        print(f"  ❌ Error formatting JSON: {e}")
        import traceback
        traceback.print_exc()
    
    return True

if __name__ == "__main__":
    debug_json_preview()