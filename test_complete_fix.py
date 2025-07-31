#!/usr/bin/env python3
"""
Test the complete fix for carrier auto-mapping and JSON API preview.
This test simulates the full workflow to ensure both issues are resolved.
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
from src.frontend.ui_components import get_effective_required_fields, _will_carrier_auto_mapping_provide_dot_mc, get_full_api_schema
from carrier_config_parser import carrier_config_parser

def test_complete_fix():
    """Test the complete fix for both issues"""
    print("=== COMPLETE FIX VERIFICATION ===\n")
    
    # 1. Setup test data
    brokerage_name = "TestBrokerage"
    
    # Sample data with Estes Express
    sample_df = pd.DataFrame({
        'load_number': ['LOAD001'],
        'carrier_name': ['Estes Express'],
        'pickup_city': ['Nampa'],
        'delivery_city': ['Fairgrove'],
        'mode': ['LTL'],
        'rate_type': ['CONTRACT'],
        'status': ['IN_TRANSIT'],
        'pro_number': ['2221294463']
    })
    
    print("1. TESTING CARRIER AUTO-MAPPING DATA IMPORT")
    print("-" * 50)
    
    # Setup database with correct auto-mapping
    db_manager = DatabaseManager()
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    # Clear existing mappings first
    import sqlite3
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM brokerage_carrier_mappings WHERE brokerage_name = ?", (brokerage_name,))
    conn.commit()
    conn.close()
    
    # Import carrier template with corrected API field names
    template = carrier_config_parser.get_brokerage_template()
    db_manager.import_carrier_template(brokerage_name, template)
    print(f"Imported {len(template)} carriers to database")
    
    # Get the carrier mappings
    carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
    estes_mapping = None
    
    print(f"Found {len(carrier_mappings)} total mappings")
    print("Available carriers:")
    for carrier_id, mapping in carrier_mappings.items():
        carrier_name = mapping.get('carrier.name', 'Unknown')
        print(f"  - {carrier_id}: {carrier_name}")
        if 'Estes' in carrier_name:
            estes_mapping = mapping
            break
    
    if estes_mapping:
        print("✅ Estes Express mapping found with correct API field format:")
        for field, value in estes_mapping.items():
            if value:
                print(f"  {field}: {value}")
    else:
        print("❌ Estes Express mapping not found")
        return False
    
    # 2. Test field requirement detection
    print(f"\n2. TESTING FIELD REQUIREMENT DETECTION")
    print("-" * 50)
    
    # Field mappings - user maps carrier name
    field_mappings = {
        'carrier.name': 'carrier_name'
    }
    
    # Check auto-mapping detection
    will_auto_provide = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
    print(f"Auto-mapping will provide DOT/MC: {will_auto_provide}")
    
    # Get effective required fields
    api_schema = get_full_api_schema()
    required_fields = get_effective_required_fields(api_schema, field_mappings, brokerage_name)
    
    dot_required = 'carrier.dotNumber' in required_fields
    mc_required = 'carrier.mcNumber' in required_fields
    
    print(f"carrier.dotNumber required: {dot_required}")
    print(f"carrier.mcNumber required: {mc_required}")
    
    requirement_test_pass = will_auto_provide and not dot_required and not mc_required
    print(f"✅ Field requirement test: {'PASSED' if requirement_test_pass else 'FAILED'}")
    
    # 3. Test data processing with auto-mapping
    print(f"\n3. TESTING DATA PROCESSING WITH AUTO-MAPPING")
    print("-" * 50)
    
    data_processor = DataProcessor()
    
    # Apply carrier mapping
    mapped_df = data_processor.apply_carrier_mapping(sample_df, brokerage_name, db_manager)
    
    print("Mapped DataFrame columns:")
    carrier_columns = [col for col in mapped_df.columns if col.startswith('carrier.')]
    for col in carrier_columns:
        value = mapped_df[col].iloc[0] if not mapped_df.empty else 'N/A'
        print(f"  {col}: {value}")
    
    # Check if critical carrier fields are present
    has_dot = 'carrier.dotNumber' in mapped_df.columns and not pd.isna(mapped_df['carrier.dotNumber'].iloc[0])
    has_mc = 'carrier.mcNumber' in mapped_df.columns and not pd.isna(mapped_df['carrier.mcNumber'].iloc[0])
    has_scac = 'carrier.scac' in mapped_df.columns and not pd.isna(mapped_df['carrier.scac'].iloc[0])
    has_contact_role = 'carrier.contacts.0.role' in mapped_df.columns and not pd.isna(mapped_df['carrier.contacts.0.role'].iloc[0])
    
    data_processing_test_pass = has_dot and has_mc and has_scac and has_contact_role
    print(f"✅ Data processing test: {'PASSED' if data_processing_test_pass else 'FAILED'}")
    
    if has_dot:
        print(f"  ✅ DOT Number: {mapped_df['carrier.dotNumber'].iloc[0]}")
    if has_mc:
        print(f"  ✅ MC Number: {mapped_df['carrier.mcNumber'].iloc[0]}")
    if has_scac:
        print(f"  ✅ SCAC: {mapped_df['carrier.scac'].iloc[0]}")
    if has_contact_role:
        print(f"  ✅ Contact Role: {mapped_df['carrier.contacts.0.role'].iloc[0]}")
    
    # 4. Test JSON API formatting
    print(f"\n4. TESTING JSON API FORMATTING")
    print("-" * 50)
    
    # Simulate complete field mappings
    complete_field_mappings = {
        'load.loadNumber': 'load_number',
        'load.mode': 'mode',
        'load.rateType': 'rate_type',
        'load.status': 'status',
        'carrier.name': 'carrier_name',
        'load.referenceNumbers.0.value': 'pro_number',
        # Auto-mapped fields will be added by carrier mapping
    }
    
    try:
        # Apply field mappings to create a properly formatted row
        test_row = mapped_df.iloc[0].to_dict()
        
        # Format for API (pass a DataFrame with one row as expected by the method)
        test_df = pd.DataFrame([test_row])
        formatted_payloads = data_processor.format_for_api(test_df, preview_mode=True)
        
        if formatted_payloads:
            formatted_payload = formatted_payloads[0]  # Get first payload
            print("JSON API Preview structure:")
        else:
            print("❌ No formatted payload returned")
            json_test_pass = False
            formatted_payload = {}
        
        # Check carrier structure
        if formatted_payloads and 'carrier' in formatted_payload:
            carrier = formatted_payload['carrier']
            print(f"✅ Carrier object present")
            
            expected_fields = ['name', 'dotNumber', 'mcNumber', 'scac']
            missing_fields = [f for f in expected_fields if f not in carrier or not carrier[f]]
            
            if not missing_fields:
                print(f"  ✅ All carrier fields present:")
                for field in expected_fields:
                    print(f"    {field}: {carrier[field]}")
                    
                # Check contacts structure
                if 'contacts' in carrier and len(carrier['contacts']) > 0:
                    contact = carrier['contacts'][0]
                    if 'role' in contact:
                        print(f"    contacts[0].role: {contact['role']}")
                        json_test_pass = True
                    else:
                        print("  ❌ Contact role missing")
                        json_test_pass = False
                else:
                    print("  ❌ Contacts structure missing")
                    json_test_pass = False
            else:
                print(f"  ❌ Missing carrier fields: {missing_fields}")
                json_test_pass = False
        else:
            print("❌ Carrier object missing from JSON")
            json_test_pass = False
            
        print(f"✅ JSON API formatting test: {'PASSED' if json_test_pass else 'FAILED'}")
        
    except Exception as e:
        print(f"❌ Error testing JSON formatting: {e}")
        json_test_pass = False
    
    # 5. Overall result
    print(f"\n{'='*50}")
    print("COMPLETE FIX VERIFICATION RESULT:")
    
    overall_success = requirement_test_pass and data_processing_test_pass and json_test_pass
    
    if overall_success:
        print("✅ ALL TESTS PASSED")
        print("\n🎯 ISSUES RESOLVED:")
        print("  ✅ Issue 1: DOT Number no longer required when auto-mapping enabled")
        print("  ✅ Issue 2: JSON API Preview includes auto-populated carrier fields")
        print("\n🚀 EXPECTED USER EXPERIENCE:")
        print("  1. User uploads CSV with Estes Express carrier")
        print("  2. User maps carrier_name column to carrier.name field")
        print("  3. UI detects auto-mapping and removes DOT/MC requirements")
        print("  4. JSON Preview shows complete carrier object with:")
        print("     • carrier.name: 'Estes Express'")
        print("     • carrier.dotNumber: 205764")
        print("     • carrier.mcNumber: 105764")
        print("     • carrier.scac: 'EXLA'")
        print("     • carrier.contacts[0].role: 'DISPATCHER'")
    else:
        print("❌ SOME TESTS FAILED")
        print(f"  - Field requirement detection: {'PASS' if requirement_test_pass else 'FAIL'}")
        print(f"  - Data processing with auto-mapping: {'PASS' if data_processing_test_pass else 'FAIL'}")
        print(f"  - JSON API formatting: {'PASS' if json_test_pass else 'FAIL'}")
    
    return overall_success

if __name__ == "__main__":
    success = test_complete_fix()
    exit(0 if success else 1)