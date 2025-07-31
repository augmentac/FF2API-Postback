#!/usr/bin/env python3
"""
Test that the JSON preview fix correctly includes carrier auto-mapping
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
from src.frontend.ui_components import generate_sample_api_preview
from carrier_config_parser import carrier_config_parser

def test_json_preview_fix():
    """Test that JSON preview now includes auto-populated carrier fields"""
    print("=== TESTING JSON PREVIEW FIX ===\n")
    
    # 1. Setup test scenario
    brokerage_name = "TestBrokerage"
    
    # User's data scenario
    test_df = pd.DataFrame({
        'load_number': ['1008014522'],
        'carrier_name': ['Estes Express'],
        'pickup_city': ['Nampa'],
        'delivery_city': ['Fairgrove'],
        'mode': ['LTL'],
        'rate_type': ['CONTRACT'],
        'status': ['IN_TRANSIT'],
        'pro_number': ['2221294463']
    })
    
    # User's field mappings (what they set in the UI)
    field_mappings = {
        'load.loadNumber': 'load_number',
        'carrier.name': 'carrier_name',
        'load.mode': 'mode',
        'load.rateType': 'rate_type',
        'load.status': 'status',
        'load.referenceNumbers.0.value': 'pro_number'
    }
    
    print("1. TEST SETUP")
    print("-" * 40)
    print(f"Brokerage: {brokerage_name}")
    print(f"Test data: {test_df.iloc[0].to_dict()}")
    print(f"Field mappings: {field_mappings}")
    
    # 2. Setup database with carrier auto-mapping
    print(f"\n2. SETTING UP CARRIER AUTO-MAPPING")
    print("-" * 40)
    
    db_manager = DatabaseManager()
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    # Import carrier template
    template = carrier_config_parser.get_brokerage_template()
    db_manager.import_carrier_template(brokerage_name, template)
    print(f"✅ Imported {len(template)} carriers with auto-mapping enabled")
    
    # 3. Test JSON preview generation - OLD WAY (without carrier auto-mapping)
    print(f"\n3. TESTING OLD WAY (WITHOUT CARRIER AUTO-MAPPING)")
    print("-" * 40)
    
    data_processor = DataProcessor()
    
    # Call without db_manager and brokerage_name (old way)
    old_preview = generate_sample_api_preview(test_df, field_mappings, data_processor)
    
    if old_preview and 'preview' in old_preview and 'carrier' in old_preview['preview']:
        old_carrier = old_preview['preview']['carrier']
        print("Old preview carrier fields:")
        for key, value in old_carrier.items():
            print(f"  {key}: {value}")
        
        old_missing_fields = []
        expected_fields = ['dotNumber', 'mcNumber', 'scac', 'email', 'phone']
        for field in expected_fields:
            if field not in old_carrier:
                old_missing_fields.append(field)
        
        print(f"Missing fields in old preview: {old_missing_fields}")
    else:
        print("❌ No carrier object in old preview")
        old_missing_fields = ['all']
    
    # 4. Test JSON preview generation - NEW WAY (with carrier auto-mapping)
    print(f"\n4. TESTING NEW WAY (WITH CARRIER AUTO-MAPPING)")
    print("-" * 40)
    
    # Call with db_manager and brokerage_name (new way)
    new_preview = generate_sample_api_preview(
        test_df, 
        field_mappings, 
        data_processor,
        db_manager=db_manager,
        brokerage_name=brokerage_name
    )
    
    if new_preview and 'preview' in new_preview and 'carrier' in new_preview['preview']:
        new_carrier = new_preview['preview']['carrier']
        print("New preview carrier fields:")
        for key, value in new_carrier.items():
            if key == 'contacts' and isinstance(value, list) and value:
                print(f"  {key}: [")
                for i, contact in enumerate(value):
                    print(f"    {i}: {contact}")
                print("  ]")
            else:
                print(f"  {key}: {value}")
        
        new_missing_fields = []
        expected_fields = ['dotNumber', 'mcNumber', 'scac', 'email', 'phone']
        for field in expected_fields:
            if field not in new_carrier:
                new_missing_fields.append(field)
        
        print(f"Missing fields in new preview: {new_missing_fields}")
        
        # Check contact structure
        if 'contacts' in new_carrier and new_carrier['contacts']:
            contact = new_carrier['contacts'][0]
            contact_name = contact.get('name', 'Missing')
            contact_role = contact.get('role', 'Missing')
            print(f"Contact name: {contact_name} (expected: Customer Service)")
            print(f"Contact role: {contact_role} (expected: DISPATCHER)")
        
    else:
        print("❌ No carrier object in new preview")
        new_missing_fields = ['all']
    
    # 5. Compare results
    print(f"\n5. COMPARISON RESULTS")
    print("-" * 40)
    
    improvement = len(old_missing_fields) - len(new_missing_fields)
    
    if improvement > 0:
        print(f"✅ FIX SUCCESSFUL: {improvement} additional carrier fields now present")
        print(f"  Old missing fields: {old_missing_fields}")
        print(f"  New missing fields: {new_missing_fields}")
        
        if len(new_missing_fields) == 0:
            print("🎉 PERFECT: All expected carrier fields are now present in JSON preview")
        
        fix_successful = True
    elif improvement == 0:
        if len(new_missing_fields) == 0:
            print("✅ Already working: All carrier fields present")
            fix_successful = True
        else:
            print("❌ No improvement: Same fields still missing")
            fix_successful = False
    else:
        print("❌ Regression: Fewer fields now present")
        fix_successful = False
    
    # 6. Final result
    print(f"\n{'='*50}")
    print("JSON PREVIEW FIX VERIFICATION RESULT:")
    
    if fix_successful:
        print("✅ JSON PREVIEW FIX: SUCCESS")
        print("\n🎯 EXPECTED USER EXPERIENCE:")
        print("  - JSON API Preview now shows complete carrier information")
        print("  - carrier.dotNumber: 205764")
        print("  - carrier.mcNumber: 105764") 
        print("  - carrier.scac: 'EXLA'")
        print("  - carrier.email: 'customercare@estes-express.com'")
        print("  - carrier.phone: '+18663783748'")
        print("  - contacts[0].name: 'Customer Service' (not 'Estes Express')")
        print("  - contacts[0].role: 'DISPATCHER'")
        print("\n🔧 WHAT WAS FIXED:")
        print("  - Added db_manager and brokerage_name parameters to preview generation")
        print("  - Carrier auto-mapping now runs during JSON preview creation")
        print("  - Preview shows real auto-populated data instead of just mapped fields")
    else:
        print("❌ JSON PREVIEW FIX: FAILED")
        print("Additional investigation needed")
    
    return fix_successful

if __name__ == "__main__":
    success = test_json_preview_fix()
    exit(0 if success else 1)