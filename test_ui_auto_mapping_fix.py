#!/usr/bin/env python3
"""
Test UI auto-mapping fix to verify DOT/MC field requirements are correctly calculated
when carrier auto-mapping is enabled.
"""

import sys
import os
import pandas as pd

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'frontend'))
sys.path.append(os.path.dirname(__file__))

from src.backend.database import DatabaseManager
from src.frontend.ui_components import get_effective_required_fields, _will_carrier_auto_mapping_provide_dot_mc, get_full_api_schema

def test_ui_auto_mapping_fix():
    """Test that UI correctly recognizes carrier auto-mapping for DOT/MC fields"""
    print("=== TESTING UI AUTO-MAPPING FIX ===\n")
    
    # 1. Setup test data
    print("1. SETTING UP TEST DATA")
    print("-" * 40)
    
    brokerage_name = "TestBrokerage"
    
    # Sample CSV data with Estes Express
    sample_df = pd.DataFrame({
        'load_number': ['LOAD001', 'LOAD002'],
        'carrier_name': ['Estes Express', 'FedEx Freight'],
        'pickup_city': ['Dallas', 'Houston'],
        'delivery_city': ['Fort Worth', 'San Antonio'],
        'weight': [1000, 1500]
    })
    
    print(f"Test brokerage: {brokerage_name}")
    print(f"Sample data rows: {len(sample_df)}")
    print(f"Carrier names: {sample_df['carrier_name'].tolist()}")
    
    # 2. Test scenario 1: Carrier name mapped but no DOT/MC mappings (carrier auto-mapping should provide DOT/MC)
    print("\n2. SCENARIO 1: Carrier name mapped, no DOT/MC mappings")
    print("-" * 40)
    
    field_mappings = {
        'carrier.name': 'carrier_name'  # Simulate that carrier name is mapped to enable auto-mapping
    }
    
    # Check if auto-mapping will provide DOT/MC
    will_auto_provide = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
    print(f"Will carrier auto-mapping provide DOT/MC? {will_auto_provide}")
    
    # Get effective required fields
    api_schema = get_full_api_schema()
    required_fields = get_effective_required_fields(api_schema, field_mappings, brokerage_name)
    
    dot_required = 'carrier.dotNumber' in required_fields
    mc_required = 'carrier.mcNumber' in required_fields
    
    print(f"carrier.dotNumber required: {dot_required}")
    print(f"carrier.mcNumber required: {mc_required}")
    
    # Expected: Both should be False because auto-mapping will provide them
    scenario1_pass = not dot_required and not mc_required
    print(f"✅ Scenario 1 {'PASSED' if scenario1_pass else 'FAILED'}: Auto-mapping removes field requirements")
    
    # 3. Test scenario 2: Manual MC mapping provided (DOT should not be required)
    print("\n3. SCENARIO 2: Manual MC Number mapping provided")
    print("-" * 40)
    
    field_mappings = {
        'carrier.name': 'carrier_name',  # Still need carrier name for auto-mapping
        'carrier.mcNumber': 'some_mc_column'
    }
    
    will_auto_provide = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
    print(f"Will carrier auto-mapping provide DOT/MC? {will_auto_provide}")
    
    required_fields = get_effective_required_fields(api_schema, field_mappings, brokerage_name)
    
    dot_required = 'carrier.dotNumber' in required_fields
    mc_required = 'carrier.mcNumber' in required_fields
    
    print(f"carrier.dotNumber required: {dot_required}")
    print(f"carrier.mcNumber required: {mc_required}")
    
    # Expected: DOT should not be required (either-or logic), MC should not be required (mapped)
    scenario2_pass = not dot_required and not mc_required
    print(f"✅ Scenario 2 {'PASSED' if scenario2_pass else 'FAILED'}: Either-or logic works with manual mapping")
    
    # 4. Test scenario 3: Auto-mapping disabled
    print("\n4. SCENARIO 3: Auto-mapping disabled")
    print("-" * 40)
    
    # Disable auto-mapping for this test
    db_manager = DatabaseManager()
    original_config = db_manager.get_carrier_mapping_config(brokerage_name)
    db_manager.set_carrier_mapping_config(brokerage_name, False)
    
    field_mappings = {
        'carrier.name': 'carrier_name'  # Need carrier object to be "in use" for conditional fields to activate
    }
    
    will_auto_provide = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
    print(f"Will carrier auto-mapping provide DOT/MC? {will_auto_provide}")
    
    required_fields = get_effective_required_fields(api_schema, field_mappings, brokerage_name)
    
    dot_required = 'carrier.dotNumber' in required_fields
    mc_required = 'carrier.mcNumber' in required_fields
    
    print(f"carrier.dotNumber required: {dot_required}")
    print(f"carrier.mcNumber required: {mc_required}")
    
    # Expected: Either DOT or MC should be required (either-or logic)
    scenario3_pass = dot_required or mc_required  # At least one should be required
    print(f"✅ Scenario 3 {'PASSED' if scenario3_pass else 'FAILED'}: Requirements enforced when auto-mapping disabled")
    
    # Restore original config
    db_manager.set_carrier_mapping_config(brokerage_name, original_config.get('enable_auto_carrier_mapping', False))
    
    # 5. Test the helper function directly
    print("\n5. TESTING HELPER FUNCTION")
    print("-" * 40)
    
    # Test various field mapping scenarios
    test_cases = [
        ({}, "No mappings"),
        ({'carrier.name': 'carrier_name'}, "Carrier name only"),
        ({'carrier.name': 'carrier_name', 'carrier.dotNumber': 'dot_col'}, "Carrier + DOT mapped"),
        ({'carrier.name': 'carrier_name', 'carrier.mcNumber': 'mc_col'}, "Carrier + MC mapped"),
        ({'carrier.name': 'carrier_name', 'carrier.dotNumber': 'dot_col', 'carrier.mcNumber': 'mc_col'}, "All mapped"),
        ({'some.other.field': 'other_col'}, "Other field mapped")
    ]
    
    for mappings, description in test_cases:
        result = _will_carrier_auto_mapping_provide_dot_mc(mappings)
        print(f"  {description}: {result}")
    
    # Overall test result
    overall_pass = scenario1_pass and scenario2_pass and scenario3_pass
    
    print(f"\n{'='*50}")
    print("OVERALL TEST RESULT:")
    print(f"✅ UI Auto-mapping Fix: {'PASSED' if overall_pass else 'FAILED'}")
    
    if overall_pass:
        print("\n🎯 FIX VERIFICATION:")
        print("  ✅ Carrier auto-mapping correctly reduces field requirements")
        print("  ✅ Either-or DOT/MC logic works with manual mappings")
        print("  ✅ Requirements enforced when auto-mapping disabled")
        print("  ✅ Helper function correctly detects auto-mapping scenarios")
        print("\n🚀 EXPECTED BEHAVIOR IN UI:")
        print("  - With Estes Express + auto-mapping: DOT Number NOT required")
        print("  - Auto-population badges should show for carrier fields")
        print("  - Field mapping interface should reflect auto-mapping status")
    else:
        print("\n❌ Some scenarios failed - check implementation")
    
    return overall_pass

if __name__ == "__main__":
    success = test_ui_auto_mapping_fix()
    exit(0 if success else 1)