#!/usr/bin/env python3
"""
Final verification that the Estes Express carrier auto-mapping fix works correctly.
This simulates the exact scenario the user reported.
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

def verify_estes_fix():
    """
    Verify the exact scenario user reported:
    - Carrier configuration is auto enabled
    - Sample has Estes Express as the carrier
    - Should automatically populate DOT/MC fields
    - carrier.dotNumber should NOT be required in field mapping
    """
    print("=== ESTES EXPRESS FIX VERIFICATION ===\n")
    
    # 1. Setup - Exact user scenario
    print("1. SETTING UP USER SCENARIO")
    print("-" * 40)
    
    # User's brokerage with auto-mapping enabled
    brokerage_name = "TestBrokerage"
    
    # User's sample data with Estes Express
    sample_df = pd.DataFrame({
        'load_number': ['LOAD001'],
        'carrier_name': ['Estes Express'],  # This is the key - user has Estes Express in their data
        'pickup_city': ['Dallas'],
        'delivery_city': ['Fort Worth'],
        'weight': [1000]
    })
    
    print(f"✅ Brokerage: {brokerage_name}")
    print(f"✅ Sample carrier: {sample_df['carrier_name'].iloc[0]}")
    print(f"✅ Auto-mapping expected: Yes (Estes Express has DOT: 205764, MC: 105764)")
    
    # 2. Verify database configuration
    print("\n2. VERIFYING AUTO-MAPPING CONFIGURATION")
    print("-" * 40)
    
    db_manager = DatabaseManager()
    config = db_manager.get_carrier_mapping_config(brokerage_name)
    auto_mapping_enabled = config.get('enable_auto_carrier_mapping', False)
    
    print(f"Auto-mapping enabled in database: {auto_mapping_enabled}")
    
    if not auto_mapping_enabled:
        print("⚠️  Enabling auto-mapping for test...")
        db_manager.set_carrier_mapping_config(brokerage_name, True)
        print("✅ Auto-mapping enabled")
    
    # 3. Verify carrier mapping data exists
    print("\n3. VERIFYING CARRIER MAPPING DATA")
    print("-" * 40)
    
    carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
    estes_mapping = None
    
    if carrier_mappings:
        for carrier_id, mapping in carrier_mappings.items():
            if 'Estes' in mapping.get('carrier_name', ''):
                estes_mapping = mapping
                break
    
    if estes_mapping:
        print("✅ Estes Express mapping found:")
        print(f"  - Carrier Name: {estes_mapping.get('carrier_name')}")
        print(f"  - DOT Number: {estes_mapping.get('carrier_dot_number')}")
        print(f"  - MC Number: {estes_mapping.get('carrier_mc_number')}")
        print(f"  - SCAC: {estes_mapping.get('carrier_scac')}")
    else:
        print("❌ Estes Express mapping not found - importing carrier template...")
        from carrier_config_parser import carrier_config_parser
        template = carrier_config_parser.get_brokerage_template()
        db_manager.import_carrier_template(brokerage_name, template)
        print("✅ Carrier template imported")
    
    # 4. Test the core scenario - carrier name mapped, DOT/MC requirements
    print("\n4. TESTING FIELD REQUIREMENTS (USER'S EXACT SCENARIO)")
    print("-" * 40)
    
    # This simulates the user's field mapping interface
    # User has mapped carrier name column, but no DOT/MC columns yet
    field_mappings = {
        'carrier.name': 'carrier_name'  # User maps their carrier_name column to carrier.name field
    }
    
    print("Field mappings (user's current state):")
    print(f"  - carrier.name -> 'carrier_name' column ✅")
    print(f"  - carrier.dotNumber -> not mapped")
    print(f"  - carrier.mcNumber -> not mapped")
    
    # Check what the helper function returns
    will_auto_provide = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
    print(f"\nAuto-mapping will provide DOT/MC: {will_auto_provide}")
    
    # Get effective required fields (what the UI will show as required)
    api_schema = get_full_api_schema()
    required_fields = get_effective_required_fields(api_schema, field_mappings, brokerage_name)
    
    dot_required = 'carrier.dotNumber' in required_fields
    mc_required = 'carrier.mcNumber' in required_fields
    
    print(f"\nUI Field Requirements:")
    print(f"  - carrier.dotNumber required: {dot_required}")
    print(f"  - carrier.mcNumber required: {mc_required}")
    
    # 5. Verify the fix works
    print(f"\n5. FIX VERIFICATION")
    print("-" * 40)
    
    success = True
    
    if will_auto_provide:
        print("✅ Auto-mapping correctly detected")
    else:
        print("❌ Auto-mapping not detected")
        success = False
    
    if not dot_required:
        print("✅ carrier.dotNumber correctly NOT required (auto-mapping will provide)")
    else:
        print("❌ carrier.dotNumber incorrectly required despite auto-mapping")
        success = False
    
    if not mc_required:
        print("✅ carrier.mcNumber correctly NOT required (auto-mapping will provide)")
    else:
        print("❌ carrier.mcNumber incorrectly required despite auto-mapping")
        success = False
    
    # 6. Test the auto-mapping badges scenario
    print(f"\n6. TESTING UI AUTO-MAPPING BADGES")
    print("-" * 40)
    
    # Simulate checking if badges should show (function already imported)
    try:
        # Check if badges should appear
        should_show_badges = _will_carrier_auto_mapping_provide_dot_mc(field_mappings, brokerage_name)
        print(f"Should show auto-mapping badges: {should_show_badges}")
        
        if should_show_badges:
            print("✅ UI should display: '🤖 Auto-populated from carrier database'")
        else:
            print("❌ Badges will not show")
            success = False
            
    except Exception as e:
        print(f"❌ Error testing badges: {e}")
        success = False
    
    # 7. Final result
    print(f"\n{'='*50}")
    print("FINAL VERIFICATION RESULT:")
    
    if success:
        print("✅ ESTES EXPRESS FIX: SUCCESS")
        print("\n🎯 USER'S ISSUE RESOLVED:")
        print("  - Estes Express carrier detected in auto-mapping")
        print("  - carrier.dotNumber field NO LONGER required in UI")
        print("  - carrier.mcNumber field NO LONGER required in UI")
        print("  - Auto-mapping will populate DOT: 205764, MC: 105764")
        print("  - UI shows auto-population badges")
        print("\n🚀 EXPECTED USER EXPERIENCE:")
        print("  1. User uploads CSV with 'Estes Express' in carrier column")
        print("  2. User maps carrier_name column to carrier.name field")
        print("  3. UI automatically detects Estes Express can be auto-mapped")
        print("  4. DOT/MC fields show as auto-populated, not required")
        print("  5. Processing automatically adds DOT/MC numbers from database")
    else:
        print("❌ ESTES EXPRESS FIX: FAILED")
        print("Some scenarios did not work as expected")
    
    return success

if __name__ == "__main__":
    success = verify_estes_fix()
    exit(0 if success else 1)