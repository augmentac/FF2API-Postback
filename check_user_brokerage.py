#!/usr/bin/env python3
"""
Check user's actual brokerage configuration and diagnose carrier auto-mapping issues.
"""

import sys
import os
import sqlite3

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
from src.backend.database import DatabaseManager

def diagnose_user_carrier_mapping():
    """Diagnose the user's carrier mapping setup"""
    print("=== USER CARRIER AUTO-MAPPING DIAGNOSIS ===\n")
    
    db_manager = DatabaseManager()
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    
    # 1. Find user's actual brokerage names
    print("1. FINDING USER'S BROKERAGE CONFIGURATIONS")
    print("-" * 50)
    
    # Check brokerage_configurations table for actual user brokerages
    cursor.execute("SELECT DISTINCT brokerage_name FROM brokerage_configurations")
    user_brokerages = [row[0] for row in cursor.fetchall()]
    print(f"User brokerages in brokerage_configurations: {user_brokerages}")
    
    # Check upload_history for brokerages that have been used
    cursor.execute("SELECT DISTINCT brokerage_name FROM upload_history")
    upload_brokerages = [row[0] for row in cursor.fetchall()]
    print(f"Brokerages with upload history: {upload_brokerages}")
    
    # Check any other tables that might have brokerage names
    try:
        cursor.execute("SELECT DISTINCT brokerage_name FROM customer_mappings")
        mapping_brokerages = [row[0] for row in cursor.fetchall()]
        print(f"Brokerages with customer mappings: {mapping_brokerages}")
    except sqlite3.OperationalError:
        mapping_brokerages = []
        print("Brokerages with customer mappings: [] (table structure different)")
    
    # Combine all brokerages
    all_brokerages = list(set(user_brokerages + upload_brokerages + mapping_brokerages))
    print(f"\nAll discovered brokerages: {all_brokerages}")
    
    if not all_brokerages:
        print("⚠️  No user brokerages found! User may not have configured any brokerages yet.")
        return
    
    # 2. Check carrier auto-mapping configuration for each brokerage
    print(f"\n2. CHECKING CARRIER AUTO-MAPPING CONFIGURATION")
    print("-" * 50)
    
    for brokerage in all_brokerages:
        print(f"\nBrokerage: {brokerage}")
        
        # Check carrier config
        config = db_manager.get_carrier_mapping_config(brokerage)
        auto_mapping_enabled = config.get('enable_auto_carrier_mapping', False)
        print(f"  Auto-mapping enabled: {auto_mapping_enabled}")
        
        # Check carrier mappings
        carrier_mappings = db_manager.get_carrier_mappings(brokerage)
        mapping_count = len(carrier_mappings) if carrier_mappings else 0
        print(f"  Carrier mappings: {mapping_count}")
        
        if mapping_count > 0:
            # Check if Estes is in the mappings
            estes_mapping = None
            for carrier_id, mapping in carrier_mappings.items():
                if 'Estes' in mapping.get('carrier_name', ''):
                    estes_mapping = mapping
                    break
            
            if estes_mapping:
                print(f"  ✅ Estes mapping found: {estes_mapping.get('carrier_name')}")
            else:
                print("  ❌ No Estes mapping found")
                print("  Available carriers:")
                for carrier_id, mapping in list(carrier_mappings.items())[:5]:  # Show first 5
                    print(f"    - {mapping.get('carrier_name', 'Unknown')}")
                if mapping_count > 5:
                    print(f"    ... and {mapping_count - 5} more")
        
        # Diagnosis for this brokerage
        print(f"  DIAGNOSIS:")
        if not auto_mapping_enabled:
            print("    ❌ Auto-mapping is DISABLED")
            print("    🔧 FIX: Enable auto-mapping in the UI settings")
        elif mapping_count == 0:
            print("    ❌ No carrier mappings imported")
            print("    🔧 FIX: Import carrier template in the UI")
        elif mapping_count > 0 and not estes_mapping:
            print("    ❌ Carrier mappings exist but Estes is missing")
            print("    🔧 FIX: Re-import carrier template or add Estes manually")
        else:
            print("    ✅ Configuration looks correct")
            print("    🔍 Issue might be in column detection or data format")
    
    # 3. Provide step-by-step resolution
    print(f"\n3. STEP-BY-STEP RESOLUTION GUIDE")
    print("-" * 50)
    
    problematic_brokerage = None
    for brokerage in all_brokerages:
        config = db_manager.get_carrier_mapping_config(brokerage)
        carrier_mappings = db_manager.get_carrier_mappings(brokerage)
        
        if not config.get('enable_auto_carrier_mapping', False) or len(carrier_mappings or {}) == 0:
            problematic_brokerage = brokerage
            break
    
    if problematic_brokerage:
        print(f"Primary issue found with brokerage: {problematic_brokerage}")
        print(f"\nTo fix the carrier auto-mapping issue:")
        print(f"1. Go to the Streamlit UI")
        print(f"2. Navigate to 'Carrier Management' section")
        print(f"3. Select brokerage: {problematic_brokerage}")
        print(f"4. Click 'Enable Auto-Mapping' if not enabled")
        print(f"5. Click 'Import Carrier Template' to import all standard carriers")
        print(f"6. Verify that 'Estes Express' appears in the carrier list")
        print(f"7. Test with a CSV that has a column named 'carrier_name', 'carrier', or 'Carrier' containing 'Estes'")
    else:
        print("All brokerages appear to be correctly configured.")
        print("The issue might be:")
        print("1. CSV column naming - ensure carrier data is in columns named:")
        print("   'carrier_name', 'carrier', 'scac', 'carrier_scac', 'Carrier', 'Carrier Name', 'SCAC', 'Carrier SCAC'")
        print("2. CSV data format - ensure 'Estes' value is exactly as written (not 'Estes Express Lines' etc.)")
        print("3. Brokerage selection - ensure you're selecting the correct brokerage in the UI")
    
    conn.close()

if __name__ == "__main__":
    diagnose_user_carrier_mapping()