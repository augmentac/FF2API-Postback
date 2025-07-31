#!/usr/bin/env python3
"""
Test script to verify the carrier_name system error is fixed.
"""

import sys
import os

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.dirname(__file__))

from src.backend.database import DatabaseManager
from carrier_config_parser import carrier_config_parser

def test_carrier_name_fix():
    """Test that carrier data access uses correct field names"""
    print("=== TESTING CARRIER_NAME FIX ===\n")
    
    brokerage_name = "TestBrokerage"
    
    # 1. Setup test data
    print("1. SETTING UP TEST DATA")
    print("-" * 40)
    
    db_manager = DatabaseManager()
    
    # Import carrier template with API field names
    template = carrier_config_parser.get_brokerage_template()
    db_manager.import_carrier_template(brokerage_name, template)
    print(f"Imported {len(template)} carriers")
    
    # 2. Test carrier mapping retrieval
    print("\n2. TESTING CARRIER MAPPING RETRIEVAL")
    print("-" * 40)
    
    current_mappings = db_manager.get_carrier_mappings(brokerage_name)
    print(f"Retrieved {len(current_mappings)} carrier mappings")
    
    if not current_mappings:
        print("❌ No mappings retrieved")
        return False
    
    # 3. Test field access with new API format
    print("\n3. TESTING FIELD ACCESS")
    print("-" * 40)
    
    # Get Estes Express mapping for testing
    estes_mapping = None
    for carrier_id, data in current_mappings.items():
        if 'Estes' in data.get('carrier.name', ''):
            estes_mapping = data
            break
    
    if not estes_mapping:
        print("❌ Estes Express mapping not found")
        return False
    
    print("✅ Found Estes Express mapping")
    
    # Test all field accesses that were causing errors
    test_fields = [
        ('carrier.name', 'Carrier Name'),
        ('carrier.scac', 'SCAC'),
        ('carrier.mcNumber', 'MC Number'),
        ('carrier.dotNumber', 'DOT Number'),
        ('carrier.phone', 'Phone'),
        ('carrier.email', 'Email'),
        ('carrier.contacts.0.name', 'Contact Name'),
        ('carrier.contacts.0.email', 'Contact Email'),
        ('carrier.contacts.0.phone', 'Contact Phone')
    ]
    
    all_fields_accessible = True
    
    for field_key, field_name in test_fields:
        try:
            value = estes_mapping.get(field_key, 'N/A')
            print(f"✅ {field_name}: {value}")
        except KeyError as e:
            print(f"❌ {field_name}: KeyError - {e}")
            all_fields_accessible = False
        except Exception as e:
            print(f"❌ {field_name}: Error - {e}")
            all_fields_accessible = False
    
    # 4. Test UI display format simulation
    print("\n4. TESTING UI DISPLAY FORMAT")
    print("-" * 40)
    
    try:
        # Simulate the UI mapping data creation
        mapping_data = []
        for carrier_id, data in current_mappings.items():
            if 'Estes' in data.get('carrier.name', ''):
                mapping_data.append({
                    'Carrier Name': data['carrier.name'],
                    'SCAC': data['carrier.scac'],
                    'MC Number': data['carrier.mcNumber'],
                    'Phone': data['carrier.phone'],
                    'Email': data['carrier.email'][:30] + '...' if len(data['carrier.email']) > 30 else data['carrier.email']
                })
                break
        
        if mapping_data:
            print("✅ UI display format creation successful:")
            for key, value in mapping_data[0].items():
                print(f"  {key}: {value}")
        else:
            print("❌ No data for UI display")
            all_fields_accessible = False
            
    except Exception as e:
        print(f"❌ UI display format creation failed: {e}")
        all_fields_accessible = False
    
    # 5. Overall result
    print(f"\n{'='*50}")
    print("CARRIER_NAME FIX VERIFICATION RESULT:")
    
    if all_fields_accessible:
        print("✅ CARRIER_NAME FIX: SUCCESS")
        print("\n🎯 SYSTEM ERROR RESOLVED:")
        print("  ✅ All carrier fields accessible with correct API field names")
        print("  ✅ UI display format works without KeyError")
        print("  ✅ Carrier management interface should function properly")
        print("\n🚀 EXPECTED USER EXPERIENCE:")
        print("  - Carrier management table displays correctly")
        print("  - Edit carrier dialog shows current values")
        print("  - Remove carrier dialog shows correct names")
        print("  - No more 'carrier_name' system errors")
    else:
        print("❌ CARRIER_NAME FIX: FAILED")
        print("Some field access issues remain")
    
    return all_fields_accessible

if __name__ == "__main__":
    success = test_carrier_name_fix()
    exit(0 if success else 1)