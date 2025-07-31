#!/usr/bin/env python3
"""
Debug the carrier template import to understand why it's failing.
"""

import sys
import os

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.dirname(__file__))

from src.backend.database import DatabaseManager
from carrier_config_parser import carrier_config_parser

def debug_carrier_import():
    """Debug the carrier template import process"""
    print("=== CARRIER IMPORT DEBUG ===\n")
    
    brokerage_name = "TestBrokerage"
    
    print("1. TESTING CARRIER CONFIG PARSER")
    print("-" * 40)
    
    # Get the template
    template = carrier_config_parser.get_brokerage_template()
    print(f"Template contains {len(template)} carriers")
    
    # Check Estes specifically
    if 'Estes Express' in template:
        estes_data = template['Estes Express']
        print(f"\nEstes Express template data:")
        for key, value in estes_data.items():
            print(f"  {key}: {value}")
    else:
        print("❌ Estes Express not found in template")
        return False
    
    print(f"\n2. TESTING DATABASE IMPORT")
    print("-" * 40)
    
    db_manager = DatabaseManager()
    
    # Clear existing mappings
    import sqlite3
    conn = sqlite3.connect(db_manager.db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM brokerage_carrier_mappings WHERE brokerage_name = ?", (brokerage_name,))
    conn.commit()
    conn.close()
    
    print(f"Cleared existing mappings for {brokerage_name}")
    
    # Try to import just Estes
    estes_data = template['Estes Express']
    try:
        db_manager.save_carrier_mapping(brokerage_name, 'Estes Express', estes_data)
        print("✅ Successfully saved Estes Express mapping")
    except Exception as e:
        print(f"❌ Error saving Estes Express: {e}")
        return False
    
    print(f"\n3. TESTING RETRIEVAL")
    print("-" * 40)
    
    # Try to retrieve the mapping
    mappings = db_manager.get_carrier_mappings(brokerage_name)
    print(f"Retrieved {len(mappings)} mappings")
    
    if mappings:
        for carrier_id, mapping in mappings.items():
            print(f"\nCarrier ID: {carrier_id}")
            for field, value in mapping.items():
                print(f"  {field}: {value}")
    else:
        print("❌ No mappings retrieved")
        return False
    
    return True

if __name__ == "__main__":
    success = debug_carrier_import()
    exit(0 if success else 1)