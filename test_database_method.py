#!/usr/bin/env python3
"""
Quick test to verify the database method works correctly.
This will help diagnose why the error persists.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from backend.database import DatabaseManager

def test_database_method():
    """Test that the database method exists and works."""
    print("Testing database method availability...")
    
    try:
        db_manager = DatabaseManager()
        print("✅ DatabaseManager initialized successfully")
        
        # Test the method exists
        if hasattr(db_manager, 'get_brokerage_configurations'):
            print("✅ get_brokerage_configurations method exists")
        else:
            print("❌ get_brokerage_configurations method NOT found")
            
        if hasattr(db_manager, 'get_all_brokerage_configurations'):
            print("⚠️  OLD get_all_brokerage_configurations method still exists")
        else:
            print("✅ Old get_all_brokerage_configurations method correctly removed")
            
        # Test calling the method
        result = db_manager.get_brokerage_configurations("test_brokerage")
        print(f"✅ Method call successful, returned: {type(result)} with {len(result) if result else 0} items")
        
    except Exception as e:
        print(f"❌ Error testing database method: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database_method()