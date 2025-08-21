#!/usr/bin/env python3
"""
Test email automation import and method call to identify the source of the error.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_email_automation_import():
    """Test importing and using email automation components."""
    print("Testing email automation import...")
    
    try:
        from src.frontend.email_automation import EmailAutomationManager
        print("✅ EmailAutomationManager imported successfully")
        
        # Test creating an instance
        manager = EmailAutomationManager("test_brokerage")
        print("✅ EmailAutomationManager instance created successfully")
        
        # Test getting field mappings (this is where the error was occurring)
        try:
            field_mappings = manager._get_saved_field_mappings()
            print(f"✅ _get_saved_field_mappings() called successfully, returned {len(field_mappings)} mappings")
        except Exception as e:
            print(f"❌ Error calling _get_saved_field_mappings(): {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"❌ Error importing email automation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_email_automation_import()