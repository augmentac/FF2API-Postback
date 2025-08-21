#!/usr/bin/env python3
"""
Test importing email_monitor to see if we can reproduce the error.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_email_monitor():
    """Test importing and using email monitor."""
    print("Testing email_monitor import...")
    
    try:
        # Import the email monitor
        import email_monitor
        print("✅ email_monitor imported successfully")
        
        # Try to create the service (this would use credential_manager)
        try:
            from credential_manager import credential_manager
            service = email_monitor.EmailMonitorService(credential_manager)
            print("✅ EmailMonitorService created successfully")
        except Exception as e:
            print(f"⚠️  Could not create EmailMonitorService (expected): {e}")
        
        # Check if there are any references to the old method in the imported module
        import inspect
        source = inspect.getsource(email_monitor)
        if 'get_all_brokerage_configurations' in source:
            print("❌ Found old method name in email_monitor source!")
            # Find the line
            lines = source.split('\n')
            for i, line in enumerate(lines):
                if 'get_all_brokerage_configurations' in line:
                    print(f"   Line {i+1}: {line.strip()}")
        else:
            print("✅ No old method name found in email_monitor source")
            
    except Exception as e:
        print(f"❌ Error testing email_monitor: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_email_monitor()