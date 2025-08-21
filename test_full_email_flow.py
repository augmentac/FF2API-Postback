#!/usr/bin/env python3
"""
Test the complete email processing flow to reproduce the error.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
import io

def test_complete_email_flow():
    """Test the complete email processing flow."""
    print("Testing complete email processing flow...")
    
    try:
        # Import email automation
        from src.frontend.email_automation import EmailAutomationManager
        print("✅ Imported EmailAutomationManager")
        
        # Create a sample CSV data
        sample_data = """
shipment_id,origin,destination,weight
SH001,New York,Los Angeles,1000
SH002,Chicago,Miami,2000
        """.strip()
        
        sample_bytes = sample_data.encode('utf-8')
        
        # Create the email automation manager
        manager = EmailAutomationManager("eshipping")
        print("✅ Created EmailAutomationManager for eshipping")
        
        # This is the call that should trigger the error based on the logs
        print("🔍 Calling process_email_attachment...")
        result = manager.process_email_attachment(sample_bytes, "test_file.csv")
        print(f"✅ process_email_attachment completed: {result}")
        
    except Exception as e:
        print(f"❌ Error during email processing flow: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_complete_email_flow()