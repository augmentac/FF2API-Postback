#!/usr/bin/env python3
"""
Test the actual UI generate_sample_api_preview function to verify the fix works in the real application
"""

import sys
import os
import pandas as pd
import logging

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.dirname(__file__))

from src.backend.data_processor import DataProcessor
from src.backend.database import DatabaseManager
from src.frontend.ui_components import generate_sample_api_preview

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_real_ui_scenario():
    """Test a realistic UI scenario where user uploads CSV and maps carrier field"""
    
    print("=== Testing Real UI Scenario ===\n")
    
    # Initialize components
    data_processor = DataProcessor()
    db_manager = DatabaseManager()
    brokerage_name = "test_brokerage"
    
    # Simulate uploaded CSV data with carrier name
    uploaded_csv_data = {
        'LoadNum': ['LOAD123', 'LOAD124'],
        'CarrierName': ['Estes Express', 'YRC Freight'],
        'CustomerName': ['ABC Corp', 'XYZ Ltd'],
        'Mode': ['FTL', 'LTL'],
        'PickupCity': ['Atlanta', 'Chicago'],
        'PickupState': ['GA', 'IL'],
        'DeliveryCity': ['Miami', 'New York'],
        'DeliveryState': ['FL', 'NY']
    }
    
    df = pd.DataFrame(uploaded_csv_data)
    print(f"Uploaded CSV data (first 2 rows):\n{df.head(2)}\n")
    
    # Simulate user field mappings from UI
    field_mappings = {
        'load.loadNumber': 'LoadNum',
        'carrier.name': 'CarrierName',  # This triggers carrier auto-mapping
        'customer.name': 'CustomerName',
        'load.mode': 'MANUAL_VALUE:FTL',  # Manual value from dropdown
        'load.route.0.address.city': 'PickupCity',
        'load.route.0.address.stateOrProvince': 'PickupState',
        'load.route.1.address.city': 'DeliveryCity',
        'load.route.1.address.stateOrProvince': 'DeliveryState'
    }
    
    print(f"User field mappings:\n{field_mappings}\n")
    
    # Set up carrier mapping (this would be done once per brokerage)
    print("=== Setting up carrier auto-mapping ===")
    db_manager.set_carrier_mapping_config(brokerage_name, True)
    
    try:
        from carrier_config_parser import CARRIER_DETAILS
        
        # Add carrier mappings for both carriers
        for carrier_name, details in CARRIER_DETAILS.items():
            if carrier_name in ['Estes Express', 'YRC Freight']:
                carrier_mapping = {
                    'carrier.name': carrier_name,
                    'carrier.dotNumber': details.get('dotNumber', ''),
                    'carrier.mcNumber': details.get('mcNumber', ''),
                    'carrier.scac': details.get('scac', ''),
                    'carrier.email': details.get('email', ''),
                    'carrier.phone': details.get('phone', ''),
                    'carrier.contacts.0.name': 'Customer Service',
                    'carrier.contacts.0.email': details.get('email', ''),
                    'carrier.contacts.0.phone': details.get('phone', ''),
                    'carrier.contacts.0.role': 'DISPATCHER'
                }
                
                db_manager.save_carrier_mapping(brokerage_name, carrier_name, carrier_mapping)
                print(f"✅ Set up auto-mapping for {carrier_name}")
        
    except ImportError as e:
        print(f"❌ Could not import carrier details: {e}")
        return
    
    # Test the JSON API Preview generation (this is what happens in the UI)
    print("\n=== Testing JSON API Preview Generation ===")
    preview_result = generate_sample_api_preview(df, field_mappings, data_processor, db_manager, brokerage_name)
    
    print(f"Preview message: {preview_result.get('message', 'No message')}")
    
    if 'preview' in preview_result:
        print("\n📋 **Generated JSON API Preview:**")
        import json
        print(json.dumps(preview_result['preview'], indent=2))
        
        # Check carrier section specifically
        if 'carrier' in preview_result['preview']:
            carrier_section = preview_result['preview']['carrier']
            print(f"\n✅ **Carrier Auto-Mapping Results:**")
            print(f"  Name: {carrier_section.get('name', 'Missing')}")
            print(f"  DOT Number: {carrier_section.get('dotNumber', 'Missing')}")
            print(f"  MC Number: {carrier_section.get('mcNumber', 'Missing')}")
            print(f"  SCAC: {carrier_section.get('scac', 'Missing')}")
            print(f"  Email: {carrier_section.get('email', 'Missing')}")
            print(f"  Phone: {carrier_section.get('phone', 'Missing')}")
            
            if 'contacts' in carrier_section:
                contacts = carrier_section['contacts']
                if len(contacts) > 0:
                    contact = contacts[0]
                    print(f"  Contact Name: {contact.get('name', 'Missing')}")
                    print(f"  Contact Email: {contact.get('email', 'Missing')}")
                    print(f"  Contact Phone: {contact.get('phone', 'Missing')}")
                    print(f"  Contact Role: {contact.get('role', 'Missing')}")
            
            # Verify all expected fields are present
            expected_carrier_fields = ['name', 'dotNumber', 'mcNumber', 'scac', 'email', 'phone', 'contacts']
            missing_fields = [field for field in expected_carrier_fields if field not in carrier_section]
            
            if missing_fields:
                print(f"\n❌ Missing carrier fields: {missing_fields}")
            else:
                print(f"\n🎉 **SUCCESS: All carrier fields auto-populated!**")
        else:
            print(f"\n❌ No carrier section found in preview")
    else:
        print(f"\n❌ No preview generated")

if __name__ == "__main__":
    test_real_ui_scenario()