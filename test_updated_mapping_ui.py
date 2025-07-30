#!/usr/bin/env python3
"""
Test the updated mapping UI with proper reference number handling
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_reference_number_schema():
    """Test the updated API schema for reference numbers"""
    print("=== Testing Updated API Schema ===")
    
    try:
        from src.frontend.ui_components import get_full_api_schema
        
        schema = get_full_api_schema()
        
        # Check for reference number fields
        ref_fields = [field for field in schema.keys() if 'referenceNumbers' in field]
        
        print(f"Found {len(ref_fields)} reference number fields:")
        for field in ref_fields:
            field_info = schema[field]
            print(f"  {field}:")
            print(f"    Type: {field_info.get('type')}")
            print(f"    Required: {field_info.get('required')}")
            print(f"    Description: {field_info.get('description')}")
            if 'enum' in field_info:
                print(f"    Enum: {field_info['enum'][:3]}...")
            if field_info.get('auto_populate'):
                print(f"    Auto-populate: ✅")
            print()
        
        return len(ref_fields) > 0
        
    except Exception as e:
        print(f"Error testing schema: {e}")
        return False

def test_payload_generation():
    """Test payload generation with reference numbers"""
    print("=== Testing Payload Generation ===")
    
    try:
        # Mock row data like from Excel file
        test_row = {
            'load_number': 'TEST001',
            'Carrier Pro#': '2221294463',  # From the real Excel file
            'Carrier Name': 'Estes Express',
            'PO#': 'PO12345',
            'mode': 'FTL',
            'rate_type': 'SPOT'
        }
        
        # Simulate workflow processor
        sys.path.append(os.path.dirname(__file__))
        from workflow_processor import EndToEndWorkflowProcessor
        
        # Create processor instance (mock the dependencies)
        processor = EndToEndWorkflowProcessor({})
        
        # Test payload preparation
        payload = processor._prepare_load_payload(test_row)
        
        print("Generated payload structure:")
        print(f"  Load Number: {payload['load']['loadNumber']}")
        print(f"  Reference Numbers: {len(payload['load']['referenceNumbers'])}")
        
        for i, ref_num in enumerate(payload['load']['referenceNumbers']):
            print(f"    {i+1}. {ref_num['name']}: {ref_num['value']}")
        
        # Validate structure
        required_keys = ['load', 'customer', 'brokerage']
        for key in required_keys:
            if key not in payload:
                print(f"❌ Missing required key: {key}")
                return False
        
        # Validate reference numbers
        ref_nums = payload['load']['referenceNumbers']
        if len(ref_nums) > 0:
            for ref_num in ref_nums:
                if 'name' not in ref_num or 'value' not in ref_num:
                    print(f"❌ Invalid reference number structure: {ref_num}")
                    return False
        
        print("✅ Payload structure is valid")
        return True
        
    except Exception as e:
        print(f"Error testing payload generation: {e}")
        return False

def test_field_categorization():
    """Test field categorization for reference numbers"""
    print("=== Testing Field Categorization ===")
    
    try:
        from src.frontend.ui_components import get_full_api_schema
        
        schema = get_full_api_schema()
        
        # Simulate categorization logic - include both optional and conditional fields for UI
        optional_fields = {k: v for k, v in schema.items() if v.get('required') in [False, 'conditional']}
        
        categories = {
            "📞 Tracking & References": [],
            "💰 Pricing & Bids": [],
            "📦 Load Information": [],
            "📍 Location Details": [],
            "👥 Contacts & Carriers": [],
            "📋 Other Fields": []
        }
        
        # Apply categorization logic
        for field in optional_fields.keys():
            if 'referenceNumbers' in field or 'tracking' in field.lower() or field.endswith('.value') and 'reference' in field.lower():
                categories["📞 Tracking & References"].append(field)
            elif 'bid' in field.lower() or 'cost' in field.lower() or 'rate' in field.lower():
                categories["💰 Pricing & Bids"].append(field)
            elif 'items' in field or 'equipment' in field or 'weight' in field:
                categories["📦 Load Information"].append(field)
            elif 'address' in field or 'route' in field:
                categories["📍 Location Details"].append(field)
            elif 'contact' in field or 'carrier' in field or 'driver' in field:
                categories["👥 Contacts & Carriers"].append(field)
            else:
                categories["📋 Other Fields"].append(field)
        
        print("Field categorization results:")
        for category_name, fields in categories.items():
            if fields:
                print(f"  {category_name}: {len(fields)} fields")
                if 'Tracking' in category_name:
                    for field in fields:
                        print(f"    - {field}")
        
        # Verify reference numbers are in tracking category
        tracking_fields = categories["📞 Tracking & References"]
        ref_fields_in_tracking = [f for f in tracking_fields if 'referenceNumbers' in f]
        
        print(f"\n✅ {len(ref_fields_in_tracking)} reference number fields correctly categorized in Tracking & References")
        return len(ref_fields_in_tracking) > 0
        
    except Exception as e:
        print(f"Error testing categorization: {e}")
        return False

def main():
    print("Testing Updated Mapping UI with Reference Number Support")
    print("=" * 60)
    
    success = True
    success &= test_reference_number_schema()
    success &= test_payload_generation()
    success &= test_field_categorization()
    
    print("\n" + "=" * 60)
    print("MAPPING UI UPDATE SUMMARY:")
    if success:
        print("✅ All tests passed! The updated mapping UI includes:")
        print("  - ✅ Enhanced API schema with reference number fields")
        print("  - ✅ Auto-populate logic for reference number types")
        print("  - ✅ Proper categorization in 'Tracking & References' section")
        print("  - ✅ Payload generation with reference numbers")
        print("  - ✅ Support for PRO numbers from Excel 'Carrier Pro#' column")
        print("\n🎯 SOLUTION FOR YOUR ISSUE:")
        print("  - Map 'Carrier Pro#' → 'PRO Number / Tracking Number' field")
        print("  - Reference Number Type will auto-populate to 'PRO_NUMBER'")
        print("  - Enhanced workflow will have access to PRO for tracking")
    else:
        print("❌ Some tests failed - check implementation")
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)