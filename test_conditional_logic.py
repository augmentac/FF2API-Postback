#!/usr/bin/env python3
"""
Test the conditional requirement logic for reference numbers
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_reference_number_conditional_logic():
    """Test that mapping one reference number doesn't make others required"""
    print("=== Testing Reference Number Conditional Logic ===")
    
    try:
        from src.frontend.ui_components import get_full_api_schema, get_effective_required_fields
        
        api_schema = get_full_api_schema()
        
        # Test 1: No mappings - no reference numbers should be required
        print("\n1. Test with no mappings:")
        current_mappings = {}
        effective_required = get_effective_required_fields(api_schema, current_mappings)
        ref_required = [f for f in effective_required.keys() if 'referenceNumbers' in f]
        print(f"   Reference numbers required: {len(ref_required)}")
        for f in ref_required:
            print(f"   - {f}")
        
        # Test 2: Map only the first reference number value
        print("\n2. Test with only first reference number value mapped:")
        current_mappings = {
            'load.referenceNumbers.0.value': 'Carrier Pro#'
        }
        effective_required = get_effective_required_fields(api_schema, current_mappings)
        ref_required = [f for f in effective_required.keys() if 'referenceNumbers' in f]
        print(f"   Reference numbers required: {len(ref_required)}")
        for f in ref_required:
            print(f"   - {f}")
        
        # Test 3: Map the first reference number name and value
        print("\n3. Test with first reference number name and value mapped:")
        current_mappings = {
            'load.referenceNumbers.0.name': 'MANUAL_VALUE:PRO_NUMBER',
            'load.referenceNumbers.0.value': 'Carrier Pro#'
        }
        effective_required = get_effective_required_fields(api_schema, current_mappings)
        ref_required = [f for f in effective_required.keys() if 'referenceNumbers' in f]
        print(f"   Reference numbers required: {len(ref_required)}")
        for f in ref_required:
            print(f"   - {f}")
        
        # Test 4: Check that secondary and third reference numbers are NOT required
        print("\n4. Checking secondary and third reference numbers are NOT required:")
        secondary_fields = ['load.referenceNumbers.1.name', 'load.referenceNumbers.1.value']
        third_fields = ['load.referenceNumbers.2.name', 'load.referenceNumbers.2.value']
        
        secondary_required = [f for f in effective_required.keys() if f in secondary_fields]
        third_required = [f for f in effective_required.keys() if f in third_fields]
        
        print(f"   Secondary reference numbers required: {len(secondary_required)} (should be 0)")
        print(f"   Third reference numbers required: {len(third_required)} (should be 0)")
        
        # Success check
        success = len(secondary_required) == 0 and len(third_required) == 0
        if success:
            print("\n✅ SUCCESS: Mapping first reference number does NOT make others required!")
        else:
            print("\n❌ FAILURE: Other reference numbers are incorrectly being marked as required")
            print(f"   Secondary required: {secondary_required}")
            print(f"   Third required: {third_required}")
        
        return success
        
    except Exception as e:
        print(f"Error testing logic: {e}")
        return False

if __name__ == "__main__":
    success = test_reference_number_conditional_logic()
    exit(0 if success else 1)