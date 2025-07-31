#!/usr/bin/env python3
"""
Debug script to investigate why validated_df has 0 rows but 31 columns.
This script will help identify what validation logic is filtering out all rows.
"""

import pandas as pd
import sys
import os
import logging
from typing import Dict, Any, List, Tuple

# Add the src directory to Python path so we can import the modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'frontend'))

try:
    from data_processor import DataProcessor
except ImportError as e:
    print(f"Error importing DataProcessor: {e}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    sys.exit(1)

def debug_validation_process():
    """Debug the validation process to understand why all rows are being filtered"""
    
    print("=== VALIDATION DEBUG ANALYSIS ===")
    
    # Set up logging to capture debug info
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Create a DataProcessor instance
    data_processor = DataProcessor()
    
    print("\n1. EXAMINING REQUIRED FIELDS IN VALIDATION")
    print("Required fields that are checked in _validate_chunk:")
    
    # These are the required fields from the validation code
    required_fields = [
        # Core load fields (always required)
        'load.loadNumber', 'load.mode', 'load.rateType', 'load.status',
        
        # Route fields (at least one stop required)
        'load.route.0.stopActivity',
        'load.route.0.address.addressLine1', 'load.route.0.address.city',
        'load.route.0.address.state', 'load.route.0.address.postalCode',
        'load.route.0.address.country', 'load.route.0.expectedArrivalWindowStart',
        'load.route.0.expectedArrivalWindowEnd',
        
        # Customer fields (top-level required)
        'customer.customerId', 'customer.name'
    ]
    
    for i, field in enumerate(required_fields, 1):
        print(f"  {i:2d}. {field}")
    
    print(f"\nTotal required fields: {len(required_fields)}")
    
    print("\n2. VALIDATION LOGIC ANALYSIS")
    print("The validation fails a row if ANY of these conditions are true:")
    print("  - Field is missing from the row")
    print("  - Field value is NaN/null")
    print("  - Field value is empty string after strip()")
    
    print("\n3. POTENTIAL ISSUES")
    print("Common reasons why ALL rows might be filtered out:")
    print("  A. Missing field mappings - if key required fields weren't mapped")
    print("  B. Empty data values - if source data has empty values for required fields")
    print("  C. Column naming mismatch - if mapped column names don't match expected format")
    print("  D. Data type issues - if values can't be processed correctly")
    
    print("\n4. DEBUGGING STEPS TO TAKE")
    print("To debug this issue, you need to:")
    print("  1. Check the 'mapped_df' that goes INTO validate_data()")
    print("     - What columns does it have?")
    print("     - What does the first row look like?")
    print("     - Are the required fields present as columns?")
    print("  2. Add debug logging in _validate_chunk() to see:")
    print("     - Which specific required fields are missing for each row")
    print("     - What the row data actually contains")
    print("  3. Check field mappings to ensure required fields are mapped")
    
    print("\n5. SUGGESTED DEBUG CODE TO ADD")
    print("Add this debug code in the _validate_chunk method around line 1011:")
    print("""
    # DEBUG: Log missing fields for first few rows
    if actual_row_index < 3:  # Only log first 3 rows to avoid spam
        missing_fields = []
        for field in required_fields:
            if field not in row or pd.isna(row.get(field)) or str(row.get(field, '')).strip() == '':
                missing_fields.append(field)
        
        print(f"ROW {actual_row_index + 1} DEBUG:")
        print(f"  Available columns: {list(row.keys())[:10]}...")  # First 10 columns
        print(f"  Missing required fields: {missing_fields}")
        if missing_fields:
            print(f"  Values for missing fields:")
            for field in missing_fields[:5]:  # Show first 5 missing
                value = row.get(field, 'NOT_FOUND')
                print(f"    {field}: '{value}' (type: {type(value)})")
    """)
    
    print("\n6. COMMON FIXES")
    print("Once you identify the issue, common fixes include:")
    print("  - Update field mappings to include missing required fields")
    print("  - Modify validation logic to be less strict for optional-but-marked-required fields")
    print("  - Handle empty values by providing defaults for non-critical fields")
    print("  - Fix data preprocessing to ensure required fields have valid values")

if __name__ == "__main__":
    debug_validation_process()