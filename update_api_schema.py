#!/usr/bin/env python3
"""
Update API schema based on the latest CSV schema analysis
"""

import pandas as pd

def analyze_csv_schema():
    """Analyze the CSV schema to create updated API schema"""
    
    # Read the CSV schema
    df = pd.read_csv('/Users/augiecon2025/Downloads/API_Schema_with_Enums (1).csv')
    
    # Create schema dictionary organized by requirement level
    schema = {
        'required': {},
        'conditional': {}, 
        'optional': {}
    }
    
    for _, row in df.iterrows():
        field_path = row['Field Path']
        field_type = row['Type']
        requirement = row['Requirement Level']
        valid_values = row['Valid Values']
        
        # Skip empty rows
        if pd.isna(field_path):
            continue
            
        # Parse enum values
        enum_values = None
        if pd.notna(valid_values) and valid_values.strip():
            # Clean up enum values
            enum_str = valid_values.strip()
            if enum_str.startswith('[') and enum_str.endswith(']'):
                # Parse array format
                enum_str = enum_str[1:-1]  # Remove brackets
                enum_values = [v.strip() for v in enum_str.split(',')]
        
        # Create field info
        field_info = {
            'type': field_type,
            'description': field_path.split('.')[-1].replace('[]', '').title().replace('_', ' '),
            'path': field_path  
        }
        
        if enum_values:
            field_info['enum'] = enum_values
            
        # Categorize by requirement level
        if requirement == 'Required':
            schema['required'][field_path] = field_info
        elif requirement == 'Conditionally Required':
            schema['conditional'][field_path] = field_info
        else:  # Optional
            schema['optional'][field_path] = field_info
    
    return schema

def generate_updated_schema_function():
    """Generate the updated get_full_api_schema function"""
    
    schema_data = analyze_csv_schema()
    
    function_code = '''def get_full_api_schema():
    """Get the complete API schema for validation - Updated from CSV schema analysis"""
    return {
        # ============ REQUIRED FIELDS ============
        # These fields must always be provided
'''
    
    # Add required fields
    for field_path, field_info in schema_data['required'].items():
        enum_str = f", 'enum': {field_info['enum']}" if 'enum' in field_info else ""
        function_code += f"        '{field_path}': {{'type': '{field_info['type']}', 'required': True, 'description': '{field_info['description']}'{enum_str}}},\n"
    
    function_code += '''
        # ============ CONDITIONALLY REQUIRED FIELDS ============  
        # These become required when their parent object is used or specific conditions are met
'''
    
    # Add conditional fields
    for field_path, field_info in schema_data['conditional'].items():
        enum_str = f", 'enum': {field_info['enum']}" if 'enum' in field_info else ""
        function_code += f"        '{field_path}': {{'type': '{field_info['type']}', 'required': 'conditional', 'description': '{field_info['description']}'{enum_str}}},\n"
    
    function_code += '''
        # ============ OPTIONAL FIELDS ============
        # These fields can be provided to enhance the data but are not required
'''
    
    # Add optional fields  
    for field_path, field_info in schema_data['optional'].items():
        enum_str = f", 'enum': {field_info['enum']}" if 'enum' in field_info else ""
        function_code += f"        '{field_path}': {{'type': '{field_info['type']}', 'required': False, 'description': '{field_info['description']}'{enum_str}}},\n"
    
    function_code += '''    }'''
    
    return function_code

def print_reference_number_analysis():
    """Print analysis of reference number fields"""
    schema_data = analyze_csv_schema()
    
    print("REFERENCE NUMBER ANALYSIS")
    print("=" * 50)
    
    ref_fields = [field for field in schema_data['conditional'].keys() if 'referenceNumbers' in field]
    
    for field in ref_fields:
        field_info = schema_data['conditional'][field]
        print(f"Field: {field}")
        print(f"  Type: {field_info['type']}")
        print(f"  Description: {field_info['description']}")
        if 'enum' in field_info:
            print(f"  Enum Values: {field_info['enum']}")
        print()
    
    print("\nKEY FINDINGS:")
    print("- load.referenceNumbers[].name: Must be one of PRO_NUMBER, PICKUP_NUMBER, etc.")
    print("- load.referenceNumbers[].value: The actual reference value (like PRO number)")
    print("- Both are Conditionally Required - only needed if reference numbers are provided")
    print("- This allows mapping CSV PRO column to load.referenceNumbers[].value")
    print("- And setting load.referenceNumbers[].name to PRO_NUMBER programmatically")

if __name__ == "__main__":
    print_reference_number_analysis()
    print("\n" + "=" * 70)
    print("UPDATED SCHEMA FUNCTION:")
    print("=" * 70)
    print(generate_updated_schema_function())