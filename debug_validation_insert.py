"""
Debug code snippet to insert into the _validate_chunk method around line 1011
This will help identify why all rows are being filtered out.
"""

# INSERT THIS CODE IN data_processor.py in the _validate_chunk method around line 1011
# (right after the required_fields list is defined, before the validation loop)

debug_validation_code = '''
# =========================
# DEBUG CODE - REMOVE AFTER FIXING
# =========================
if actual_row_index < 3:  # Only debug first 3 rows to avoid log spam
    self.logger.info(f"=== DEBUG ROW {actual_row_index + 1} VALIDATION ===")
    self.logger.info(f"Row has {len(row)} columns total")
    self.logger.info(f"Row columns: {list(row.keys())}")
    
    missing_fields = []
    present_fields = []
    empty_fields = []
    
    for field in required_fields:
        if field not in row:
            missing_fields.append(field)
        elif pd.isna(row.get(field)):
            empty_fields.append(f"{field} (NaN)")
        elif str(row.get(field, '')).strip() == '':
            empty_fields.append(f"{field} (empty string)")
        else:
            present_fields.append(field)
    
    self.logger.info(f"PRESENT required fields ({len(present_fields)}): {present_fields}")
    self.logger.info(f"MISSING required fields ({len(missing_fields)}): {missing_fields}")
    self.logger.info(f"EMPTY required fields ({len(empty_fields)}): {empty_fields}")
    
    if missing_fields or empty_fields:
        self.logger.error(f"ROW {actual_row_index + 1} WILL BE REJECTED due to {len(missing_fields + empty_fields)} failed validations")
        # Show some sample values
        self.logger.info("Sample row values:")
        for key, value in list(row.items())[:10]:
            self.logger.info(f"  {key}: '{value}' (type: {type(value).__name__})")
    else:
        self.logger.info(f"ROW {actual_row_index + 1} PASSES required field validation")

# If we're at the last row or row 5, show overall statistics
if actual_row_index >= len(df) - 1 or actual_row_index == 4:
    self.logger.info("=== VALIDATION SUMMARY ===")
    self.logger.info(f"Total rows being validated: {len(df)}")
    self.logger.info(f"Required fields count: {len(required_fields)}")
    self.logger.info("Required fields list:")
    for i, field in enumerate(required_fields, 1):
        self.logger.info(f"  {i:2d}. {field}")
# =========================
# END DEBUG CODE
# =========================
'''

print("To debug the validation filtering issue, add this code to data_processor.py:")
print("Location: In the _validate_chunk method, around line 1011")
print("Position: Right after the required_fields list is defined, before the validation loop starts")
print("\nCode to insert:")
print(debug_validation_code)

print("\nAlternatively, you can apply this fix by editing the file directly.")
print("The debug code will log:")
print("1. What columns are present in each row")
print("2. Which required fields are missing or empty")
print("3. Why each row is being rejected")
print("4. Sample values from the first few rows")