#!/usr/bin/env python3
"""
Test UI compatibility helpers to prevent field compatibility issues
"""

import sys
import os

# Add the src paths  
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'frontend'))

from src.frontend.ui_components import get_carrier_field_with_fallback, ensure_carrier_contacts_structure

def test_ui_compatibility_helpers():
    """Test that UI compatibility helpers prevent field access errors"""
    print("=== TESTING UI COMPATIBILITY HELPERS ===\n")
    
    # Test 1: get_carrier_field_with_fallback with missing deprecated field
    print("1. TESTING get_carrier_field_with_fallback")
    print("-" * 50)
    
    # Simulate data from database that no longer includes deprecated fields
    carrier_data_missing_deprecated = {
        'carrier.name': 'Estes Express',
        'carrier.mcNumber': '105764',
        'carrier.dotNumber': '205764', 
        'carrier.scac': 'EXLA',
        # carrier.email and carrier.phone are missing (as they should be)
        'carrier.contacts.0.name': 'Customer Service',
        'carrier.contacts.0.email': 'customercare@estes-express.com',
        'carrier.contacts.0.phone': '+18663783748',
        'carrier.contacts.0.role': 'DISPATCHER'
    }
    
    # Test safe access to deprecated fields
    phone = get_carrier_field_with_fallback(
        carrier_data_missing_deprecated, 
        'carrier.phone',           # Deprecated field (missing)
        'carrier.contacts.0.phone', # Fallback field (present)
        'N/A'                      # Default
    )
    
    email = get_carrier_field_with_fallback(
        carrier_data_missing_deprecated,
        'carrier.email',           # Deprecated field (missing) 
        'carrier.contacts.0.email', # Fallback field (present)
        'N/A'                      # Default
    )
    
    print(f"✅ Safe phone access: '{phone}' (expected: '+18663783748')")
    print(f"✅ Safe email access: '{email}' (expected: 'customercare@estes-express.com')")
    
    # Test with completely missing data
    empty_data = {}
    phone_default = get_carrier_field_with_fallback(empty_data, 'carrier.phone', 'carrier.contacts.0.phone', 'NO_PHONE')
    print(f"✅ Default fallback: '{phone_default}' (expected: 'NO_PHONE')")
    
    # Test 2: ensure_carrier_contacts_structure
    print(f"\n2. TESTING ensure_carrier_contacts_structure")
    print("-" * 50)
    
    # Simulate data that still has deprecated fields (from UI form submission)
    carrier_data_with_deprecated = {
        'carrier.name': 'Test Carrier',
        'carrier.scac': 'TEST',
        'carrier.mcNumber': '123456',
        'carrier.dotNumber': '654321',
        'carrier.email': 'deprecated@test.com',      # Should be removed
        'carrier.phone': '+1234567890',             # Should be removed
        'carrier.contacts.0.name': 'Test Contact',
        'carrier.contacts.0.email': 'contact@test.com',
        'carrier.contacts.0.phone': '+0987654321'
    }
    
    print("Before cleaning:")
    for key, value in carrier_data_with_deprecated.items():
        print(f"  {key}: {value}")
    
    # Clean the data
    cleaned_data = ensure_carrier_contacts_structure(carrier_data_with_deprecated)
    
    print("\nAfter cleaning:")
    for key, value in cleaned_data.items():
        print(f"  {key}: {value}")
    
    # Verify deprecated fields were removed
    deprecated_removed = 'carrier.email' not in cleaned_data and 'carrier.phone' not in cleaned_data
    contacts_preserved = 'carrier.contacts.0.email' in cleaned_data and 'carrier.contacts.0.phone' in cleaned_data
    
    print(f"\n✅ Deprecated fields removed: {deprecated_removed}")
    print(f"✅ Contact structure preserved: {contacts_preserved}")
    
    # Test 3: Data migration scenario
    print(f"\n3. TESTING DATA MIGRATION SCENARIO")
    print("-" * 50)
    
    # Simulate data that has deprecated fields but no contact structure
    legacy_data = {
        'carrier.name': 'Legacy Carrier',
        'carrier.email': 'legacy@carrier.com',
        'carrier.phone': '+1111111111'
        # No contacts structure
    }
    
    print("Legacy data before migration:")
    for key, value in legacy_data.items():
        print(f"  {key}: {value}")
    
    migrated_data = ensure_carrier_contacts_structure(legacy_data)
    
    print("\nAfter migration:")
    for key, value in migrated_data.items():
        print(f"  {key}: {value}")
    
    # Verify migration worked
    email_migrated = migrated_data.get('carrier.contacts.0.email') == 'legacy@carrier.com'
    phone_migrated = migrated_data.get('carrier.contacts.0.phone') == '+1111111111'
    deprecated_removed = 'carrier.email' not in migrated_data and 'carrier.phone' not in migrated_data
    
    print(f"\n✅ Email migrated to contacts: {email_migrated}")
    print(f"✅ Phone migrated to contacts: {phone_migrated}")
    print(f"✅ Deprecated fields removed: {deprecated_removed}")
    
    # Final assessment
    print(f"\n{'='*60}")
    print("UI COMPATIBILITY HELPERS TEST RESULT:")
    
    all_tests_passed = (
        phone == '+18663783748' and
        email == 'customercare@estes-express.com' and
        phone_default == 'NO_PHONE' and
        deprecated_removed and
        contacts_preserved and
        email_migrated and
        phone_migrated
    )
    
    if all_tests_passed:
        print("✅ ALL TESTS PASSED")
        print("\n🛡️ SAFEGUARDS IMPLEMENTED:")
        print("  - get_carrier_field_with_fallback() prevents KeyError exceptions")
        print("  - ensure_carrier_contacts_structure() prevents API validation errors")
        print("  - Data migration support for legacy carrier data")
        print("  - Backward compatibility maintained while ensuring API compliance")
        print("\n🚨 PREVENTION MECHANISMS:")
        print("  - UI can safely access deprecated fields without crashes")
        print("  - Deprecated fields are automatically cleaned before API submission")
        print("  - Contact information is preserved in proper API schema structure")
    else:
        print("❌ SOME TESTS FAILED")
        print("Additional investigation needed")
    
    return all_tests_passed

if __name__ == "__main__":
    success = test_ui_compatibility_helpers()
    exit(0 if success else 1)