#!/usr/bin/env python3
"""
Debug the exact API validation failure with detailed response info
"""

import sys
import os
import requests
import json
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_validation_failure():
    """Debug exactly what happens during API validation"""
    print("=== API Validation Failure Debug ===")
    
    try:
        from src.backend.api_client import LoadsAPIClient
        
        # Use the same parameters as the configuration save
        base_url = "https://load.prod.goaugment.com"  # Default from config
        api_key = "test-api-key-123"  # The test key that's failing
        
        print(f"🔗 Testing with Base URL: {base_url}")
        print(f"🔑 Testing with API Key: {api_key}")
        
        # Create client exactly like the configuration save does
        client = LoadsAPIClient(base_url, api_key=api_key, auth_type='api_key')
        
        print(f"\n--- API Client State ---")
        print(f"Base URL: {client.base_url}")
        print(f"Auth Type: {client.auth_type}")
        print(f"Bearer Token Set: {client.bearer_token is not None}")
        if client.bearer_token:
            print(f"Bearer Token (first 15 chars): {client.bearer_token[:15]}...")
        
        # Get the session headers
        print(f"Session Headers: {dict(client.session.headers)}")
        
        # Test the validate_connection method
        print(f"\n--- Testing validate_connection() ---")
        result = client.validate_connection()
        
        print(f"Validation Result: {result}")
        
        # Let's also test the raw API call that validate_connection makes
        print(f"\n--- Raw API Test ---")
        test_payload = {
            "load": {
                "loadNumber": "TEST123",
                "mode": "FTL",
                "rateType": "SPOT",
                "status": "DRAFT",
                "equipment": {"equipmentType": "DRY_VAN"},
                "route": [
                    {
                        "sequence": 1,
                        "stopActivity": "PICKUP",
                        "address": {
                            "street1": "123 Test St",
                            "city": "Test City",
                            "stateOrProvince": "CA",
                            "country": "US",
                            "postalCode": "90210"
                        },
                        "expectedArrivalWindowStart": "2024-01-01T08:00:00Z",
                        "expectedArrivalWindowEnd": "2024-01-01T17:00:00Z"
                    }
                ],
                "items": [{"quantity": 1, "totalWeightLbs": 1000}]
            },
            "brokerage": {"contacts": [{"name": "Test", "email": "test@test.com", "phone": "555-1234", "role": "ACCOUNT_MANAGER"}]},
            "customer": {"name": "Test Customer"}
        }
        
        try:
            raw_response = client.session.post(f"{base_url}/v2/loads", json=test_payload, timeout=30)
            print(f"Raw Response Status: {raw_response.status_code}")
            print(f"Raw Response Headers: {dict(raw_response.headers)}")
            
            try:
                raw_json = raw_response.json()
                print(f"Raw Response JSON: {json.dumps(raw_json, indent=2)}")
            except:
                print(f"Raw Response Text: {raw_response.text}")
                
        except Exception as e:
            print(f"Raw API call failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error debugging validation: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_validation_failure()
    exit(0 if success else 1)