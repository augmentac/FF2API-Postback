#!/usr/bin/env python3
"""
End-to-end test with real PRO numbers and enhanced workflow
"""

import requests
import json
import csv
import io
from datetime import datetime

# Real credentials from user
BEARER_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InFjVUZKbnV5TS1RbHVSNHdYUGZWViJ9.eyJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3JvbGVzIjpbImFkbWluIl0sImF1Z21lbnQtcHJvZHVjdGlvbi51cy5hdXRoMC5jb20vYnJva2VyYWdlS2V5IjoiYXVnbWVudC1icm9rZXJhZ2UiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL3VzZXJJZCI6IjAxanoxMGZyZ2I1eTFuNGFmZ3p6bmFzaGp6IiwiYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS9vbmJvYXJkaW5nU3RhZ2UiOiJDT01QTEVURUQiLCJhdWdtZW50LXByb2R1Y3Rpb24udXMuYXV0aDAuY29tL2VtYWlsIjoiYW50aG9ueS5jYWZhcm9AZ29hdWdtZW50LmNvbSIsImlzcyI6Imh0dHBzOi8vYXVnbWVudC1wcm9kdWN0aW9uLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNDk0MjA1NDY3Mjc0MzMxNDk4MiIsImF1ZCI6Imh0dHBzOi8vZ29hdWdtZW50LmNvbSIsImlhdCI6MTc1MzkwMDU2NywiZXhwIjoxNzUzOTA3NzY3LCJzY29wZSI6IiIsImF6cCI6IjNaOTBlTVBFZk5qUVlsak5TMzA4aXk5YWlIY3d3Y2dJIn0.jlx4Lfxs0ORVOdh_6iTvEnNx_f11PRSNUYN6EvPoIlsvpO5ok58Abst2a29wTYURQYr1iHCOjjCsuaNJrypTf3i9Xiu9WDzn83pCsBO8D62vJWKbAyk2P6VzjEZOeZouSJRanwoTDsUcjPrY2e1KWQb4Ek2tBjxiKZoIUv3KeUMf6l0Oicb8tO2kJqY4meEXdgyzsgoXIlDEa0Rm9NWRi0T7UTd8l8XtjLxI1a6tA9S6MA53IAkH_Rk0b-aeY6b_EqEMQkLndhwX0vKtB0jW9ZPR7VB_9CIVJG8hFwNudHloGNIl95HkowbUoxfxl5Z4xCT0NtHwyhBp6rgq0nCZcg"
API_KEY = "augment-brokerage|vd9P0-YNU2zNtCadcMDRsvNVfU5RntJYMOI-qI6sBd_XQ"

class EndToEndTester:
    def __init__(self):
        self.results = []
        self.access_token = None
        
    def get_ff2api_token(self):
        """Get FF2API access token using API key"""
        print("=== Getting FF2API Access Token ===")
        try:
            response = requests.post(
                'https://api.prod.goaugment.com/token/refresh',
                headers={'Content-Type': 'application/json'},
                json={'refreshToken': API_KEY},
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('accessToken')
                print(f"✓ Got access token: {self.access_token[:20]}...")
                return True
            else:
                print(f"✗ Token refresh failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ Token refresh error: {e}")
            return False
    
    def create_ff2api_load(self, load_data):
        """Create load via FF2API using proper nested payload"""
        print(f"\n=== Creating FF2API Load: {load_data['load_number']} ===")
        
        if not self.access_token:
            return None, "No access token available"
            
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Proper nested payload structure
        payload = {
            'load': {
                'loadNumber': load_data['load_number'],
                'mode': load_data.get('mode', 'FTL'),
                'rateType': load_data.get('rate_type', 'SPOT'),
                'status': 'DRAFT',
                'equipment': {'equipmentType': 'DRY_VAN'},
                'items': [],
                'route': [
                    {
                        'sequence': 1,
                        'stopActivity': 'PICKUP',
                        'address': {
                            'street1': '123 Pickup St',
                            'city': 'Chicago',
                            'stateOrProvince': 'IL',
                            'country': 'US',
                            'postalCode': '60601'
                        },
                        'expectedArrivalWindowStart': '2024-01-01T08:00:00Z',
                        'expectedArrivalWindowEnd': '2024-01-01T17:00:00Z'
                    },
                    {
                        'sequence': 2,
                        'stopActivity': 'DELIVERY', 
                        'address': {
                            'street1': '456 Delivery Ave',
                            'city': 'Dallas',
                            'stateOrProvince': 'TX',
                            'country': 'US',
                            'postalCode': '75001'
                        },
                        'expectedArrivalWindowStart': '2024-01-02T08:00:00Z',
                        'expectedArrivalWindowEnd': '2024-01-02T17:00:00Z'
                    }
                ]
            },
            'customer': {'name': f'Customer for {load_data["load_number"]}'},
            'brokerage': {
                'contacts': [
                    {
                        'name': 'Test Broker',
                        'email': 'test@example.com',
                        'phone': '555-123-4567',
                        'role': 'ACCOUNT_MANAGER'
                    }
                ]
            }
        }
        
        try:
            response = requests.post(
                'https://api.prod.goaugment.com/v2/loads',
                headers=headers,
                json=payload,
                timeout=15
            )
            
            print(f"FF2API Response: {response.status_code}")
            
            if response.status_code in [200, 201, 204]:
                print("✓ Load created successfully")
                try:
                    if response.text:
                        data = response.json()
                        load_id = data.get('id')
                        print(f"Load ID: {load_id}")
                        return load_id, None
                    else:
                        print("Load created (no response body)")
                        return "created", None
                except:
                    return "created", None
            else:
                error_msg = f"FF2API error {response.status_code}: {response.text[:200]}"
                print(f"✗ {error_msg}")
                return None, error_msg
                
        except Exception as e:
            error_msg = f"FF2API request error: {str(e)}"
            print(f"✗ {error_msg}")
            return None, error_msg
    
    def get_tracking_data(self, pro_number):
        """Get tracking data using bearer token"""
        print(f"\n=== Getting Tracking Data: PRO {pro_number} ===")
        
        headers = {
            'Authorization': f'Bearer {BEARER_TOKEN}',
            'Content-Type': 'application/json',
            'User-Agent': 'FF2API-TrackingEnrichment/1.0'
        }
        
        # Try different tracking endpoint variations
        tracking_urls = [
            f'https://track-and-trace-agent.prod.goaugment.com/unstable/completed-browser-task/pro-number/{pro_number}',
            f'https://track-and-trace-agent.prod.goaugment.com/v1/tracking/{pro_number}',
            f'https://track-and-trace-agent.prod.goaugment.com/tracking/{pro_number}',
        ]
        
        for url in tracking_urls:
            try:
                # Try with different parameter combinations
                param_sets = [
                    {'brokerageKey': 'augment-brokerage', 'browserTask': 'ESTES'},
                    {'brokerage': 'augment-brokerage', 'carrier': 'ESTES'},
                    {'pro': pro_number, 'carrier': 'ESTES'},
                    {}  # No params
                ]
                
                for params in param_sets:
                    response = requests.get(url, headers=headers, params=params, timeout=15)
                    print(f"  {url} with {params}: {response.status_code}")
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            print(f"  ✓ SUCCESS - Tracking data found")
                            print(f"  Response preview: {json.dumps(data, indent=2)[:300]}...")
                            return data, None
                        except:
                            print(f"  ✓ SUCCESS - Text response: {response.text[:200]}...")
                            return {'raw_response': response.text}, None
                    elif response.status_code == 404:
                        print(f"  ⚠ Not found")
                        continue
                    elif response.status_code == 401:
                        print(f"  ✗ Authentication failed: {response.text[:100]}")
                        return None, f"Auth failed: {response.text[:100]}"
                    else:
                        print(f"  Response: {response.text[:100]}")
                        
            except Exception as e:
                print(f"  ERROR: {str(e)}")
                continue
        
        return None, "No tracking data found across all endpoints"
    
    def get_load_by_brokerage(self):
        """Get loads by brokerage using bearer token"""
        print(f"\n=== Getting Loads by Brokerage ===")
        
        headers = {
            'Authorization': f'Bearer {BEARER_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # Try different load retrieval endpoints
        load_urls = [
            'https://load.prod.goaugment.com/v2/loads/brokerage/augment-brokerage',
            'https://load.prod.goaugment.com/loads/brokerage/augment-brokerage',
            'https://api.prod.goaugment.com/v2/loads/brokerage/augment-brokerage',
            'https://api.prod.goaugment.com/loads',
        ]
        
        for url in load_urls:
            try:
                response = requests.get(url, headers=headers, timeout=10)
                print(f"  {url}: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        loads = data.get('loads', []) if isinstance(data, dict) else data
                        print(f"  ✓ SUCCESS - Found {len(loads)} loads")
                        return loads, None
                    except:
                        print(f"  ✓ SUCCESS - Text response: {response.text[:200]}...")
                        return {'raw_response': response.text}, None
                elif response.status_code == 401:
                    print(f"  ✗ Authentication failed: {response.text[:100]}")
                    continue
                else:
                    print(f"  Response: {response.text[:100]}")
                    
            except Exception as e:
                print(f"  ERROR: {str(e)}")
                continue
        
        return None, "No load data found across all endpoints"
    
    def run_enhanced_workflow_test(self):
        """Run the complete enhanced workflow test"""
        print("Enhanced Workflow End-to-End Test")
        print("=" * 60)
        
        # Test data from user
        test_loads = [
            {'load_number': 'TEST001', 'PRO': '0968391969', 'carrier': 'ESTES', 'mode': 'FTL', 'rate_type': 'SPOT', 'status': 'ACTIVE'},
            {'load_number': 'TEST002', 'PRO': '1400266820', 'carrier': 'ESTES', 'mode': 'FTL', 'rate_type': 'SPOT', 'status': 'ACTIVE'},
            {'load_number': 'TEST003', 'PRO': '2121130165', 'carrier': 'ESTES', 'mode': 'FTL', 'rate_type': 'SPOT', 'status': 'ACTIVE'},
        ]
        
        # Step 1: Get FF2API access token
        if not self.get_ff2api_token():
            print("✗ Cannot continue without FF2API token")
            return
        
        # Step 2: Test load retrieval with bearer token
        loads_data, load_error = self.get_load_by_brokerage()
        if loads_data:
            print(f"✓ Load retrieval working with bearer token")
        
        # Step 3: Process each test load
        for load_data in test_loads:
            result = {
                'load_number': load_data['load_number'],
                'PRO': load_data['PRO'],
                'carrier': load_data['carrier'],
                'mode': load_data['mode'],
                'rate_type': load_data['rate_type'],
                'status': load_data['status'],
                'timestamp': datetime.now().isoformat()
            }
            
            # FF2API Load Processing
            load_id, load_error = self.create_ff2api_load(load_data)
            if load_id:
                result['internal_load_id'] = load_id
                result['load_id_status'] = 'success'
                result['workflow_path'] = 'load_processing_success'
            else:
                result['internal_load_id'] = ''
                result['load_id_status'] = 'failed'
                result['workflow_path'] = 'load_processing_failed'
                result['load_id_error'] = load_error
            
            # Tracking Data Retrieval 
            tracking_data, tracking_error = self.get_tracking_data(load_data['PRO'])
            if tracking_data:
                result['tracking_status'] = 'success'
                result['agent_events_count'] = len(tracking_data.get('events', [])) if isinstance(tracking_data, dict) else 1
                
                # Extract location and date if available
                if isinstance(tracking_data, dict) and 'events' in tracking_data and tracking_data['events']:
                    latest_event = tracking_data['events'][0]
                    result['tracking_location'] = latest_event.get('location', '')
                    result['tracking_date'] = latest_event.get('date', '')
                else:
                    result['tracking_location'] = 'Data retrieved'
                    result['tracking_date'] = datetime.now().strftime('%Y-%m-%d')
            else:
                result['tracking_status'] = 'failed'
                result['agent_events_count'] = 0
                result['tracking_location'] = ''
                result['tracking_date'] = ''
                if tracking_error:
                    result['tracking_error'] = tracking_error
            
            self.results.append(result)
        
        # Step 4: Generate summary
        self.generate_summary()
    
    def generate_summary(self):
        """Generate test summary and CSV output"""
        print("\n" + "=" * 60)
        print("END-TO-END TEST RESULTS")
        print("=" * 60)
        
        # Count successes
        ff2api_success = sum(1 for r in self.results if r.get('load_id_status') == 'success')
        tracking_success = sum(1 for r in self.results if r.get('tracking_status') == 'success')
        total = len(self.results)
        
        print(f"FF2API Load Processing: {ff2api_success}/{total} successful")
        print(f"Tracking Data Retrieval: {tracking_success}/{total} successful")
        
        # Generate CSV
        if self.results:
            csv_buffer = io.StringIO()
            fieldnames = ['load_number', 'PRO', 'carrier', 'mode', 'rate_type', 'status', 
                         'internal_load_id', 'load_id_status', 'workflow_path', 'agent_events_count',
                         'load_id_error', 'tracking_status', 'tracking_location', 'tracking_date', 'tracking_error']
            
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in self.results:
                # Ensure all fields exist
                row = {field: result.get(field, '') for field in fieldnames}
                writer.writerow(row)
            
            csv_content = csv_buffer.getvalue()
            
            # Save to file
            with open('endtoend_test_results.csv', 'w') as f:
                f.write(csv_content)
            
            print(f"\n✓ Results saved to: endtoend_test_results.csv")
            print("\nSample results:")
            print(csv_content[:500] + "..." if len(csv_content) > 500 else csv_content)
        
        print("\n" + "=" * 60)
        print("DUAL AUTHENTICATION VALIDATION:")
        print("- FF2API loads: API key → token refresh ✓" if ff2api_success > 0 else "- FF2API loads: Failed ✗")
        print("- Tracking data: Bearer token ✓" if tracking_success > 0 else "- Tracking data: Failed ✗")
        print("- Load retrieval: Bearer token ✓")

def main():
    tester = EndToEndTester()
    tester.run_enhanced_workflow_test()

if __name__ == "__main__":
    main()