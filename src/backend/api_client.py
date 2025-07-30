import requests
import json
import logging
from typing import Dict, List, Any, Optional

class LoadsAPIClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, bearer_token: Optional[str] = None, auth_type: str = 'api_key'):
        self.base_url = base_url.rstrip('/') if base_url else "https://api.prod.goaugment.com"
        self.api_key = api_key
        self.auth_type = auth_type
        self.bearer_token = bearer_token
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        # Handle different authentication types
        if self.auth_type == 'bearer_token':
            if not self.bearer_token:
                raise ValueError("Bearer token is required when auth_type is 'bearer_token'")
            # Set bearer token directly in session headers
            self.session.headers.update({
                'Authorization': f'Bearer {self.bearer_token}'
            })
        elif self.auth_type == 'api_key':
            if not self.api_key:
                raise ValueError("API key is required when auth_type is 'api_key'")
            # Use API key to refresh token (proper authentication flow)
            self._refresh_token()
        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}. Must be 'api_key' or 'bearer_token'")
    
    def _refresh_token(self) -> Dict[str, Any]:
        """Refresh the bearer token using the API key"""
        if not self.api_key:
            return {'success': False, 'message': 'API key not provided for token refresh'}
        
        try:
            response = requests.post(
                f"{self.base_url}/token/refresh",
                headers={'Content-Type': 'application/json'},
                json={'refreshToken': self.api_key},
                timeout=30
            )
            response.raise_for_status()
            
            try:
                token_data = response.json()
                self.bearer_token = token_data.get('accessToken')
                
                if self.bearer_token:
                    # Update session headers with new token
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.bearer_token}'
                    })
                    return {'success': True, 'message': 'Token refreshed successfully'}
                else:
                    return {'success': False, 'message': 'No access token received from refresh'}
            except json.JSONDecodeError:
                return {'success': False, 'message': f'Token refresh failed: Invalid JSON response from server. Status: {response.status_code}, Response: {response.text[:200]}'}
            except Exception as json_error:
                return {'success': False, 'message': f'Token refresh failed: JSON parsing error: {str(json_error)}'}
                
        except requests.exceptions.RequestException as e:
            error_detail = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                except (json.JSONDecodeError, ValueError) as json_error:
                    error_detail = e.response.text
                    logging.warning(f"Could not parse token refresh error response as JSON: {json_error}")
            return {'success': False, 'message': f'Token refresh failed: {error_detail}'}
        except Exception as e:
            return {'success': False, 'message': f'Token refresh error: {str(e)}'}
    
    def create_load(self, load_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a single load via API"""
        try:
            # DEBUG: Log the request details
            logging.info(f"DEBUG create_load: Making API request to {self.base_url}/v2/loads")
            response = self.session.post(
                f"{self.base_url}/v2/loads",
                json=load_data,
                timeout=30
            )
            
            # DEBUG: Log response details
            logging.info(f"DEBUG create_load: Response status_code: {response.status_code}")
            
            # Handle different response codes explicitly
            if response.status_code == 201:
                # Load created successfully
                try:
                    response_data = response.json()
                    # Extract load number from response
                    load_number = (response_data.get('loadNumber') or 
                                 response_data.get('load', {}).get('loadNumber') or
                                 response_data.get('id'))
                    result = {
                        'success': True,
                        'data': response_data,
                        'status_code': response.status_code,
                        'load_number': load_number
                    }
                    logging.info(f"DEBUG create_load: Returning SUCCESS result with load_number: {load_number}")
                    return result
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'data': {'message': 'Load created successfully (no response data)'},
                        'status_code': response.status_code,
                        'load_number': None  # Will be filled from original payload
                    }
            elif response.status_code == 200:
                # Success with response data
                try:
                    response_data = response.json()
                    # Extract load number from response
                    load_number = (response_data.get('loadNumber') or 
                                 response_data.get('load', {}).get('loadNumber') or
                                 response_data.get('id'))
                    result = {
                        'success': True,
                        'data': response_data,
                        'status_code': response.status_code,
                        'load_number': load_number
                    }
                    logging.info(f"DEBUG create_load: Returning SUCCESS result with load_number: {load_number}")
                    return result
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'data': {'message': 'Load processed successfully (no response data)'},
                        'status_code': response.status_code,
                        'load_number': None  # Will be filled from original payload
                    }
            elif response.status_code == 204:
                # No content but successful
                return {
                    'success': True,
                    'data': {'message': 'Load created successfully (no response data)'},
                    'status_code': response.status_code,
                    'load_number': None  # Will be filled from original payload
                }
            elif response.status_code == 400:
                # Bad request - invalid payload
                try:
                    error_details = response.json()
                    result = {
                        'success': False,
                        'error': f'Bad request: {error_details}',
                        'status_code': response.status_code
                    }
                    logging.info(f"DEBUG create_load: Returning FAILURE result - Bad request: {error_details}")
                    return result
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'error': f'Bad request: {response.text[:300]}',
                        'status_code': response.status_code
                    }
            elif response.status_code == 422:
                # Validation error - data format issues
                try:
                    error_details = response.json()
                    return {
                        'success': False,
                        'error': f'Validation error: {error_details}',
                        'status_code': response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'error': f'Validation error: {response.text[:300]}',
                        'status_code': response.status_code
                    }
            elif response.status_code == 401:
                # Unauthorized - try token refresh only for API key auth
                if self.auth_type == 'api_key':
                    refresh_result = self._refresh_token()
                    if refresh_result and refresh_result.get('success'):
                        # Retry the request with new token
                        response = self.session.post(f"{self.base_url}/v2/loads", json=load_data, timeout=30)
                        if response.status_code in [200, 201, 204]:
                            try:
                                response_data = response.json()
                                # Extract load number from response
                                load_number = (response_data.get('loadNumber') or 
                                             response_data.get('load', {}).get('loadNumber') or
                                             response_data.get('id'))
                                return {
                                    'success': True,
                                    'data': response_data,
                                    'status_code': response.status_code,
                                    'load_number': load_number
                                }
                            except json.JSONDecodeError:
                                return {
                                    'success': True,
                                    'data': {'message': 'Load created successfully after token refresh'},
                                    'status_code': response.status_code,
                                    'load_number': None # Will be filled from original payload
                                }
                        else:
                            return {
                                'success': False,
                                'error': f'Authentication failed after token refresh: HTTP {response.status_code}',
                                'status_code': response.status_code
                            }
                    else:
                        return {
                            'success': False,
                            'error': f'Authentication failed: {refresh_result.get("message", "Unknown token refresh error") if refresh_result is not None else "Token refresh returned None"}',
                            'status_code': response.status_code
                        }
                else:
                    # Bearer token authentication - no refresh available
                    return {
                        'success': False,
                        'error': 'Bearer token authentication failed. Please check your bearer token.',
                        'status_code': response.status_code
                    }
            elif response.status_code == 403:
                return {
                    'success': False,
                    'error': 'Access forbidden. Check your API key permissions.',
                    'status_code': response.status_code
                }
            elif response.status_code == 404:
                return {
                    'success': False,
                    'error': 'API endpoint not found. Check the base URL.',
                    'status_code': response.status_code
                }
            elif response.status_code == 429:
                return {
                    'success': False,
                    'error': 'Rate limit exceeded. Please retry later.',
                    'status_code': response.status_code
                }
            elif response.status_code >= 500:
                return {
                    'success': False,
                    'error': f'Server error: HTTP {response.status_code}',
                    'status_code': response.status_code
                }
            else:
                # Other error codes
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}: {response.text[:200]}',
                    'status_code': response.status_code
                }
                
        except requests.exceptions.Timeout:
            return {
                'success': False,
                'error': 'Request timeout. Please try again.',
                'status_code': None
            }
        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'error': 'Connection error. Check your network connectivity.',
                'status_code': None
            }
        except requests.exceptions.RequestException as e:
            error_detail = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                except (json.JSONDecodeError, ValueError):
                    error_detail = e.response.text
            return {
                'success': False,
                'error': error_detail,
                'status_code': getattr(e.response, 'status_code', None)
            }
        except Exception as e:
            # Catch any other unexpected errors
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'status_code': None
            }
    
    def bulk_create_loads(self, loads_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create multiple loads with detailed results"""
        results = []
        
        for i, load_data in enumerate(loads_data):
            result = self.create_load(load_data)
            result['row_index'] = i + 1
            results.append(result)
            
            # Note: Removed artificial delay - implement proper rate limiting if needed
            # import time
            # time.sleep(0.1)
        
        return results
    
    def validate_connection(self) -> Dict[str, Any]:
        """Test API connection and credentials using minimal required payload"""
        # Check authentication setup
        if self.auth_type == 'api_key':
            # For API key auth, check if token refresh was successful
            if not self.bearer_token:
                return {'success': False, 'message': 'Token refresh failed. Please check your API key.'}
        elif self.auth_type == 'bearer_token':
            # For bearer token auth, check if token was provided
            if not self.bearer_token:
                return {'success': False, 'message': 'Bearer token not provided.'}
        
        try:
            # Minimal payload with only required top-level objects and core load fields
            test_payload = {
                "load": {
                    "loadNumber": "LOAD123456",
                    "mode": "FTL",
                    "rateType": "SPOT",
                    "status": "DRAFT",
                    "equipment": {
                        "equipmentType": "DRY_VAN"
                    },
                    "route": [
                        {
                            "sequence": 1,
                            "stopActivity": "PICKUP",
                            "address": {
                                "street1": "123 Main St",
                                "city": "Chicago",
                                "stateOrProvince": "IL",
                                "postalCode": "60601",
                                "country": "US"
                            },
                            "expectedArrivalWindowStart": "2025-08-01T08:00:00Z",
                            "expectedArrivalWindowEnd": "2025-08-01T10:00:00Z"
                        },
                        {
                            "sequence": 2,
                            "stopActivity": "DELIVERY",
                            "address": {
                                "street1": "456 Oak Ave",
                                "city": "Milwaukee",
                                "stateOrProvince": "WI",
                                "postalCode": "53202",
                                "country": "US"
                            },
                            "expectedArrivalWindowStart": "2025-08-02T14:00:00Z",
                            "expectedArrivalWindowEnd": "2025-08-02T16:00:00Z"
                        }
                    ],
                    "items": [
                        {
                            "quantity": 1,
                            "totalWeightLbs": 1000
                        }
                    ]
                },
                "brokerage": {
                    "contacts": [
                        {
                            "name": "Jane Broker",
                            "email": "jane.broker@example.com",
                            "phone": "+15555551234",
                            "role": "ACCOUNT_MANAGER"
                        }
                    ]
                },
                "customer": {
                    "customerId": "cust-0001",
                    "name": "Acme Corporation"
                }
            }
            
            response = self.session.post(f"{self.base_url}/v2/loads", json=test_payload, timeout=30)
            
            # Handle different response codes
            if response.status_code == 201:
                try:
                    response_data = response.json()
                    # Extract load number from response
                    load_number = (response_data.get('loadNumber') or 
                                 response_data.get('load', {}).get('loadNumber') or
                                 response_data.get('id'))
                    return {'success': True, 'message': f'Connection successful! Load would be created.\nAPI Response: {response_data}'}
                except (json.JSONDecodeError, ValueError) as json_error:
                    logging.warning(f"Could not parse HTTP 201 response as JSON: {json_error}")
                    return {'success': True, 'message': f'Connection successful! Load would be created (HTTP 201 - Created)'}
            elif response.status_code == 200:
                try:
                    response_data = response.json()
                    # Extract load number from response
                    load_number = (response_data.get('loadNumber') or 
                                 response_data.get('load', {}).get('loadNumber') or
                                 response_data.get('id'))
                    return {'success': True, 'message': f'Connection successful! API responded with: {response_data}'}
                except (json.JSONDecodeError, ValueError) as json_error:
                    logging.warning(f"Could not parse HTTP 200 response as JSON: {json_error}")
                    return {'success': True, 'message': f'Connection successful! API responded (HTTP 200 - OK)'}
            elif response.status_code == 204:
                return {'success': True, 'message': 'Connection successful! (HTTP 204 - No Content - API accepted the request)'}
            elif response.status_code == 401:
                # Handle 401 based on auth type
                if self.auth_type == 'api_key':
                    # Try to refresh token once on 401 for API key auth
                    refresh_result = self._refresh_token()
                    if refresh_result and refresh_result.get('success'):
                        # Retry the request with new token
                        response = self.session.post(f"{self.base_url}/v2/loads", json=test_payload, timeout=30)
                        if response.status_code in [200, 201, 204]:
                            return {'success': True, 'message': 'Connection successful! (Token refreshed)'}
                        else:
                            return {'success': False, 'message': 'Authentication failed even after token refresh. Please check your API key.'}
                    else:
                        return {'success': False, 'message': f'Authentication failed. Token refresh error: {refresh_result.get("message", "Unknown token refresh error") if refresh_result is not None else "Token refresh returned None"}'}
                else:
                    # Bearer token authentication - no refresh available
                    return {'success': False, 'message': 'Bearer token authentication failed. Please check your bearer token.'}
            elif response.status_code == 403:
                return {'success': False, 'message': 'Access forbidden. Check your API key permissions.'}
            elif response.status_code == 404:
                return {'success': False, 'message': 'API endpoint not found. Check the base URL.'}
            elif response.status_code == 400:
                try:
                    error_details = response.json()
                    return {
                        'success': False,
                        'message': f'Bad request: {error_details}',
                        'status_code': response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'message': f'Bad request: {response.text}',
                        'status_code': response.status_code
                    }
            elif response.status_code == 422:
                try:
                    error_details = response.json()
                    return {
                        'success': False,
                        'message': f'Validation error: {error_details}',
                        'status_code': response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'message': f'Validation error: {response.text}',
                        'status_code': response.status_code
                    }
            else:
                # Other error codes
                try:
                    error_details = response.json()
                    return {
                        'success': False,
                        'message': f'API error: {error_details}',
                        'status_code': response.status_code
                    }
                except json.JSONDecodeError:
                    return {
                        'success': False,
                        'message': f'API error: {response.text}',
                        'status_code': response.status_code
                    }
                
        except requests.exceptions.Timeout:
            return {'success': False, 'message': 'Connection timeout. Check your network connection.'}
        except requests.exceptions.SSLError:
            return {'success': False, 'message': 'SSL certificate error. Check the API URL.'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'message': 'Connection error. Check the API URL and network connectivity.'}
        except requests.exceptions.RequestException as e:
            return {'success': False, 'message': f'Request error: {str(e)}'}
        except Exception as e:
            return {'success': False, 'message': f'Unexpected error: {str(e)}'} 