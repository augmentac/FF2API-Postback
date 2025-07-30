#!/usr/bin/env python3
"""
Debug what's actually stored in the database configuration
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def debug_database_config():
    """Debug what configurations are stored in the database"""
    print("=== Database Configuration Debug ===")
    
    try:
        from src.backend.database import DatabaseManager
        
        db_manager = DatabaseManager()
        brokerage_name = 'augment-brokerage'
        
        print(f"🔍 Looking for configurations for brokerage: {brokerage_name}")
        
        # Get all configurations for this brokerage
        configurations = db_manager.get_brokerage_configurations(brokerage_name)
        
        print(f"📦 Found {len(configurations)} configurations:")
        
        for i, config in enumerate(configurations):
            print(f"\n--- Configuration {i+1} ---")
            print(f"  ID: {config.get('id')}")
            print(f"  Name: {config.get('name')}")
            print(f"  Description: {config.get('description', 'No description')}")
            print(f"  Auth Type: {config.get('auth_type', 'Not set')}")
            print(f"  Created: {config.get('created_at')}")
            print(f"  Last Used: {config.get('last_used_at')}")
            
            # Get the full configuration details
            try:
                full_config = db_manager.get_brokerage_configuration(brokerage_name, config['name'])
                if full_config:
                    api_creds = full_config.get('api_credentials', {})
                    print(f"  API Credentials Keys: {list(api_creds.keys())}")
                    if 'api_key' in api_creds:
                        api_key = api_creds['api_key']
                        print(f"  API Key (first 15 chars): '{api_key[:15]}...'")
                        print(f"  API Key (full length): {len(api_key)} characters")
                    if 'base_url' in api_creds:
                        print(f"  Base URL: {api_creds['base_url']}")
                    
                    bearer_token = full_config.get('bearer_token')
                    if bearer_token:
                        print(f"  Bearer Token (first 15 chars): '{bearer_token[:15]}...'")
                        print(f"  Bearer Token (full length): {len(bearer_token)} characters")
                    else:
                        print(f"  Bearer Token: None")
                        
                    print(f"  Field Mappings: {len(full_config.get('field_mappings', {}))} fields mapped")
                else:
                    print(f"  ❌ Could not retrieve full configuration details")
                    
            except Exception as e:
                print(f"  ❌ Error retrieving configuration details: {e}")
        
        if not configurations:
            print("❌ No configurations found in database!")
            print("This suggests the configuration was never saved properly.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error debugging database config: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_database_config()
    exit(0 if success else 1)