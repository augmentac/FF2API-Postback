#!/usr/bin/env python3
"""
Debug script to identify why Estes carrier auto-mapping is not working.
This script will systematically test each step of the carrier mapping process.
"""

import sys
import os
import pandas as pd
import sqlite3
from pathlib import Path

# Add the src paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'backend'))
sys.path.append(os.path.dirname(__file__))

from src.backend.database import DatabaseManager
from src.backend.data_processor import DataProcessor
from carrier_config_parser import carrier_config_parser

def test_estes_mapping_debug():
    """Comprehensive debug test for Estes carrier mapping"""
    print("=== ESTES CARRIER AUTO-MAPPING DEBUG ===\n")
    
    # 1. Test carrier config parser
    print("1. TESTING CARRIER CONFIG PARSER")
    print("-" * 40)
    
    carrier_names = carrier_config_parser.get_carrier_list()
    estes_carriers = [name for name in carrier_names if 'Estes' in name or 'ESTES' in name.upper()]
    print(f"Found Estes carriers in config: {estes_carriers}")
    
    # Test fuzzy matching
    test_inputs = ['Estes', 'ESTES', 'estes', 'Estes Express', 'EXLA']
    print("\nFuzzy matching tests:")
    for test_input in test_inputs:
        match = carrier_config_parser.find_best_carrier_match(test_input, carrier_names)
        print(f'  "{test_input}" -> {match}')
    
    # 2. Test database setup
    print("\n2. TESTING DATABASE SETUP")
    print("-" * 40)
    
    db_manager = DatabaseManager()
    
    # Check if database exists
    db_path = db_manager.db_path
    print(f"Database path: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    
    if os.path.exists(db_path):
        # Check tables
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"Database tables: {[table[0] for table in tables]}")
        
        # Check brokerage_carrier_config table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='brokerage_carrier_config'")
        if cursor.fetchone():
            cursor.execute("SELECT * FROM brokerage_carrier_config")
            config_rows = cursor.fetchall()
            print(f"Carrier config rows: {len(config_rows)}")
            for row in config_rows:
                print(f"  Brokerage: {row[0]}, Auto-mapping enabled: {row[1]}")
        else:
            print("⚠️  brokerage_carrier_config table does not exist!")
        
        # Check brokerage_carrier_mappings table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='brokerage_carrier_mappings'")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM brokerage_carrier_mappings")
            mapping_count = cursor.fetchone()[0]
            print(f"Total carrier mappings: {mapping_count}")
            
            cursor.execute("SELECT DISTINCT brokerage_name FROM brokerage_carrier_mappings")
            brokerages = cursor.fetchall()
            print(f"Brokerages with mappings: {[b[0] for b in brokerages]}")
            
            # Check for Estes mappings
            cursor.execute("SELECT * FROM brokerage_carrier_mappings WHERE carrier_name LIKE '%Estes%'")
            estes_mappings = cursor.fetchall()
            print(f"Estes mappings found: {len(estes_mappings)}")
            for mapping in estes_mappings:
                print(f"  Brokerage: {mapping[0]}, Carrier: {mapping[2]}, Active: {mapping[10]}")
        else:
            print("⚠️  brokerage_carrier_mappings table does not exist!")
        
        conn.close()
    
    # 3. Test with sample data
    print("\n3. TESTING WITH SAMPLE DATA")
    print("-" * 40)
    
    # Create sample CSV data with Estes
    sample_data = pd.DataFrame({
        'carrier_name': ['Estes', 'FedEx Freight', 'Old Dominion'],
        'pickup_city': ['Dallas', 'Houston', 'Austin'],
        'delivery_city': ['Fort Worth', 'San Antonio', 'El Paso'],
        'weight': [1000, 1500, 2000]
    })
    
    print("Sample data:")
    print(sample_data.to_string(index=False))
    
    # Test data processing
    data_processor = DataProcessor()
    
    # First, let's check what brokerages exist and their config
    test_brokerage = "TestBrokerage"
    
    print(f"\n4. TESTING CARRIER MAPPING FOR BROKERAGE: {test_brokerage}")
    print("-" * 40)
    
    # Get carrier mapping config
    config = db_manager.get_carrier_mapping_config(test_brokerage)
    print(f"Auto-mapping enabled: {config.get('enable_auto_carrier_mapping', False)}")
    
    # Get carrier mappings
    carrier_mappings = db_manager.get_carrier_mappings(test_brokerage)
    print(f"Carrier mappings count: {len(carrier_mappings) if carrier_mappings else 0}")
    
    if carrier_mappings:
        print("Available carrier mappings:")
        for carrier_id, mapping in carrier_mappings.items():
            print(f"  {carrier_id}: {mapping.get('carrier_name', 'Unknown')}")
    else:
        print("⚠️  No carrier mappings found for this brokerage!")
        print("\n🔧 SUGGESTED FIX: Import carrier template first")
        
        # Let's try importing carrier template
        print("\n5. IMPORTING CARRIER TEMPLATE")
        print("-" * 40)
        
        # Get brokerage template
        template = carrier_config_parser.get_brokerage_template()
        print(f"Template carriers available: {len(template)}")
        
        # Enable auto-mapping
        db_manager.set_carrier_mapping_config(test_brokerage, True)
        print(f"✅ Enabled auto-mapping for {test_brokerage}")
        
        # Import carrier template
        db_manager.import_carrier_template(test_brokerage, template)
        print(f"✅ Imported {len(template)} carriers for {test_brokerage}")
        
        # Re-test
        carrier_mappings = db_manager.get_carrier_mappings(test_brokerage)
        print(f"Carrier mappings after import: {len(carrier_mappings) if carrier_mappings else 0}")
    
    # 6. Test the actual carrier mapping process
    print("\n6. TESTING CARRIER MAPPING PROCESS")
    print("-" * 40)
    
    try:
        mapped_df = data_processor.apply_carrier_mapping(sample_data, test_brokerage, db_manager)
        
        print("Mapping results:")
        print(f"Original columns: {list(sample_data.columns)}")
        print(f"Mapped columns: {list(mapped_df.columns)}")
        
        # Check if carrier fields were added
        carrier_fields = [col for col in mapped_df.columns if col.startswith('carrier_')]
        print(f"Carrier fields added: {carrier_fields}")
        
        if carrier_fields:
            print("\nFirst row carrier data:")
            first_row = mapped_df.iloc[0]
            for field in carrier_fields:
                value = first_row.get(field, 'N/A')
                print(f"  {field}: {value}")
        else:
            print("⚠️  No carrier fields were added to the DataFrame!")
            
            # Debug why mapping failed
            print("\n🔍 DEBUGGING MAPPING FAILURE:")
            
            # Check column matching
            potential_carrier_columns = [
                'carrier_name', 'carrier', 'scac', 'carrier_scac', 
                'Carrier', 'Carrier Name', 'SCAC', 'Carrier SCAC'
            ]
            
            found_columns = [col for col in potential_carrier_columns if col in sample_data.columns]
            print(f"Potential carrier columns in data: {found_columns}")
            
            if found_columns:
                for col in found_columns:
                    values = sample_data[col].tolist()
                    print(f"Values in {col}: {values}")
            else:
                print("❌ No carrier columns found in sample data!")
                print("This explains why auto-mapping failed.")
                
    except Exception as e:
        print(f"❌ Error during carrier mapping: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_estes_mapping_debug()