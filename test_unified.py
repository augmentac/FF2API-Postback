#!/usr/bin/env python3
"""
Test script for unified load processing system.
"""

import sys
import os

# Add paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'src'))

def test_imports():
    """Test all critical imports for unified system."""
    print("Testing imports for unified load processing system...")
    
    try:
        print("Testing core backend...")
        from src.backend.unified_processor import UnifiedLoadProcessor, ProcessingMode
        print("✅ UnifiedLoadProcessor imported successfully")
        
        from src.backend.database import DatabaseManager
        print("✅ DatabaseManager imported successfully")
        
        print("\nTesting UI components...")
        from src.frontend.ui_components import get_full_api_schema
        print("✅ UI components imported successfully")
        
        print("\nTesting credential manager...")
        from credential_manager import credential_manager
        print("✅ Credential manager imported successfully")
        
        print("\nTesting load mapping...")
        from load_id_mapper import LoadIDMapper
        print("✅ Load ID mapper imported successfully")
        
        print("\nTesting enrichment...")
        from enrichment.manager import EnrichmentManager
        print("✅ Enrichment manager imported successfully")
        
        print("\nTesting postback...")
        from postback.router import PostbackRouter
        print("✅ Postback router imported successfully")
        
        print("\n✅ All critical imports successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        return False

def test_processor():
    """Test unified processor initialization."""
    print("\nTesting unified processor initialization...")
    
    try:
        from src.backend.unified_processor import UnifiedLoadProcessor
        
        # Test configuration
        config = {
            'brokerage_key': 'test-brokerage',
            'api_timeout': 30,
            'retry_count': 3,
            'enrichment': {'sources': []},
            'postback': {'handlers': []}
        }
        
        # Initialize processor
        processor = UnifiedLoadProcessor(config, 'manual')
        print("✅ Processor initialized in manual mode")
        
        # Test mode switching
        processor.switch_processing_mode('endtoend')
        print("✅ Successfully switched to end-to-end mode")
        
        # Test mode config
        mode_config = processor.get_processing_mode_config()
        print(f"✅ Mode config retrieved: {mode_config.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Processor test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("UNIFIED LOAD PROCESSING SYSTEM TEST")
    print("=" * 60)
    
    # Test imports
    imports_ok = test_imports()
    
    if imports_ok:
        # Test processor functionality
        processor_ok = test_processor()
        
        if processor_ok:
            print("\n" + "=" * 60)
            print("🎉 ALL TESTS PASSED - System ready for deployment!")
            print("=" * 60)
        else:
            print("\n" + "=" * 60)
            print("⚠️  PARTIAL SUCCESS - Imports work but processor has issues")
            print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("❌ TESTS FAILED - Critical import issues need to be resolved")
        print("=" * 60)

if __name__ == "__main__":
    main()