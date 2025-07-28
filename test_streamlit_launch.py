#!/usr/bin/env python3
"""
Test launching the unified Streamlit app.
"""

import sys
import os

# Add paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'src'))

def test_streamlit_app_import():
    """Test importing the main streamlit app."""
    print("Testing main Streamlit app import...")
    
    try:
        # Test main app import
        from streamlit_app import main
        print("✅ Main streamlit_app imported successfully")
        
        # Test unified app import
        from src.frontend.unified_app import main as unified_main
        print("✅ Unified app imported successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ Streamlit app import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("STREAMLIT APP IMPORT TEST")
    print("=" * 60)
    
    success = test_streamlit_app_import()
    
    if success:
        print("\n✅ App imports successful - ready to run with 'streamlit run streamlit_app.py'")
    else:
        print("\n❌ App import failed - check error messages above")
    
    print("=" * 60)