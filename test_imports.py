#!/usr/bin/env python3
"""
Test script to verify imports work correctly.
"""
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")
print(f"Python path: {sys.path[:3]}")  # Show first 3 entries

try:
    print("\n=== Testing streamlit_google_sso import ===")
    from streamlit_google_sso import streamlit_google_sso
    print(f"✅ streamlit_google_sso imported: {type(streamlit_google_sso)}")
    print(f"   is_configured method: {hasattr(streamlit_google_sso, 'is_configured')}")
    print(f"   _generate_auth_url method: {hasattr(streamlit_google_sso, '_generate_auth_url')}")
    print(f"   _handle_manual_auth_code method: {hasattr(streamlit_google_sso, '_handle_manual_auth_code')}")
    
except ImportError as e:
    print(f"❌ streamlit_google_sso import failed: {e}")

try:
    print("\n=== Testing email_monitor import ===")
    from email_monitor import email_monitor
    print(f"✅ email_monitor imported: {type(email_monitor)}")
    print(f"   get_monitoring_status method: {hasattr(email_monitor, 'get_monitoring_status')}")
    print(f"   configure_oauth_monitoring method: {hasattr(email_monitor, 'configure_oauth_monitoring')}")
    print(f"   start_monitoring method: {hasattr(email_monitor, 'start_monitoring')}")
    print(f"   check_inbox_now method: {hasattr(email_monitor, 'check_inbox_now')}")
    
except ImportError as e:
    print(f"❌ email_monitor import failed: {e}")

print("\n=== Testing from src/frontend context ===")
# Simulate the import context from src/frontend/enhanced_ff2api.py
src_frontend_dir = os.path.join(project_root, 'src', 'frontend')
print(f"src/frontend directory: {src_frontend_dir}")

# Change to the src/frontend directory context
os.chdir(src_frontend_dir)
print(f"Current working directory: {os.getcwd()}")

try:
    # Reset and test imports as if from enhanced_ff2api.py
    if 'streamlit_google_sso' in sys.modules:
        del sys.modules['streamlit_google_sso']
    if 'email_monitor' in sys.modules:
        del sys.modules['email_monitor']
    
    from streamlit_google_sso import streamlit_google_sso
    from email_monitor import email_monitor
    
    print("✅ Both imports successful from src/frontend context")
    
except ImportError as e:
    print(f"❌ Import failed from src/frontend context: {e}")
    
    # Test with project_root in path
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    try:
        from streamlit_google_sso import streamlit_google_sso  
        from email_monitor import email_monitor
        print("✅ Both imports successful after adding project_root to path")
        
    except ImportError as e2:
        print(f"❌ Still failed after adding project_root: {e2}")

print(f"\n=== Final status ===")
print(f"streamlit_google_sso type: {type(streamlit_google_sso) if 'streamlit_google_sso' in locals() else 'Not available'}")
print(f"email_monitor type: {type(email_monitor) if 'email_monitor' in locals() else 'Not available'}")