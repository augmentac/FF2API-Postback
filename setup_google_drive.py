#!/usr/bin/env python3
"""
Streamlit page for Google Drive backup setup.
Run this as a standalone app: streamlit run setup_google_drive.py
"""

import streamlit as st
from google_drive_auth import google_drive_auth

def main():
    st.set_page_config(
        page_title="Google Drive Backup Setup",
        page_icon="🔄",
        layout="wide"
    )
    
    st.title("🔄 Google Drive Backup Setup")
    st.markdown("Configure Google Drive authentication for SQLite database backup")
    
    # Check current configuration status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        has_credentials = google_drive_auth.is_configured()
        st.metric(
            "OAuth Credentials", 
            "✅ Configured" if has_credentials else "❌ Missing",
            help="client_id and client_secret in secrets"
        )
    
    with col2:
        has_tokens = google_drive_auth.has_valid_tokens()
        st.metric(
            "Access Tokens",
            "✅ Ready" if has_tokens else "❌ Required", 
            help="access_token and refresh_token in secrets"
        )
    
    with col3:
        backup_ready = has_credentials and has_tokens
        st.metric(
            "Backup System",
            "✅ Active" if backup_ready else "⏳ Setup Required",
            help="Ready for database backup/restore"
        )
    
    st.divider()
    
    # Show authentication interface
    if google_drive_auth.render_auth_interface():
        st.success("🎉 Google Drive backup is ready to use!")
        
        # Show next steps
        st.markdown("### ✅ Setup Complete!")
        st.markdown("""
        Your database backup system is now configured. The system will:
        
        - 🔄 **Automatically restore** your database when the app starts
        - 💾 **Backup changes** every 15 minutes  
        - 📤 **Upload after operations** (saves, processing, configuration)
        - 🔐 **Refresh tokens** automatically when they expire
        
        **Next Steps:**
        1. Restart your main FF2API application
        2. Watch the logs for `[db_manager]` messages
        3. Check Google Drive for the `ff.sqlite` file after first backup
        """)
    else:
        st.info("👆 Complete the authentication steps above to enable database backup.")

if __name__ == "__main__":
    main()