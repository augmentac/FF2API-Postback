"""
Fixed Email Automation Sidebar Functions
This contains the cleaned up email automation sidebar implementation.
"""

import streamlit as st
import logging
from typing import Dict, Any
from datetime import datetime
import time

logger = logging.getLogger(__name__)

def _render_email_automation_sidebar():
    """Render email automation configuration in sidebar"""
    
    st.markdown("---")
    st.markdown("### 📧 Email Automation")
    
    brokerage_name = st.session_state.get('brokerage_name', 'default')
    
    try:
        # Simplified OAuth status check
        auth_key = f'gmail_auth_{brokerage_name.replace("-", "_")}'
        gmail_oauth_credentials = st.session_state.get(auth_key, {})
        
        # Check if Gmail is authenticated
        gmail_authenticated = (
            gmail_oauth_credentials.get('authenticated', False) and 
            gmail_oauth_credentials.get('oauth_active', False) and
            gmail_oauth_credentials.get('user_email') and
            gmail_oauth_credentials.get('user_email') != 'user@gmail.com'
        )
        
        # Check email monitor status
        monitor_status = _get_email_monitor_status()
        monitor_running = monitor_status.get('active', False)
        
        if gmail_authenticated:
            user_email = gmail_oauth_credentials.get('user_email', 'Gmail account')
            st.success(f"✅ **Gmail Connected**")
            st.caption(f"📧 {user_email}")
            
            # Configure email monitoring if not already configured
            if not monitor_status.get('configured', False):
                _configure_email_monitoring(brokerage_name, gmail_oauth_credentials)
            
            # Show automation status
            if monitor_running:
                st.success("🟢 **Email Automation Active**")
                st.caption("Monitoring Gmail for freight emails")
                
                # Show automation controls
                _render_email_automation_controls(brokerage_name)
            else:
                st.info("📧 **Gmail Connected - Automation Inactive**")
                if st.button("▶️ Start Email Monitoring", key="start_monitoring"):
                    _start_email_monitoring(brokerage_name)
                    st.rerun()
        else:
            # Gmail not authenticated - show setup
            _render_gmail_setup_interface(brokerage_name)
            
    except Exception as e:
        st.error(f"❌ Email automation error: {str(e)}")
        logger.error(f"Email automation sidebar error: {e}")


def _get_email_monitor_status() -> Dict[str, Any]:
    """Get simplified email monitor status"""
    try:
        # Import email_monitor here to avoid import issues
        from email_monitor import email_monitor
        
        if hasattr(email_monitor, 'get_monitoring_status'):
            status = email_monitor.get_monitoring_status()
            if isinstance(status, dict):
                return {
                    'active': status.get('monitoring_active', False),
                    'configured': status.get('oauth_credentials_count', 0) > 0,
                    'brokerages': status.get('monitored_brokerages', [])
                }
    except Exception as e:
        logger.warning(f"Could not get email monitor status: {e}")
    
    return {'active': False, 'configured': False, 'brokerages': []}


def _configure_email_monitoring(brokerage_name: str, oauth_credentials: Dict[str, Any]):
    """Configure email monitoring with OAuth credentials"""
    try:
        from email_monitor import email_monitor
        from streamlit_google_sso import streamlit_google_sso
        
        st.info("🔄 Configuring email monitoring...")
        
        # Get complete OAuth credentials
        if hasattr(streamlit_google_sso, '_get_stored_auth'):
            stored_creds = streamlit_google_sso._get_stored_auth(brokerage_name)
            if stored_creds:
                oauth_credentials = {**oauth_credentials, **stored_creds}
        
        # Configure email monitor
        config_result = email_monitor.configure_oauth_monitoring(
            brokerage_key=brokerage_name,
            oauth_credentials=oauth_credentials,
            email_filters={
                'sender_filter': st.session_state.get('email_sender_filter', ''),
                'subject_filter': st.session_state.get('email_subject_filter', '')
            }
        )
        
        if config_result and config_result.get('success'):
            st.success("✅ Email monitoring configured")
            return True
        else:
            error_msg = config_result.get('message', 'Unknown error') if config_result else 'No response'
            st.warning(f"⚠️ Configuration failed: {error_msg}")
            return False
            
    except Exception as e:
        st.error(f"❌ Configuration error: {str(e)}")
        return False


def _start_email_monitoring(brokerage_name: str):
    """Start email monitoring service"""
    try:
        from email_monitor import email_monitor
        
        start_result = email_monitor.start_monitoring()
        
        if start_result and start_result.get('success'):
            st.success("✅ Email monitoring started")
        else:
            error_msg = start_result.get('message', 'Unknown error') if start_result else 'No response'
            st.error(f"❌ Failed to start monitoring: {error_msg}")
            
    except Exception as e:
        st.error(f"❌ Start monitoring error: {str(e)}")


def _render_email_automation_controls(brokerage_name: str):
    """Render email automation control buttons"""
    with st.expander("⚙️ Email Automation Controls", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📨 Check Inbox Now", key="check_inbox_now"):
                _check_inbox_now(brokerage_name)
        
        with col2:
            if st.button("⏸️ Stop Monitoring", key="stop_monitoring"):
                _stop_email_monitoring()


def _check_inbox_now(brokerage_name: str):
    """Manually check inbox for new emails"""
    try:
        from email_monitor import email_monitor
        
        with st.spinner("🔍 Checking Gmail inbox..."):
            if hasattr(email_monitor, 'check_inbox_now'):
                result = email_monitor.check_inbox_now(brokerage_name)
                
                if result and result.success:
                    if result.processed_count > 0:
                        st.success(f"✅ Processed {result.processed_count} file(s)")
                        # Store results for display
                        st.session_state.email_processing_results = {
                            'success': True,
                            'processed_files': result.file_info.get('processed_files', []) if result.file_info else [],
                            'timestamp': datetime.now(),
                            'source': 'manual_check'
                        }
                        st.session_state.show_email_results_dashboard = True
                        st.rerun()
                    else:
                        st.info("📭 No new emails with attachments found")
                else:
                    error_msg = result.message if result else 'No response'
                    st.error(f"❌ Inbox check failed: {error_msg}")
            else:
                st.error("❌ Email monitor does not support manual inbox checking")
                
    except Exception as e:
        st.error(f"❌ Error checking inbox: {str(e)}")


def _stop_email_monitoring():
    """Stop email monitoring service"""
    try:
        from email_monitor import email_monitor
        
        stop_result = email_monitor.stop_monitoring()
        
        if stop_result and stop_result.get('success'):
            st.success("⏸️ Email monitoring stopped")
            st.rerun()
        else:
            error_msg = stop_result.get('message', 'Unknown error') if stop_result else 'No response'
            st.error(f"❌ Failed to stop monitoring: {error_msg}")
            
    except Exception as e:
        st.error(f"❌ Stop monitoring error: {str(e)}")


def _render_gmail_setup_interface(brokerage_name: str):
    """Render Gmail setup interface"""
    try:
        from streamlit_google_sso import streamlit_google_sso
        
        if not hasattr(streamlit_google_sso, 'is_configured') or not streamlit_google_sso.is_configured():
            st.info("🔐 **Gmail Setup Required**")
            st.markdown("Gmail OAuth is not configured. Check your Google API credentials.")
            return
        
        st.info("🔐 **Connect Gmail Account**")
        st.markdown("Enable email automation by connecting your Gmail account.")
        
        # Generate OAuth URL
        auth_url = streamlit_google_sso._generate_auth_url(brokerage_name)
        
        if auth_url:
            st.markdown(f"[🔐 Connect Gmail Account]({auth_url})", unsafe_allow_html=True)
            
            # Manual auth code input
            with st.expander("Manual Setup (if link doesn't work)", expanded=False):
                st.markdown(f"1. Visit: {auth_url}")
                st.markdown("2. Complete authentication")
                st.markdown("3. Copy the authorization code")
                
                auth_code = st.text_input(
                    "Authorization Code:",
                    placeholder="Paste authorization code here",
                    key=f"auth_code_{brokerage_name}"
                )
                
                if st.button("✅ Complete Setup", key=f"complete_setup_{brokerage_name}"):
                    if auth_code:
                        _process_auth_code(brokerage_name, auth_code)
                    else:
                        st.error("Please enter the authorization code")
        else:
            st.error("❌ Could not generate OAuth URL")
            
    except Exception as e:
        st.error(f"❌ Gmail setup error: {str(e)}")


def _process_auth_code(brokerage_name: str, auth_code: str):
    """Process OAuth authorization code"""
    try:
        from streamlit_google_sso import streamlit_google_sso
        
        with st.spinner("🔄 Processing authentication..."):
            result = streamlit_google_sso._handle_manual_auth_code(brokerage_name, auth_code)
            
            if result.get('success'):
                st.success("✅ Gmail authentication successful!")
                st.info("Email automation is now available. Page will refresh...")
                time.sleep(2)
                st.rerun()
            else:
                error_msg = result.get('message', 'Unknown error')
                st.error(f"❌ Authentication failed: {error_msg}")
                
    except Exception as e:
        st.error(f"❌ Authentication processing error: {str(e)}")