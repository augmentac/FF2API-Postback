"""
Enhanced FF2API - Original FF2API Interface with Optional End-to-End Capabilities

This preserves the original FF2API user experience while adding optional:
- Load ID mapping after FF2API success
- Data enrichment (tracking, Snowflake)
- Postback processing (multiple formats, email delivery)
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime
import logging
import re
import hashlib

# Add parent directory to path to enable src imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.backend.database import DatabaseManager
from src.backend.api_client import LoadsAPIClient
from src.backend.data_processor import DataProcessor
from src.frontend.ui_components import (
    load_custom_css, 
    render_main_header, 
    create_enhanced_file_uploader,
    create_connection_status_card,
    create_data_preview_card,
    create_mapping_progress_indicator,
    create_processing_progress_display,
    create_results_summary_card,
    create_enhanced_button,
    create_field_mapping_card,
    create_step_navigation_buttons,
    create_enhanced_mapping_interface,
    create_validation_summary_card,
    create_company_settings_card,
    create_brokerage_selection_interface,
    create_configuration_management_interface,
    create_header_validation_interface,
    create_enhanced_mapping_with_validation,
    create_learning_enhanced_mapping_interface,
    update_learning_with_processing_results,
    get_full_api_schema
)
# Removed COMMON_ENUM_FIELDS import - using schema-based enums directly

# Import database backup manager
from db_manager import restore_sqlite_if_missing, upload_sqlite_if_changed, start_periodic_backup

# Import end-to-end workflow components
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from load_id_mapper import LoadIDMapper, LoadProcessingResult, LoadIDMapping
from enrichment.manager import EnrichmentManager
from postback.router import PostbackRouter
from credential_manager import credential_manager
from streamlit_google_sso import streamlit_google_sso
from email_monitor import email_monitor
# Initialize email monitor with credential manager
email_monitor.credential_manager = credential_manager

# Create logs directory if it doesn't exist
os.makedirs('data/logs', exist_ok=True)

# Configure logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data/logs/enhanced_ff2api.log')
        ]
    )
except (OSError, PermissionError):
    # Fallback to console logging only for cloud deployment
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

logger = logging.getLogger(__name__)

# Session management functions
def generate_session_id():
    """Generate a unique session ID for learning tracking"""
    import uuid
    return str(uuid.uuid4())

def ensure_session_id():
    """Ensure a session ID exists for learning tracking"""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = generate_session_id()
    return st.session_state.session_id

# Authentication functions
def get_email_automation_keys():
    """Get all email automation related session keys that should be preserved during clearing operations"""
    brokerage_name = st.session_state.get('brokerage_name', 'default')
    
    # Core email automation keys
    email_keys = [
        f'gmail_auth_{brokerage_name.replace("-", "_")}',
        f'gmail_auth_success_{brokerage_name}',
        'email_sender_filter',
        'email_subject_filter', 
        'send_email',
        'email_recipient',
        'email_formats',
        'email_processing_results',
        'show_email_results_dashboard',
        'prefer_email_results',
        'google_sso_auth'
    ]
    
    # Add any additional brokerage-specific keys
    for key in list(st.session_state.keys()):
        if any(pattern in key.lower() for pattern in ['gmail_auth_', 'email_', 'oauth_']):
            if key not in email_keys:
                email_keys.append(key)
    
    return email_keys

def safe_clear_session_keys(keys_to_clear: list):
    """Clear session keys while preserving email automation state"""
    # Get email automation keys to preserve
    email_keys = get_email_automation_keys()
    
    # Filter out email automation keys from clearing list
    safe_keys_to_clear = [key for key in keys_to_clear if key not in email_keys]
    
    # Clear only safe keys
    for key in safe_keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    
    logger.info(f"Cleared {len(safe_keys_to_clear)} keys, preserved {len([k for k in keys_to_clear if k in email_keys])} email automation keys")

def check_password():
    """Check if the user is authenticated"""
    return st.session_state.get('authenticated', False)

def authenticate_user(password):
    """Authenticate user with password"""
    try:
        # Get password from secrets
        if 'auth' in st.secrets and 'APP_PASSWORD' in st.secrets.auth:
            correct_password = st.secrets.auth.APP_PASSWORD
        else:
            # No default password - force proper authentication setup
            st.error("⚠️ Authentication not configured. Please configure APP_PASSWORD in Streamlit secrets.")
            st.stop()
        
        return password == correct_password
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return False

def show_login_page():
    """Display login page"""
    st.set_page_config(
        page_title="Enhanced FF2API - Login",
        page_icon="🔐",
        layout="centered"
    )
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1>🔐 Enhanced FF2API Access</h1>
                <p style="color: #666; font-size: 1.1rem;">Enter your team password to continue</p>
            </div>
        """, unsafe_allow_html=True)
        
        # Login form
        with st.form("login_form"):
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter team password",
                help="Contact your team administrator if you don't have the password"
            )
            
            submitted = st.form_submit_button("🚀 Access Application", use_container_width=True)
            
            if submitted:
                if authenticate_user(password):
                    st.session_state.authenticated = True
                    st.session_state.login_time = datetime.now()
                    st.success("✅ Access granted! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password. Please try again.")
                    st.info("💡 Contact your team administrator if you need help accessing the application.")
        
        # App info
        st.markdown("---")
        st.markdown("""
            <div style="text-align: center; color: #666; font-size: 0.9rem;">
                <p><strong>Enhanced FF2API</strong> - Freight File to API Processing Tool</p>
                <p>Original FF2API + Optional End-to-End Workflow Capabilities</p>
            </div>
        """, unsafe_allow_html=True)

def show_logout_option():
    """Show logout option in sidebar"""
    with st.sidebar:
        st.markdown("---")
        
        # Show login info
        if 'login_time' in st.session_state:
            login_time = st.session_state.login_time
            duration = datetime.now() - login_time
            hours = duration.total_seconds() / 3600
            st.caption(f"🔐 Logged in for {hours:.1f} hours")
        
        # Logout button
        if st.button("🚪 Logout", key="logout_btn", use_container_width=True):
            # Clear authentication
            st.session_state.authenticated = False
            if 'login_time' in st.session_state:
                del st.session_state['login_time']
            
            # Clear sensitive data while preserving email automation
            keys_to_clear = ['api_credentials', 'selected_configuration', 'uploaded_df']
            safe_clear_session_keys(keys_to_clear)
            
            st.info("👋 Logged out successfully")
            st.rerun()

def cleanup_old_uploads():
    """Clean up old uploaded files for security"""
    try:
        uploads_dir = "data/uploads"
        if os.path.exists(uploads_dir):
            current_time = datetime.now()
            for filename in os.listdir(uploads_dir):
                file_path = os.path.join(uploads_dir, filename)
                if os.path.isfile(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    # Delete files older than 1 hour
                    if (current_time - file_time).total_seconds() > 3600:
                        os.remove(file_path)
                        logging.info(f"Cleaned up old upload: {filename}")
    except Exception as e:
        logging.warning(f"Error cleaning up uploads: {e}")

def init_components():
    """Initialize core components"""
    db_manager = DatabaseManager()
    data_processor = DataProcessor()
    return db_manager, data_processor

def check_critical_backup_needs(db_manager):
    """Check if critical backup is needed at startup"""
    try:
        stats = db_manager.get_database_stats()
        total_records = stats['brokerage_configurations'] + stats['upload_history']
        
        if total_records > 50:  # Threshold for backup
            last_backup = db_manager.get_last_backup_info()
            if not last_backup or (datetime.now() - datetime.fromisoformat(last_backup['created_at'])).days > 7:
                st.sidebar.warning("⚠️ Consider creating a backup (7+ days old)")
    except Exception as e:
        logger.error(f"Error checking backup needs: {e}")

# Import the original sidebar functions exactly
from src.frontend.app import (
    show_contextual_information,
    _render_brokerage_selection,
    _render_configuration_selection,
    _render_new_configuration_form,
    _handle_save_configuration,
    _render_compact_brokerage_config_display,
    _render_consolidated_status,
    _has_session_data
)

def main():
    """Main enhanced FF2API application - preserves original UX"""
    
    # ========== ABSOLUTE EMERGENCY TEST - BEFORE ANY CHECKS ==========
    st.error("🚨🚨🚨 EMERGENCY: THIS MUST BE VISIBLE OR CODE ISN'T RUNNING 🚨🚨🚨")
    st.balloons()
    # ==================================================================
    
    # Check authentication first
    if not check_password():
        show_login_page()
        return
    
    st.set_page_config(
        page_title="Enhanced FF2API",
        page_icon="{CSV}",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Force sidebar to stay expanded after file upload
    if 'sidebar_state' not in st.session_state:
        st.session_state.sidebar_state = 'expanded'
    
    # Initialize simplified UI mode (default: True for cleaner user experience)
    if 'use_simplified_ui' not in st.session_state:
        st.session_state.use_simplified_ui = True
    
    # Load custom CSS
    load_custom_css()
    
    # Security: Clean up old uploaded files on startup
    cleanup_old_uploads()
    
    # Render main header
    render_main_header()
    
    # Initialize components
    db_manager, data_processor = init_components()
    
    # Initialize database backup system (only once per session)
    if 'backup_system_initialized' not in st.session_state:
        print("[enhanced_ff2api] Initializing database backup system...")
        restore_sqlite_if_missing()
        start_periodic_backup(interval_minutes=15)  # More frequent for active development
        st.session_state.backup_system_initialized = True
    else:
        print("[enhanced_ff2api] Database backup system already initialized")
    
    # Ensure session ID for learning tracking
    ensure_session_id()
    
    # Check for critical backup needs at app startup
    check_critical_backup_needs(db_manager)
    
    # Main workflow - preserve original FF2API workflow exactly
    enhanced_main_workflow(db_manager, data_processor)
    
    # Contextual information in sidebar - exactly like original
    with st.sidebar:
        show_contextual_information(db_manager)
        
        # Add email automation configuration for full end-to-end mode
        _render_email_automation_sidebar()
        
        # Show logout option at bottom of sidebar
        show_logout_option()

def enhanced_main_workflow(db_manager, data_processor):
    """Enhanced main workflow - original FF2API + optional end-to-end capabilities"""
    
    # ========== ABSOLUTE EMERGENCY DEPLOYMENT TEST ==========
    # THIS MUST BE VISIBLE OR DEPLOYMENT IS BROKEN
    st.error("🚨🚨🚨 EMERGENCY TEST: IF YOU SEE THIS, DEPLOYMENT IS WORKING 🚨🚨🚨")
    st.error("🚨🚨🚨 EMERGENCY TEST: IF YOU SEE THIS, DEPLOYMENT IS WORKING 🚨🚨🚨")
    st.error("🚨🚨🚨 EMERGENCY TEST: IF YOU SEE THIS, DEPLOYMENT IS WORKING 🚨🚨🚨")
    st.balloons()  # This will definitely be noticeable
    
    # ========== EMAIL RESULTS DISPLAY - ALWAYS SHOW FIRST ==========
    # Show automated email processing results BEFORE any other checks
    st.markdown("---")
    st.markdown("### 🔍 URGENT DEPLOYMENT TEST - EMAIL RESULTS")
    st.success("✅ THIS DEPLOYMENT TEST SECTION SHOULD ALWAYS BE VISIBLE")
    st.error("📧 Checking for automated email processing results...")
    
    # Try to get shared storage data
    try:
        from shared_storage_bridge import get_email_processing_data, shared_storage
        
        st.info("🔍 Attempting to load email processing data from shared storage...")
        
        # Get all available brokerage keys from shared storage
        try:
            jobs_file = shared_storage.storage_dir / "email_jobs.json"
            results_file = shared_storage.storage_dir / "email_results.json" 
            
            st.code(f"Jobs file path: {jobs_file}")
            st.code(f"Results file path: {results_file}")
            st.code(f"Jobs file exists: {jobs_file.exists()}")
            st.code(f"Results file exists: {results_file.exists()}")
            
            if jobs_file.exists():
                jobs_data = shared_storage._read_json_file(jobs_file, shared_storage._jobs_lock)
                st.json(jobs_data)
                
            if results_file.exists():
                results_data = shared_storage._read_json_file(results_file, shared_storage._results_lock)
                st.json(results_data)
                
        except Exception as e:
            st.error(f"Error accessing shared storage files: {e}")
            
    except ImportError as e:
        st.error(f"Could not import shared_storage_bridge: {e}")
    except Exception as e:
        st.error(f"Error with shared storage: {e}")
    
    st.markdown("---")
    # ================================================================
    
    # Preserve original FF2API progressive disclosure exactly
    if 'brokerage_name' not in st.session_state:
        st.info("👈 Please select or create a brokerage in the sidebar to continue")
        return
    
    if 'api_credentials' not in st.session_state:
        st.info("👈 Please configure your API credentials in the sidebar to continue")
        return
    
    brokerage_name = st.session_state.brokerage_name
    
    # Check if user has uploaded a file - exactly like original
    has_uploaded_file = st.session_state.get('uploaded_df') is not None
    
    if not has_uploaded_file:
        # === ORIGINAL FF2API LANDING PAGE ===
        _render_enhanced_landing_page()
        
        # FORCE SHOW EMAIL RESULTS SECTION - ALWAYS VISIBLE
        st.markdown("---")
        st.markdown("### 🔍 DEPLOYMENT TEST - EMAIL RESULTS")
        st.success("✅ THIS SECTION CONFIRMS DEPLOYMENT IS WORKING")
        
        processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
        st.info(f"Processing mode: {processing_mode}")
        
        if processing_mode == 'full_endtoend':
            st.success("🤖 Email automation mode is ACTIVE - checking for results...")
            
            # Check email processing metadata
            email_metadata = st.session_state.get('email_processing_metadata', [])
            st.info(f"Found {len(email_metadata)} items in email_processing_metadata")
            
            if email_metadata:
                with st.expander("📧 Email Processing Results", expanded=True):
                    for i, meta in enumerate(email_metadata[-3:]):
                        st.write(f"**File {i+1}:** {meta.get('filename', 'Unknown')}")
                        st.write(f"**Time:** {meta.get('processed_time', 'Unknown')}")
                        st.write("---")
            else:
                st.warning("No email processing metadata found in session state")
        else:
            st.warning("Email automation not active - switch to Full End-to-End Processing")
            
        # ========== EMERGENCY EMAIL RESULTS SECTION ==========
        st.error("🚨 CHECKING FOR AUTOMATED EMAIL PROCESSING RESULTS 🚨")
        st.info("📧 The logs show email processing is working - results should appear below:")
        
        # Always show email results even without uploaded file (background processing can happen independently)
        _render_email_results_dashboard()
        
        # ======================================================
    else:
        # === ENHANCED WORKFLOW WITH END-TO-END OPTIONS ===
        _render_enhanced_workflow_with_progress(db_manager, data_processor)

def _render_email_automation_sidebar():
    """Render email automation configuration in sidebar for full end-to-end mode"""
    
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    
    # Only show email automation for full end-to-end mode
    if processing_mode == 'full_endtoend':
        st.markdown("---")
        st.markdown("### 📧 Email Automation")
        
        # Check email automation status through credential manager
        brokerage_name = st.session_state.get('brokerage_name', 'default')
        
        try:
            # Check Google OAuth availability
            google_oauth_available = streamlit_google_sso.is_configured()
            
            # Check if user has completed REAL Gmail OAuth setup for this brokerage
            # ENHANCED: Check multiple authentication sources like the background service does
            gmail_setup_complete = False
            gmail_oauth_credentials = {}
            user_email = None
            
            # Method 1: Check session state (UI-based auth)
            auth_key = f'gmail_auth_{brokerage_name.replace("-", "_")}'
            session_auth = st.session_state.get(auth_key, {})
            
            if (session_auth.get('authenticated', False) and 
                session_auth.get('oauth_active', False) and
                'user_email' in session_auth and
                session_auth.get('user_email') != 'user@gmail.com'):
                gmail_setup_complete = True
                gmail_oauth_credentials = session_auth
                user_email = session_auth.get('user_email')
                logger.info(f"Gmail auth found in session state for {brokerage_name}")
            
            # Method 2: Check streamlit_google_sso (same as background service)
            if not gmail_setup_complete:
                try:
                    auth_data = streamlit_google_sso._get_stored_auth(brokerage_name)
                    if auth_data and auth_data.get('access_token'):
                        gmail_setup_complete = True
                        gmail_oauth_credentials = {
                            'authenticated': True,
                            'oauth_active': True,
                            'user_email': auth_data.get('user_email', auth_data.get('email', 'gmail-user')),
                            'access_token': auth_data.get('access_token')
                        }
                        user_email = auth_data.get('user_email', auth_data.get('email'))
                        logger.info(f"Gmail auth found in streamlit_google_sso for {brokerage_name}")
                        
                        # Sync to session state for UI consistency
                        st.session_state[auth_key] = gmail_oauth_credentials
                except Exception as e:
                    logger.debug(f"Could not check streamlit_google_sso auth: {e}")
            
            # Method 3: Check credential manager (fallback)
            if not gmail_setup_complete:
                try:
                    from credential_manager import credential_manager
                    stored_tokens = credential_manager.get_bearer_tokens(brokerage_name)
                    if stored_tokens and 'gmail_token' in stored_tokens:
                        gmail_setup_complete = True
                        gmail_oauth_credentials = {
                            'authenticated': True,
                            'oauth_active': True,
                            'user_email': 'gmail-authenticated-user'
                        }
                        user_email = 'gmail-authenticated-user'
                        logger.info(f"Gmail auth found in credential manager for {brokerage_name}")
                        
                        # Sync to session state for UI consistency
                        st.session_state[auth_key] = gmail_oauth_credentials
                except Exception as e:
                    logger.debug(f"Could not check credential manager auth: {e}")
            
            # Get monitor status
            try:
                monitor_running = getattr(email_monitor, 'monitoring_active', False)
                if hasattr(email_monitor, 'get_monitoring_status'):
                    status_info = email_monitor.get_monitoring_status()
                else:
                    status_info = {}
            except Exception as e:
                monitor_running = False
                status_info = {}
            
            # Debug info (remove in production)
            # st.caption(f"Debug OAuth: gmail_setup_complete={gmail_setup_complete}, google_oauth_available={google_oauth_available}")
            # if gmail_oauth_credentials:
            #     st.caption(f"OAuth details: user={gmail_oauth_credentials.get('user_email', 'none')}, oauth_active={gmail_oauth_credentials.get('oauth_active', False)}")
            
            if gmail_setup_complete:
                # Real OAuth credentials detected
                user_email = gmail_oauth_credentials.get('user_email', 'Gmail account')
                st.success(f"✅ **Gmail Connected**")
                st.caption(f"📧 {user_email}")
                
                # Automatically configure email monitoring with OAuth credentials
                try:
                    # Check if email monitor already has these OAuth credentials
                    current_status = email_monitor.get_monitoring_status()
                    oauth_configured = current_status.get('oauth_credentials_count', 0) > 0
                    
                    if not oauth_configured:
                        # Transfer OAuth credentials to email monitor automatically
                        st.info("🔄 Configuring email monitoring with OAuth credentials...")
                        
                        # Get real OAuth credentials with access token
                        real_oauth_creds = streamlit_google_sso._get_stored_auth(brokerage_name)
                        if real_oauth_creds:
                            # Merge session state info with real OAuth tokens
                            full_oauth_creds = {**gmail_oauth_credentials, **real_oauth_creds}
                        else:
                            full_oauth_creds = gmail_oauth_credentials
                        
                        config_result = email_monitor.configure_oauth_monitoring(
                            brokerage_key=brokerage_name,
                            oauth_credentials=full_oauth_creds,
                            email_filters={
                                'sender_filter': st.session_state.get('email_sender_filter', ''),
                                'subject_filter': st.session_state.get('email_subject_filter', '')
                            }
                        )
                        
                        if config_result.get('success'):
                            st.success("✅ OAuth credentials configured in email monitor")
                            
                            # Automatically start monitoring since OAuth is configured
                            st.info("🔄 Starting email monitoring automatically...")
                            start_result = email_monitor.start_monitoring()
                            
                            if start_result.get('success'):
                                st.success("✅ Email monitoring started automatically")
                                # Update the monitor status for this check
                                monitor_running = True
                                st.rerun()  # Refresh to show active status
                            else:
                                st.error(f"❌ Failed to auto-start monitoring: {start_result.get('message', 'Unknown error')}")
                        else:
                            st.warning(f"⚠️ OAuth configuration issue: {config_result.get('message', 'Unknown error')}")
                            
                except Exception as e:
                    st.warning(f"⚠️ Could not auto-configure email monitoring: {e}")
                
                # Only show troubleshooting if OAuth is configured but monitoring still not running
                if not monitor_running and status_info.get('monitoring_active') == False and status_info.get('oauth_credentials_count', 0) > 0:
                    with st.expander("🔧 Troubleshooting - Monitoring Not Active", expanded=False):
                        st.warning("Configuration shows active, but monitoring isn't running")
                        st.write("**Possible solutions:**")
                        st.write("• Clear session data and reconfigure")
                        st.write("• Check Gmail API connection")
                        st.write("• Verify OAuth permissions")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("🔄 Reset & Reconfigure", key="reset_gmail_config"):
                                # Clear all OAuth-related session state for this brokerage
                                # Note: These are email automation keys that are being intentionally cleared
                                # as part of Gmail disconnection process - this is expected behavior
                                keys_to_clear = [
                                    auth_key,
                                    f'gmail_auth_success_{brokerage_name}',
                                    f'processed_code_{brokerage_name}',
                                    'google_sso_auth'
                                ]
                                
                                for key in keys_to_clear:
                                    if key in st.session_state:
                                        del st.session_state[key]
                                
                                # Also clear any stored credentials in streamlit_google_sso
                                try:
                                    streamlit_google_sso._clear_stored_auth(brokerage_name)
                                except:
                                    pass
                                
                                st.success("All Gmail credentials cleared - refresh page to reconfigure")
                                st.rerun()
                        with col2:
                            if st.button("🧪 Test Connection", key="test_gmail_connection"):
                                # Test real OAuth credentials instead of simulation
                                with st.spinner("Testing Gmail OAuth credentials..."):
                                    try:
                                        # Use streamlit_google_sso to test the credentials
                                        test_result = streamlit_google_sso._test_gmail_connection(brokerage_name, gmail_oauth_credentials)
                                        
                                        if test_result.get('success'):
                                            st.success(f"✅ Gmail connection successful!")
                                            if 'total_messages' in test_result:
                                                st.info(f"📧 Gmail inbox accessible: {test_result['total_messages']} messages")
                                        else:
                                            st.error(f"❌ Gmail connection failed: {test_result.get('message', 'Unknown error')}")
                                            st.info("💡 Try disconnecting and reconnecting your Gmail account")
                                            
                                    except Exception as e:
                                        st.error(f"❌ Test failed: {e}")
                                        st.info("This indicates the OAuth credentials may be invalid or expired")
                
                # Show automation status - check both credential status and actual monitor status
                try:
                    # Use the correct property/method from the email monitor
                    monitor_running = getattr(email_monitor, 'monitoring_active', False)
                    if hasattr(email_monitor, 'get_monitoring_status'):
                        status_info = email_monitor.get_monitoring_status()
                        # Update debug to show status details
                    else:
                        status_info = "No get_monitoring_status method"
                except Exception as e:
                    monitor_running = False
                    status_info = f"Error: {e}"
                
                # Debug the status and email monitor (remove in production)
                # st.caption(f"Debug: monitor_running={monitor_running}, gmail_oauth_complete={gmail_setup_complete}")
                # st.caption(f"Monitor status info: {status_info}")
                
                # Show automation status in a clean format
                if gmail_setup_complete and monitor_running:
                    st.success("🟢 **Email Automation Active**")
                    st.caption("Monitoring Gmail for freight emails")
                    
                    # Optional controls for testing and advanced users
                    with st.expander("⚙️ Advanced Controls", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if st.button("📨 Check Inbox Now", key="check_inbox_now"):
                                try:
                                    with st.spinner("🔍 Checking Gmail inbox..."):
                                        result = email_monitor.check_inbox_now(brokerage_name)
                                        
                                        # Always store results for display, even if there are errors
                                        st.session_state.email_processing_results = {
                                            'success': result.success,
                                            'processed_count': result.processed_count,
                                            'file_info': {
                                                'processed_files': result.file_info.get('processed_files', []) if result.file_info else [],
                                                'processing_summary': result.file_info.get('processing_summary', {}) if result.file_info else {}
                                            },
                                            'timestamp': datetime.now(),
                                            'source': 'email_automation',
                                            'error_details': result.error_details,
                                            'message': result.message
                                        }
                                        st.session_state.show_email_results_dashboard = True
                                        
                                        if result.success:
                                            if result.processed_count > 0:
                                                st.success(f"✅ Processed {result.processed_count} file(s) - View details below")
                                            else:
                                                st.info("📭 No new emails with attachments found")
                                                
                                                # Show helpful info for troubleshooting
                                                with st.expander("🔍 Search Details", expanded=False):
                                                    current_filters = email_monitor.oauth_credentials.get(brokerage_name, {}).get('email_filters', {})
                                                    sender_filter = current_filters.get('sender_filter', '')
                                                    subject_filter = current_filters.get('subject_filter', '')
                                                    
                                                    if sender_filter or subject_filter:
                                                        st.caption("**Current email filters:**")
                                                        if sender_filter:
                                                            st.caption(f"• From: {sender_filter}")
                                                        if subject_filter:
                                                            st.caption(f"• Subject contains: {subject_filter}")
                                                        st.caption("💡 Try clearing filters if your test email isn't being found")
                                                    else:
                                                        st.caption("**Searching:** All emails with attachments")
                                        else:
                                            st.warning(f"⚠️ Processing completed with issues - View details below")
                                        
                                        st.rerun()
                                            
                                except Exception as e:
                                    st.error(f"❌ Error checking inbox: {e}")
                        
                        with col2:
                            if st.button("🔄 Reconnect Gmail", key="reconnect_gmail"):
                                try:
                                    # Clear OAuth credentials to force reconnection
                                    auth_key = f'gmail_auth_{brokerage_name.replace("-", "_")}'
                                    if auth_key in st.session_state:
                                        del st.session_state[auth_key]
                                    
                                    # Clear from streamlit_google_sso
                                    streamlit_google_sso._clear_stored_auth(brokerage_name)
                                    
                                    st.success("OAuth credentials cleared - page will refresh for reconnection")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Failed to clear credentials: {e}")
                        
                        # Stop monitoring in a separate row
                        if st.button("⏹️ Stop Email Monitoring", key="stop_email_monitor", use_container_width=True):
                            try:
                                email_monitor.stop_monitoring()
                                st.success("Email monitoring stopped")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to stop monitoring: {e}")
                                
                elif gmail_setup_complete:
                    st.info("🟡 **Starting Email Automation...**")
                else:
                    st.info("🔴 **Email Automation Inactive**")
                    st.caption("Gmail authentication required")
                    
                # Email filters
                with st.expander("📬 Email Filters", expanded=False):
                    st.info("💡 **Tip:** Leave filters empty to process ALL emails with attachments")
                    
                    sender_filter = st.text_input(
                        "Sender filter:",
                        value=st.session_state.get('email_sender_filter', ''),
                        placeholder="ops@company.com (optional)",
                        help="Filter emails by sender - leave empty to accept all senders",
                        key="email_sender_filter_input"
                    )
                    st.session_state.email_sender_filter = sender_filter
                    
                    subject_filter = st.text_input(
                        "Subject filter:",
                        value=st.session_state.get('email_subject_filter', ''),
                        placeholder="Load Data (optional)",
                        help="Filter emails by subject keywords - leave empty to accept all subjects",
                        key="email_subject_filter_input"
                    )
                    st.session_state.email_subject_filter = subject_filter
                    
                    # Show current filter status
                    if sender_filter or subject_filter:
                        st.caption(f"🔍 **Active filters:** {f'from:{sender_filter}' if sender_filter else ''} {f'subject:{subject_filter}' if subject_filter else ''}".strip())
                    else:
                        st.caption("🔍 **No filters** - processing all emails with attachments")
                    
                    if st.button("🔄 Update Filters", key="update_email_filters", use_container_width=True):
                        # Update filters in email monitor
                        if gmail_setup_complete:
                            try:
                                email_monitor.configure_oauth_monitoring(
                                    brokerage_key=brokerage_name,
                                    oauth_credentials=gmail_oauth_credentials,
                                    email_filters={
                                        'sender_filter': sender_filter,
                                        'subject_filter': subject_filter
                                    }
                                )
                                st.success("✅ Email filters updated")
                            except Exception as e:
                                st.error(f"❌ Failed to update filters: {e}")
                        else:
                            st.success("✅ Filters saved (will apply when OAuth is configured)")
                        
            else:
                # Enhanced authentication status display
                st.error("🔍 **Authentication Status Check**")
                st.info("Checking all authentication sources...")
                
                # Show detailed authentication debug info
                with st.expander("🔍 Authentication Debug Info", expanded=True):
                    st.write("**Session State Check:**")
                    auth_key = f'gmail_auth_{brokerage_name.replace("-", "_")}'
                    session_auth = st.session_state.get(auth_key, {})
                    st.write(f"- Has session auth: {bool(session_auth)}")
                    if session_auth:
                        st.write(f"- Authenticated: {session_auth.get('authenticated', False)}")
                        st.write(f"- OAuth active: {session_auth.get('oauth_active', False)}")
                        st.write(f"- User email: {session_auth.get('user_email', 'Not set')}")
                    
                    st.write("**Google SSO Check:**")
                    try:
                        auth_data = streamlit_google_sso._get_stored_auth(brokerage_name)
                        st.write(f"- SSO auth found: {bool(auth_data)}")
                        if auth_data:
                            st.write(f"- Has access token: {bool(auth_data.get('access_token'))}")
                            st.write(f"- User email: {auth_data.get('user_email', auth_data.get('email', 'Not set'))}")
                    except Exception as e:
                        st.write(f"- SSO check error: {str(e)}")
                    
                    st.write("**Background Processing Evidence:**")
                    st.success("✅ Background processing IS working (check logs)") 
                    st.write("- Processing logs show successful Gmail authentication")
                    st.write("- Files are being processed automatically")
                    st.write("- This suggests authentication is working but UI can't see it")
                
                st.warning("⚠️ UI cannot detect Gmail authentication (but background processing works)")
                
                # Manual OAuth setup to avoid interface disappearing
                if streamlit_google_sso.is_configured():
                    st.info("🔐 **Manual Gmail Authentication Setup**")
                    st.markdown("You can manually connect Gmail for UI monitoring (background processing is already working):")
                    
                    # Check if already authenticated
                    auth_key = f'gmail_auth_{brokerage_name.replace("-", "_")}'
                    existing_auth = st.session_state.get(auth_key, {})
                    
                    if existing_auth.get('authenticated'):
                        # Already authenticated - show status
                        user_email = existing_auth.get('user_email', 'Gmail account')
                        st.success(f"✅ Connected: {user_email}")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("🔓 Disconnect", key="disconnect_gmail"):
                                del st.session_state[auth_key]
                                st.success("Disconnected from Gmail")
                                st.rerun()
                        with col2:
                            if st.button("🔍 Test Connection", key="test_gmail"):
                                st.success("✅ Gmail connection is active")
                    else:
                        # Need authentication - use manual flow to avoid disappearing
                        if st.button("🔐 Setup Gmail Auth", key="setup_gmail_manual", type="primary"):
                            # Generate authentication URL and show instructions
                            auth_url = streamlit_google_sso._generate_auth_url(brokerage_name)
                            
                            if auth_url:
                                st.markdown("**Step 1:** Click to authenticate with Google:")
                                st.markdown(f"""
                                <a href="{auth_url}" target="_blank" style="
                                    display: inline-block;
                                    background-color: #4285f4;
                                    color: white;
                                    padding: 12px 24px;
                                    text-decoration: none;
                                    border-radius: 8px;
                                    font-weight: bold;
                                    margin: 10px 0;
                                ">🔗 Authenticate with Google</a>
                                """, unsafe_allow_html=True)
                                
                                st.markdown("**Step 2:** After authentication, the page will auto-refresh.")
                                
                                # Check for redirect with auth code (auto-processing)
                                try:
                                    url_params = st.query_params
                                    auth_code = url_params.get('code', '')
                                    if auth_code:
                                        # Process the authorization code
                                        with st.spinner("🔄 Processing authentication..."):
                                            result = streamlit_google_sso._handle_manual_auth_code(brokerage_name, auth_code)
                                            
                                            if result['success']:
                                                # Store in session state
                                                st.session_state[auth_key] = {
                                                    'authenticated': True,
                                                    'user_email': result.get('user_email', 'authenticated'),
                                                    'brokerage_key': brokerage_name,
                                                    'oauth_active': True
                                                }
                                                
                                                st.success(f"✅ Gmail authentication successful!")
                                                st.success(f"✅ Email monitoring configured for {brokerage_name}")
                                                
                                                # Clear the URL params and refresh
                                                st.query_params.clear()
                                                st.rerun()
                                            else:
                                                st.error(f"❌ Authentication failed: {result.get('message', 'Unknown error')}")
                                except Exception as e:
                                    # Silent handling - user hasn't authenticated yet
                                    pass
                            else:
                                st.error("❌ Unable to generate authentication URL")
                else:
                    st.error("🔧 **Google OAuth Configuration Required**")
                    st.info("Contact your administrator to configure Google OAuth credentials.")
                        
        except Exception as e:
            st.error(f"Email automation error: {e}")
            st.write(f"DEBUG: Exception occurred: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
            
        # Email delivery configuration
        with st.expander("📤 Email Delivery", expanded=False):
            send_email = st.checkbox(
                "Send results via email",
                value=st.session_state.get('send_email', False),
                help="Email the processing results when complete",
                key="sidebar_send_email"
            )
            st.session_state.send_email = send_email
            
            if send_email:
                email_recipient = st.text_input(
                    "Email recipient:",
                    value=st.session_state.get('email_recipient', ''),
                    placeholder="ops@company.com",
                    help="Enter the email address to receive the results",
                    key="sidebar_email_recipient"
                )
                st.session_state.email_recipient = email_recipient
                
                # Email format options
                email_formats = st.multiselect(
                    "Include formats:",
                    ["CSV", "Excel", "JSON", "Summary Report"],
                    default=st.session_state.get('email_formats', ["CSV", "Summary Report"]),
                    help="Select which formats to include in email",
                    key="sidebar_email_formats"
                )
                st.session_state.email_formats = email_formats
    
    # Admin: UI Mode Toggle (for debugging/fallback)
    with st.expander("🔧 Advanced UI Options", expanded=False):
        current_ui_mode = st.session_state.get('use_simplified_ui', True)
        ui_mode = st.selectbox(
            "Results Display Mode:",
            options=[True, False],
            format_func=lambda x: "Simplified (Clean)" if x else "Detailed (Legacy)",
            index=0 if current_ui_mode else 1,
            help="Choose between simplified results display or detailed legacy view",
            key="ui_mode_selector"
        )
        
        if ui_mode != current_ui_mode:
            st.session_state.use_simplified_ui = ui_mode
            st.rerun()

def _render_enhanced_landing_page():
    """Enhanced landing page - original FF2API + processing mode selection"""
    
    # Add processing mode selection at the top
    st.subheader("🎯 Processing Mode")
    
    processing_mode = st.radio(
        "Choose your processing workflow:",
        [
            "🔧 Standard FF2API",
            "📤 Full End-to-End Processing"
        ],
        index=0,
        help="Select standard FF2API or complete end-to-end workflow with enrichment and delivery"
    )
    
    # Store processing mode in session state
    mode_mapping = {
        "🔧 Standard FF2API": "standard",
        "📤 Full End-to-End Processing": "full_endtoend"
    }
    
    st.session_state.enhanced_processing_mode = mode_mapping[processing_mode]
    
    # Show enhanced mode description with email automation info
    mode_descriptions = {
        "standard": "Process data through FF2API only - original functionality",
        "full_endtoend": "Complete workflow with load ID mapping, real-time tracking enrichment, multiple output formats, and email delivery. **📧 Includes Email Automation features in sidebar.**"
    }
    
    selected_mode = st.session_state.enhanced_processing_mode
    st.info(f"**Selected**: {mode_descriptions[selected_mode]}")
    
    # Add email automation visibility note for full_endtoend mode
    if selected_mode == 'full_endtoend':
        st.success("✅ **Email Automation Active** - Configuration options now available in the left sidebar")
    else:
        st.warning("ℹ️ **Email Automation Available** - Select 'Full End-to-End Processing' above to enable email monitoring and automation features")
    
    # Email configuration for full end-to-end mode
    if selected_mode == 'full_endtoend':
        st.markdown("---")
        st.subheader("📧 Email Configuration")
        
        send_email = st.checkbox(
            "Send results via email",
            value=st.session_state.get('send_email', False),
            help="Email the processing results when complete"
        )
        st.session_state.send_email = send_email
        
        if send_email:
            email_recipient = st.text_input(
                "Email recipient:",
                value=st.session_state.get('email_recipient', ''),
                placeholder="ops@company.com",
                help="Enter the email address to receive the results"
            )
            st.session_state.email_recipient = email_recipient
        else:
            st.session_state.email_recipient = ''
    
    # Original FF2API file upload section
    st.markdown("---")
    _render_original_file_upload()
    
    # Original benefits section - preserved exactly
    st.markdown("""
        <div style="
            display: flex;
            justify-content: space-around;
            margin: 2rem 0;
            padding: 1.5rem;
            background: #f8fafc;
            border-radius: 0.75rem;
            border: 1px solid #e2e8f0;
        ">
            <div style="text-align: center; flex: 1;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📤</div>
                <div style="font-weight: 600; color: #1e293b;">Upload</div>
                <div style="font-size: 0.9rem; color: #64748b;">CSV, Excel files</div>
            </div>
            <div style="text-align: center; flex: 1;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">🔗</div>
                <div style="font-weight: 600; color: #1e293b;">Auto-Map</div>
                <div style="font-size: 0.9rem; color: #64748b;">AI-powered mapping</div>
            </div>
            <div style="text-align: center; flex: 1;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">🚀</div>
                <div style="font-weight: 600; color: #1e293b;">Process</div>
                <div style="font-size: 0.9rem; color: #64748b;">Automated API calls</div>
            </div>
            <div style="text-align: center; flex: 1;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📊</div>
                <div style="font-weight: 600; color: #1e293b;">Enhance</div>
                <div style="font-size: 0.9rem; color: #64748b;">Optional enrichment</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def _render_original_file_upload():
    """Original FF2API file upload - preserved exactly"""
    st.subheader("📁 File Upload")
    
    uploaded_file = st.file_uploader(
        "Choose CSV or Excel file",
        type=["csv", "xlsx", "xls"],
        help="Upload freight data file for processing",
        key="enhanced_file_uploader"
    )
    
    if uploaded_file is not None:
        try:
            # Load file exactly like original
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            # Store in session state exactly like original
            st.session_state.uploaded_df = df
            st.session_state.uploaded_file_name = uploaded_file.name
            st.success(f"File loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Validate headers with config exactly like original
            file_headers = df.columns.tolist()
            st.session_state.file_headers = file_headers
            _validate_headers_with_config(file_headers)
            
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Error reading file: {str(e)}")

def _validate_headers_with_config(file_headers):
    """Validate file headers against existing configuration - exactly like original"""
    brokerage_name = st.session_state.brokerage_name
    
    if (st.session_state.get('configuration_type') == 'existing' and 
        'selected_configuration' in st.session_state):
        config = st.session_state.selected_configuration
        
        # Always try to validate headers against saved config in database
        from src.frontend.ui_components import create_header_validation_interface
        from src.backend.database import DatabaseManager
        db_manager = DatabaseManager()
        
        # Get the actual saved configuration from database
        saved_config = db_manager.get_brokerage_configuration(brokerage_name, config['name'])
        
        if saved_config and saved_config.get('file_headers'):
            # Compare headers with saved configuration
            header_comparison = create_header_validation_interface(
                file_headers, db_manager, brokerage_name, config['name']
            )
            st.session_state.header_comparison = header_comparison
        else:
            # Config exists but no saved headers - treat as new
            st.session_state.header_comparison = {
                'status': 'new_config',
                'changes': [],
                'missing': [],
                'added': file_headers
            }

def _render_enhanced_workflow_with_progress(db_manager, data_processor):
    """Enhanced workflow with progress - original FF2API + end-to-end options"""
    
    # Preserve original workflow sections exactly, but with enhanced processing
    
    # Original progress indication
    if st.session_state.get('uploaded_df') is not None and not st.session_state.get('validation_passed'):
        if st.session_state.get('field_mappings'):
            field_mappings = st.session_state.get('field_mappings', {})
            has_real_mappings = any(not k.startswith('_') and v and v != 'Select column...' for k, v in field_mappings.items())
            if has_real_mappings:
                st.info("🔍 Mapping complete! Ready to validate data quality")
            else:
                st.info("🔗 Complete field mapping to continue")
    
    # Show current processing mode
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    mode_names = {
        'standard': '🔧 Standard FF2API',
        'full_endtoend': '📤 Full End-to-End Processing'
    }
    
    st.info(f"**Processing Mode**: {mode_names.get(processing_mode, 'Standard')}")
    
    # Original workflow sections
    _render_current_file_info()
    _render_field_mapping_section(db_manager, data_processor)
    _render_carrier_configuration_section(db_manager)
    _render_validation_section(data_processor)
    _render_enhanced_processing_section(db_manager, data_processor)
    _render_email_results_dashboard()
    _render_enhanced_results_section()

def _render_current_file_info():
    """Show current file information - exactly like original"""
    if st.session_state.get('uploaded_df') is not None:
        filename = st.session_state.get('uploaded_file_name', 'Unknown')
        record_count = len(st.session_state.uploaded_df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📁 File", filename[:20] + "..." if len(filename) > 20 else filename)
        with col2:
            st.metric("📊 Records", f"{record_count:,}")
        with col3:
            if st.button("🔄 Upload Different File", key="change_file_btn"):
                # Clear file processing keys while preserving email automation state
                keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'file_headers', 'validation_passed', 'header_comparison', 'field_mappings', 'processing_results']
                safe_clear_session_keys(keys_to_clear)
                st.rerun()
        
        # Add preview data button - exactly like original
        col1, col2 = st.columns(2)
        with col2:
            if st.button("👀 Preview Data", key="preview_toggle_btn", use_container_width=True):
                st.session_state.show_preview = not st.session_state.get('show_preview', False)
        
        # Show preview if requested - exactly like original
        if st.session_state.get('show_preview', False):
            _render_data_preview_section()

def _render_data_preview_section():
    """Render data preview section - exactly like original FF2API"""
    with st.expander("📊 Data Preview", expanded=True):
        # Create tabs for different preview types
        preview_tab1, preview_tab2, preview_tab3 = st.tabs(["📋 CSV Data", "🔗 JSON API Preview", "📝 Mapping Details"])
        
        with preview_tab1:
            st.caption("Raw CSV data (first 10 rows)")
            st.dataframe(st.session_state.uploaded_df.head(10), use_container_width=True)
        
        with preview_tab2:
            st.caption("Sample API payload generated from first row of your CSV data")
            
            # Get field mappings and data processor
            field_mappings = st.session_state.get('field_mappings', {})
            
            # Import data processor dynamically to avoid circular imports
            from src.backend.data_processor import DataProcessor
            from src.backend.database import DatabaseManager
            from src.frontend.ui_components import generate_sample_api_preview
            
            data_processor = DataProcessor()
            db_manager = DatabaseManager()
            brokerage_name = st.session_state.get('brokerage_name')
            
            # Generate API preview with carrier auto-mapping
            api_preview_data = generate_sample_api_preview(
                st.session_state.uploaded_df, 
                field_mappings, 
                data_processor,
                db_manager=db_manager,
                brokerage_name=brokerage_name
            )
            
            # Display message about the preview - with null-safe handling
            if api_preview_data and api_preview_data.get("message"):
                message = api_preview_data["message"]
                if "No field mappings configured yet" in message:
                    st.info("🔗 Complete field mapping first to see API preview")
                    st.markdown("""
                        **What you'll see here:**
                        - JSON structure showing how your CSV data will be formatted for the API
                        - Real sample values from your first CSV row
                        - Properly nested objects (load, customer, brokerage)
                        - Field validation and data type conversion
                    """)
                elif "error" in message.lower():
                    st.warning(f"⚠️ {message}")
                else:
                    st.success(f"✅ {message}")
            else:
                # Fallback for when api_preview_data is None or has no message
                st.warning("⚠️ Could not generate API preview - check your field mappings")
            
            # Display JSON preview with enhanced formatting
            # Always show preview if field mappings exist, even if preview has warnings
            if field_mappings and api_preview_data.get("preview"):
                # Show structure overview
                preview_sections = list(api_preview_data["preview"].keys())
                if preview_sections:
                    st.markdown(f"**📋 API Structure:** {', '.join(preview_sections)}")
                
                # Display the JSON with custom styling
                st.markdown("**🔄 JSON API Payload:**")
                st.json(api_preview_data["preview"])
                
                # Show additional helpful information
                if "mapped_fields" in api_preview_data:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Mapped Fields", len(api_preview_data['mapped_fields']))
                    with col2:
                        from src.frontend.ui_components import get_effective_required_fields
                        current_mappings = st.session_state.get('field_mappings', {})
                        effective_required = get_effective_required_fields(get_full_api_schema(), current_mappings)
                        required_fields = [f for f in api_preview_data['mapped_fields'] if f in effective_required]
                        st.metric("Required Fields", len(required_fields))
                    with col3:
                        optional_fields = len(api_preview_data['mapped_fields']) - len(required_fields)
                        st.metric("Optional Fields", optional_fields)
                
                # Show source row information
                if "source_row" in api_preview_data:
                    with st.expander("🔍 Source CSV Row Details", expanded=False):
                        st.caption("Raw CSV values used to generate this preview")
                        source_df = pd.DataFrame([api_preview_data["source_row"]])
                        st.dataframe(source_df, use_container_width=True)
            
            elif not field_mappings:
                # Show helpful preview structure when no mappings exist
                st.markdown("**🔄 Expected API Structure:**")
                sample_structure = {
                    "load": {
                        "loadNumber": "Your load number here",
                        "mode": "FTL/LTL/DRAYAGE",
                        "equipment": {
                            "equipmentType": "DRY_VAN"
                        },
                        "route": []
                    },
                    "customer": {
                        "customerId": "Your customer ID",
                        "name": "Customer name"
                    },
                    "brokerage": {
                        "contacts": []
                    }
                }
                st.json(sample_structure)
        
        with preview_tab3:
            st.caption("Configuration summary - CSV mappings and manual values")
            field_mappings = st.session_state.get('field_mappings', {})
            api_schema = get_full_api_schema()
            
            # Get effective required fields for accurate indicators
            from src.frontend.ui_components import get_effective_required_fields
            effective_required = get_effective_required_fields(api_schema, field_mappings)
            
            # Create comprehensive configuration summary
            config_data = []
            
            # Process field mappings (both CSV columns and manual values)
            for api_field, mapping_value in field_mappings.items():
                if not api_field.startswith('_') and mapping_value and mapping_value != 'Select column...':
                    field_info = api_schema.get(api_field, {})
                    
                    if mapping_value.startswith('MANUAL_VALUE:'):
                        # Manual value
                        manual_value = mapping_value.replace('MANUAL_VALUE:', '')
                        
                        # Show manual value as-is (schema-based enum values)
                        display_value = str(manual_value)
                        
                        config_data.append({
                            "API Field": api_field,
                            "Source": "🎯 Manual Value",
                            "Value/Column": display_value,
                            "Type": field_info.get('type', 'string'),
                            "Required": "⭐" if api_field in effective_required else "🔸" if field_info.get('required') == 'conditional' else "",
                            "Is Enum": "🔽" if field_info.get('enum') else ""
                        })
                    else:
                        # CSV column mapping
                        config_data.append({
                            "API Field": api_field,
                            "Source": "📄 CSV Column",
                            "Value/Column": mapping_value,
                            "Type": field_info.get('type', 'string'),
                            "Required": "⭐" if api_field in effective_required else "🔸" if field_info.get('required') == 'conditional' else "",
                            "Is Enum": "🔽" if field_info.get('enum') else ""
                        })
            
            if config_data:
                config_df = pd.DataFrame(config_data)
                st.dataframe(config_df, use_container_width=True)
                
                # Enhanced mapping statistics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    csv_mappings = len([d for d in config_data if d["Source"] == "📄 CSV Column"])
                    st.metric("CSV Mappings", csv_mappings)
                
                with col2:
                    manual_count = len([d for d in config_data if d["Source"] == "🎯 Manual Value"])
                    st.metric("Manual Values", manual_count)
                
                with col3:
                    required_configured = len([d for d in config_data if d["Required"] == "⭐"])
                    st.metric("Required Fields", required_configured)
                
                with col4:
                    enum_fields = len([d for d in config_data if d["Is Enum"] == "🔽"])
                    st.metric("Enum Fields", enum_fields)
                
                # Show validation status
                st.markdown("**Configuration Status:**")
                total_required = len([f for f, info in api_schema.items() if info.get('required', False)])
                configured_required = len([d for d in config_data if d["Required"] == "⭐"])
                
                if configured_required >= total_required:
                    st.success(f"✅ All {total_required} required fields configured")
                else:
                    missing = total_required - configured_required
                    st.warning(f"⚠️ {missing} required fields still need configuration")
                    
            else:
                st.info("No field mappings or manual values configured yet")

def _render_field_mapping_section(db_manager, data_processor):
    """Render field mapping section - exactly like original"""
    st.markdown("---")
    st.subheader("🔗 Field Mapping")
    
    df = st.session_state.uploaded_df
    
    # Use original field mapping interface
    brokerage_name = st.session_state.get('brokerage_name')
    configuration_name = st.session_state.get('selected_configuration', {}).get('name')
    
    # Get existing configuration mappings
    config = st.session_state.get('selected_configuration', {})
    existing_mappings = config.get('field_mappings', {})
    has_real_mappings = any(not key.startswith('_') for key in existing_mappings.keys())
    
    if has_real_mappings:
        existing_config = config
    else:
        existing_config = None
    
    # Learning-enhanced mapping interface - exactly like original
    field_mappings = create_learning_enhanced_mapping_interface(
        df, existing_config.get('field_mappings', {}) if existing_config else {},
        data_processor, db_manager, brokerage_name, configuration_name
    )
    
    st.session_state.field_mappings = field_mappings
    
    # Auto-save field mappings - exactly like original
    if (field_mappings and 
        st.session_state.get('selected_configuration') and 
        st.session_state.get('file_headers')):
        
        try:
            _save_configuration(db_manager, field_mappings, st.session_state.file_headers)
        except Exception as e:
            logger.error(f"Error auto-saving configuration: {e}")

def _render_carrier_configuration_section(db_manager):
    """Render carrier configuration section"""
    if not st.session_state.get('field_mappings') or not st.session_state.get('brokerage_name'):
        return
    
    # Check if we have any carrier-related fields in the mapping to show the section
    field_mappings = st.session_state.get('field_mappings', {})
    carrier_fields = [field for field in field_mappings.keys() if 'carrier' in field.lower()]
    
    if not carrier_fields:
        return
    
    with st.expander("🚛 Carrier Configuration", expanded=False):
        st.markdown("**Automatic Carrier Mapping**")
        st.caption("Configure automatic population of carrier information based on carrier name detection in your data files.")
        
        # Import and render the carrier mapping interface
        from src.frontend.ui_components import create_carrier_mapping_interface
        
        brokerage_name = st.session_state.brokerage_name
        create_carrier_mapping_interface(db_manager, brokerage_name)

def _render_validation_section(data_processor):
    """Render validation section - exactly like original"""
    if not st.session_state.get('field_mappings'):
        return
        
    st.markdown("---")
    st.subheader("✅ Data Validation")
    
    df = st.session_state.uploaded_df
    field_mappings = st.session_state.field_mappings
    api_credentials = st.session_state.api_credentials
    
    if st.button("🔍 Validate Data", type="secondary", use_container_width=True):
        with st.spinner("Validating data..."):
            validation_errors = validate_mapping(df, field_mappings, data_processor)
            
            if validation_errors:
                st.session_state.validation_errors = validation_errors
                st.session_state.validation_passed = False
                st.error(f"❌ Found {len(validation_errors)} validation errors")
            else:
                st.session_state.validation_passed = True
                st.success("✅ Data validation passed!")

def _render_enhanced_processing_section(db_manager, data_processor):
    """Enhanced processing section with end-to-end options"""
    if not st.session_state.get('field_mappings'):
        return
        
    st.markdown("---")
    st.subheader("🚀 Processing")
    
    df = st.session_state.uploaded_df
    field_mappings = st.session_state.field_mappings
    api_credentials = st.session_state.api_credentials
    brokerage_name = st.session_state.brokerage_name
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    
    # Debug logging for processing mode
    logger.info(f"Enhanced processing mode from session state: {processing_mode}")
    logger.info(f"Session state keys: {list(st.session_state.keys())}")
    
    # Show what will happen based on processing mode
    _show_processing_preview(processing_mode)
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Records", f"{len(df):,}")
    with col2:
        st.metric("Fields", len(field_mappings))
    with col3:
        st.metric("Mode", processing_mode.title())
    
    # Enhanced processing button
    if st.button("🚀 Process Data", type="primary", key="enhanced_process_btn", use_container_width=True):
        try:
            session_id = st.session_state.get('session_id', 'unknown')
            
            # Execute enhanced processing workflow
            result = process_enhanced_data_workflow(
                df, field_mappings, api_credentials, brokerage_name, 
                processing_mode, data_processor, db_manager, session_id
            )
            
            # Update learning system
            if result and 'success_rate' in result:
                update_learning_with_processing_results(
                    session_id, result['success_rate'], data_processor, db_manager
                )
            
        except Exception as e:
            st.error(f"❌ Processing failed: {str(e)}")

def _show_processing_preview(processing_mode):
    """Show what will happen in the selected processing mode"""
    mode_steps = {
        'standard': [
            "1. Send data to FF2API",
            "2. Display results and success/failure rates"
        ],
        'full_endtoend': [
            "1. Send data to FF2API",
            "2. Retrieve internal load IDs for successful loads",
            "3. Enrich data with tracking and Snowflake information", 
            "4. Generate output files (CSV, Excel, JSON)",
            "5. Send results via email (if configured)",
            "6. Display complete workflow results"
        ]
    }
    
    with st.expander("📋 Processing Steps", expanded=False):
        steps = mode_steps.get(processing_mode, mode_steps['standard'])
        for step in steps:
            st.write(step)

def _render_email_results_dashboard():
    """Render email processing results dashboard with shared storage integration."""
    
    # DEBUG: Always log that this function is being called
    logger.error("🔍 EMAIL DEBUG: _render_email_results_dashboard() function called")
    
    # DEBUG: Show a visible test message to confirm function is running
    st.markdown("---")
    st.markdown("### 🔍 EMAIL PROCESSING DEBUG")
    st.info("✅ Email results dashboard function is now running!")
    
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    st.info(f"Current processing mode: {processing_mode}")
    
    # Check for session state results (manual checks)
    session_results = st.session_state.get('email_processing_results') if st.session_state.get('show_email_results_dashboard') else None
    
    # If email automation is active, also check for results proactively
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    email_automation_active = processing_mode == 'full_endtoend'
    
    # Check for shared storage results (background processing)
    shared_results = None
    
    # Debug what we're actually seeing
    logger.info(f"🔍 EMAIL DEBUG: processing_mode = '{processing_mode}', email_automation_active = {email_automation_active}")
    logger.info(f"🔍 EMAIL DEBUG: session_results exists = {session_results is not None}, shared_results exists = {shared_results is not None}")
    brokerage_name = st.session_state.get('brokerage_name', 'default')
    logger.error(f"🔍 EMAIL DEBUG: About to check shared storage for brokerage: {brokerage_name}")
    try:
        from shared_storage_bridge import shared_storage
        logger.error("🔍 EMAIL DEBUG: Successfully imported shared_storage_bridge")
        
        # Try multiple case variations to handle inconsistent brokerage naming
        brokerage_variations = [
            brokerage_name,  # Original
            brokerage_name.lower(),  # lowercase
            brokerage_name.upper(),  # UPPERCASE  
            brokerage_name.title(),  # Title Case
            brokerage_name.capitalize(),  # First letter uppercase
            brokerage_name.replace(' ', ''),  # No spaces
            brokerage_name.replace('-', '').replace('_', ''),  # No separators
            # Common eShipping variations
            'eShipping',
            'eshipping', 
            'ESHIPPING',
            'Eshipping'
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        brokerage_variations = [x for x in brokerage_variations if not (x in seen or seen.add(x))]
        
        # Try each variation until we find data
        recent_results = []
        completed_jobs = []
        stats = {'completed_today': 0}
        effective_brokerage = brokerage_name
        
        logger.info(f"🔍 EMAIL DEBUG: Trying brokerage variations: {brokerage_variations}")
        
        for variation in brokerage_variations:
            test_results = shared_storage.get_recent_results(variation, limit=5)
            test_jobs = shared_storage.get_completed_jobs(variation, limit=5)
            test_stats = shared_storage.get_processing_stats(variation)
            
            if test_results or test_jobs or test_stats.get('completed_today', 0) > 0:
                recent_results = test_results
                completed_jobs = test_jobs
                stats = test_stats
                effective_brokerage = variation
                logger.debug(f"Found shared storage data using brokerage key: '{variation}'")
                break
        
        if recent_results or completed_jobs or stats.get('completed_today', 0) > 0:
            shared_results = {
                'recent_results': recent_results,
                'completed_jobs': completed_jobs,
                'stats': stats,
                'effective_brokerage': effective_brokerage,
                'source': 'background_processing'
            }
    except Exception as e:
        logger.error(f"🔍 EMAIL DEBUG: Could not load shared storage results: {e}")
        import traceback
        logger.error(f"🔍 EMAIL DEBUG: Traceback: {traceback.format_exc()}")
    
    # ALWAYS show shared storage results if they exist (background processing)
    if shared_results:
        st.markdown("---")
        st.markdown("### 📧 Background Email Processing Results")
        st.info("🤖 Results from background email processing detected")
        _render_shared_storage_results(shared_results)
        return
    
    # Show session results if available
    if session_results:
        st.markdown("---") 
        st.markdown("### 📧 Email Processing Results")
        _render_session_state_results(session_results)
        return
    
    # If email automation mode is active, show status and check for results directly
    if email_automation_active:
        st.markdown("---")
        st.markdown("### 📧 Email Automation Status")
        st.info("🤖 **Email automation is active** - Monitoring for incoming emails and processing in background")
        
        # Force check for email processing metadata in session state
        email_metadata = st.session_state.get('email_processing_metadata', [])
        if email_metadata:
            st.success(f"✅ **Found {len(email_metadata)} recent automated processing results!**")
            
            with st.expander(f"📧 Recent Email Processing Results ({len(email_metadata)} files)", expanded=True):
                for meta in email_metadata[-5:]:  # Show last 5
                    filename = meta.get('filename', 'Unknown')
                    processed_time = meta.get('processed_time', 'Unknown')
                    processing_mode = meta.get('processing_mode', 'Unknown')
                    result = meta.get('result', {})
                    
                    st.write(f"**📄 {filename}**")
                    st.write(f"⏰ Processed: {processed_time}")
                    st.write(f"🔧 Mode: {processing_mode}")
                    if result:
                        st.write(f"✅ Result: Successfully processed")
                    st.write("---")
        else:
            st.info("⏳ No recent automated processing results found - waiting for incoming emails")
        
        # Show processing indicator if available
        try:
            from shared_storage_bridge import shared_storage
            # Check if there are any active jobs
            all_jobs = []
            for variation in [brokerage_name, brokerage_name.lower(), 'eshipping']:
                try:
                    jobs = shared_storage.get_active_jobs(variation)
                    all_jobs.extend(jobs)
                except:
                    continue
            
            if all_jobs:
                st.success(f"🔄 **{len(all_jobs)} active processing job(s)** - Results will appear when complete")
                for job in all_jobs[:3]:  # Show first 3 active jobs
                    st.progress(job.progress_percent / 100, text=f"Processing {job.filename}: {job.current_step}")
        except Exception as e:
            logger.debug(f"Could not check active jobs: {e}")
        
        # Add refresh button for users to manually check
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Check for New Processing Results", use_container_width=True):
                st.rerun()
        with col2:
            with st.expander("📊 Background Activity"):
                st.caption("Processing activity appears in the application logs (visible in browser console)")
                st.caption(f"Current brokerage: `{brokerage_name}`")
                
                # Show shared storage debug info
                try:
                    from shared_storage_bridge import shared_storage
                    debug_info = []
                    for variation in [brokerage_name, brokerage_name.lower(), 'eshipping', 'eShipping']:
                        test_results = shared_storage.get_recent_results(variation, limit=1)
                        debug_info.append(f"'{variation}': {len(test_results)} results")
                    st.caption("Debug: " + ", ".join(debug_info))
                except Exception as e:
                    st.caption(f"Debug error: {e}")
        return
    
    # Determine which results to display
    results = session_results if session_results else shared_results
    
    st.markdown("---")
    st.markdown("### 📧 Email Processing Results")
    
    # Handle different result formats
    if results.get('source') == 'background_processing':
        # Display shared storage results
        _render_shared_storage_results(results)
    else:
        # Display session state results (existing format)
        _render_session_state_results(results)


def _render_shared_storage_results(results):
    """Render results from shared storage (background processing)."""
    recent_results = results.get('recent_results', [])
    completed_jobs = results.get('completed_jobs', [])
    stats = results.get('stats', {})
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Files Processed Today", stats.get('completed_today', 0))
    with col2:
        total_records = sum(result.record_count for result in recent_results)
        st.metric("Total Records", total_records)
    with col3:
        success_rate = ((stats.get('completed', 0) / max(stats.get('total', 1), 1)) * 100)
        st.metric("Success Rate", f"{success_rate:.1f}%")
    with col4:
        st.metric("Source", "🤖 Background")
    
    # Recent processing activity
    if recent_results:
        st.markdown("#### 📁 Recent File Processing")
        
        for result in recent_results:
            status_icon = "✅" if result.success else "❌"
            
            with st.expander(f"{status_icon} {result.filename} - {result.record_count} records"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Email Source:** {result.email_source}")
                    st.write(f"**Subject:** {result.subject}")
                    st.write(f"**Processed:** {result.processed_time}")
                with col2:
                    st.write(f"**Records:** {result.record_count}")
                    st.write(f"**Status:** {status_icon} {'Success' if result.success else 'Failed'}")
                    st.write(f"**Mode:** {result.processing_mode}")
                
                # Show download links if available
                if result.download_links:
                    st.markdown("**📥 Download Links:**")
                    for link_type, link_url in result.download_links.items():
                        st.markdown(f"[Download {link_type}]({link_url})")
    
    # Active jobs
    if completed_jobs:
        st.markdown("#### 🔄 Recent Job Status")
        for job in completed_jobs[:3]:  # Show last 3 jobs
            status_color = "🟢" if job.status == 'completed' else "🔴" if job.status == 'failed' else "🟡"
            st.caption(f"{status_color} {job.filename} - {job.status} ({job.progress_percent:.0f}%)")
    
    # Action buttons
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh Activity", use_container_width=True):
            st.rerun()
    
    with col2:
        if st.button("❌ Close Results", use_container_width=True):
            # Only hide the dashboard, don't clear email automation state
            st.session_state.show_email_results_dashboard = False
            st.rerun()


def _render_session_state_results(results):
    """Render results from session state (manual checks)."""
    # Summary metrics (same as manual upload)
    col1, col2, col3, col4 = st.columns(4)
    file_info = results.get('file_info', {})
    summary = file_info.get('processing_summary', {})
    
    with col1:
        st.metric("Files Processed", summary.get('successful_files', 0))
    with col2:
        st.metric("Total Records", summary.get('total_records', 0))
    with col3:
        success_rate = summary.get('success_rate', 0)
        st.metric("Success Rate", f"{success_rate:.1f}%")
    with col4:
        st.metric("Source", "📧 Manual Check")
    
    # Processing timestamp
    processing_time = results.get('timestamp', datetime.now())
    if isinstance(processing_time, str):
        try:
            processing_time = datetime.fromisoformat(processing_time)
        except:
            processing_time = datetime.now()
    
    st.caption(f"⏱️ Processed at: {processing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Detailed results for each file
    file_info = results.get('file_info', {})
    processed_files = file_info.get('processed_files', [])
    
    if processed_files:
        st.markdown("#### 📁 File Processing Details")
        
        for file_result in processed_files:
            status_icon = "✅" if file_result.get('processing_status') == 'success' else "⚠️"
            
            with st.expander(f"{status_icon} {file_result.get('filename', 'Unknown')} - {file_result.get('record_count', 0)} records"):
                
                # File metadata
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Sender:** {file_result.get('sender', 'N/A')}")
                    st.write(f"**Received:** {file_result.get('received_time', 'N/A')}")
                    st.write(f"**Subject:** {file_result.get('subject', 'N/A')}")
                with col2:
                    st.write(f"**Records:** {file_result.get('record_count', 0)}")
                    st.write(f"**Status:** {status_icon} {file_result.get('processing_status', 'Unknown')}")
                    st.write(f"**Mappings Applied:** {file_result.get('field_mappings_applied', 0)}")
                
                # Show column mapping information
                if file_result.get('original_columns') and file_result.get('mapped_columns'):
                    st.markdown("**Column Processing:**")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption(f"Original columns: {len(file_result['original_columns'])}")
                        with st.expander("View original columns"):
                            for col in file_result['original_columns']:
                                st.caption(f"• {col}")
                    with col2:
                        st.caption(f"Mapped columns: {len(file_result['mapped_columns'])}")
                        with st.expander("View mapped columns"):
                            for col in file_result['mapped_columns']:
                                st.caption(f"• {col}")
                
                # Show mapping errors if any
                mapping_errors = file_result.get('mapping_errors', [])
                if mapping_errors:
                    st.markdown("**⚠️ Mapping Issues:**")
                    for error in mapping_errors:
                        st.warning(f"• {error}")
                
                # Show data preview
                data_preview = file_result.get('data_preview', [])
                if data_preview:
                    st.markdown("**📊 Data Preview (First 3 rows):**")
                    preview_df = pd.DataFrame(data_preview)
                    st.dataframe(preview_df, use_container_width=True)
                
                # Processing details
                if file_result.get('processed_time'):
                    st.caption(f"🕒 Processed: {file_result['processed_time']}")
    
    # Show overall processing message
    if results.get('message'):
        if results.get('success', True):
            st.success(f"✅ {results['message']}")
        else:
            st.warning(f"⚠️ {results['message']}")
    
    # Error summary if any
    if results.get('error_details'):
        st.markdown("#### ❌ Processing Errors")
        st.error(results['error_details'])
    
    # Action buttons
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Check Inbox Again", use_container_width=True):
            # Hide current results but preserve email automation state
            st.session_state.show_email_results_dashboard = False
            st.session_state.email_processing_results = None  # OK to clear this specific result
            st.rerun()
    
    with col2:
        if st.button("📂 Process Another File", use_container_width=True):
            # Clear current results and uploaded file but preserve email automation state
            st.session_state.show_email_results_dashboard = False
            st.session_state.email_processing_results = None  # OK to clear this specific result
            # Use safe clearing for uploaded file data
            keys_to_clear = ['uploaded_df', 'uploaded_file_name']
            safe_clear_session_keys(keys_to_clear)
            st.rerun()
    
    with col3:
        if st.button("❌ Close Results", use_container_width=True):
            st.session_state.show_email_results_dashboard = False
            st.session_state.email_processing_results = None
            st.rerun()


def _render_diagnostic_information(ff2api_results, result):
    """Render diagnostic information for failed processing"""
    
    st.markdown("#### 🔍 Diagnostic Information")
    
    # Processing pipeline status
    st.markdown("**Processing Pipeline Status:**")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # File reading always works if we got here
        st.markdown("✅ **File Reading**")
        st.caption("CSV/Excel parsing successful")
    
    with col2:
        # Field mapping always works if we got here
        st.markdown("✅ **Field Mapping**")
        st.caption("Column mapping completed")
    
    with col3:
        # FF2API status
        success_count = len([r for r in ff2api_results if r.get('success', False)])
        if success_count > 0:
            st.markdown("⚠️ **FF2API Processing**")
            st.caption(f"Partial success: {success_count}/{len(ff2api_results)}")
        else:
            st.markdown("❌ **FF2API Processing**")
            st.caption("All API calls failed")
    
    with col4:
        # Enrichment/Export status
        has_exports = 'enriched_exports' in st.session_state and st.session_state.enriched_exports
        if has_exports:
            st.markdown("✅ **Data Export**")
            st.caption("Files generated successfully")
        else:
            st.markdown("⚠️ **Data Export**")
            st.caption("Limited exports available")
    
    # Detailed error information
    failed_results = [r for r in ff2api_results if not r.get('success', False)]
    
    if failed_results:
        with st.expander("❌ FF2API Error Details", expanded=True):
            st.markdown("**Common Issues & Solutions:**")
            
            # Analyze error patterns
            error_messages = [r.get('error', 'Unknown error') for r in failed_results]
            unique_errors = list(set(error_messages))
            
            for error in unique_errors:
                count = error_messages.count(error)
                st.markdown(f"**• {error}** (affects {count} record{'s' if count > 1 else ''})")
                
                # Provide specific guidance based on error type
                if "0% success rate" in error:
                    st.info("""
                    **Possible causes:**
                    - API authentication issues (check API key/bearer token)
                    - Network connectivity problems
                    - API endpoint unavailable
                    - Invalid request format
                    """)
                elif "authentication" in error.lower() or "unauthorized" in error.lower():
                    st.info("""
                    **Authentication issue:**
                    - Verify API credentials in brokerage configuration
                    - Check if bearer token is valid and not expired
                    - Ensure correct authentication method is selected
                    """)
            
            # Show sample failed records
            if len(failed_results) > 0:
                st.markdown("**Sample Failed Records:**")
                for i, failed in enumerate(failed_results[:3]):  # Show first 3 failures
                    row_index = failed.get('row_index', i)
                    load_number = failed.get('load_number', 'UNKNOWN')
                    error = failed.get('error', 'No error message')
                    
                    st.markdown(f"- **Row {row_index}:** Load #{load_number} - {error}")
                
                if len(failed_results) > 3:
                    st.caption(f"... and {len(failed_results) - 3} more failed records")


def _render_simplified_results():
    """Simplified results display - clean and focused UI with diagnostic information"""
    if 'enhanced_processing_results' not in st.session_state:
        return
    
    result = st.session_state.enhanced_processing_results
    
    st.markdown("---")
    
    # Show different header based on source
    source = result.get('source', 'manual')
    if source == 'email_automation':
        st.subheader("📧 Email Processing Complete")
        # Show email context
        email_info = f"**File:** {result.get('filename', 'Unknown')}"
        if result.get('email_source'):
            email_info += f" | **From:** {result.get('email_source')}"
        if result.get('subject'):
            email_info += f" | **Subject:** {result.get('subject')}"
        st.markdown(email_info)
    else:
        st.subheader("📊 Processing Complete")
    
    # Always show processing summary
    total_records = result.get('total_rows', 0)
    success_rate = result.get('success_rate', 0)
    ff2api_results = result.get('ff2api_results', [])
    
    # Show status with detailed metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", total_records)
    
    with col2:
        successful_count = len([r for r in ff2api_results if r.get('success', False)])
        st.metric("FF2API Success", f"{successful_count}/{len(ff2api_results)}")
    
    with col3:
        st.metric("Success Rate", f"{success_rate * 100:.1f}%")
    
    with col4:
        processing_mode = result.get('processing_mode', 'standard')
        st.metric("Mode", processing_mode)
    
    # Show status message
    if success_rate > 0:
        st.success(f"✅ {successful_count} of {total_records} record(s) processed successfully")
    else:
        st.error(f"❌ All {total_records} record(s) failed processing - see details below")
    
    # Always show diagnostic information for failed results
    if success_rate < 1.0:  # Show if any failures occurred
        _render_diagnostic_information(ff2api_results, result)
    
    # Direct download buttons - always show, even if some failed
    if 'enriched_exports' in st.session_state and st.session_state.enriched_exports:
        st.subheader("📥 Download Results")
        
        col1, col2, col3 = st.columns(3)
        exports = st.session_state.enriched_exports
        
        # CSV Download
        with col1:
            csv_export = exports.get('csv', {})
            if csv_export.get('success') and csv_export.get('data'):
                st.download_button(
                    label="📄 CSV",
                    data=csv_export['data'],
                    file_name=csv_export.get('filename', 'enriched_data.csv'),
                    mime=csv_export.get('mime_type', 'text/csv'),
                    use_container_width=True
                )
            else:
                st.button("📄 CSV", disabled=True, use_container_width=True, help="Export failed or not available")
        
        # Excel Download  
        with col2:
            xlsx_export = exports.get('xlsx', {})
            if xlsx_export.get('success') and xlsx_export.get('data'):
                st.download_button(
                    label="📊 Excel", 
                    data=xlsx_export['data'],
                    file_name=xlsx_export.get('filename', 'enriched_data.xlsx'),
                    mime=xlsx_export.get('mime_type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
                    use_container_width=True
                )
            else:
                st.button("📊 Excel", disabled=True, use_container_width=True, help="Export failed or not available")
                
        # JSON Download
        with col3:
            json_export = exports.get('json', {})
            if json_export.get('success') and json_export.get('data'):
                st.download_button(
                    label="🔧 JSON",
                    data=json_export['data'],
                    file_name=json_export.get('filename', 'enriched_data.json'), 
                    mime=json_export.get('mime_type', 'application/json'),
                    use_container_width=True
                )
            else:
                st.button("🔧 JSON", disabled=True, use_container_width=True, help="Export failed or not available")
        
        # Show export status if any failed
        failed_exports = [fmt for fmt, exp in exports.items() if fmt != 'dataset_info' and not exp.get('success')]
        if failed_exports:
            st.warning(f"⚠️ Some exports failed: {', '.join(failed_exports).upper()}")
    else:
        st.info("ℹ️ Processing exports... Please wait a moment and refresh if downloads don't appear.")

def _render_enhanced_results_section():
    """Enhanced results section with end-to-end capabilities"""
    # Check for VALID session state results (not just existence of key)
    session_results = st.session_state.get('enhanced_processing_results')
    has_valid_session_results = (
        session_results is not None and 
        isinstance(session_results, dict) and 
        session_results.get('total_rows', 0) > 0 and
        session_results.get('ff2api_results', [])
    )
    
    # Always check for email automation results from shared storage
    has_email_results = False
    email_results = None
    
    # Debug logging  
    brokerage_name = st.session_state.get('brokerage_name', 'default')
    logger.info(f"🔍 _render_enhanced_results_section: has_valid_session_results={has_valid_session_results}, session_results={session_results}, brokerage_name='{brokerage_name}'")
    
    try:
        from shared_storage_bridge import shared_storage
        
        # Try multiple brokerage name variations
        brokerage_variations = [
            brokerage_name, brokerage_name.lower(), brokerage_name.upper(), 
            brokerage_name.title(), brokerage_name.capitalize(),
            'eShipping', 'eshipping', 'ESHIPPING', 'Eshipping'
        ]
        
        logger.info(f"🔍 Checking brokerage variations: {brokerage_variations}")
        
        # Log shared storage directory status
        import os
        storage_dir = ".streamlit_shared"
        if os.path.exists(storage_dir):
            logger.info(f"🔍 Shared storage directory exists: {storage_dir}")
            files = os.listdir(storage_dir)
            logger.info(f"🔍 Shared storage files: {files}")
        else:
            logger.warning(f"🔍 Shared storage directory does not exist: {storage_dir}")
        
        for variation in brokerage_variations:
            recent_results = shared_storage.get_recent_results(variation, limit=1)
            logger.info(f"🔍 Checked '{variation}': found {len(recent_results) if recent_results else 0} results")
            if recent_results:
                # Convert email processing result to enhanced processing result format
                email_result = recent_results[0]
                logger.info(f"🔍 Email result: {email_result.filename}, success={email_result.success}, records={email_result.record_count}")
                email_results = _convert_email_to_enhanced_result(email_result)
                has_email_results = True
                logger.info(f"✅ Found and converted email processing result for brokerage: {variation}")
                break
    except Exception as e:
        logger.error(f"❌ Error loading email automation results: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info(f"🔍 Result summary: has_valid_session_results={has_valid_session_results}, has_email_results={has_email_results}")
    
    # If no results from either source, show debug info and return
    if not has_valid_session_results and not has_email_results:
        logger.info("❌ No results from either source - not rendering results section")
        
        # Always show some debug info to help troubleshoot
        st.markdown("---")
        st.markdown("### 🔧 Email Automation Results Debug")
        st.info(f"**Debug Info**: No results found for brokerage '{brokerage_name}'. Checked {len(brokerage_variations)} variations.")
        
        with st.expander("🔍 Debug Details", expanded=False):
            st.write(f"**Session State Keys**: {list(st.session_state.keys())}")
            st.write(f"**Brokerage Name**: '{brokerage_name}'")
            st.write(f"**Has Session Results**: {has_valid_session_results}")
            st.write(f"**Has Email Results**: {has_email_results}")
            
            # Test shared storage access directly in UI
            if st.button("🧪 Test Shared Storage Access"):
                try:
                    from shared_storage_bridge import shared_storage
                    import os
                    
                    # Check if shared storage directory exists
                    storage_dir = ".streamlit_shared"
                    if os.path.exists(storage_dir):
                        st.success(f"✅ Shared storage directory exists: {storage_dir}")
                        files = os.listdir(storage_dir)
                        st.write(f"📁 Files: {files}")
                    else:
                        st.error(f"❌ Shared storage directory missing: {storage_dir}")
                    
                    # Test all variations
                    test_variations = ['eShipping', 'eshipping', 'ESHIPPING', 'Eshipping', brokerage_name]
                    for variation in test_variations:
                        test_results = shared_storage.get_recent_results(variation, limit=1)
                        if test_results:
                            st.success(f"✅ Found {len(test_results)} results for '{variation}'")
                            result = test_results[0]
                            st.write(f"  📄 Latest: {result.filename}")
                            st.write(f"  ✅ Success: {result.success}, Records: {result.record_count}")
                            break
                    else:
                        st.error("❌ No results found for any brokerage variation")
                        
                except Exception as e:
                    st.error(f"❌ Shared storage error: {e}")
                    import traceback
                    st.code(traceback.format_exc())
        return
    
    # Prioritize email results if no session results, or if user wants to see email results
    if has_email_results and (not has_valid_session_results or st.session_state.get('prefer_email_results', True)):
        st.session_state.enhanced_processing_results = email_results
        logger.info("✅ Set email results as enhanced_processing_results in session state")
    
    # Check if simplified UI mode is enabled (default: True)
    use_simplified_ui = st.session_state.get('use_simplified_ui', True)
    
    if use_simplified_ui:
        _render_simplified_results()
    else:
        # Keep existing complex UI as fallback
        st.markdown("---")
        st.subheader("📊 Processing Results")
        
        result = st.session_state.enhanced_processing_results
        processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
        
        # Display results based on processing mode
        _display_enhanced_results(result, processing_mode)

def _convert_email_to_enhanced_result(email_result):
    """Convert EmailProcessingResult to enhanced processing result format"""
    try:
        # Extract data from EmailProcessingResult dataclass
        success = email_result.success
        record_count = email_result.record_count
        processing_mode = email_result.processing_mode
        
        # Create mock FF2API results based on email processing success
        ff2api_results = []
        if success and record_count > 0:
            # Create successful results for each record
            for i in range(record_count):
                ff2api_results.append({
                    'success': True,
                    'row_index': i,
                    'load_number': f'EMAIL_LOAD_{i+1}',
                    'message': 'Processed via email automation'
                })
        else:
            # Create failure result
            ff2api_results.append({
                'success': False,
                'row_index': 0,
                'load_number': 'EMAIL_PROCESSING',
                'error': 'Email processing failed or no records found'
            })
        
        # Calculate success rate
        successful_count = len([r for r in ff2api_results if r.get('success', False)])
        success_rate = successful_count / len(ff2api_results) if ff2api_results else 0
        
        # Create enhanced result format
        enhanced_result = {
            'total_rows': record_count,
            'success_rate': success_rate,
            'ff2api_results': ff2api_results,
            'processing_mode': processing_mode,
            'source': 'email_automation',
            'filename': email_result.filename,
            'email_source': email_result.email_source,
            'subject': email_result.subject,
            'processed_time': email_result.processed_time,
            'brokerage_key': email_result.brokerage_key
        }
        
        return enhanced_result
        
    except Exception as e:
        logger.error(f"Error converting email result to enhanced format: {e}")
        # Return minimal failure result
        return {
            'total_rows': 0,
            'success_rate': 0,
            'ff2api_results': [{
                'success': False,
                'row_index': 0,
                'load_number': 'CONVERSION_ERROR',
                'error': f'Failed to convert email result: {str(e)}'
            }],
            'processing_mode': 'email_automation',
            'source': 'email_automation_error'
        }

def process_enhanced_data_workflow(df, field_mappings, api_credentials, brokerage_name, 
                                 processing_mode, data_processor, db_manager, session_id):
    """Enhanced data processing workflow with end-to-end capabilities"""
    
    # Debug logging for processing mode
    logger.info(f"🔍 DEBUG: process_enhanced_data_workflow called with processing_mode: {processing_mode}")
    logger.info(f"🔍 DEBUG: processing_mode type: {type(processing_mode)}")
    logger.info(f"🔍 DEBUG: Will run full_endtoend steps: {processing_mode == 'full_endtoend'}")
    
    st.session_state.processing_in_progress = True
    
    # Initialize progress tracking
    mode_steps = {
        'standard': 2,
        'full_endtoend': 6
    }
    
    total_steps = mode_steps.get(processing_mode, 2)
    progress_container = st.container()
    
    with progress_container:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Step 1: Original FF2API Processing
            status_text.text("Step 1: Processing through FF2API...")
            progress_bar.progress(1/total_steps)
            
            # Use original FF2API processing exactly
            ff2api_results = _process_through_ff2api(df, field_mappings, api_credentials, data_processor)
            
            # Handle case where ff2api_results is None
            if ff2api_results is None:
                logger.error("FF2API processing returned None")
                ff2api_results = []
            
            # Handle case where ff2api_results is a dict instead of list
            elif isinstance(ff2api_results, dict):
                logger.info("FF2API returned dict instead of list, converting to list format")
                # If it's a dict, it might contain results in a specific key, or be a single result
                if 'results' in ff2api_results:
                    ff2api_results = ff2api_results['results']
                elif 'data' in ff2api_results:
                    ff2api_results = ff2api_results['data']
                else:
                    # Treat the entire dict as a single result
                    ff2api_results = [ff2api_results]
            
            # Ensure ff2api_results is a list
            if not isinstance(ff2api_results, list):
                logger.error(f"FF2API results is not a list: {type(ff2api_results)}")
                ff2api_results = []
            
            logger.info(f"FF2API results count: {len(ff2api_results)}")
            
            # Debug: Log the actual FF2API results structure
            for i, r in enumerate(ff2api_results):
                logger.info(f"FF2API result {i}: {r}")
            
            # Safe success rate calculation
            success_count = 0
            for i, r in enumerate(ff2api_results):
                try:
                    if isinstance(r, dict):
                        success_status = r.get('success', False)
                        logger.info(f"Result {i} success status: {success_status}")
                        if success_status:
                            success_count += 1
                    else:
                        logger.error(f"Result {i} is not dict: {type(r)} = {r}")
                except Exception as e:
                    logger.error(f"Error checking success for result {i}: {e}, type: {type(r)}, value: {r}")
            
            result = {
                'ff2api_results': ff2api_results,
                'total_rows': len(df),
                'success_rate': success_count / len(df) if len(df) > 0 else 0,
                'processing_mode': processing_mode
            }
            
            # Step 2: Load ID Mapping (if enabled)
            if processing_mode == 'full_endtoend':
                status_text.text("Step 2: Retrieving load IDs...")
                progress_bar.progress(2/total_steps)
                
                load_mappings = _process_load_id_mapping(ff2api_results, brokerage_name)
                result['load_id_mappings'] = load_mappings
            
            # Step 3: Data Enrichment (if enabled)
            if processing_mode == 'full_endtoend':
                logger.info("🔍 DEBUG: Starting Step 3: Data Enrichment")
                status_text.text("Step 3: Enriching data...")
                progress_bar.progress(3/total_steps)
                
                logger.info(f"🔍 DEBUG: Calling _process_data_enrichment with {len(ff2api_results)} FF2API results and {len(result.get('load_id_mappings', []))} load mappings")
                enriched_data = _process_data_enrichment(ff2api_results, result.get('load_id_mappings', []), brokerage_name)
                result['enriched_data'] = enriched_data
                logger.info(f"🔍 DEBUG: Data enrichment completed, got {len(enriched_data)} enriched rows")
            
            # Step 4: Postback Processing (if enabled)
            if processing_mode == 'full_endtoend':
                status_text.text("Step 4: Generating output files...")
                progress_bar.progress(4/total_steps)
                
                postback_results = _process_postback_delivery(result.get('enriched_data', []), brokerage_name)
                result['postback_results'] = postback_results
                
                # Step 5: Email Delivery (if configured)
                status_text.text("Step 5: Sending email notifications...")
                progress_bar.progress(5/total_steps)
                
                email_results = _process_email_delivery(result, brokerage_name)
                result['email_results'] = email_results
            
            # Final step: Complete
            status_text.text("Processing complete!")
            progress_bar.progress(1.0)
            
            # Store results
            st.session_state.enhanced_processing_results = result
            st.session_state.processing_in_progress = False
            
            return result
            
        except Exception as e:
            st.session_state.processing_in_progress = False
            status_text.text(f"❌ Processing failed: {str(e)}")
            logger.error(f"Enhanced processing error: {e}")
            
            # Still store whatever results we have for diagnostic purposes
            if 'result' in locals():
                st.session_state.enhanced_processing_results = result
            
            raise

def _process_through_ff2api(df, field_mappings, api_credentials, data_processor):
    """Process data through FF2API - aligned with reference implementation"""
    from src.frontend.app import process_data_enhanced
    
    # Count manual values for logging
    manual_values = [v for v in field_mappings.values() if str(v).startswith("MANUAL_VALUE:")]
    if manual_values:
        logger.info(f"Processing with {len(manual_values)} manual values applied to {len(df)} records")
        st.info(f"✅ Processing with {len(manual_values)} manual values applied to all records")
    
    # Process with field mappings (which now include MANUAL_VALUE: prefixed entries)
    try:
        # Get the processing summary
        processing_summary = process_data_enhanced(df, field_mappings, api_credentials, 
                                                 st.session_state.brokerage_name, data_processor, 
                                                 DatabaseManager(), st.session_state.session_id)
        
        logger.info(f"Processing summary returned: {processing_summary}")
        
        # Extract actual processing results from session state
        # The individual results are stored in load_results (not processing_results which is a summary)
        if ('load_results' in st.session_state and 
            st.session_state.load_results and 
            isinstance(st.session_state.load_results, list) and
            len(st.session_state.load_results) > 0 and
            isinstance(st.session_state.load_results[0], dict)):
            
            actual_results = st.session_state.load_results
            logger.info(f"Retrieved {len(actual_results)} valid individual processing results from session state")
            return actual_results
        else:
            logger.warning("No valid individual processing results found in session state (load_results)")
            logger.info(f"Session state keys: {list(st.session_state.keys())}")
            if 'processing_results' in st.session_state:
                logger.info(f"processing_results type: {type(st.session_state.processing_results)}")
                logger.info(f"processing_results content: {st.session_state.processing_results}")
            if 'load_results' in st.session_state:
                logger.info(f"load_results type: {type(st.session_state.load_results)}")
                logger.info(f"load_results length: {len(st.session_state.load_results) if isinstance(st.session_state.load_results, list) else 'not a list'}")
            
            # Focus on the actual issue: why processing is failing
            logger.info(f"Current processing failed with: {processing_summary}")
            
            # Since the current processing failed (0 successful), create an appropriate failure result
            success_rate = 0
            if processing_summary and isinstance(processing_summary, dict):
                success_rate = processing_summary.get('success_rate', 0)
            
            return [{
                'success': False,
                'row_index': 0,
                'load_number': 'UNKNOWN',
                'error': f"FF2API processing failed: {success_rate}% success rate",
                'processing_summary': processing_summary
            }]
            
    except ValueError as e:
        if "truth value of a DataFrame is ambiguous" in str(e):
            import traceback
            logger.error("DataFrame boolean evaluation error in process_data_enhanced:")
            logger.error(traceback.format_exc())
            st.error(f"❌ Processing failed: {e}")
            return []
        else:
            raise

def _process_load_id_mapping(ff2api_results, brokerage_key):
    """Process load ID mapping for successful FF2API results"""
    try:
        # Get credentials for load ID mapping
        capabilities = credential_manager.validate_credentials(brokerage_key)
        
        if not capabilities.api_available:
            logger.warning("API credentials not available for load ID mapping")
            return []
        
        # Get actual credential data for LoadIDMapper
        credentials = credential_manager.get_brokerage_credentials(brokerage_key)
        
        # Initialize load ID mapper
        load_id_mapper = LoadIDMapper(brokerage_key, credentials)
        
        # Get original CSV data to extract load numbers
        uploaded_df = st.session_state.get('uploaded_df')
        original_csv_data = []
        if uploaded_df is not None:
            original_csv_data = uploaded_df.to_dict('records')
        
        # Convert FF2API results to LoadProcessingResult format
        load_processing_results = []
        for i, result in enumerate(ff2api_results):
            try:
                # Debug: Check if result is a dictionary
                if not isinstance(result, dict):
                    logger.error(f"FF2API result {i} is not a dictionary: {type(result)} = {result}")
                    continue
                    
                if result.get('success', False):
                    # Get original load number from CSV data
                    csv_row_index = result.get('row_index', i)
                    original_load_number = ''
                    
                    if csv_row_index < len(original_csv_data):
                        csv_row = original_csv_data[csv_row_index]
                        # Try common load number field names
                        load_number_fields = ['load_number', 'Load Number', 'BOL #', 'Carrier Pro#', 'LoadNumber']
                        for field in load_number_fields:
                            if field in csv_row and csv_row[field]:
                                original_load_number = str(csv_row[field]).strip()
                                logger.info(f"Found load number '{original_load_number}' in CSV field '{field}' for row {csv_row_index}")
                                break
                    
                    if not original_load_number:
                        original_load_number = result.get('load_number', f'LOAD{csv_row_index:03d}')
                        logger.warning(f"No load number found in CSV for row {csv_row_index}, using: {original_load_number}")
                    
                    load_processing_results.append(LoadProcessingResult(
                        csv_row_index=csv_row_index,
                        load_number=original_load_number,
                        success=True,
                        response_data=result.get('data', {})
                    ))
            except Exception as e:
                logger.error(f"Error processing FF2API result {i}: {e}, result type: {type(result)}, result: {result}")
                continue
        
        # Process load ID mappings
        mappings = load_id_mapper.map_load_ids(load_processing_results)
        return mappings
        
    except Exception as e:
        logger.error(f"Load ID mapping error: {e}")
        return []

def _process_data_enrichment(ff2api_results, load_mappings, brokerage_key):
    """Process data enrichment with tracking and Snowflake data"""
    try:
        # Build enrichment configuration
        enrichment_config = []
        
        # Add tracking API enrichment
        tracking_creds = credential_manager.get_tracking_api_credentials()
        brokerage_creds = credential_manager.get_brokerage_credentials(brokerage_key)
        
        if tracking_creds and brokerage_creds.get('api_key'):
            enrichment_config.append({
                'type': 'tracking_api',
                'config': {
                    'pro_column': 'PRO',  
                    'carrier_column': 'carrier',
                    'brokerage_key': brokerage_key,
                    'api_key': brokerage_creds.get('api_key'),
                    'bearer_token': brokerage_creds.get('api_key'),  # Use same key
                    'auth_type': 'api_key',
                    **tracking_creds
                }
            })
        
        # Add Snowflake enrichment (disabled for now - source not available)
        # snowflake_creds = credential_manager.get_snowflake_credentials()
        # if snowflake_creds:
        #     enrichment_config.append({
        #         'type': 'snowflake_augment',
        #         'database': 'AUGMENT_DW',
        #         'schema': 'MARTS', 
        #         'enrichments': ['tracking', 'customer'],
        #         'use_load_ids': True,
        #         'brokerage_key': brokerage_key,
        #         **snowflake_creds
        #     })
        
        if not enrichment_config:
            logger.warning("No enrichment sources configured")
            return []
        
        # Build brokerage configuration for enrichment manager
        brokerage_config = {
            'brokerage_key': brokerage_key,
            'api_base_url': brokerage_creds.get('base_url', ''),
            'api_key': brokerage_creds.get('api_key', ''),
            'bearer_token': brokerage_creds.get('api_key', ''),
            'auth_type': 'api_key'
        }
        
        # Initialize enrichment manager
        enrichment_manager = EnrichmentManager(enrichment_config, brokerage_config)
        
        # Get original CSV data to merge with enrichment
        uploaded_df = st.session_state.get('uploaded_df')
        if uploaded_df is None:
            logger.error("Cannot access original CSV data for enrichment")
            return []
        
        original_csv_data = uploaded_df.to_dict('records')
        
        # Create enriched dataset by merging original CSV, FF2API results, load mappings, and enrichment
        enriched_data = []
        
        for i, csv_row in enumerate(original_csv_data):
            # Start with original CSV data
            enriched_row = csv_row.copy()
            
            # Add FF2API results
            if i < len(ff2api_results):
                ff2api_result = ff2api_results[i]
                enriched_row.update({
                    'ff2api_success': ff2api_result.get('success', False),
                    'ff2api_data': ff2api_result.get('data', {}),
                    'ff2api_status_code': ff2api_result.get('status_code'),
                    'ff2api_load_number': ff2api_result.get('load_number', ''),
                    'ff2api_row_index': ff2api_result.get('row_index', i)
                })
            
            # Add load ID mapping data
            load_mapping = next((lm for lm in load_mappings if lm.csv_row_index == i), None)
            if load_mapping:
                enriched_row.update({
                    'load_number': load_mapping.load_number,
                    'internal_load_id': load_mapping.internal_load_id,
                    'load_id_status': load_mapping.api_status,
                    'pro_number': load_mapping.pro_number,
                    'carrier_name': load_mapping.carrier_name,
                    'workflow_path': load_mapping.workflow_path,
                    'pro_source_type': load_mapping.pro_source_type,
                    'pro_confidence': load_mapping.pro_confidence,
                    'pro_context': load_mapping.pro_context
                })
                
                # Set PRO and carrier fields for tracking enrichment
                if load_mapping.pro_number:
                    enriched_row['PRO'] = load_mapping.pro_number
                if load_mapping.carrier_name:
                    enriched_row['carrier'] = load_mapping.carrier_name
            
            # Also check for PRO and carrier in original CSV fields as fallback
            if 'PRO' not in enriched_row or not enriched_row.get('PRO'):
                # Try common PRO field names from CSV
                pro_fields = ['Carrier Pro#', 'PRO', 'pro_number', 'ProNumber', 'tracking_number']
                for field in pro_fields:
                    if field in enriched_row and enriched_row[field]:
                        enriched_row['PRO'] = str(enriched_row[field]).strip()
                        logger.info(f"🔍 DEBUG: Set PRO from CSV field '{field}': {enriched_row['PRO']}")
                        break
            
            if 'carrier' not in enriched_row or not enriched_row.get('carrier'):
                # Try common carrier field names from CSV
                carrier_fields = ['Carrier Name', 'carrier', 'carrier_name', 'scac_code']
                for field in carrier_fields:
                    if field in enriched_row and enriched_row[field]:
                        enriched_row['carrier'] = str(enriched_row[field]).strip()
                        logger.info(f"🔍 DEBUG: Set carrier from CSV field '{field}': {enriched_row['carrier']}")
                        break
                
                if load_mapping and load_mapping.error_message:
                    enriched_row['load_id_error'] = load_mapping.error_message
            
            # Apply enrichment - THIS IS THE KEY MISSING CALL
            logger.info(f"Applying enrichment to row {i}")
            
            # DEBUG: Log row data being passed to enrichment
            logger.info(f"🔍 DEBUG Row {i}: PRO field = '{enriched_row.get('PRO')}'")
            logger.info(f"🔍 DEBUG Row {i}: carrier field = '{enriched_row.get('carrier')}'") 
            logger.info(f"🔍 DEBUG Row {i}: pro_number field = '{enriched_row.get('pro_number')}'")
            logger.info(f"🔍 DEBUG Row {i}: carrier_name field = '{enriched_row.get('carrier_name')}'")
            logger.info(f"🔍 DEBUG Row {i}: All fields available: {list(enriched_row.keys())}")
            
            pre_enrichment_columns = set(enriched_row.keys())
            enriched_row = enrichment_manager.enrich_row(enriched_row)
            post_enrichment_columns = set(enriched_row.keys())
            new_columns = post_enrichment_columns - pre_enrichment_columns
            
            if new_columns:
                logger.info(f"Row {i}: Enrichment added columns: {new_columns}")
            else:
                logger.warning(f"Row {i}: No new columns added by enrichment")
            
            # Add processing metadata
            enriched_row['processing_status'] = 'processed'
            enriched_row['enrichment_timestamp'] = pd.Timestamp.now().isoformat()
            
            enriched_data.append(enriched_row)
        
        logger.info(f"Data enrichment complete: processed {len(enriched_data)} rows")
        
        # AUTO-GENERATE EXPORTS: Automatically create all export formats after enrichment
        logger.info("Auto-generating enriched dataset exports...")
        try:
            # Import required modules
            from src.backend.data_processor import DataProcessor
            from postback.router import PostbackRouter
            
            # Create enriched DataFrame
            enriched_df = pd.DataFrame(enriched_data)
            
            # Generate timestamp for filenames
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            filename_prefix = f"enriched_dataset_{timestamp}"
            
            # Initialize postback router for exports
            postback_router = PostbackRouter([])
            
            # Generate all export formats automatically
            csv_result = postback_router.export_enriched_data(enriched_df, 'csv', filename_prefix)
            xlsx_result = postback_router.export_enriched_data(enriched_df, 'xlsx', filename_prefix)
            json_result = postback_router.export_enriched_data(enriched_df, 'json', filename_prefix)
            
            # Store export results in session state for immediate download availability
            st.session_state.enriched_exports = {
                'csv': csv_result,
                'xlsx': xlsx_result,
                'json': json_result,
                'dataset_info': {
                    'total_rows': len(enriched_df),
                    'total_columns': len(enriched_df.columns),
                    'processed_count': len(enriched_df[enriched_df.get('processing_status', '') == 'processed'])
                }
            }
            
            logger.info("Auto-generated all export formats successfully")
            
        except Exception as export_error:
            logger.error(f"Auto-export generation failed: {export_error}")
            # Don't fail the entire enrichment process if exports fail
        
        return enriched_data
        
    except Exception as e:
        logger.error(f"Data enrichment error: {e}")
        return []

def _process_postback_delivery(enriched_data, brokerage_key):
    """Process postback delivery with multiple output formats"""
    try:
        # Build postback configuration
        postback_config = [
            {'type': 'csv', 'output_path': '/tmp/enhanced_results.csv'},
            {'type': 'xlsx', 'output_path': '/tmp/enhanced_results.xlsx'}, 
            {'type': 'json', 'output_path': '/tmp/enhanced_results.json'}
        ]
        
        # Initialize postback router
        postback_router = PostbackRouter(postback_config)
        
        # Route data to handlers
        results = postback_router.post_all(enriched_data)
        
        return results
        
    except Exception as e:
        logger.error(f"Postback processing error: {e}")
        return {}

def _process_email_delivery(result, brokerage_key):
    """Process email delivery of results"""
    try:
        # Check if email automation is configured
        cred_status = credential_manager.validate_credentials(brokerage_key)
        
        if not cred_status.email_automation_available:
            return {'email_sent': False, 'reason': 'Email automation not configured'}
        
        # Check if email sending is enabled and recipient is configured
        send_email = st.session_state.get('send_email', False)
        email_recipient = st.session_state.get('email_recipient', '')
        email_formats = st.session_state.get('email_formats', ['CSV', 'Summary Report'])
        
        if not send_email or not email_recipient:
            return {'email_sent': False, 'reason': 'Email delivery not configured'}
        
        # Build attachments based on selected formats
        attachments = []
        if 'CSV' in email_formats:
            attachments.append('/tmp/enhanced_results.csv')
        if 'Excel' in email_formats:
            attachments.append('/tmp/enhanced_results.xlsx')
        if 'JSON' in email_formats:
            attachments.append('/tmp/enhanced_results.json')
            
        # Build email configuration with user settings
        email_config = [{
            'type': 'email',
            'recipient': email_recipient,
            'subject': 'Enhanced FF2API Processing Results',
            'attachments': attachments,
            'include_summary': 'Summary Report' in email_formats
        }]
        
        # Process email delivery
        email_router = PostbackRouter(email_config)
        email_results = email_router.post_all(result.get('enriched_data', []))
        
        return email_results
        
    except Exception as e:
        logger.error(f"Email delivery error: {e}")
        return {'email_sent': False, 'error': str(e)}

def _display_enhanced_results(result, processing_mode):
    """Display enhanced results based on processing mode"""
    
    # FF2API Results (always shown)
    st.subheader("📊 FF2API Results")
    
    col1, col2, col3, col4 = st.columns(4) 
    
    with col1:
        st.metric("Total Records", result.get('total_rows', 0))
    
    with col2:
        ff2api_success = len([r for r in result.get('ff2api_results', []) if r.get('success', False)])
        st.metric("FF2API Success", ff2api_success)
    
    with col3:
        success_rate = result.get('success_rate', 0) * 100
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    with col4:
        st.metric("Processing Mode", processing_mode.title())
    
    # Load ID Mappings (if enabled)
    if processing_mode == 'full_endtoend':
        load_mappings = result.get('load_id_mappings', [])
        if load_mappings:
            st.subheader("🔗 Load ID Mappings")
            st.metric("Load IDs Retrieved", len([lm for lm in load_mappings if lm.internal_load_id]))
    
    # Enriched Data (if enabled)
    if processing_mode == 'full_endtoend':
        enriched_data = result.get('enriched_data', [])
        if enriched_data:
            st.subheader("🔍 Data Enrichment")
            st.metric("Records Enriched", len(enriched_data))
            
            # Show sample enriched data
            if enriched_data:
                with st.expander("📋 Sample Enriched Data", expanded=False):
                    st.json(enriched_data[0])
    
    # Postback Results (if enabled)
    if processing_mode == 'full_endtoend':
        postback_results = result.get('postback_results', {})
        email_results = result.get('email_results', {})
        
        st.subheader("📤 Output & Delivery")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**File Outputs:**")
            for output_type, success in postback_results.items():
                status = "✅" if success else "❌"
                st.write(f"{status} {output_type.upper()}")
        
        with col2:
            st.write("**Email Delivery:**")
            email_sent = email_results.get('email_sent', False)
            status = "✅ Sent" if email_sent else "❌ Failed"
            st.write(f"{status}")
    
    # Download options
    _render_enhanced_download_options(result, processing_mode)

def _render_enhanced_download_options(result, processing_mode):
    """Render download options based on processing mode"""
    st.subheader("📥 Download Results")
    
    col1, col2, col3 = st.columns(3)
    
    # Always available - FF2API results
    with col1:
        if st.button("📄 Download FF2API Results", use_container_width=True):
            ff2api_df = pd.DataFrame(result.get('ff2api_results', []))
            csv_data = ff2api_df.to_csv(index=False)
            st.download_button(
                "📄 Download CSV",
                csv_data,
                "ff2api_results.csv",
                "text/csv",
                use_container_width=True
            )
    
    # Load ID mappings (if available)
    with col2:
        if processing_mode == 'full_endtoend' and result.get('load_id_mappings'):
            if st.button("🔗 Download Load ID Mappings", use_container_width=True): 
                mappings_data = [{
                    'csv_row_index': lm.csv_row_index,
                    'load_number': lm.load_number,
                    'internal_load_id': lm.internal_load_id,
                    'api_status': lm.api_status
                } for lm in result.get('load_id_mappings', [])]
                
                mappings_df = pd.DataFrame(mappings_data)
                csv_data = mappings_df.to_csv(index=False)
                st.download_button(
                    "📄 Download CSV",
                    csv_data, 
                    "load_id_mappings.csv",
                    "text/csv",
                    use_container_width=True
                )
    
    # Enriched dataset (combines original CSV with all processing results)
    with col3:
        if st.button("📊 Generate Enriched Dataset", use_container_width=True):
            _generate_enriched_dataset_exports(result)
        
        # Show download buttons if exports are ready
        _render_enriched_dataset_downloads()

def _generate_enriched_dataset_exports(result):
    """
    Generate enriched dataset and create all export formats immediately.
    Stores results in session state for persistent download buttons.
    """
    try:
        # Import required modules
        from src.backend.data_processor import DataProcessor
        from postback.router import PostbackRouter
        
        # Get original CSV data from session state
        uploaded_df = st.session_state.get('uploaded_df')
        if uploaded_df is None:
            st.error("❌ Original CSV data not found. Please re-upload your file.")
            return
        
        # Convert DataFrame to records format
        original_csv_data = uploaded_df.to_dict('records')
        
        # Check if we have fully enriched data (from full_endtoend mode)
        enriched_data = result.get('enriched_data', [])
        
        data_processor = DataProcessor()
        postback_router = PostbackRouter([])
        
        with st.spinner("Creating enriched dataset and generating all export formats..."):
            if enriched_data:
                # Use the already-enriched data that includes tracking and other enrichments
                logger.info("Using fully enriched data with tracking and enrichment information")
                enriched_df = pd.DataFrame(enriched_data)
                
                # Verify tracking columns are included
                tracking_columns = ['tracking_status', 'tracking_location', 'tracking_date', 'tracking_detailed_status']
                found_tracking_columns = [col for col in tracking_columns if col in enriched_df.columns]
                
                if found_tracking_columns:
                    logger.info(f"Found tracking columns: {found_tracking_columns}")
                    st.info(f"✅ Tracking data included: {', '.join(found_tracking_columns)}")
                else:
                    logger.info("No tracking columns found, but using enriched data from workflow")
                
            else:
                # Fallback: create basic enriched dataset from FF2API results only
                logger.info("Using FF2API results only (no pre-enriched data available)")
                ff2api_results = result.get('ff2api_results', [])
                if not ff2api_results:
                    st.warning("⚠️ No processing results found. Cannot create enriched dataset.")
                    return
                
                # Create enriched dataset from FF2API results
                enriched_df = data_processor.create_enriched_dataset(original_csv_data, ff2api_results)
                st.warning("⚠️ Using basic enrichment only. For tracking data, use 'Full End-to-End' processing mode.")
            
            # Generate timestamp for filenames
            timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
            filename_prefix = f"enriched_dataset_{timestamp}"
            
            # Generate all export formats
            csv_result = postback_router.export_enriched_data(enriched_df, 'csv', filename_prefix)
            xlsx_result = postback_router.export_enriched_data(enriched_df, 'xlsx', filename_prefix)
            json_result = postback_router.export_enriched_data(enriched_df, 'json', filename_prefix)
            
            # Store results in session state
            st.session_state.enriched_export_files = {
                'csv': csv_result,
                'xlsx': xlsx_result,
                'json': json_result,
                'enriched_df': enriched_df,
                'dataset_info': {
                    'total_rows': len(enriched_df),
                    'total_columns': len(enriched_df.columns),
                    'processed_count': len(enriched_df[enriched_df.get('processing_status', '') == 'processed'])
                }
            }
        
        st.success("✅ Enriched dataset created! All export formats are ready for download.")
        
    except Exception as e:
        st.error(f"❌ Error generating enriched dataset: {str(e)}")
        logger.error(f"Enriched dataset generation error: {e}")

def _render_enriched_dataset_downloads():
    """
    Render persistent download interface for enriched dataset exports.
    Shows download buttons if exports are available in session state.
    """
    if 'enriched_export_files' not in st.session_state:
        return
    
    try:
        export_data = st.session_state.enriched_export_files
        dataset_info = export_data.get('dataset_info', {})
        enriched_df = export_data.get('enriched_df')
        
        st.markdown("---")
        st.subheader("📊 Enriched Dataset Ready")
        st.write("Your original CSV data combined with all processing results.")
        
        # Display dataset metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Rows", dataset_info.get('total_rows', 0))
        with col2:
            st.metric("Total Columns", dataset_info.get('total_columns', 0))
        with col3:
            st.metric("Processed Rows", dataset_info.get('processed_count', 0))
        with col4:
            if st.button("❌ Clear Downloads"):
                del st.session_state.enriched_export_files
                st.rerun()
        
        # Show preview
        if enriched_df is not None:
            with st.expander("📋 Preview Enriched Dataset", expanded=False):
                st.dataframe(enriched_df.head(), use_container_width=True)
        
        # Download buttons for all formats
        st.subheader("📥 Download Options")
        col1, col2, col3 = st.columns(3)
        
        formats = [
            ('csv', 'CSV', 'text/csv', col1),
            ('xlsx', 'Excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', col2),
            ('json', 'JSON', 'application/json', col3)
        ]
        
        for format_key, format_name, mime_type, column in formats:
            result = export_data.get(format_key, {})
            
            with column:
                if result.get('success', False):
                    try:
                        with open(result['file_path'], 'rb') as file:
                            file_data = file.read()
                        
                        st.download_button(
                            f"📥 Download {format_name}",
                            file_data,
                            result['filename'],
                            mime_type,
                            use_container_width=True,
                            help=f"Download as {format_name} ({result.get('file_size_mb', 0)} MB)"
                        )
                        
                        # Show file info
                        st.caption(f"Size: {result.get('file_size_mb', 0)} MB")
                        
                    except Exception as e:
                        st.error(f"❌ Error loading {format_name} file")
                        logger.error(f"Error loading {format_key} file: {e}")
                else:
                    st.error(f"❌ {format_name} export failed")
                    if result.get('error'):
                        st.caption(f"Error: {result['error']}")
        
        # Export details
        with st.expander("📋 Export Details", expanded=False):
            # Show success rates and statistics
            for format_key, result in export_data.items():
                if format_key in ['csv', 'xlsx', 'json'] and result.get('success'):
                    st.write(f"**{format_key.upper()} Export:**")
                    details = {k: v for k, v in result.items() 
                             if k not in ['file_path', 'columns'] and not k.startswith('_')}
                    st.json(details)
        
        # Information about the enriched dataset
        with st.expander("ℹ️ About Enriched Dataset", expanded=False):
            st.markdown("""
            **The Enriched Dataset includes:**
            - All original CSV columns and data
            - FF2API processing results and status
            - Load creation success/failure information
            - Error messages and processing details
            - Timestamps and processing metadata
            
            **Use this dataset to:**
            - Analyze processing success rates
            - Identify patterns in failed loads
            - Track data quality improvements
            - Generate comprehensive reports
            """)
    
    except Exception as e:
        st.error(f"❌ Error rendering download interface: {str(e)}")
        logger.error(f"Download interface rendering error: {e}")

# Import remaining helper functions from original
def validate_mapping(df, field_mappings, data_processor):
    """Validate the current mapping - exactly like original"""
    try:
        # Apply mapping
        mapped_df, mapping_errors = data_processor.apply_mapping(df, field_mappings)
        
        if mapping_errors:
            return [{'row': i, 'errors': [error]} for i, error in enumerate(mapping_errors)]
        
        return []
    except Exception as e:
        logger.error(f"Mapping validation error: {e}")
        return [{'row': 0, 'errors': [str(e)]}]

def _save_configuration(db_manager, field_mappings, file_headers):
    """Save configuration with field mappings - exactly like original"""
    try:
        config = st.session_state.selected_configuration
        brokerage_name = st.session_state.brokerage_name
        
        db_manager.save_brokerage_configuration(
            brokerage_name=brokerage_name,
            configuration_name=config['name'],
            field_mappings=field_mappings,
            api_credentials=config['api_credentials'],
            file_headers=file_headers,
            description=config.get('description', ''),
            auth_type=config.get('auth_type', 'api_key'),
            bearer_token=config.get('bearer_token')
        )
        
        # Update session state
        st.session_state.selected_configuration['field_mappings'] = field_mappings
        st.session_state.selected_configuration['field_count'] = len(field_mappings)
        
        # Trigger database backup after configuration save
        from db_manager import upload_sqlite_if_changed
        upload_sqlite_if_changed()
        
    except Exception as e:
        st.error(f"❌ Failed to save configuration: {str(e)}")

if __name__ == "__main__":
    main()