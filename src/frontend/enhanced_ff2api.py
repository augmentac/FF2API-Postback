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
    create_learning_analytics_dashboard,
    update_learning_with_processing_results,
    get_full_api_schema
)
# Removed COMMON_ENUM_FIELDS import - using schema-based enums directly

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
            # Fallback for local development
            correct_password = "admin123"
        
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
            
            # Clear sensitive data
            keys_to_clear = ['api_credentials', 'selected_configuration', 'uploaded_df']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            
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
    _has_session_data,
    render_database_management_section
)

def main():
    """Main enhanced FF2API application - preserves original UX"""
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
    
    # Load custom CSS
    load_custom_css()
    
    # Security: Clean up old uploaded files on startup
    cleanup_old_uploads()
    
    # Render main header
    render_main_header()
    
    # Initialize components
    db_manager, data_processor = init_components()
    
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
            # Check if email automation is configured for this brokerage
            cred_status = credential_manager.validate_credentials(brokerage_name)
            
            if cred_status.email_automation_available:
                st.success("✅ Gmail automation configured")
                
                # Show automation status
                if cred_status.email_automation_active:
                    st.info("🟢 Email automation active")
                else:
                    st.info("🔴 Email automation inactive")
                
                # Email monitoring controls
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("▶️ Start Monitor", key="start_email_monitor", use_container_width=True):
                        try:
                            email_monitor.start_monitoring()
                            st.success("Email monitoring started")
                        except Exception as e:
                            st.error(f"Failed to start monitoring: {e}")
                
                with col2:
                    if st.button("⏹️ Stop Monitor", key="stop_email_monitor", use_container_width=True):
                        try:
                            email_monitor.stop_monitoring()
                            st.info("Email monitoring stopped")
                        except Exception as e:
                            st.error(f"Failed to stop monitoring: {e}")
                    
                # Email filters
                with st.expander("📬 Email Filters", expanded=False):
                    sender_filter = st.text_input(
                        "Sender filter:",
                        value=st.session_state.get('email_sender_filter', ''),
                        placeholder="ops@company.com",
                        help="Filter emails by sender",
                        key="email_sender_filter_input"
                    )
                    st.session_state.email_sender_filter = sender_filter
                    
                    subject_filter = st.text_input(
                        "Subject filter:",
                        value=st.session_state.get('email_subject_filter', ''),
                        placeholder="Load Data",
                        help="Filter emails by subject keywords",
                        key="email_subject_filter_input"
                    )
                    st.session_state.email_subject_filter = subject_filter
                    
                    if st.button("🔄 Update Filters", key="update_email_filters", use_container_width=True):
                        st.success("Email filters updated")
                        
            else:
                st.warning("⚠️ Gmail automation not configured")
                
                # Use the Google SSO authentication interface
                if st.button("🔐 Setup Gmail Auth", key="setup_gmail", use_container_width=True):
                    auth_result = streamlit_google_sso.render_google_auth_button(
                        brokerage_name, 
                        "Setup Gmail Authentication"
                    )
                    
                    if auth_result.get('authenticated'):
                        st.success("Gmail authentication successful!")
                        st.rerun()
                        
        except Exception as e:
            st.error(f"Email automation error: {e}")
            
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
    
    # Show mode description
    mode_descriptions = {
        "standard": "Process data through FF2API only - original functionality",
        "full_endtoend": "Complete workflow with load ID mapping, data enrichment, multiple output formats, and email delivery"
    }
    
    st.info(f"**Selected**: {mode_descriptions[st.session_state.enhanced_processing_mode]}")
    
    # Email configuration for full end-to-end mode
    if st.session_state.enhanced_processing_mode == 'full_endtoend':
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
    _render_validation_section(data_processor)
    _render_enhanced_processing_section(db_manager, data_processor)
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
                keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'file_headers', 'validation_passed', 'header_comparison', 'field_mappings', 'processing_results']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
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
            from src.frontend.ui_components import generate_sample_api_preview
            
            data_processor = DataProcessor()
            
            # Generate API preview
            api_preview_data = generate_sample_api_preview(
                st.session_state.uploaded_df, 
                field_mappings, 
                data_processor
            )
            
            # Display message about the preview
            if "No field mappings configured yet" in api_preview_data["message"]:
                st.info("🔗 Complete field mapping first to see API preview")
                st.markdown("""
                    **What you'll see here:**
                    - JSON structure showing how your CSV data will be formatted for the API
                    - Real sample values from your first CSV row
                    - Properly nested objects (load, customer, brokerage)
                    - Field validation and data type conversion
                """)
            elif "error" in api_preview_data["message"].lower():
                st.warning(f"⚠️ {api_preview_data['message']}")
            else:
                st.success(f"✅ {api_preview_data['message']}")
            
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
                        required_fields = [f for f in api_preview_data['mapped_fields'] if get_full_api_schema().get(f, {}).get('required', False)]
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
                            "Required": "⭐" if field_info.get('required', False) else "",
                            "Is Enum": "🔽" if field_info.get('enum') else ""
                        })
                    else:
                        # CSV column mapping
                        config_data.append({
                            "API Field": api_field,
                            "Source": "📄 CSV Column",
                            "Value/Column": mapping_value,
                            "Type": field_info.get('type', 'string'),
                            "Required": "⭐" if field_info.get('required', False) else "",
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

def _render_enhanced_results_section():
    """Enhanced results section with end-to-end capabilities"""
    if 'enhanced_processing_results' not in st.session_state:
        return
        
    st.markdown("---")
    st.subheader("📊 Processing Results")
    
    result = st.session_state.enhanced_processing_results
    processing_mode = st.session_state.get('enhanced_processing_mode', 'standard')
    
    # Display results based on processing mode
    _display_enhanced_results(result, processing_mode)

def process_enhanced_data_workflow(df, field_mappings, api_credentials, brokerage_name, 
                                 processing_mode, data_processor, db_manager, session_id):
    """Enhanced data processing workflow with end-to-end capabilities"""
    
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
            
            result = {
                'ff2api_results': ff2api_results,
                'total_rows': len(df),
                'success_rate': len([r for r in ff2api_results if r.get('success', False)]) / len(df) if df else 0,
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
                status_text.text("Step 3: Enriching data...")
                progress_bar.progress(3/total_steps)
                
                enriched_data = _process_data_enrichment(ff2api_results, result.get('load_id_mappings', []), brokerage_name)
                result['enriched_data'] = enriched_data
            
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
    return process_data_enhanced(df, field_mappings, api_credentials, 
                               st.session_state.brokerage_name, data_processor, 
                               DatabaseManager(), st.session_state.session_id)

def _process_load_id_mapping(ff2api_results, brokerage_key):
    """Process load ID mapping for successful FF2API results"""
    try:
        # Get credentials for load ID mapping
        credentials = credential_manager.validate_credentials(brokerage_key)
        
        if not credentials.api_available:
            logger.warning("API credentials not available for load ID mapping")
            return []
        
        # Initialize load ID mapper
        load_id_mapper = LoadIDMapper(brokerage_key, credentials.__dict__)
        
        # Convert FF2API results to LoadProcessingResult format
        load_processing_results = []
        for result in ff2api_results:
            if result.get('success', False):
                load_processing_results.append(LoadProcessingResult(
                    csv_row_index=result.get('row_index', 0),
                    load_number=result.get('load_number', ''),
                    success=True,
                    response_data=result.get('data', {})
                ))
        
        # Process load ID mappings
        mappings = load_id_mapper.process_load_results(load_processing_results)
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
        if tracking_creds:
            enrichment_config.append({
                'type': 'tracking_api',
                'pro_column': 'PRO',  
                'carrier_column': 'carrier',
                'brokerage_key': brokerage_key,
                **tracking_creds
            })
        
        # Add Snowflake enrichment
        snowflake_creds = credential_manager.get_snowflake_credentials()
        if snowflake_creds:
            enrichment_config.append({
                'type': 'snowflake_augment',
                'database': 'AUGMENT_DW',
                'schema': 'MARTS', 
                'enrichments': ['tracking', 'customer'],
                'use_load_ids': True,
                'brokerage_key': brokerage_key,
                **snowflake_creds
            })
        
        if not enrichment_config:
            logger.warning("No enrichment sources configured")
            return []
        
        # Initialize enrichment manager
        enrichment_manager = EnrichmentManager(enrichment_config)
        
        # Prepare data for enrichment
        enrichment_data = []
        for result in ff2api_results:
            if result.get('success', False):
                data = result.get('data', {})
                
                # Add load ID if available
                load_mapping = next((lm for lm in load_mappings 
                                   if lm.load_number == data.get('load_number')), None)
                if load_mapping:
                    data['load_id'] = load_mapping.internal_load_id
                
                enrichment_data.append(data)
        
        # Apply enrichment
        enriched_data = []
        for row in enrichment_data:
            enriched_row = enrichment_manager.enrich_data(row)
            enriched_data.append(enriched_row)
        
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
        results = postback_router.route_data(enriched_data)
        
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
        email_results = email_router.route_data(result.get('enriched_data', []))
        
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
    
    # Enriched data (if available)
    with col3:
        if processing_mode == 'full_endtoend' and result.get('enriched_data'):
            if st.button("🔍 Download Enriched Data", use_container_width=True):
                enriched_df = pd.DataFrame(result.get('enriched_data', []))
                csv_data = enriched_df.to_csv(index=False)
                st.download_button(
                    "📄 Download CSV",
                    csv_data,
                    "enriched_results.csv", 
                    "text/csv",
                    use_container_width=True
                )

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
        
    except Exception as e:
        st.error(f"❌ Failed to save configuration: {str(e)}")

if __name__ == "__main__":
    main()