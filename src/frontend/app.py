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

# Create logs directory if it doesn't exist
os.makedirs('data/logs', exist_ok=True)

# Configure logging
try:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data/logs/app.log')
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
        page_title="FF2API - Login",
        page_icon="🔐",
        layout="centered"
    )
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1>🔐 FF2API Access</h1>
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
                <p><strong>FF2API</strong> - Freight File to API Processing Tool</p>
                <p>For internal company use only</p>
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
                del st.session_state.login_time
            
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

def validate_mapping(df, field_mappings, data_processor):
    """Validate the current mapping"""
    try:
        # Apply mapping
        mapped_df, mapping_errors = data_processor.apply_mapping(df, field_mappings)
        
        if mapping_errors:
            return [{'row': i, 'errors': [error]} for i, error in enumerate(mapping_errors)]
        
        # Use the full API schema for validation
        api_schema = get_full_api_schema()
        valid_df, validation_errors = data_processor.validate_data(mapped_df, api_schema)
        
        return validation_errors
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return [{'row': 1, 'errors': [f"Validation failed: {str(e)}"]}]

def get_api_credentials():
    """Get API credentials from session state"""
    return st.session_state.get('api_credentials')

def clear_api_credentials():
    """Clear API credentials from session state"""
    if 'api_credentials' in st.session_state:
        del st.session_state.api_credentials

def normalize_column_names(df):
    """Normalize column names for consistency"""
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    return df

def init_components():
    db_manager = DatabaseManager()
    data_processor = DataProcessor()
    return db_manager, data_processor

def main():
    # Check authentication first
    if not check_password():
        show_login_page()
        return
    
    st.set_page_config(
        page_title="FF2API",
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
    
    # Main workflow - no tabs, single clean interface
    main_workflow(db_manager, data_processor)
    
    # Contextual information in sidebar
    with st.sidebar:
        show_contextual_information(db_manager)
        
        # Show logout option at bottom of sidebar
        show_logout_option()



def show_workflow_summary():
    """Show simplified workflow summary with correct information"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        brokerage_name = st.session_state.get('brokerage_name', 'Not selected')
        st.info(f"🏢 **Brokerage:** {brokerage_name}")
    
    with col2:
        api_status = "✅ Connected" if 'api_credentials' in st.session_state else "❌ Not connected"
        st.info(f"🔐 **API Status:** {api_status}")
    
    with col3:
        file_status = "✅ Uploaded" if 'uploaded_df' in st.session_state else "❌ No file"
        st.info(f"📂 **File Status:** {file_status}")

def _render_configuration_status(config):
    """Render enhanced configuration status with visual indicators"""
    import streamlit as st
    
    # Determine configuration readiness state
    field_mappings = config.get('field_mappings', {})
    has_real_mappings = any(not key.startswith('_') for key in field_mappings.keys())
    
    # Only consider fields "mapped" if there's a file uploaded and mappings exist
    file_uploaded = 'uploaded_df' in st.session_state
    fields_actually_mapped = file_uploaded and has_real_mappings
    
    # Fix API connection check to handle both auth types
    auth_type = config.get('auth_type', 'api_key')
    if auth_type == 'api_key':
        api_connected = 'api_credentials' in config and config['api_credentials'].get('api_key')
    else:  # bearer_token
        api_connected = 'api_credentials' in config and config.get('bearer_token')
    
    # Fix headers validation logic - headers can only be validated if file is uploaded
    headers_validated = (file_uploaded and 
                        st.session_state.get('header_comparison', {}).get('status') == 'identical')
    
    validation_passed = st.session_state.get('validation_passed', False)
    
    # Calculate readiness score with real-time mapping progress
    # Check actual mapping completeness for uploaded files - always check when file is uploaded
    mapping_complete = False
    mapped_required = 0
    total_required = 0
    
    if file_uploaded:
        from src.frontend.ui_components import get_full_api_schema, get_effective_required_fields
        api_schema = get_full_api_schema()
        current_mappings = st.session_state.get('field_mappings', field_mappings)
        effective_required_fields = get_effective_required_fields(api_schema, current_mappings)
        mapped_required = len([f for f in effective_required_fields.keys() if f in current_mappings and current_mappings[f] and current_mappings[f] != 'Select column...'])
        total_required = len(effective_required_fields)
        mapping_complete = mapped_required >= total_required and total_required > 0
    
    readiness_checks = [
        ('API Connected', api_connected),
        ('Fields Mapped', fields_actually_mapped),
        ('File Uploaded', file_uploaded),
        ('Headers Validated', headers_validated or (not fields_actually_mapped and not file_uploaded)),
        ('Mapping Ready', mapping_complete)  # All required fields actually mapped
    ]
    
    ready_count = sum(1 for _, is_ready in readiness_checks if is_ready)
    total_checks = len(readiness_checks)
    readiness_percentage = (ready_count / total_checks) * 100
    
    # Determine overall status
    if readiness_percentage == 100:
        status = "LOCKED 🔒"
        status_color = "#10b981"
        bg_color = "rgba(16, 185, 129, 0.05)"
        border_color = "#10b981"
    elif readiness_percentage >= 60:
        status = "READY ✓"
        status_color = "#3b82f6"
        bg_color = "rgba(59, 130, 246, 0.05)"
        border_color = "#3b82f6"
    elif readiness_percentage >= 40:
        status = "ACTIVE"
        status_color = "#f59e0b"
        bg_color = "rgba(245, 158, 11, 0.05)"
        border_color = "#f59e0b"
    else:
        status = "SETUP"
        status_color = "#6b7280"
        bg_color = "rgba(107, 114, 128, 0.05)"
        border_color = "#6b7280"
    
    # Use a simpler, more reliable rendering approach
    # Status header
    col1, col2 = st.columns([2, 1])
    with col1:
        if status == "LOCKED 🔒":
            st.success(f"🔒 {status}")
        elif status == "READY ✓":
            st.info(f"✅ {status}")
        elif status == "ACTIVE":
            st.warning(f"🔄 {status}")
        else:
            st.caption(f"⚙️ {status}")
    with col2:
        st.caption(f"{ready_count}/{total_checks} checks")
    
    # Progress bar
    progress = readiness_percentage / 100
    st.progress(progress, text=f"{readiness_percentage:.0f}% Ready")
    
    # Configuration details
    auth_type = config.get('auth_type', 'api_key')
    auth_display = "🔑 API Key" if auth_type == 'api_key' else "🎫 Bearer Token"
    st.caption(f"📅 Created: {config['created_at'][:10]}")
    st.caption(f"🔗 {config['field_count']} fields mapped")
    st.caption(f"🔐 Auth: {auth_display}")
    st.caption(f"🔄 Updated: {config.get('updated_at', config['created_at'])[:10]}")
    
    # Show detailed status checks
    with st.expander("🔍 Configuration Status Details", expanded=False):
        for check_name, is_ready in readiness_checks:
            icon = "✅" if is_ready else "⏳"
            st.write(f"{icon} {check_name}")
    
    # Quick actions based on status
    if readiness_percentage == 100:
        st.markdown("### ⚡ Quick Actions")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 Process Now", type="primary", use_container_width=True, key="sidebar_process_now"):
                # Scroll to process section or trigger processing
                st.session_state.trigger_processing = True
        with col2:
            if st.button("📋 View Summary", use_container_width=True, key="sidebar_view_summary"):
                st.session_state.show_config_summary = True
    elif readiness_percentage >= 40:
        if not file_uploaded:
            st.info("📁 Upload a file to continue with field mapping")
        elif not fields_actually_mapped:
            st.info("🔗 Complete field mapping to lock configuration")
    
    # Connection status indicator
    if api_connected:
        api_url = config['api_credentials'].get('base_url', '')
        short_url = api_url.replace('https://', '').replace('http://', '')[:15] + '...' if len(api_url) > 18 else api_url
        st.success(f"🟢 {short_url} Ready ✅")
    else:
        st.error("🔴 API Not Connected ❌")

def show_contextual_information(db_manager):
    """Compact, polished sidebar with better styling"""
    
    # Extremely compact CSS for minimal spacing
    st.markdown("""
        <style>
        /* Extremely compact sidebar styling */
        .element-container {
            margin-bottom: 0.05rem !important;
        }
        .stExpander {
            margin: 0.05rem 0 !important;
        }
        .stProgress {
            margin: 0.05rem 0 !important;
        }
        .stMarkdown {
            margin-bottom: 0.05rem !important;
        }
        .stSelectbox {
            margin-bottom: 0.1rem !important;
        }
        .stButton {
            margin: 0.05rem 0 !important;
        }
        .stTextInput {
            margin-bottom: 0.1rem !important;
        }
        /* Remove ALL excessive padding from sidebar */
        .css-1d391kg {
            padding-top: 0.5rem !important;
        }
        /* Tighten up form elements */
        .stFormSubmitButton {
            margin-top: 0.1rem !important;
        }
        /* Remove spacing between sidebar elements */
        .css-1d391kg .element-container {
            margin-bottom: 0.05rem !important;
        }
        /* Compact selectbox labels */
        .stSelectbox label {
            margin-bottom: 0.1rem !important;
        }
        /* Compact button containers */
        .stButton > button {
            margin-bottom: 0.05rem !important;
        }
        /* Additional sidebar compacting */
        .css-1d391kg {
            padding-top: 0.25rem !important;
            padding-bottom: 0.25rem !important;
        }
        /* Target Streamlit's default sidebar container */
        .css-1l02zno {
            padding-top: 0.25rem !important;
        }
        /* Make sure all sidebar elements are tight */
        .css-1d391kg > div {
            margin-bottom: 0.05rem !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Extremely compact header
    st.markdown("""
        <div style="
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            border-radius: 4px;
            padding: 4px 8px;
            margin: 0 0 4px 0;
            border: 1px solid #e2e8f0;
        ">
            <h4 style="
                margin: 0;
                color: #1e293b;
                font-size: 0.95rem;
                font-weight: 600;
                display: flex;
                align-items: center;
                gap: 4px;
            ">
                ⚙️ Configuration
            </h4>
        </div>
    """, unsafe_allow_html=True)
    
    # Extremely compact brokerage section
    selected_brokerage = st.session_state.get('brokerage_name')
    selected_configuration = st.session_state.get('selected_configuration')
    
    # If both brokerage and configuration are selected, show compact combined view
    if selected_brokerage and selected_configuration:
        _render_compact_brokerage_config_display(db_manager, selected_brokerage, selected_configuration)
    else:
        # Show standard selection interfaces
        st.markdown('<div style="margin: 2px 0 1px 0;"><strong style="color: #374151; font-size: 0.85rem;">🏢 Brokerage</strong></div>', unsafe_allow_html=True)
        _render_brokerage_selection(db_manager)
        
        # Extremely compact configuration selection
        if selected_brokerage:
            st.markdown('<div style="margin: 4px 0 1px 0;"><strong style="color: #374151; font-size: 0.85rem;">⚙️ Configuration</strong></div>', unsafe_allow_html=True)
            _render_configuration_selection(db_manager, selected_brokerage)
    
    # Extremely compact status section
    if selected_brokerage and selected_configuration:
        st.markdown('<div style="margin: 4px 0 2px 0; border-top: 1px solid #e5e7eb;"></div>', unsafe_allow_html=True)
        _render_consolidated_status()
    
    # Extremely compact actions section  
    if selected_brokerage:
        st.markdown('<div style="margin: 4px 0 2px 0; border-top: 1px solid #e5e7eb;"></div>', unsafe_allow_html=True)
        _render_smart_actions()
    
    # Expandable sections
    if selected_brokerage:
        with st.expander("📊 Advanced Info", expanded=False):
            _render_advanced_info(db_manager)
    
    if _has_session_data():
        with st.expander("🔧 Session Details", expanded=False):
            _render_session_details()
    
    # Database backup status dashboard
    from db_manager import render_backup_status_dashboard
    render_backup_status_dashboard()
    
    # Learning analytics section
    # Learning Analytics section removed - not currently being used

def _render_brokerage_selection(db_manager):
    """Render compact brokerage selection"""
    
    # Display success message if brokerage was just created
    if 'brokerage_creation_success' in st.session_state:
        st.success(st.session_state.brokerage_creation_success)
        # Mark for clearing instead of clearing immediately
        st.session_state.brokerage_creation_success_shown = True
    
    # Display error message if brokerage creation failed
    if 'brokerage_creation_error' in st.session_state:
        st.error(st.session_state.brokerage_creation_error)
        # Mark for clearing instead of clearing immediately
        st.session_state.brokerage_creation_error_shown = True
    
    try:
        brokerages = db_manager.get_all_brokerages()
        brokerage_options = [b['name'] for b in brokerages] if brokerages else []
    except:
        brokerage_options = []
    
    # Add newly created brokerage to options if it's not in database yet
    current_brokerage = st.session_state.get('brokerage_name', '')
    if current_brokerage and current_brokerage not in brokerage_options:
        brokerage_options.append(current_brokerage)
    
    if brokerage_options:
        # Improved dropdown with better default selection
        current_brokerage = st.session_state.get('brokerage_name', '')
        if current_brokerage in brokerage_options:
            default_index = brokerage_options.index(current_brokerage) + 1
        else:
            default_index = 0
            
        selected_brokerage = st.selectbox(
            "Select Brokerage",
            options=["-- Choose a brokerage --"] + brokerage_options,
            index=default_index,
            key="sidebar_brokerage_select",
            help="Choose your brokerage from the list"
        )
        
        # Compact new brokerage option
        if st.button("➕ New Brokerage", key="new_brokerage_btn", use_container_width=True):
            st.session_state.show_new_brokerage_form = True
        
        if st.session_state.get('show_new_brokerage_form'):
            new_brokerage = st.text_input(
                "New brokerage name",
                placeholder="Enter brokerage name",
                key="sidebar_new_brokerage_input"
            )
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Create", key="create_brokerage_btn", use_container_width=True):
                    if new_brokerage.strip():
                        st.session_state.brokerage_name = new_brokerage.strip()
                        st.session_state.show_new_brokerage_form = False
                        # Store success message in session state to persist across rerun
                        st.session_state.brokerage_creation_success = f"✅ Created: {new_brokerage.strip()}"
                        st.rerun()
                    else:
                        # Store error message in session state to persist across rerun
                        st.session_state.brokerage_creation_error = "❌ Please enter a brokerage name"
            with col2:
                if st.button("Cancel", key="cancel_brokerage_btn", use_container_width=True):
                    st.session_state.show_new_brokerage_form = False
                    st.rerun()
    else:
        # Direct input for first brokerage
        new_brokerage = st.text_input(
            "Enter brokerage name",
            value=st.session_state.get('brokerage_name', ''),
            placeholder="Your brokerage name",
            key="sidebar_brokerage_input",
            help="Enter the name of your brokerage company"
        )
        if new_brokerage.strip():
            selected_brokerage = new_brokerage.strip()
        else:
            selected_brokerage = None
    
    # Update session state
    if selected_brokerage and selected_brokerage.strip() and selected_brokerage != "-- Choose a brokerage --":
        # Check if brokerage selection has changed
        current_brokerage = st.session_state.get('brokerage_name', '')
        new_brokerage = selected_brokerage.strip()
        
        if current_brokerage != new_brokerage:
            # Clear any existing configuration when changing brokerage
            if 'selected_configuration' in st.session_state:
                del st.session_state['selected_configuration']
            if 'api_credentials' in st.session_state:
                del st.session_state['api_credentials']
            
            st.session_state.brokerage_name = new_brokerage
            
            # Don't rerun if we just created a brokerage and have a success message to show
            if not st.session_state.get('brokerage_creation_success'):
                st.rerun()
        else:
            st.session_state.brokerage_name = new_brokerage
    elif selected_brokerage == "-- Choose a brokerage --":
        # Only clear brokerage selection if user intentionally selects default option
        # Don't clear if we have a newly created brokerage that just isn't in DB yet
        if ('brokerage_name' in st.session_state and 
            not st.session_state.get('brokerage_creation_success')):
            del st.session_state['brokerage_name']
            if 'selected_configuration' in st.session_state:
                del st.session_state['selected_configuration']
            if 'api_credentials' in st.session_state:
                del st.session_state['api_credentials']
            st.rerun()
    
    # Clear success/error messages after all processing is done
    if st.session_state.get('brokerage_creation_success_shown'):
        if 'brokerage_creation_success' in st.session_state:
            del st.session_state.brokerage_creation_success
        del st.session_state.brokerage_creation_success_shown
    
    if st.session_state.get('brokerage_creation_error_shown'):
        if 'brokerage_creation_error' in st.session_state:
            del st.session_state.brokerage_creation_error
        del st.session_state.brokerage_creation_error_shown

def _render_configuration_selection(db_manager, brokerage_name):
    """Render compact configuration selection"""
    try:
        configurations = db_manager.get_brokerage_configurations(brokerage_name)
    except:
        configurations = []
    
    if configurations:
        config_options = [c['name'] for c in configurations]
        
        # Smart default selection
        default_index = 0
        if 'auto_select_config' in st.session_state:
            auto_select = st.session_state.auto_select_config
            if auto_select in config_options:
                default_index = config_options.index(auto_select) + 1
            del st.session_state.auto_select_config
        elif 'selected_configuration' in st.session_state:
            current_config = st.session_state.selected_configuration
            if current_config['name'] in config_options:
                default_index = config_options.index(current_config['name']) + 1
        
        selected_config_display = st.selectbox(
            "Select Configuration",
            options=["-- Choose a configuration --"] + config_options + ["➕ Create New"],
            index=default_index,
            key="sidebar_config_select",
            help="Choose an existing configuration or create a new one"
        )
        
        if selected_config_display and selected_config_display not in ["-- Choose a configuration --", "➕ Create New"]:
            selected_config = next(c for c in configurations if c['name'] == selected_config_display)
            
            # Update session state if new selection
            current_selection = st.session_state.get('selected_configuration', {}).get('name', '')
            if current_selection != selected_config_display:
                st.session_state.selected_configuration = selected_config
                st.session_state.configuration_type = 'existing'
                st.session_state.api_credentials = selected_config['api_credentials']
                
                # Update last used
                try:
                    db_manager.update_configuration_last_used(brokerage_name, selected_config['name'])
                except:
                    pass
                
                # Clear workflow state and validation state
                keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'file_headers', 'validation_passed', 'header_comparison', 'field_mappings', 'mapping_tab_index', 'processing_results', 'load_results', 'processing_in_progress', 'validation_errors', 'mapping_section_expanded', 'processing_completed']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                
                st.rerun()
                
        elif selected_config_display == "➕ Create New":
            st.session_state.configuration_type = 'new'
            _render_new_configuration_form(brokerage_name, db_manager)
    else:
        # No configurations - show create form
        st.session_state.configuration_type = 'new'
        st.info("💡 Create your first configuration")
        _render_new_configuration_form(brokerage_name, db_manager)

def _render_compact_brokerage_config_display(db_manager, brokerage_name, configuration):
    """Render compact display of both brokerage and configuration when both are selected"""
    
    # Determine auth type display
    auth_type = configuration.get('auth_type', 'api_key')
    auth_icon = "🔑" if auth_type == 'api_key' else "🎫"
    
    # Compact combined display
    st.markdown(f"""
        <div style="
            background: #f8fafc; 
            border-radius: 6px; 
            padding: 8px; 
            margin: 2px 0 4px 0;
            border: 1px solid #e2e8f0;
        ">
            <div style="
                font-size: 0.85rem; 
                color: #1e293b; 
                font-weight: 600;
                margin-bottom: 4px;
            ">
                🏢 {brokerage_name}
            </div>
            <div style="
                font-size: 0.8rem; 
                color: #475569; 
                margin-bottom: 4px;
            ">
                ⚙️ {configuration['name']} {auth_icon}
            </div>
            <div style="
                font-size: 0.7rem; 
                color: #64748b; 
                display: flex;
                justify-content: space-between;
                align-items: center;
            ">
                <span>📁 {configuration.get('field_count', 0)} fields</span>
                <span>📅 {configuration.get('created_at', '')[:10] if configuration.get('created_at') else 'N/A'}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # Collapsible change options
    with st.expander("🔄 Change Settings", expanded=False):
        st.markdown("**Change Brokerage:**")
        _render_brokerage_selection(db_manager)
        
        st.markdown("**Change Configuration:**")
        _render_configuration_selection(db_manager, brokerage_name)

def _render_consolidated_status():
    """Render compact, polished status information"""
    config = st.session_state.get('selected_configuration', {})
    
    # Calculate readiness
    field_mappings = config.get('field_mappings', {})
    has_real_mappings = any(not key.startswith('_') for key in field_mappings.keys())
    
    # Only consider fields "mapped" if there's a file uploaded and mappings exist
    file_uploaded = 'uploaded_df' in st.session_state
    fields_actually_mapped = file_uploaded and has_real_mappings
    
    # Fix API connection check to handle both auth types
    auth_type = config.get('auth_type', 'api_key')
    if auth_type == 'api_key':
        api_connected = 'api_credentials' in config and config['api_credentials'].get('api_key')
    else:  # bearer_token
        api_connected = 'api_credentials' in config and config.get('bearer_token')
    
    # Fix headers validation logic - headers can only be validated if file is uploaded
    headers_validated = (file_uploaded and 
                        st.session_state.get('header_comparison', {}).get('status') == 'identical')
    
    validation_passed = st.session_state.get('validation_passed', False)
    processing_completed = st.session_state.get('processing_completed', False)
    
    # Calculate mapping completeness in real-time - always check when file is uploaded
    mapping_complete = False
    mapped_required = 0
    total_required = 0
    
    if file_uploaded:
        from src.frontend.ui_components import get_full_api_schema, get_effective_required_fields
        api_schema = get_full_api_schema()
        current_mappings = st.session_state.get('field_mappings', field_mappings)
        effective_required_fields = get_effective_required_fields(api_schema, current_mappings)
        mapped_required = len([f for f in effective_required_fields.keys() if f in current_mappings and current_mappings[f] and current_mappings[f] != 'Select column...'])
        total_required = len(effective_required_fields)
        mapping_complete = mapped_required >= total_required and total_required > 0
    
    readiness_checks = [
        ('API Connected', api_connected),
        ('Fields Mapped', fields_actually_mapped),
        ('File Uploaded', file_uploaded),
        ('Headers Validated', headers_validated or (not fields_actually_mapped and not file_uploaded)),
        ('Mapping Ready', mapping_complete)  # All required fields actually mapped
    ]
    
    ready_count = sum(1 for _, is_ready in readiness_checks if is_ready)
    total_checks = len(readiness_checks)
    readiness_percentage = (ready_count / total_checks) * 100
    
    # Override status if processing is completed
    if processing_completed:
        readiness_percentage = 100
        status_text = "🎉 PROCESSED"
        status_color = "#059669"  # Modern emerald
        bg_color = "linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%)"
        border_gradient = "linear-gradient(135deg, #10b981 0%, #059669 100%)"
    else:
        # Modern status determination with updated terminology - prioritize mapping completeness
        if file_uploaded and total_required > 0 and not mapping_complete:
            # File uploaded but mapping incomplete - show mapping status
            status_text = "📋 MAPPING"
            status_color = "#d97706"  # Modern amber
            bg_color = "linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%)"
            border_gradient = "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)"
        elif readiness_percentage == 100:
            status_text = "✅ COMPLETE"
            status_color = "#059669"  # Modern emerald
            bg_color = "linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%)"
            border_gradient = "linear-gradient(135deg, #10b981 0%, #059669 100%)"
        elif readiness_percentage >= 80:  # Only show READY at 80%+ (4/5 checks)
            status_text = "✅ READY"
            status_color = "#2563eb"  # Modern blue
            bg_color = "linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%)"
            border_gradient = "linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)"
        elif readiness_percentage >= 60:  # Show ACTIVE at 60%+ (3/5 checks)
            status_text = "🔄 ACTIVE"
            status_color = "#d97706"  # Modern amber
            bg_color = "linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%)"
            border_gradient = "linear-gradient(135deg, #f59e0b 0%, #d97706 100%)"
        elif readiness_percentage >= 40:  # Show PROGRESS at 40%+ (2/5 checks)
            status_text = "⚡ PROGRESS"
            status_color = "#7c3aed"  # Modern violet
            bg_color = "linear-gradient(135deg, #f5f3ff 0%, #ede9fe 100%)"
            border_gradient = "linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)"
        else:
            status_text = "⚙️ SETUP"
            status_color = "#6b7280"
            bg_color = "linear-gradient(135deg, #f9fafb 0%, #f3f4f6 100%)"
            border_gradient = "linear-gradient(135deg, #6b7280 0%, #4b5563 100%)"
    
    # Modern, polished status display with enhanced styling
    progress = readiness_percentage / 100  # Use readiness_percentage which accounts for processing completion
    progress_gradient = f"linear-gradient(90deg, {status_color} 0%, {status_color}dd 100%)"
    glow_effect = f"0 0 10px {status_color}33" if readiness_percentage == 100 else "none"
    
    st.markdown(f"""
        <div style="
            background: {bg_color};
            border: 1px solid {status_color}40;
            border-radius: 12px;
            padding: 12px;
            margin: 8px 0;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08), {glow_effect};
            backdrop-filter: blur(10px);
            transition: all 0.3s ease;
        ">
            <div style="
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 8px;
            ">
                <div style="
                    color: {status_color}; 
                    font-size: 0.9rem; 
                    font-weight: 500;
                    letter-spacing: 0.025em;
                ">📊 Status: {status_text}</div>
                <div style="
                    color: {status_color}; 
                    font-size: 0.75rem; 
                    font-weight: 600;
                    background: rgba(255, 255, 255, 0.6);
                    padding: 2px 6px;
                    border-radius: 8px;
">{"5/5" if processing_completed else f"{mapped_required}/{total_required}" if file_uploaded and total_required > 0 else f"{ready_count}/{total_checks}"}</div>
            </div>
            <div style="
                background: rgba(255, 255, 255, 0.4);
                height: 6px;
                border-radius: 6px;
                overflow: hidden;
                box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
            ">
                <div style="
                    background: {progress_gradient};
                    height: 100%;
                    width: {progress * 100}%;
                    border-radius: 6px;
                    transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
                    box-shadow: {glow_effect};
                "></div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # Modern guidance messaging with enhanced styling
    if processing_completed:
        st.markdown('''
            <div style="margin: 8px 0;">
                <div style="
                    background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
                    color: #065f46; 
                    padding: 6px 10px; 
                    border-radius: 8px; 
                    font-size: 0.85rem;
                    font-weight: 500;
                    box-shadow: 0 2px 6px rgba(16, 185, 129, 0.15);
                    border: 1px solid rgba(16, 185, 129, 0.2);
                    text-align: center;
                ">✅ Complete</div>
            </div>
        ''', unsafe_allow_html=True)
    elif file_uploaded and total_required > 0 and not mapping_complete:
        # Show mapping progress message
        st.markdown(f'''
            <div style="margin: 8px 0;">
                <div style="
                    background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
                    color: #92400e; 
                    padding: 8px 12px; 
                    border-radius: 10px; 
                    font-size: 0.9rem;
                    font-weight: 500;
                    box-shadow: 0 2px 8px rgba(245, 158, 11, 0.2);
                    border: 1px solid rgba(245, 158, 11, 0.3);
                    text-align: center;
                    letter-spacing: 0.025em;
                ">📋 Map {total_required - mapped_required} more fields</div>
            </div>
        ''', unsafe_allow_html=True)
    elif readiness_percentage == 100:
        st.markdown('''
            <div style="margin: 8px 0;">
                <div style="
                    background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
                    color: #065f46; 
                    padding: 8px 12px; 
                    border-radius: 10px; 
                    font-size: 0.9rem;
                    font-weight: 500;
                    box-shadow: 0 2px 8px rgba(16, 185, 129, 0.2);
                    border: 1px solid rgba(16, 185, 129, 0.3);
                    text-align: center;
                    letter-spacing: 0.025em;
                ">🚀 Ready to process!</div>
            </div>
        ''', unsafe_allow_html=True)
    elif readiness_percentage >= 80:
        if not file_uploaded:
            st.markdown('''
                <div style="margin: 6px 0;">
                    <div style="
                        background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
                        color: #1e40af; 
                        padding: 6px 10px; 
                        border-radius: 8px; 
                        font-size: 0.85rem;
                        font-weight: 500;
                        box-shadow: 0 2px 6px rgba(59, 130, 246, 0.15);
                        border: 1px solid rgba(59, 130, 246, 0.2);
                    ">💡 Upload a file to continue</div>
                </div>
            ''', unsafe_allow_html=True)
        elif not validation_passed:
            st.markdown('''
                <div style="margin: 6px 0;">
                    <div style="
                        background: linear-gradient(135deg, #dbeafe 0%, #bfdbfe 100%);
                        color: #1e40af; 
                        padding: 6px 10px; 
                        border-radius: 8px; 
                        font-size: 0.85rem;
                        font-weight: 500;
                        box-shadow: 0 2px 6px rgba(59, 130, 246, 0.15);
                        border: 1px solid rgba(59, 130, 246, 0.2);
                    ">💡 Validate data quality</div>
                </div>
            ''', unsafe_allow_html=True)
        else:
            st.markdown('''
                <div style="margin: 6px 0;">
                    <div style="
                        background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
                        color: #065f46; 
                        padding: 6px 10px; 
                        border-radius: 8px; 
                        font-size: 0.85rem;
                        font-weight: 500;
                        box-shadow: 0 2px 6px rgba(16, 185, 129, 0.15);
                        border: 1px solid rgba(16, 185, 129, 0.2);
                    ">🎯 Almost ready!</div>
                </div>
            ''', unsafe_allow_html=True)
    elif readiness_percentage >= 60:
        missing_items = []
        if not api_connected:
            missing_items.append("API connection")
        if not fields_actually_mapped:
            missing_items.append("field mappings")
        if not file_uploaded:
            missing_items.append("file upload")
        st.markdown(f'''
            <div style="margin: 4px 0;">
                <div style="
                    background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
                    color: #92400e; 
                    padding: 4px 8px; 
                    border-radius: 6px; 
                    font-size: 0.75rem;
                    font-weight: 500;
                    box-shadow: 0 1px 4px rgba(245, 158, 11, 0.15);
                    border: 1px solid rgba(245, 158, 11, 0.2);
                ">⚡ Missing: {", ".join(missing_items)}</div>
            </div>
        ''', unsafe_allow_html=True)
    elif readiness_percentage >= 40:
        st.markdown('''
            <div style="margin: 4px 0;">
                <div style="
                    background: linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%);
                    color: #5b21b6; 
                    padding: 4px 8px; 
                    border-radius: 6px; 
                    font-size: 0.75rem;
                    font-weight: 500;
                    box-shadow: 0 1px 4px rgba(139, 92, 246, 0.15);
                    border: 1px solid rgba(139, 92, 246, 0.2);
                ">📈 Making progress</div>
            </div>
        ''', unsafe_allow_html=True)
    else:
        st.markdown('''
            <div style="margin: 4px 0;">
                <div style="
                    background: linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 100%);
                    color: #374151; 
                    padding: 4px 8px; 
                    border-radius: 6px; 
                    font-size: 0.75rem;
                    font-weight: 500;
                    box-shadow: 0 1px 4px rgba(107, 114, 128, 0.15);
                    border: 1px solid rgba(107, 114, 128, 0.2);
                ">⚙️ Start by connecting API</div>
            </div>
        ''', unsafe_allow_html=True)
    
    # Compact details
    with st.expander("📋 Status Details"):
        for name, is_ready in readiness_checks:
            icon = "✅" if is_ready else "⏳"
            st.caption(f"{icon} {name}")
        
        if processing_completed:
            st.caption("✅ Processing Completed")
        
        if config.get('created_at'):
            st.caption(f"📅 Created: {config['created_at'][:10]}")
        if config.get('field_count', 0) > 0:
            st.caption(f"🔗 Fields: {config['field_count']}")

def _render_smart_actions():
    """Render clean, functional action buttons"""
    
    # Simple actions header
    st.markdown("**⚡ Actions**")
    
    # Get state variables safely
    uploaded_df = st.session_state.get('uploaded_df')
    validation_passed = st.session_state.get('validation_passed', False)
    selected_configuration = st.session_state.get('selected_configuration')
    
    # Primary action based on workflow state - removed redundant Process Data button
    if uploaded_df is not None and not validation_passed:
        if st.button("🔍 Validate Data Quality", type="primary", use_container_width=True, key="primary_action"):
            st.session_state.trigger_validation = True
    elif selected_configuration and uploaded_df is None:
        # Skip upload file action - it's redundant with main upload area
        pass
    elif not selected_configuration:
        if st.button("⚙️ Setup Configuration", type="primary", use_container_width=True, key="primary_action"):
            st.session_state.focus_config = True
    
    # Compact secondary actions
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Reset", key="reset_action", use_container_width=True):
            keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'field_mappings', 'file_headers', 'validation_passed', 'header_comparison', 'mapping_tab_index', 'processing_results', 'load_results', 'processing_in_progress', 'validation_errors', 'mapping_section_expanded', 'processing_completed']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear All", key="clear_all_action", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

def _render_advanced_info(db_manager):
    """Render clean advanced information"""
    brokerage_name = st.session_state.get('brokerage_name')
    if not brokerage_name:
        st.info("Select a brokerage to view analytics")
        return
    
    # Simple metrics
    try:
        configurations = db_manager.get_brokerage_configurations(brokerage_name)
        recent_uploads = db_manager.get_brokerage_upload_history(brokerage_name, limit=5)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("⚙️ Configurations", len(configurations))
        with col2:
            st.metric("📤 Recent Uploads", len(recent_uploads))
        
        # Recent activity (only if exists)
        if recent_uploads:
            st.markdown("**Recent Activity:**")
            for upload in recent_uploads[:3]:
                success_rate = (upload[5] / upload[4] * 100) if upload[4] > 0 else 0
                icon = "✅" if success_rate > 90 else "⚠️" if success_rate > 50 else "❌"
                date_str = upload[11][:10] if len(upload) > 11 else "Unknown"
                st.caption(f"{icon} {upload[4]} records • {success_rate:.1f}% success • {date_str}")
        
    except Exception as e:
        st.error("Unable to load analytics data")

def _render_session_details():
    """Render clean session details"""
    # Show essential session info
    details = []
    
    if 'brokerage_name' in st.session_state:
        details.append(f"🏢 {st.session_state.brokerage_name}")
    
    if 'selected_configuration' in st.session_state:
        config = st.session_state.selected_configuration
        details.append(f"⚙️ {config['name']}")
    
    if st.session_state.get('uploaded_df') is not None:
        df = st.session_state.uploaded_df
        filename = st.session_state.get('uploaded_file_name', 'Unknown')
        display_name = filename[:20] + '...' if len(filename) > 20 else filename
        details.append(f"📁 {display_name} ({len(df)} rows)")
    
    if st.session_state.get('validation_passed'):
        details.append("✅ Validated")
    elif st.session_state.get('uploaded_df') is not None:
        details.append("⏳ Pending Validation")
    
    if details:
        for detail in details:
            st.caption(detail)
    else:
        st.info("No active session data")

def _render_new_configuration_form(brokerage_name, db_manager):
    """Render compact new configuration form"""
    
    # Display success message if configuration was just saved
    if 'config_save_success' in st.session_state:
        st.success(st.session_state.config_save_success)
        # Mark for clearing instead of clearing immediately
        st.session_state.config_save_success_shown = True
    
    # Display error message if configuration save failed
    if 'config_save_error' in st.session_state:
        st.error(st.session_state.config_save_error)
        # Mark for clearing instead of clearing immediately
        st.session_state.config_save_error_shown = True
    
    if 'config_form_state' not in st.session_state:
        st.session_state.config_form_state = {
            'config_name': '',
            'config_description': '',
            'api_base_url': 'https://api.prod.goaugment.com',
            'api_key': '',
            'auth_type': 'api_key',
            'bearer_token': ''
        }
    
    # Compact form
    config_name = st.text_input(
        "Configuration name",
        value=st.session_state.config_form_state['config_name'],
        placeholder="e.g., Standard Mapping",
        key="new_config_name_input"
    )
    
    # Authentication type selection
    auth_type = st.selectbox(
        "Authentication Type",
        options=['api_key', 'bearer_token'],
        index=0 if st.session_state.config_form_state['auth_type'] == 'api_key' else 1,
        format_func=lambda x: "API Key (with token refresh)" if x == 'api_key' else "Bearer Token (direct)",
        key="auth_type_select",
        help="Choose authentication method - API Key automatically refreshes tokens, Bearer Token uses your token directly"
    )
    
    # API Base URL (always needed)
    api_base_url = st.text_input(
        "API Base URL",
        value=st.session_state.config_form_state['api_base_url'],
        placeholder="https://api.prod.goaugment.com",
        key="new_config_api_url_input"
    )
    
    # Conditional authentication fields based on auth type
    api_key = ""
    bearer_token = ""
    
    if auth_type == 'api_key':
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config_form_state['api_key'],
            type="password",
            placeholder="Your API key (used to refresh bearer tokens)",
            key="new_config_api_key_input"
        )
    else:  # bearer_token
        bearer_token = st.text_input(
            "Bearer Token",
            value=st.session_state.config_form_state['bearer_token'],
            type="password",
            placeholder="Your bearer token (used directly for API calls)",
            key="new_config_bearer_token_input"
        )
    
    # Update form state
    st.session_state.config_form_state.update({
        'config_name': config_name,
        'api_base_url': api_base_url,
        'auth_type': auth_type,
        'api_key': api_key,
        'bearer_token': bearer_token
    })
    
    # Action buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Save", type="primary", use_container_width=True, key="save_config_btn"):
            _handle_save_configuration(brokerage_name, db_manager)
    
    with col2:
        if st.button("🔄 Reset", use_container_width=True, key="reset_config_btn"):
            st.session_state.config_form_state = {
                'config_name': '',
                'config_description': '',
                'api_base_url': 'https://api.prod.goaugment.com',
                'api_key': '',
                'auth_type': 'api_key',
                'bearer_token': ''
            }
            st.rerun()
    
    # Clear success/error messages after all processing is done
    if st.session_state.get('config_save_success_shown'):
        if 'config_save_success' in st.session_state:
            del st.session_state.config_save_success
        del st.session_state.config_save_success_shown
    
    if st.session_state.get('config_save_error_shown'):
        if 'config_save_error' in st.session_state:
            del st.session_state.config_save_error
        del st.session_state.config_save_error_shown

def _has_session_data():
    """Check if there's relevant session data to show"""
    return any(st.session_state.get(key) is not None for key in ['brokerage_name', 'selected_configuration', 'uploaded_df', 'field_mappings'])

def _handle_save_configuration(brokerage_name, db_manager):
    """Handle saving new configuration"""
    form_state = st.session_state.config_form_state
    
    config_name = form_state['config_name'].strip()
    config_description = form_state['config_description'].strip()
    api_base_url = form_state['api_base_url'].strip()
    auth_type = form_state['auth_type']
    api_key = form_state['api_key'].strip()
    bearer_token = form_state['bearer_token'].strip()
    
    # Validate required fields based on auth type
    if not config_name:
        st.session_state.config_save_error = "Please provide a configuration name"
        return
    
    if auth_type == 'api_key' and not api_key:
        st.session_state.config_save_error = "Please provide an API key for API key authentication"
        return
    elif auth_type == 'bearer_token' and not bearer_token:
        st.session_state.config_save_error = "Please provide a bearer token for bearer token authentication"
        return
    
    if not api_base_url:
        st.session_state.config_save_error = "Please provide an API base URL"
        return
    
    # Test API connection
    with st.spinner("Testing API connection..."):
        try:
            from src.backend.api_client import LoadsAPIClient
            
            if auth_type == 'api_key':
                client = LoadsAPIClient(api_base_url, api_key=api_key, auth_type='api_key')
            else:  # bearer_token
                client = LoadsAPIClient(api_base_url, bearer_token=bearer_token, auth_type='bearer_token')
            
            result = client.validate_connection()
        
            if result['success']:
                # Save configuration to database with placeholder field mappings
                try:
                    # Build API credentials based on auth type
                    if auth_type == 'api_key':
                        api_credentials = {
                            'base_url': api_base_url,
                            'api_key': api_key
                        }
                        save_bearer_token = None
                    else:  # bearer_token
                        api_credentials = {
                            'base_url': api_base_url
                        }
                        save_bearer_token = bearer_token
                    
                    # Save to database - start with placeholder field mappings that indicate "pending"
                    placeholder_mappings = {
                        "_status": "pending_file_upload",
                        "_created_at": str(datetime.now()),
                        "_description": "Configuration created, awaiting file upload for field mapping"
                    }
                    
                    config_id = db_manager.save_brokerage_configuration(
                        brokerage_name=brokerage_name,
                        configuration_name=config_name,
                        field_mappings=placeholder_mappings,
                        api_credentials=api_credentials,
                        file_headers=None,
                        description=config_description,
                        auth_type=auth_type,
                        bearer_token=save_bearer_token
                    )
                    
                    # Save configuration info to session and switch to 'existing' mode
                    saved_config = {
                        'id': config_id,
                        'name': config_name,
                        'brokerage_name': brokerage_name,
                        'configuration_name': config_name,
                        'description': config_description,
                        'api_credentials': api_credentials,
                        'auth_type': auth_type,
                        'bearer_token': save_bearer_token,
                        'field_mappings': placeholder_mappings,
                        'version': 1,
                        'created_at': str(datetime.now()),
                        'field_count': len(placeholder_mappings)
                    }
                    
                    # Set as selected configuration (treat as existing now that it's saved)
                    st.session_state.selected_configuration = saved_config
                    st.session_state.api_credentials = api_credentials
                    st.session_state.brokerage_name = brokerage_name
                    st.session_state.configuration_type = 'existing'
                    
                    # Store the saved config name to auto-select it in sidebar
                    st.session_state.auto_select_config = config_name
                    
                    # Clear new configuration since it's now saved
                    if 'new_configuration' in st.session_state:
                        del st.session_state['new_configuration']
                    
                    # Clear the form state
                    st.session_state.config_form_state = {
                        'config_name': '',
                        'config_description': '',
                        'api_base_url': 'https://api.prod.goaugment.com',
                        'api_key': '',
                        'auth_type': 'api_key',
                        'bearer_token': ''
                    }
                    
                    # Store success message in session state to persist across rerun
                    st.session_state.config_save_success = "✅ Configuration saved! Upload a file to continue."
                    
                    # Trigger backup suggestion after configuration creation
                    auto_backup_suggestion()
                    
                    st.rerun()
                    
                except Exception as db_error:
                    st.session_state.config_save_error = f"❌ Failed to save configuration: {str(db_error)}"
                    
            else:
                st.session_state.config_save_error = f"❌ API connection failed: {result['message']}"
        except Exception as e:
            st.session_state.config_save_error = f"❌ Failed to test connection: {str(e)}"

def main_workflow(db_manager, data_processor):
    """Modern streamlined workflow with enhanced UX and progressive disclosure"""
    
    # Check if configuration is ready
    if 'brokerage_name' not in st.session_state:
        st.info("👈 Please select or create a brokerage in the sidebar to continue")
        return
    
    if 'api_credentials' not in st.session_state:
        st.info("👈 Please configure your API credentials in the sidebar to continue")
        return
    
    brokerage_name = st.session_state.brokerage_name
    
    # === PROGRESSIVE DISCLOSURE LANDING PAGE ===
    # Only show what's needed at each step
    
    # Check if user has uploaded a file
    has_uploaded_file = st.session_state.get('uploaded_df') is not None
    
    if not has_uploaded_file:
        # === CLEAN LANDING STATE ===
        # Focus entirely on upload with clear value proposition
        _render_landing_page()
    else:
        # === PROGRESSIVE WORKFLOW ===
        # Show progress and workflow sections after file upload
        _render_workflow_with_progress(db_manager, data_processor)

def _render_landing_page():
    """Clean landing page focused on file upload"""
    
    # Large, prominent upload area
    _render_enhanced_file_upload()
    
    # Simple benefits section
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
                <div style="font-size: 0.9rem; color: #64748b;">Instant API calls</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def _render_enhanced_file_upload():
    """Clean file upload area without unnecessary containers"""
    
    uploaded_file = st.file_uploader(
        "Choose your file",
        type=['csv', 'xlsx', 'xls'],
        key="main_file_uploader",
        help="Maximum size: 200MB • Supported formats: CSV, Excel (.xlsx, .xls)"
    )
    
    if uploaded_file:
        _process_uploaded_file(uploaded_file)
    
    # File format indicators
    st.markdown("""
        <div style="
            text-align: center;
            margin: 1rem 0;
            color: #64748b;
            font-size: 0.9rem;
        ">
            <div style="margin-bottom: 0.5rem;">
                <strong>Supported formats:</strong>
            </div>
            <div style="display: flex; justify-content: center; gap: 1rem;">
                <span style="
                    background: #f1f5f9;
                    padding: 0.25rem 0.75rem;
                    border-radius: 0.5rem;
                    font-weight: 500;
                ">📄 CSV</span>
                <span style="
                    background: #f1f5f9;
                    padding: 0.25rem 0.75rem;
                    border-radius: 0.5rem;
                    font-weight: 500;
                ">📊 Excel</span>
                <span style="
                    background: #f1f5f9;
                    padding: 0.25rem 0.75rem;
                    border-radius: 0.5rem;
                    font-weight: 500;
                ">📈 XLSX</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

def _process_uploaded_file(uploaded_file):
    """Process the uploaded file and update session state"""
    try:
        # Clear processing state from previous session and validation state
        keys_to_clear = ['processing_completed', 'validation_passed', 'field_mappings', 'header_comparison', 'mapping_tab_index', 'processing_results', 'load_results', 'processing_in_progress', 'validation_errors', 'mapping_section_expanded']
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        # Process file upload
        with st.spinner("📖 Reading file..."):
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
        
        # Normalize and store
        df = normalize_column_names(df)
        file_headers = list(df.columns)
        
        st.session_state.uploaded_df = df
        st.session_state.uploaded_file_name = uploaded_file.name
        st.session_state.file_headers = file_headers
        st.session_state.file_size = uploaded_file.size / 1024 / 1024  # MB
        
        # Header validation with existing config
        _validate_headers_with_config(file_headers)
        
        # Success message
        st.success(f"✅ **{uploaded_file.name}** loaded successfully • {len(df):,} records • {st.session_state.file_size:.1f} MB")
        
        # Auto-rerun to show workflow
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error reading file: {str(e)}")

def _validate_headers_with_config(file_headers):
    """Validate file headers against existing configuration"""
    brokerage_name = st.session_state.brokerage_name
    
    if (st.session_state.get('configuration_type') == 'existing' and 
        'selected_configuration' in st.session_state):
        config = st.session_state.selected_configuration
        
        # Always try to validate headers against saved config in database
        # Don't rely on session state mappings which might be placeholders
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
    else:
        # New configuration
        st.session_state.header_comparison = {
            'status': 'new_config',
            'changes': [],
            'missing': [],
            'added': file_headers
        }

def _render_workflow_with_progress(db_manager, data_processor):
    """Show workflow sections with progress bar after file upload"""
    
    # Show compact status info based on current state
    if st.session_state.get('uploaded_df') is not None and not st.session_state.get('validation_passed'):
        if st.session_state.get('field_mappings'):
            # Check if we have real mappings
            field_mappings = st.session_state.get('field_mappings', {})
            has_real_mappings = any(not k.startswith('_') and v and v != 'Select column...' for k, v in field_mappings.items())
            if has_real_mappings:
                st.info("🔍 Mapping complete! Ready to validate data quality")
            else:
                st.info("🔗 Complete field mapping to continue")
    
    # Determine current step based on session state
    if st.session_state.get('validation_passed') == True:
        current_step = 4
    elif (st.session_state.get('uploaded_df') is not None and 
          'field_mappings' in st.session_state):
        field_mappings = st.session_state.get('field_mappings', {})
        has_real_mappings = any(
            not key.startswith('_') and 
            value and 
            value != 'Select column...' and 
            value.strip() != ''
            for key, value in field_mappings.items()
        )
        if has_real_mappings:
            current_step = 3
        else:
            current_step = 2
    elif st.session_state.get('uploaded_df') is not None:
        current_step = 2
    else:
        current_step = 1
    
    # Show progress bar
    _render_enhanced_progress(current_step)
    
    # Show current file info
    _render_current_file_info()
    
    # Progressive disclosure sections
    if st.session_state.get('uploaded_df') is not None:
        _render_smart_mapping_section(db_manager, data_processor)
    
    if st.session_state.get('field_mappings'):
        _render_validation_section(db_manager, data_processor)
    
    if st.session_state.get('validation_passed'):
        _render_processing_section(db_manager, data_processor)
        
        # Show results summary after processing completion  
        if st.session_state.get('processing_completed'):
            _render_results_summary_section()
    
    # Learning analytics dashboard removed - not currently being used

def _render_current_file_info():
    """Show current file information in a compact format"""
    if st.session_state.get('uploaded_df') is not None:
        filename = st.session_state.get('uploaded_file_name', 'Unknown')
        record_count = len(st.session_state.uploaded_df)
        file_size = st.session_state.get('file_size', 0)
        
        st.markdown(f"""
            <div style="
                background: #f8fafc;
                padding: 1rem;
                border-radius: 0.5rem;
                border: 1px solid #e2e8f0;
                margin: 1rem 0;
                display: flex;
                align-items: center;
                justify-content: space-between;
            ">
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <div style="font-size: 1.5rem;">📄</div>
                    <div>
                        <div style="font-weight: 600; color: #1e293b;">{filename}</div>
                        <div style="font-size: 0.9rem; color: #64748b;">{record_count:,} records • {file_size:.1f} MB</div>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        # Action buttons
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("📂 Upload Different File", key="change_file_btn", use_container_width=True):
                # Clear file-related state and validation state
                keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'file_headers', 'validation_passed', 'header_comparison', 'field_mappings', 'mapping_tab_index', 'file_size', 'processing_completed', 'processing_results', 'load_results', 'processing_in_progress', 'validation_errors', 'mapping_section_expanded']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        with col2:
            if st.button("👀 Preview Data", key="preview_toggle_btn", use_container_width=True):
                st.session_state.show_preview = not st.session_state.get('show_preview', False)
        
        # Show preview if requested
        if st.session_state.get('show_preview', False):
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
                    if api_preview_data["preview"] and field_mappings:
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
                                "status": "DRAFT/CUSTOMER_CONFIRMED/etc",
                                "route": [
                                    {"stopActivity": "PICKUP", "address": "..."},
                                    {"stopActivity": "DELIVERY", "address": "..."}
                                ]
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
                    st.caption("Field mapping summary")
                    field_mappings = st.session_state.get('field_mappings', {})
                    
                    if field_mappings:
                        mapping_df = pd.DataFrame([
                            {"API Field": api_field, "CSV Column": csv_col} 
                            for api_field, csv_col in field_mappings.items()
                        ])
                        st.dataframe(mapping_df, use_container_width=True)
                        
                        # Show mapping statistics
                        total_mapped = len(field_mappings)
                        manual_values = len([v for v in field_mappings.values() if v.startswith("MANUAL_VALUE:")])
                        csv_columns = len([v for v in field_mappings.values() if not v.startswith("MANUAL_VALUE:")])
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Mappings", total_mapped)
                        with col2:
                            st.metric("CSV Columns", csv_columns)
                        with col3:
                            st.metric("Manual Values", manual_values)
                    else:
                        st.info("No field mappings configured yet. Complete the mapping step to see details.")

def _render_enhanced_progress(current_step):
    """Enhanced progress bar with visual connections and animations"""
    steps = [
        {"num": 1, "label": "Upload", "icon": "📤"},
        {"num": 2, "label": "Map", "icon": "🔗"},
        {"num": 3, "label": "Quality Check", "icon": "✅"},
        {"num": 4, "label": "Process", "icon": "🚀"}
    ]
    
    st.markdown("""
        <div style="
            background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
            border-radius: 0.5rem;
            padding: 0.5rem;
            margin: 0.5rem 0;
            border: 1px solid #e2e8f0;
        ">
    """, unsafe_allow_html=True)
    
    cols = st.columns(len(steps))
    for i, step in enumerate(steps):
        with cols[i]:
            is_active = step["num"] == current_step
            is_completed = step["num"] < current_step
            
            if is_completed:
                st.markdown(f"""
                    <div style="text-align: center; color: #059669;">
                        <div style="font-size: 1.2rem; margin-bottom: 0.25rem;">✅</div>
                        <div style="font-size: 0.65rem; font-weight: 600; line-height: 1;">{step["label"]}</div>
                    </div>
                """, unsafe_allow_html=True)
            elif is_active:
                st.markdown(f"""
                    <div style="text-align: center; color: #2563eb;">
                        <div style="font-size: 1.2rem; margin-bottom: 0.25rem; animation: pulse 2s infinite;">{step["icon"]}</div>
                        <div style="font-size: 0.65rem; font-weight: 600; color: #2563eb; line-height: 1;">{step["label"]}</div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="text-align: center; color: #9ca3af;">
                        <div style="font-size: 1.2rem; margin-bottom: 0.25rem; opacity: 0.5;">{step["icon"]}</div>
                        <div style="font-size: 0.65rem; line-height: 1;">{step["label"]}</div>
                    </div>
                """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)

def _render_smart_mapping_section(db_manager, data_processor):
    """Smart mapping section with progressive disclosure"""
    
    # Initialize expandable state in session state if not exists
    if 'mapping_section_expanded' not in st.session_state:
        st.session_state.mapping_section_expanded = not st.session_state.get('field_mappings')
    
    # Prevent mapping section from expanding during active processing only
    # This fixes the bug where Process Data click causes mapping section to open
    is_processing_active = st.session_state.get('processing_in_progress', False)
    processing_completed = st.session_state.get('processing_completed', False)
    
    # Only prevent expansion during actual processing, not after validation
    # Users should be able to return to mapping even after validation has passed
    
    with st.expander("🔗 **Field Mapping**", expanded=st.session_state.mapping_section_expanded and not is_processing_active):
        st.caption("Map your CSV columns to API fields")
        
        df = st.session_state.uploaded_df
        header_comparison = st.session_state.get('header_comparison', {})
        
        # Get existing configuration if applicable
        existing_config = None
        if (st.session_state.get('configuration_type') == 'existing' and 
            'selected_configuration' in st.session_state):
            config = st.session_state.selected_configuration
            field_mappings = config.get('field_mappings', {})
            has_real_mappings = any(not key.startswith('_') for key in field_mappings.keys())
            
            if has_real_mappings:
                existing_config = config
        
        # Learning-enhanced mapping interface
        brokerage_name = st.session_state.get('brokerage_name')
        configuration_name = st.session_state.get('selected_configuration', {}).get('name')
        
        field_mappings = create_learning_enhanced_mapping_interface(
            df, existing_config.get('field_mappings', {}) if existing_config else {},
            data_processor, db_manager, brokerage_name, configuration_name
        )
        
        st.session_state.field_mappings = field_mappings
        
        # Auto-save field mappings to database when they're updated
        # This ensures mappings are preserved even if user doesn't complete validation
        if (field_mappings and 
            st.session_state.get('selected_configuration') and 
            st.session_state.get('file_headers')):
            
            # Check if mappings have real content (not just placeholders)
            has_real_mappings = any(not key.startswith('_') for key in field_mappings.keys())
            
            if has_real_mappings:
                # Save mappings to database immediately
                try:
                    _save_configuration(db_manager, field_mappings, st.session_state.file_headers)
                    # Visual feedback to user
                    st.caption("💾 Field mappings auto-saved to configuration")
                    
                    # Re-run header validation now that we have saved headers in database
                    # This will properly set the header comparison status to 'identical' if headers match
                    if st.session_state.get('file_headers'):
                        _validate_headers_with_config(st.session_state.file_headers)
                    
                except Exception as e:
                    st.caption(f"⚠️ Could not auto-save mappings: {str(e)}")
            else:
                st.caption("📝 Complete field mapping to save configuration")
        
        # Progress indicator
        api_schema = get_full_api_schema()
        effective_required_fields = get_effective_required_fields(api_schema, field_mappings)
        mapped_required = len([f for f in effective_required_fields.keys() if f in field_mappings and field_mappings[f] and field_mappings[f] != 'Select column...'])
        total_required = len(effective_required_fields)
        
        progress = mapped_required / total_required if total_required > 0 else 0
        st.progress(progress)
        
        if mapped_required >= total_required:
            st.success(f"✅ All {mapped_required} effective required fields mapped!")
        else:
            st.info(f"📋 {mapped_required}/{total_required} effective required fields mapped")

def _render_validation_section(db_manager, data_processor):
    """Validation section with smart defaults"""
    
    # Only expand if there are validation issues
    has_issues = st.session_state.get('validation_errors')
    
    with st.expander("✅ **Data Quality Validation**", expanded=bool(has_issues)):
        st.caption("Validate data quality and format compliance before processing")
        
        df = st.session_state.uploaded_df
        field_mappings = st.session_state.field_mappings
        file_headers = st.session_state.file_headers
        
        # Run validation
        with st.spinner("🔍 Validating data..."):
            try:
                validation_errors = validate_mapping(df, field_mappings, data_processor)
                st.session_state.validation_errors = validation_errors
                
                if validation_errors:
                    can_proceed = create_validation_summary_card(validation_errors, len(df))
                    show_validation_errors(validation_errors)
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 Fix Mapping", type="primary", key="fix_mapping_btn"):
                            st.session_state.validation_errors = None
                            st.info("💡 Adjust your field mappings above")
                    
                    with col2:
                        if can_proceed and st.button("⚠️ Continue Anyway", key="continue_anyway_btn"):
                            st.session_state.validation_passed = True
                            st.rerun()
                else:
                    st.session_state.validation_passed = True
                    st.success("✅ All data validated successfully!")
                
                # Save configuration (if not already auto-saved)
                if st.session_state.get('validation_passed'):
                    # Check if configuration was already auto-saved in mapping section
                    config = st.session_state.selected_configuration
                    current_mappings = config.get('field_mappings', {})
                    has_current_mappings = any(not key.startswith('_') for key in current_mappings.keys())
                    
                    if not has_current_mappings:
                        # Only save if not already saved
                        _save_configuration(db_manager, field_mappings, file_headers)
                        st.success("✅ Configuration saved to database!")
                    else:
                        st.success("✅ Configuration already saved!")
                    
                    # Trigger backup suggestion after configuration is saved
                    auto_backup_suggestion()
                    
            except Exception as e:
                st.error(f"❌ Validation failed: {str(e)}")

def _render_processing_section(db_manager, data_processor):
    """Processing section with enhanced UX"""
    
    with st.expander("🚀 **Process & Submit**", expanded=not st.session_state.get('processing_completed', False)):
        st.caption("Process your data and submit to the API")
        
        df = st.session_state.uploaded_df
        field_mappings = st.session_state.field_mappings
        api_credentials = st.session_state.api_credentials
        brokerage_name = st.session_state.brokerage_name
        
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Records", f"{len(df):,}")
        with col2:
            st.metric("Fields", len(field_mappings))
        with col3:
            st.metric("Status", "Ready ✅")
        
        # Process button or next-step actions based on completion state
        if st.session_state.get('processing_completed', False):
            # Show next-step actions prominently after processing
            st.markdown("### ⚡ Next Steps")
            st.caption("Choose your next action")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🔄 Process Another File", type="primary", key="process_another_main", use_container_width=True):
                    keys_to_clear = ['uploaded_df', 'uploaded_file_name', 'file_headers', 'validation_passed', 'header_comparison', 'field_mappings', 'mapping_tab_index', 'processing_completed', 'processing_results', 'load_results', 'processing_in_progress', 'validation_errors', 'mapping_section_expanded']
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
            
            with col2:
                if st.button("📥 Download Results", key="download_results_main", use_container_width=True):
                    # Show download options
                    st.info("💡 Scroll down to find download options in the results section")
            
            with col3:
                if st.button("🏠 Start Over", key="start_over_main", use_container_width=True):
                    for key in list(st.session_state.keys()):
                        if key not in ['sidebar_state']:  # Keep sidebar state
                            del st.session_state[key]
                    st.rerun()
        else:
            # Show original process button
            if st.button("🚀 Process Data", type="primary", key="process_btn", use_container_width=True):
                try:
                    session_id = st.session_state.get('session_id', 'unknown')
                    
                    result = process_data_enhanced(
                        df, field_mappings, api_credentials, brokerage_name, 
                        data_processor, db_manager, session_id
                    )
                    
                    # Update learning system with processing results
                    if result and 'success_rate' in result:
                        update_learning_with_processing_results(
                            session_id, result['success_rate'], data_processor, db_manager
                        )
                    
                    # After processing, the UI will automatically update to show next steps
                    
                except Exception as e:
                    st.error(f"❌ Processing failed: {str(e)}")

def _render_results_summary_section():
    """Show results summary and download options after processing completion"""
    
    with st.expander("📋 **Results & Downloads**", expanded=True):
        st.caption("Processing complete - view results and download options")
        
        # Get processing results from session state
        results = st.session_state.get('processing_results', {})
        
        if results:
            # Get key metrics
            success_rate = results.get('success_rate', 0)
            total_records = results.get('total_records', 0)
            successful_records = results.get('successful_records', 0)
            failed_records = results.get('failed_records', 0)
            processing_time = results.get('processing_time', 0)
            
            # Performance metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", f"{total_records:,}")
            with col2:
                st.metric("Success Rate", f"{success_rate:.0f}%")
            with col3:
                st.metric("Processing Time", f"{processing_time:.1f}s")
            with col4:
                avg_time = processing_time / total_records if total_records > 0 else 0
                st.metric("Per Record", f"{avg_time:.2f}s")
            
            # Download options
            st.markdown("### 📥 Download Options")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("📄 Download Processing Report", key="download_report", use_container_width=True):
                    # Create processing report
                    report_data = {
                        'Processing Summary': {
                            'Total Records': total_records,
                            'Successful': successful_records,
                            'Failed': failed_records,
                            'Success Rate': f"{success_rate:.1f}%",
                            'Processing Time': f"{processing_time:.1f} seconds",
                            'Average per Record': f"{avg_time:.3f} seconds"
                        },
                        'Session Info': {
                            'Session ID': results.get('session_id', 'Unknown'),
                            'Configuration': results.get('configuration_name', 'Unknown'),
                            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                    }
                    
                    import json
                    report_json = json.dumps(report_data, indent=2)
                    st.download_button(
                        label="💾 Save Report",
                        data=report_json,
                        file_name=f"processing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
            
            with col2:
                if st.button("📊 View Detailed Results", key="show_details", use_container_width=True):
                    st.session_state.show_detailed_results = not st.session_state.get('show_detailed_results', False)
            
            # Show detailed results if requested
            if st.session_state.get('show_detailed_results', False):
                st.markdown("### 📋 Detailed Results")
                
                # Show load results if available
                if 'load_results' in st.session_state:
                    load_results = st.session_state.load_results
                    
                    if successful_records > 0:
                        st.markdown("**✅ Successful Loads:**")
                        successful_loads = [r for r in load_results if r.get('success', False)]
                        if successful_loads:
                            success_df = pd.DataFrame([
                                {
                                    'Load Number': r.get('load_number', 'Unknown'),
                                    'Row': r.get('row_index', 'Unknown'),
                                    'Status': '✅ Success'
                                }
                                for r in successful_loads[:20]  # Show first 20
                            ])
                            st.dataframe(success_df, use_container_width=True, hide_index=True)
                            
                            if len(successful_loads) > 20:
                                st.caption(f"... and {len(successful_loads) - 20} more successful loads")
                    
                    if failed_records > 0:
                        st.markdown("**❌ Failed Loads:**")
                        failed_loads = [r for r in load_results if not r.get('success', False)]
                        if failed_loads:
                            failed_df = pd.DataFrame([
                                {
                                    'Load Number': r.get('load_number', 'Unknown'),
                                    'Row': r.get('row_index', 'Unknown'),
                                    'Error': r.get('error', 'Unknown error')[:100] + ('...' if len(r.get('error', '')) > 100 else '')
                                }
                                for r in failed_loads[:20]  # Show first 20
                            ])
                            st.dataframe(failed_df, use_container_width=True, hide_index=True)
                            
                            if len(failed_loads) > 20:
                                st.caption(f"... and {len(failed_loads) - 20} more failed loads")
        else:
            st.info("No processing results available")

def _save_configuration(db_manager, field_mappings, file_headers):
    """Save configuration with field mappings"""
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

def process_data_enhanced(df, field_mappings, api_credentials, brokerage_name, data_processor, db_manager, session_id):
    """Enhanced data processing with detailed tracking and error handling"""
    
    # Set processing flag to prevent UI interference
    st.session_state.processing_in_progress = True
    
    # Initialize progress tracking
    total_steps = 6
    current_step = 0
    
    # Create enhanced progress components
    progress_container = st.container()
    
    with progress_container:
        # Main progress bar
        progress_bar = st.progress(0)
        
        # Status and timing information
        col1, col2, col3 = st.columns(3)
        with col1:
            status_text = st.empty()
        with col2:
            time_text = st.empty()
        with col3:
            records_text = st.empty()
        
        # Detailed step indicator
        step_indicator = st.empty()
        
        # Start timing
        import time
        start_time = time.time()
        
        def update_progress(step_name, step_number, details=""):
            nonlocal current_step
            current_step = step_number
            progress_percent = int((current_step / total_steps) * 100)
            progress_bar.progress(progress_percent)
            
            # Update status
            status_text.text(f"🔄 {step_name}")
            
            # Update timing
            elapsed = time.time() - start_time
            time_text.text(f"⏱️ {elapsed:.1f}s elapsed")
            
            # Update step indicator
            step_indicator.markdown(f"**Step {current_step}/{total_steps}:** {step_name}")
            if details:
                step_indicator.caption(details)
        
        # Update record counter
        records_text.text(f"📊 {len(df)} records")
    
    try:
        # Step 1: Pre-flight validation
        update_progress("Pre-flight validation", 1, "Validating inputs and connections...")
        
        # Validate API credentials and create client based on auth type
        config = st.session_state.get('selected_configuration', {})
        auth_type = config.get('auth_type', 'api_key')  # Default to api_key for backward compatibility
        bearer_token = config.get('bearer_token')
        
        if auth_type == 'api_key':
            client = LoadsAPIClient(api_credentials['base_url'], api_key=api_credentials['api_key'], auth_type='api_key')
        else:  # bearer_token
            client = LoadsAPIClient(api_credentials['base_url'], bearer_token=bearer_token, auth_type='bearer_token')
        
        connection_test = client.validate_connection()
        if not connection_test['success']:
            st.error(f"❌ API connection failed: {connection_test['message']}")
            st.session_state.processing_in_progress = False  # Clear processing flag on early failure
            return
            
        # Step 2: Apply field mappings
        update_progress("Applying field mappings", 2, f"Mapping {len(field_mappings)} fields to {len(df)} records...")
        
        with st.spinner("Processing field mappings..."):
            mapped_df, mapping_errors = data_processor.apply_mapping(df, field_mappings)
        
        if mapping_errors:
            st.error("❌ Mapping failed:")
            for error in mapping_errors:
                st.error(f"• {error}")
            st.session_state.processing_in_progress = False  # Clear processing flag on early failure
            return
        
        # Apply automatic carrier mapping if enabled
        update_progress("Applying carrier mappings", 2.5, "Auto-mapping carrier information...")
        with st.spinner("Applying carrier mappings..."):
            mapped_df = data_processor.apply_carrier_mapping(mapped_df, brokerage_name, db_manager)
        
        # Step 3: Data validation
        update_progress("Validating data", 3, "Checking data quality and format compliance...")
        
        with st.spinner("Validating data format and requirements..."):
            validated_df, validation_errors = data_processor.validate_data(mapped_df, get_full_api_schema())
        
        detailed_errors = []
        if validation_errors:
            st.warning(f"⚠️ Found {len(validation_errors)} validation issues (processing will continue)")
            # Convert validation errors to detailed format for database storage
            for error in validation_errors:
                detailed_errors.append({
                    'row_number': error.get('row', 1),
                    'field_name': 'general',
                    'error_type': 'validation',
                    'error_message': str(error),
                    'suggested_fix': 'Review data format and field mappings',
                    'original_value': '',
                    'expected_format': 'API compliant format'
                })
        
        # Step 4: Format for API
        update_progress("Formatting for API", 4, "Converting data to API-compatible format...")
        
        with st.spinner("Formatting data for API submission..."):
            api_payloads = data_processor.format_for_api(validated_df)
        
        # Step 5: Send to API with enhanced progress
        update_progress("Sending to API", 5, f"Submitting {len(api_payloads)} loads to the API...")
        
        # Enhanced API submission with progress tracking
        api_progress_bar = st.progress(0)
        api_status = st.empty()
        
        results = []
        successful_count = 0
        failed_count = 0
        
        for i, payload in enumerate(api_payloads):
            # Update API progress
            api_progress = int((i / len(api_payloads)) * 100)
            api_progress_bar.progress(api_progress)
            api_status.text(f"Processing load {i+1}/{len(api_payloads)} (✅ {successful_count} | ❌ {failed_count})")
            
            # Submit individual load
            result = client.create_load(payload)
            result['row_index'] = i + 1
            
            # Enhanced: Extract load number from successful responses
            if result.get('success', False):
                load_number = None
                # Try to get load number from API response
                if 'data' in result and isinstance(result['data'], dict):
                    # Check various possible locations for load number
                    load_number = (result['data'].get('loadNumber') or 
                                 result['data'].get('load', {}).get('loadNumber') or
                                 result['data'].get('id'))
                
                # Fallback to original payload if not in response
                if not load_number and 'load' in payload:
                    load_number = payload['load'].get('loadNumber')
                
                result['load_number'] = load_number or f"Load-{i+1}"
            else:
                # For failed loads, still try to get the intended load number
                load_number = None
                if 'load' in payload:
                    load_number = payload['load'].get('loadNumber')
                result['load_number'] = load_number or f"Load-{i+1}"
            
            results.append(result)
            
            # Update counters
            if result.get('success', False):
                successful_count += 1
            else:
                failed_count += 1
                # Add detailed error for failed records
                detailed_errors.append({
                    'row_number': i + 1,
                    'field_name': 'api_submission',
                    'error_type': 'api_error',
                    'error_message': result.get('error', 'Unknown API error'),
                    'suggested_fix': 'Review API response and data format',
                    'original_value': str(payload),
                    'expected_format': 'Valid API payload'
                })
            
            # Small delay for rate limiting
            time.sleep(0.05)
        
        # Clear API progress indicators
        api_progress_bar.empty()
        api_status.empty()
        
        # Step 6: Process and save results
        update_progress("Saving results", 6, "Processing results and saving to database...")
        
        processing_time = time.time() - start_time
        
        with st.spinner("Saving results to database..."):
            # Save enhanced upload history
            configuration_name = st.session_state.get('selected_configuration', {}).get('name') or st.session_state.get('new_configuration', {}).get('configuration_name', 'Unknown')
            
            upload_id = db_manager.save_upload_history_enhanced(
                brokerage_name=brokerage_name,
                configuration_name=configuration_name,
                filename=st.session_state.uploaded_file_name,
                total_records=len(df),
                successful_records=successful_count,
                failed_records=failed_count,
                error_log=json.dumps([r for r in results if not r.get('success', False)]),
                processing_time=processing_time,
                file_headers=st.session_state.file_headers,
                session_id=session_id
            )
            
            # Save detailed errors for troubleshooting
            if detailed_errors:
                db_manager.save_processing_errors(upload_id, detailed_errors)
            
            # Trigger database backup after processing results save
            from db_manager import upload_sqlite_if_changed
            upload_sqlite_if_changed()
        
        # Final progress update
        progress_bar.progress(100)
        status_text.text("✅ Processing complete!")
        
        # Clear progress indicators
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
        time_text.empty()
        records_text.empty()
        step_indicator.empty()
        
        # Single consolidated success message with all key information
        success_rate = (successful_count / len(df)) * 100 if len(df) > 0 else 0
        
        if success_rate == 100:
            st.success(f"🎉 Processing Complete! All {len(df)} records processed successfully in {processing_time:.1f}s")
        elif success_rate >= 90:
            st.warning(f"⚠️ Processing mostly successful: {successful_count}/{len(df)} records processed ({success_rate:.0f}% success) in {processing_time:.1f}s")
        else:
            st.error(f"❌ Processing issues: Only {successful_count}/{len(df)} records successful ({success_rate:.0f}%) in {processing_time:.1f}s")
        
        # Compact metrics in a single row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Successful", successful_count, f"{success_rate:.0f}%")
        with col3:
            st.metric("Failed", failed_count, f"{100-success_rate:.0f}%")
        with col4:
            st.metric("API Status", "augment-br..." if 'api_credentials' in locals() else "Ready ✅")
        
        # Session info in compact format
        st.info(f"⏱️ Total processing time: {processing_time:.1f} seconds (0.35s per record) • 📋 Session ID: {session_id} | Configuration: {configuration_name}")
        
        # Set processing completion flag for status bar
        st.session_state.processing_completed = True
        
        # Clear processing flag since we're done
        st.session_state.processing_in_progress = False
        
        # Store processing results summary for Results & Downloads section
        st.session_state.processing_results = {
            'success_rate': success_rate,
            'total_records': len(df),
            'successful_records': successful_count,
            'failed_records': failed_count,
            'processing_time': processing_time,
            'session_id': session_id,
            'configuration_name': configuration_name
        }
        
        # Store detailed load results for detailed results display
        st.session_state.load_results = results
        
        # Load Results Dropdown - NEW FEATURE
        if results:
            with st.expander("📋 Load Results Summary", expanded=False):
                st.markdown("**Load Creation Results:**")
                
                # Create results summary
                load_results = []
                for result in results:
                    load_number = result.get('load_number', f"Row {result.get('row_index', 'Unknown')}")
                    status = "✅ Success" if result.get('success', False) else "❌ Failed"
                    error_msg = result.get('error', '') if not result.get('success', False) else ''
                    
                    load_results.append({
                        'Load Number': load_number,
                        'Status': status,
                        'Error': error_msg[:50] + ('...' if len(error_msg) > 50 else '') if error_msg else ''
                    })
                
                # Display as DataFrame for easy scanning
                results_df = pd.DataFrame(load_results)
                st.dataframe(results_df, use_container_width=True, hide_index=True)
                
                # Quick filter view
                col1, col2 = st.columns(2)
                with col1:
                    successful_loads = [r for r in results if r.get('success', False)]
                    if successful_loads:
                        st.markdown("**✅ Successful Loads:**")
                        for result in successful_loads[:10]:  # Show first 10
                            load_number = result.get('load_number', f"Row {result.get('row_index', 'Unknown')}")
                            st.markdown(f"• {load_number}")
                        if len(successful_loads) > 10:
                            st.caption(f"... and {len(successful_loads) - 10} more")
                
                with col2:
                    failed_loads = [r for r in results if not r.get('success', False)]
                    if failed_loads:
                        st.markdown("**❌ Failed Loads:**")
                        for result in failed_loads[:10]:  # Show first 10
                            load_number = result.get('load_number', f"Row {result.get('row_index', 'Unknown')}")
                            st.markdown(f"• {load_number}")
                        if len(failed_loads) > 10:
                            st.caption(f"... and {len(failed_loads) - 10} more")
        
        # Suggest backup after successful processing
        auto_backup_suggestion()
        
        # Return result for learning system
        return {
            'success_rate': success_rate / 100,  # Convert to 0-1 range
            'successful_count': successful_count,
            'failed_count': failed_count,
            'total_count': len(df),
            'processing_time': processing_time
        }
        
        # Failed records details (only if there are failures)
        if failed_count > 0:
            with st.expander(f"❌ Failed Records Details ({failed_count} records)"):
                failed_records = []
                for r in results:
                    if not r.get('success', False):
                        failed_records.append({
                            'Row': r.get('row_index', 'Unknown'),
                            'Error': r.get('error', 'Unknown error'),
                            'Status Code': r.get('status_code', 'N/A')
                        })
                
                if failed_records:
                    failed_df = pd.DataFrame(failed_records)
                    st.dataframe(failed_df, use_container_width=True, hide_index=True)
                    
                    # Download failed records
                    csv_data = failed_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Failed Records Report",
                        data=csv_data,
                        file_name=f"failed_records_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
        
        # Download successful records (only if there were some failures)
        if successful_count > 0 and successful_count < len(df):
            with st.expander("📥 Download Options"):
                successful_indices = [r.get('row_index', 1) - 1 for r in results if r.get('success', False)]
                successful_df = df.iloc[successful_indices]
                csv_data = successful_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Successful Records Only",
                    data=csv_data,
                    file_name=f"successful_records_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
    except Exception as e:
        # Enhanced error handling
        st.error(f"❌ Processing failed: {str(e)}")
        logger.error(f"Processing error: {str(e)}")
        
        # Clear processing flag on error to prevent UI state issues
        st.session_state.processing_in_progress = False
        
        # Clear progress indicators on error
        try:
            progress_bar.empty()
            status_text.empty()
            time_text.empty()
            records_text.empty()
            step_indicator.empty()
        except:
            pass

def get_smart_mappings(df, data_processor):
    """Get smart field mapping suggestions"""
    try:
        api_schema = get_full_api_schema()
        return data_processor.suggest_mapping(list(df.columns), api_schema, df)
    except Exception as e:
        logger.warning(f"Smart mapping failed: {str(e)}")
        return {}

def show_mapping_suggestions(suggested_mappings, api_schema, df):
    """Show suggested field mappings with confidence scores"""
    if not suggested_mappings:
        st.info("No automatic mappings were suggested. Please map fields manually.")
        return {}
    
    st.subheader("🧠 Smart Mapping Suggestions")
    st.write("Review and adjust the suggested field mappings:")
    
    updated_mappings = {}
    
    for csv_field, api_field in suggested_mappings.items():
        if csv_field in df.columns and api_field in api_schema:
            # Get sample data for preview
            sample_data = df[csv_field].dropna().head(3).tolist()
            
            # Create mapping card
            create_field_mapping_card(csv_field, api_field, confidence=0.8, sample_data=sample_data)
            
            # Add to updated mappings
            updated_mappings[api_field] = csv_field
    
    return updated_mappings

def validate_api_input(api_key: str, base_url: str) -> tuple[bool, str]:
    """Validate API input parameters"""
    if not api_key or len(api_key.strip()) < 10:
        return False, "API key must be at least 10 characters long"
    
    if not base_url or not base_url.startswith(('http://', 'https://')):
        return False, "Base URL must start with http:// or https://"
    
    # Additional validation
    if 'localhost' in base_url.lower() or '127.0.0.1' in base_url:
        return False, "Cannot connect to localhost URLs"
    
    return True, "Valid"

def process_data(df, field_mappings, api_credentials, customer_name, data_processor, db_manager):
    """Process the data with enhanced progress indicators"""
    
    # Initialize progress tracking
    total_steps = 6
    current_step = 0
    
    # Create enhanced progress components
    progress_container = st.container()
    
    with progress_container:
        # Main progress bar
        progress_bar = st.progress(0)
        
        # Status and timing information
        col1, col2, col3 = st.columns(3)
        with col1:
            status_text = st.empty()
        with col2:
            time_text = st.empty()
        with col3:
            records_text = st.empty()
        
        # Detailed step indicator
        step_indicator = st.empty()
        
        # Start timing
        import time
        start_time = time.time()
        
        def update_progress(step_name, step_number, details=""):
            nonlocal current_step
            current_step = step_number
            progress_percent = int((current_step / total_steps) * 100)
            progress_bar.progress(progress_percent)
            
            # Update status
            status_text.text(f"🔄 {step_name}")
            
            # Update timing
            elapsed = time.time() - start_time
            time_text.text(f"⏱️ {elapsed:.1f}s elapsed")
            
            # Update step indicator
            step_indicator.markdown(f"**Step {current_step}/{total_steps}:** {step_name}")
            if details:
                step_indicator.caption(details)
        
        # Update record counter
        records_text.text(f"📊 {len(df)} records")
    
    try:
        # Step 1: Pre-flight validation
        update_progress("Pre-flight validation", 1, "Validating inputs and connections...")
        
        # Validate API credentials (this is a legacy function, using api_key auth only for now)
        client = LoadsAPIClient(api_credentials['base_url'], api_key=api_credentials['api_key'], auth_type='api_key')
        connection_test = client.validate_connection()
        if not connection_test['success']:
            st.error(f"❌ API connection failed: {connection_test['message']}")
            return
            
        # Step 2: Apply field mappings
        update_progress("Applying field mappings", 2, f"Mapping {len(field_mappings)} fields to {len(df)} records...")
        
        with st.spinner("Processing field mappings..."):
            mapped_df, mapping_errors = data_processor.apply_mapping(df, field_mappings)
        
        if mapping_errors:
            st.error("❌ Mapping failed:")
            for error in mapping_errors:
                st.error(f"• {error}")
            return
        
        # Step 3: Data validation
        update_progress("Validating data", 3, "Checking data quality and format compliance...")
        
        with st.spinner("Validating data format and requirements..."):
            validated_df, validation_errors = data_processor.validate_data(mapped_df, get_full_api_schema())
        
        return validation_errors
        
    except Exception as e:
        return [{'row': 1, 'errors': [f"Validation error: {str(e)}"]}]

def show_validation_errors(validation_errors):
    """Display validation errors in a user-friendly format"""
    if not validation_errors:
        return
    
    st.markdown("### ⚠️ Validation Issues Found")
    
    # Group errors by type for better display
    error_summary = {}
    for error in validation_errors:
        error_type = error.get('type', 'general')
        if error_type not in error_summary:
            error_summary[error_type] = []
        error_summary[error_type].append(error)
    
    # Display errors by type
    for error_type, errors in error_summary.items():
        with st.expander(f"{error_type.title()} Errors ({len(errors)} issues)", expanded=True):
            for i, error in enumerate(errors[:10]):  # Show first 10 errors
                row_info = f"Row {error.get('row', 'Unknown')}" if error.get('row') else "General"
                error_text = ', '.join(error.get('errors', ['Unknown error']))
                st.markdown(f"**{row_info}:** {error_text}")
            
            if len(errors) > 10:
                st.info(f"... and {len(errors) - 10} more {error_type} errors")

def _make_error_user_friendly(error_str):
    """Convert technical error messages to user-friendly ones"""
    
    # Field name mappings for user-friendly descriptions
    field_mappings = {
        'load.loadNumber': 'Load Number',
        'load.mode': 'Transportation Mode (FTL/LTL)',
        'load.rateType': 'Rate Type (Spot/Contract)',
        'load.status': 'Load Status',
        'load.route.0.address.street1': 'Pickup Address',
        'load.route.0.address.city': 'Pickup City',
        'load.route.0.address.stateOrProvince': 'Pickup State',
        'load.route.0.address.postalCode': 'Pickup ZIP Code',
        'load.route.0.address.country': 'Pickup Country',
        'load.route.0.expectedArrivalWindowStart': 'Pickup Date/Time',
        'load.route.0.expectedArrivalWindowEnd': 'Pickup Window End',
        'load.route.1.address.street1': 'Delivery Address',
        'load.route.1.address.city': 'Delivery City',
        'load.route.1.address.stateOrProvince': 'Delivery State',
        'load.route.1.address.postalCode': 'Delivery ZIP Code',
        'load.route.1.expectedArrivalWindowStart': 'Delivery Date/Time',
        'customer.customerId': 'Customer ID',
        'customer.name': 'Customer Name',
        'load.items.0.quantity': 'Item Quantity',
        'load.items.0.totalWeightLbs': 'Total Weight (lbs)',
        'bidCriteria.targetCostUsd': 'Target Cost ($)',
        'carrier.name': 'Carrier Name',
        'carrier.dotNumber': 'DOT Number',
        'carrier.mcNumber': 'MC Number'
    }
    
    # Replace technical field names with user-friendly ones
    user_friendly = error_str
    for technical_name, friendly_name in field_mappings.items():
        if technical_name in user_friendly:
            user_friendly = user_friendly.replace(technical_name, friendly_name)
    
    # Make other improvements
    user_friendly = user_friendly.replace("Missing required field:", "Missing:")
    user_friendly = user_friendly.replace("Invalid value", "Invalid value in")
    user_friendly = user_friendly.replace("Valid values:", "Accepted values:")
    
    return user_friendly

# Settings and history functionality integrated into main workflow and sidebar

def get_company_list(db_manager):
    """Get list of companies that have upload history"""
    try:
        # Return empty list for now - this is not used in the simplified workflow
        return []
    except Exception as e:
        logger.error(f"Failed to get company list: {str(e)}")
        return []

def render_database_management_section():
    """Add to sidebar for database backup/restore"""
    
    st.markdown("---")
    st.markdown("**💾 Database Management**")
    
    # Check if database has data
    from src.backend.database import DatabaseManager
    db_manager = DatabaseManager()
    stats = db_manager.get_database_stats()
    
    # Check if database has any data (including brokerage configurations)
    has_data = (stats['customer_mappings'] > 0 or 
                stats['upload_history'] > 0 or 
                stats['brokerage_configurations'] > 0)
    
    if has_data:
        # Database has data - show backup option
        legacy_configs = stats['customer_mappings']
        brokerage_configs = stats['brokerage_configurations']
        uploads = stats['upload_history']
        
        # Show comprehensive summary
        summary_parts = []
        if brokerage_configs > 0:
            summary_parts.append(f"{brokerage_configs} brokerage configs")
        if legacy_configs > 0:
            summary_parts.append(f"{legacy_configs} legacy configs")
        if uploads > 0:
            summary_parts.append(f"{uploads} uploads")
        
        st.caption(f"📊 {', '.join(summary_parts)}")
        
        if st.button("📥 Download Backup", help="Save current database"):
            backup_data = create_database_backup()
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="💾 Save Database Backup",
                data=backup_data,
                file_name=f"ff2api_backup_{current_time}.json",
                mime="application/json"
            )
            # Track backup creation time
            st.session_state.last_backup_time = datetime.now()
            st.success("✅ Backup ready for download!")
    else:
        # Empty database - show restore option
        st.warning("📭 Database is empty")
        uploaded_backup = st.file_uploader(
            "📤 Restore from backup", 
            type=['json'],
            help="Upload a previous database backup"
        )
        
        if uploaded_backup:
            if st.button("🔄 Restore Database"):
                try:
                    restore_result = restore_database_from_backup(uploaded_backup, db_manager)
                    if restore_result['success']:
                        # Show detailed success message
                        success_msg = "✅ Database restored successfully!"
                        details = []
                        if restore_result.get('imported_configurations', 0) > 0:
                            details.append(f"{restore_result['imported_configurations']} configurations")
                        if restore_result.get('imported_mappings', 0) > 0:
                            details.append(f"{restore_result['imported_mappings']} legacy mappings")
                        if restore_result.get('imported_history', 0) > 0:
                            details.append(f"{restore_result['imported_history']} upload records")
                        
                        if details:
                            success_msg += f"\nImported: {', '.join(details)}"
                        
                        st.success(success_msg)
                        st.rerun()
                    else:
                        st.error(f"❌ Restore failed: {restore_result['error']}")
                except Exception as e:
                    st.error(f"❌ Restore error: {str(e)}")

def create_database_backup():
    """Create comprehensive database backup"""
    from src.backend.database import DatabaseManager
    db_manager = DatabaseManager()
    
    backup_data = {
        'backup_info': {
            'created_at': datetime.now().isoformat(),
            'version': '1.0',
            'app_version': 'FF2API v1.0'
        },
        'brokerage_configurations': [],
        'upload_history': [],
        'processing_errors': []
    }
    
    # Export all brokerage configurations
    brokerages = db_manager.get_all_brokerages()
    for brokerage in brokerages:
        configs = db_manager.get_brokerage_configurations(brokerage['name'])
        for config in configs:
            # Note: Export encrypted credentials for restore
            backup_data['brokerage_configurations'].append({
                'brokerage_name': brokerage['name'],
                'configuration_name': config['name'],
                'field_mappings': config['field_mappings'],
                'api_credentials': config['api_credentials'],  # Will be re-encrypted on restore
                'auth_type': config.get('auth_type', 'api_key'),
                'bearer_token': config.get('bearer_token'),
                'file_headers': config.get('file_headers'),
                'created_at': config['created_at'],
                'updated_at': config['updated_at'],
                'description': config.get('description', '')
            })
    
    # Export upload history (last 100 records to keep size manageable)
    # Improved data structure with proper type handling
    upload_history = db_manager.get_upload_history(limit=100)
    for record in upload_history:
        # record is a tuple from SQLite, extract values safely
        try:
            # Handle different record lengths from schema evolution
            brokerage_name = record[1] if len(record) > 1 else 'Unknown'
            configuration_name = record[2] if len(record) > 2 else 'Unknown'
            filename = record[3] if len(record) > 3 else 'unknown_file.csv'
            total_records = int(record[4]) if len(record) > 4 and record[4] is not None else 0
            successful_records = int(record[5]) if len(record) > 5 and record[5] is not None else 0
            failed_records = int(record[6]) if len(record) > 6 and record[6] is not None else 0
            
            # Handle timestamp - could be in different positions due to schema changes
            upload_timestamp = None
            if len(record) > 11 and record[11] is not None:
                upload_timestamp = record[11]
            elif len(record) > 7 and record[7] is not None:
                upload_timestamp = record[7]
            else:
                upload_timestamp = datetime.now().isoformat()
            
            backup_data['upload_history'].append({
                'brokerage_name': str(brokerage_name),
                'configuration_name': str(configuration_name),
                'filename': str(filename),
                'total_records': total_records,
                'successful_records': successful_records,
                'failed_records': failed_records,
                'upload_timestamp': upload_timestamp
            })
        except (IndexError, ValueError, TypeError) as e:
            # Skip malformed records but log the issue
            logger.warning(f"Skipping malformed upload history record: {e}")
            continue
    
    return json.dumps(backup_data, indent=2)

def restore_database_from_backup(uploaded_file, db_manager):
    """Restore database from uploaded backup"""
    try:
        # Create a temporary file for the import_data method which expects a zip file
        import tempfile
        import zipfile
        
        # Read the JSON data
        uploaded_file.seek(0)  # Reset file pointer
        json_data = uploaded_file.read()
        
        # Create a temporary zip file containing the JSON
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.zip', delete=False) as temp_zip:
            with zipfile.ZipFile(temp_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr('backup.json', json_data)
            temp_zip_path = temp_zip.name
        
        try:
            # Use the improved import_data method from DatabaseManager
            result = db_manager.import_data(temp_zip_path)
            return result
        finally:
            # Clean up temporary file
            import os
            try:
                os.unlink(temp_zip_path)
            except:
                pass
        
    except Exception as e:
        return {'success': False, 'error': str(e)}



def get_container_start_time():
    """Estimate container start time based on app initialization"""
    # Store in session state when app first loads
    if 'app_start_time' not in st.session_state:
        st.session_state.app_start_time = datetime.now()
    return st.session_state.app_start_time

def check_critical_backup_needs(db_manager):
    """Check for critical backup needs at app startup"""
    try:
        stats = db_manager.get_database_stats()
        total_data_points = (stats['customer_mappings'] + 
                           stats['upload_history'] + 
                           stats['brokerage_configurations'])
        
        # Only show critical warnings if there's significant data
        if total_data_points > 0:
            container_start_time = get_container_start_time()
            if container_start_time:
                hours_running = (datetime.now() - container_start_time).total_seconds() / 3600
                
                # Show critical backup warning at top of app
                if hours_running > 168:  # 7 days
                    st.error("🚨 **CRITICAL**: Container running for 7+ days. Data loss imminent! Download backup immediately.")
                elif hours_running > 72:  # 3 days
                    st.warning("⚠️ **HIGH RISK**: Container running for 3+ days. Download backup before continuing.")
                elif hours_running > 48:  # 2 days
                    st.info("⏰ **REMINDER**: Container running for 2+ days. Consider downloading backup soon.")
                    
                # Check for large datasets without recent backups
                if total_data_points > 50:
                    last_backup_time = st.session_state.get('last_backup_time')
                    if not last_backup_time or (datetime.now() - last_backup_time).total_seconds() > 86400:  # 24 hours
                        st.warning("💾 **BACKUP RECOMMENDED**: Large dataset detected. Download backup to prevent data loss.")
    except Exception as e:
        # Don't let backup checks break the app
        pass

def auto_backup_suggestion():
    """Suggest backup after significant operations with intelligent timing"""
    # Track operations that warrant backup
    if 'significant_operations' not in st.session_state:
        st.session_state.significant_operations = 0
    
    st.session_state.significant_operations += 1
    
    # Check time since last backup
    last_backup_time = st.session_state.get('last_backup_time')
    hours_since_backup = 999  # Default to high value if no backup
    
    if last_backup_time:
        hours_since_backup = (datetime.now() - last_backup_time).total_seconds() / 3600
    
    # Get container age for risk assessment
    container_start_time = get_container_start_time()
    hours_running = 0
    if container_start_time:
        hours_running = (datetime.now() - container_start_time).total_seconds() / 3600
    
    # Calculate database stats for risk assessment
    from src.backend.database import DatabaseManager
    db_manager = DatabaseManager()
    stats = db_manager.get_database_stats()
    total_data_points = (stats['customer_mappings'] + 
                        stats['upload_history'] + 
                        stats['brokerage_configurations'])
    
    # Smart backup suggestions based on multiple factors
    suggestion_triggered = False
    
    # Critical scenarios - always suggest backup
    if hours_running > 72 and hours_since_backup > 12:
        st.error("🚨 URGENT: Container running for 3+ days. Download backup immediately!")
        suggestion_triggered = True
    elif total_data_points > 20 and hours_since_backup > 24:
        st.warning("⚠️ Large dataset + 24h since backup. Download backup now!")
        suggestion_triggered = True
    elif hours_running > 24 and hours_since_backup > 12:
        st.info("💾 Container stability risk. Consider downloading backup.")
        suggestion_triggered = True
    
    # Regular operation-based suggestions
    if not suggestion_triggered:
        # Suggest backup every 5 operations if no recent backup
        if st.session_state.significant_operations % 5 == 0 and hours_since_backup > 6:
            st.info("💡 Tip: Download database backup to preserve your recent work")
        # Suggest backup every 10 operations normally
        elif st.session_state.significant_operations % 10 == 0:
            st.caption("💾 Consider downloading a backup to preserve your work")
    
    # Show backup shortcut if critical
    if hours_running > 72 or (total_data_points > 20 and hours_since_backup > 24):
        if st.button("📥 Quick Backup", key="quick_backup_urgent", type="primary"):
            backup_data = create_database_backup()
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="💾 Download Emergency Backup",
                data=backup_data,
                file_name=f"ff2api_emergency_backup_{current_time}.json",
                mime="application/json",
                key="emergency_backup_download"
            )
            st.session_state.last_backup_time = datetime.now()
            st.success("✅ Emergency backup ready!")

if __name__ == "__main__":
    main() 