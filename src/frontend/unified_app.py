"""
Unified Load Processing Interface - Consolidates FF2API and End-to-End workflows.

This module provides a single interface that combines:
- FF2API load processing with advanced field mapping
- Load ID mapping and retrieval
- Data enrichment capabilities  
- Postback and output handling
- Email automation support
"""

import streamlit as st

# ========== ABSOLUTE EMERGENCY TEST - UNIFIED_APP.PY ==========
st.error("🚨🚨🚨 THIS IS UNIFIED_APP.PY RUNNING 🚨🚨🚨")
# ==============================================================

import pandas as pd
import json
import os
import sys
from datetime import datetime
import logging
from typing import Dict, Any, List, Optional

# Add parent directory to path to enable src imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import unified backend - always use absolute imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from backend.unified_processor import UnifiedLoadProcessor, ProcessingMode
from backend.database import DatabaseManager
from backend.api_client import LoadsAPIClient
from ui_components import (
    load_custom_css, 
    render_main_header, 
    create_enhanced_file_uploader,
    create_connection_status_card,
    create_data_preview_card,
    create_processing_progress_display,
    create_results_summary_card,
    create_enhanced_button,
    create_brokerage_selection_interface,
    create_learning_enhanced_mapping_interface,
    get_full_api_schema
)

# Import end-to-end components
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from credential_manager import credential_manager
from streamlit_google_sso import streamlit_google_sso

# Import email processing dashboard
try:
    from email_processing_dashboard import (
        render_email_processing_dashboard, 
        render_email_activity_sidebar
    )
except ImportError as e:
    logger.error(f"Failed to import email processing dashboard: {e}")
    # Fallback functions
    def render_email_processing_dashboard(brokerage_key: str):
        st.info("📧 Email processing dashboard not available")
    
    def render_email_activity_sidebar(brokerage_key: str):
        pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def authenticate_user(password):
    """Authenticate user with password"""
    try:
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
        page_title="Unified Load Processing - Login",
        page_icon="🔐",
        layout="wide"
    )
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1>🔐 Unified Load Processing Access</h1>
                <p style="color: #666; font-size: 1.1rem;">Enter your team password to continue</p>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter team password"
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


def render_processing_mode_selection():
    """Render processing mode selection interface"""
    st.subheader("🎯 Processing Mode")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        manual_selected = st.button(
            "🔧 Manual Processing",
            help="Full control with step-by-step field mapping and validation",
            use_container_width=True,
            type="primary" if st.session_state.get('processing_mode') == 'manual' else "secondary"
        )
        if manual_selected:
            st.session_state.processing_mode = 'manual'
    
    with col2:
        endtoend_selected = st.button(
            "⚡ End-to-End Pipeline", 
            help="Complete workflow with load mapping, enrichment, and postback",
            use_container_width=True,
            type="primary" if st.session_state.get('processing_mode') == 'endtoend' else "secondary"
        )
        if endtoend_selected:
            st.session_state.processing_mode = 'endtoend'
    
    with col3:
        automated_selected = st.button(
            "🤖 Automated Processing",
            help="Single-click processing using saved configuration",
            use_container_width=True,
            type="primary" if st.session_state.get('processing_mode') == 'automated' else "secondary"
        )
        if automated_selected:
            st.session_state.processing_mode = 'automated'
    
    # Set default mode
    if 'processing_mode' not in st.session_state:
        st.session_state.processing_mode = 'manual'
    
    # Show mode description
    mode = st.session_state.processing_mode
    try:
        mode_config = UnifiedLoadProcessor.PROCESSING_MODES.get(mode)
        if mode_config:
            st.info(f"**{mode_config.name}**: {mode_config.description}")
    except Exception as e:
        logger.error(f"Error getting mode config: {e}")
        st.info(f"Selected mode: {mode}")


def render_enhanced_sidebar(processor: Optional[UnifiedLoadProcessor], db_manager: DatabaseManager):
    """Render enhanced sidebar with processing options"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Brokerage selection with creation capability
        brokerage_key = render_brokerage_selection(db_manager)
        
        if not brokerage_key:
            st.warning("⚠️ Please select or create a brokerage to continue")
            return
        
        # Update session state when brokerage changes
        if st.session_state.get('brokerage_name') != brokerage_key:
            st.session_state.brokerage_name = brokerage_key
        
        st.session_state.selected_brokerage = brokerage_key
        
        # Configuration management
        st.markdown("---")
        st.subheader("💾 Saved Configurations")
        
        # Get configurations using correct method  
        try:
            saved_configs = db_manager.get_brokerage_configurations(brokerage_key)
        except Exception as e:
            logger.error(f"Error getting configurations: {e}")
            saved_configs = []
        
        if saved_configs:
            config_options = [cfg['name'] for cfg in saved_configs]
            selected_config_name = st.selectbox(
                "Select Configuration:",
                ["-- Choose a configuration --"] + config_options + ["➕ Create New"],
                key="config_selection"
            )
            
            if selected_config_name and selected_config_name not in ["-- Choose a configuration --", "➕ Create New"]:
                selected_config = next(cfg for cfg in saved_configs if cfg['name'] == selected_config_name)
                st.session_state.selected_config = selected_config
                
                # Update last used
                try:
                    db_manager.update_configuration_last_used(brokerage_key, selected_config['name'])
                except Exception as e:
                    logger.error(f"Error updating last used: {e}")
                    
            elif selected_config_name == "➕ Create New":
                st.session_state.show_config_form = True
        else:
            st.info("💡 Create your first configuration")
            if st.button("➕ Create Configuration", use_container_width=True):
                st.session_state.show_config_form = True
        
        # Show configuration creation form
        if st.session_state.get('show_config_form'):
            render_configuration_form(brokerage_key, db_manager)
        
        # Mode-specific options (only show if we have a selected config)
        mode_config = None
        if st.session_state.get('selected_config'):
            if processor:
                mode_config = processor.get_processing_mode_config()
            else:
                # Fallback if processor is not available
                processing_mode = st.session_state.get('processing_mode', 'manual')
                mode_config = UnifiedLoadProcessor.PROCESSING_MODES.get(processing_mode, UnifiedLoadProcessor.PROCESSING_MODES['manual'])
        
        if mode_config and mode_config.show_enrichment:
            st.markdown("---")
            st.subheader("🔍 Enrichment Options")
            
            enable_tracking = st.checkbox(
                "Add tracking data",
                value=True,
                help="Enrich with carrier tracking information"
            )
            
            enable_snowflake = st.checkbox(
                "Add warehouse data",
                value=False,
                help="Enrich with Snowflake data warehouse information"
            )
            
            st.session_state.enrichment_config = {
                'tracking_enabled': enable_tracking,
                'snowflake_enabled': enable_snowflake
            }
        
        if mode_config and mode_config.show_postback:
            st.markdown("---")
            st.subheader("📤 Output & Delivery")
            
            output_format = st.selectbox(
                "Output Format:",
                ["CSV", "Excel", "JSON", "XML"],
                index=0
            )
            
            send_email = st.checkbox("Email results")
            
            if send_email:
                email_recipient = st.text_input(
                    "Email address:",
                    placeholder="ops@company.com"
                )
            else:
                email_recipient = None
            
            st.session_state.postback_config = {
                'output_format': output_format,
                'send_email': send_email,
                'email_recipient': email_recipient
            }
        
        # Email automation (for end-to-end mode)
        if processor and processor.processing_mode == 'endtoend':
            st.markdown("---")
            st.subheader("📧 Email Automation")
            
            cred_status = credential_manager.validate_credentials(brokerage_key)
            
            if cred_status.email_automation_available:
                if cred_status.email_automation_active:
                    st.success("📧 Active - Processing emails automatically")
                    if st.button("Stop Email Automation"):
                        # Stop email automation logic here
                        st.info("Email automation stopped")
                else:
                    st.info("📧 Available - Ready to start")
                    if st.button("Start Email Automation"):
                        # Start email automation logic here
                        st.success("Email automation started")
            else:
                with st.expander("Setup Email Automation"):
                    auth_result = streamlit_google_sso.render_google_auth_button(
                        brokerage_key=brokerage_key,
                        button_text="🔐 Connect Gmail Account"
                    )
                    
                    if auth_result.get('authenticated'):
                        st.success("✅ Gmail connected successfully!")


def render_brokerage_selection(db_manager: DatabaseManager) -> Optional[str]:
    """Render brokerage selection with creation capability - matches original FF2API UX."""
    st.subheader("🏢 Brokerage")
    
    # Display success message if brokerage was just created
    if 'brokerage_creation_success' in st.session_state:
        st.success(st.session_state.brokerage_creation_success)
        del st.session_state.brokerage_creation_success
    
    # Get existing brokerages from database
    try:
        import sqlite3
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT brokerage_name FROM brokerage_configurations WHERE is_active = 1 ORDER BY brokerage_name')
        existing_brokerages = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        logger.error(f"Error getting brokerages: {e}")
        existing_brokerages = []
    
    # Add current session brokerage to options if not in database yet
    current_brokerage = st.session_state.get('brokerage_name', '')
    if current_brokerage and current_brokerage not in existing_brokerages:
        existing_brokerages.append(current_brokerage)
    
    if existing_brokerages:
        # Show selection with option to create new
        current_brokerage = st.session_state.get('brokerage_name', '')
        if current_brokerage in existing_brokerages:
            default_index = existing_brokerages.index(current_brokerage) + 1
        else:
            default_index = 0
            
        selected_brokerage = st.selectbox(
            "Select Brokerage:",
            ["-- Choose a brokerage --"] + existing_brokerages + ["➕ Create New"],
            index=default_index,
            key="brokerage_selection"
        )
        
        # Handle selection changes
        if selected_brokerage == "➕ Create New":
            st.session_state.show_brokerage_form = True
        elif selected_brokerage != "-- Choose a brokerage --":
            # Check if selection changed
            if current_brokerage != selected_brokerage:
                # Clear existing configuration when changing brokerage
                keys_to_clear = ['selected_config', 'api_credentials']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
            return selected_brokerage
        else:
            return None
    else:
        # No brokerages exist - direct input like original FF2API
        new_brokerage = st.text_input(
            "Enter brokerage name:",
            value=st.session_state.get('brokerage_name', ''),
            placeholder="Your brokerage name",
            help="Enter the name of your brokerage company"
        )
        if new_brokerage.strip():
            return new_brokerage.strip()
        else:
            return None
    
    # Show brokerage creation form if requested
    if st.session_state.get('show_brokerage_form'):
        created_brokerage = render_brokerage_form()
        if created_brokerage:
            return created_brokerage
    
    return None


def render_brokerage_form() -> Optional[str]:
    """Render brokerage creation form - matches original FF2API pattern."""
    st.markdown("---")
    st.subheader("➕ New Brokerage")
    
    new_brokerage = st.text_input(
        "Brokerage Name:",
        placeholder="Enter your brokerage name",
        help="This will be used to organize your configurations"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("✅ Create", type="primary", use_container_width=True):
            if new_brokerage.strip():
                # Store in session state like original FF2API
                st.session_state.brokerage_name = new_brokerage.strip()
                st.session_state.show_brokerage_form = False
                st.session_state.brokerage_creation_success = f"✅ Created: {new_brokerage.strip()}"
                
                # Clear any existing configuration when creating new brokerage
                keys_to_clear = ['selected_config', 'api_credentials']
                for key in keys_to_clear:
                    if key in st.session_state:
                        del st.session_state[key]
                
                st.rerun()
                return new_brokerage.strip()
            else:
                st.error("Please enter a brokerage name")
                return None
    
    with col2:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_brokerage_form = False
            st.rerun()
    
    return None


def render_configuration_form(brokerage_key: str, db_manager: DatabaseManager):
    """Render configuration creation/editing form."""
    st.markdown("---")
    st.subheader("⚙️ New Configuration")
    
    # Configuration form state
    if 'config_form_state' not in st.session_state:
        st.session_state.config_form_state = {
            'config_name': '',
            'config_description': '',
            'api_base_url': 'https://api.prod.goaugment.com',
            'api_key': '',
            'auth_type': 'api_key',
            'bearer_token': ''
        }
    
    # Configuration name
    config_name = st.text_input(
        "Configuration Name",
        value=st.session_state.config_form_state['config_name'],
        placeholder="e.g., Standard Mapping",
        help="Give this configuration a descriptive name"
    )
    
    # Optional description
    config_description = st.text_area(
        "Description (Optional)",
        value=st.session_state.config_form_state['config_description'],
        placeholder="Describe this configuration...",
        height=60
    )
    
    # Authentication type
    auth_type = st.selectbox(
        "Authentication Type",
        options=['api_key', 'bearer_token'],
        index=0 if st.session_state.config_form_state['auth_type'] == 'api_key' else 1,
        format_func=lambda x: "API Key (with token refresh)" if x == 'api_key' else "Bearer Token (direct)",
        help="Choose authentication method - API Key automatically refreshes tokens, Bearer Token uses your token directly"
    )
    
    # API Base URL
    api_base_url = st.text_input(
        "API Base URL",
        value=st.session_state.config_form_state['api_base_url'],
        placeholder="https://api.prod.goaugment.com",
        help="The base URL for your FF2API endpoint"
    )
    
    # Conditional authentication fields
    api_key = ""
    bearer_token = ""
    
    if auth_type == 'api_key':
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config_form_state['api_key'],
            type="password",
            placeholder="Your API key (used to refresh bearer tokens)",
            help="Your API key for authentication"
        )
    else:  # bearer_token
        bearer_token = st.text_input(
            "Bearer Token",
            value=st.session_state.config_form_state['bearer_token'],
            type="password",
            placeholder="Your bearer token (used directly for API calls)",
            help="Your bearer token for direct API authentication"
        )
    
    # Update form state
    st.session_state.config_form_state.update({
        'config_name': config_name,
        'config_description': config_description,
        'api_base_url': api_base_url,
        'auth_type': auth_type,
        'api_key': api_key,
        'bearer_token': bearer_token
    })
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🧪 Test Connection", use_container_width=True):
            test_api_connection(api_base_url, auth_type, api_key, bearer_token)
    
    with col2:
        if st.button("💾 Save Configuration", type="primary", use_container_width=True):
            save_configuration(brokerage_key, db_manager)
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_config_form = False
            st.rerun()


def test_api_connection(api_base_url: str, auth_type: str, api_key: str, bearer_token: str):
    """Test API connection with provided credentials."""
    # Validate required fields
    if auth_type == 'api_key' and not api_key:
        st.error("Please provide an API key for API key authentication")
        return
    elif auth_type == 'bearer_token' and not bearer_token:
        st.error("Please provide a bearer token for bearer token authentication")
        return
    
    if not api_base_url:
        st.error("Please provide an API base URL")
        return
    
    # Test connection
    with st.spinner("Testing API connection..."):
        try:
            if auth_type == 'api_key':
                client = LoadsAPIClient(api_base_url, api_key=api_key, auth_type='api_key')
            else:  # bearer_token
                client = LoadsAPIClient(api_base_url, bearer_token=bearer_token, auth_type='bearer_token')
            
            result = client.validate_connection()
            
            if result.get('success'):
                st.success("✅ API connection successful!")
                st.session_state.connection_tested = True
            else:
                st.error(f"❌ API connection failed: {result.get('message', 'Unknown error')}")
                st.session_state.connection_tested = False
                
        except Exception as e:
            st.error(f"❌ Connection test failed: {str(e)}")
            st.session_state.connection_tested = False


def save_configuration(brokerage_key: str, db_manager: DatabaseManager):
    """Save the configuration to database."""
    form_state = st.session_state.config_form_state
    
    config_name = form_state['config_name'].strip()
    config_description = form_state['config_description'].strip()
    api_base_url = form_state['api_base_url'].strip()
    auth_type = form_state['auth_type']
    api_key = form_state['api_key'].strip()
    bearer_token = form_state['bearer_token'].strip()
    
    # Validate required fields
    if not config_name:
        st.error("Please provide a configuration name")
        return
    
    if auth_type == 'api_key' and not api_key:
        st.error("Please provide an API key for API key authentication")
        return
    elif auth_type == 'bearer_token' and not bearer_token:
        st.error("Please provide a bearer token for bearer token authentication")
        return
    
    if not api_base_url:
        st.error("Please provide an API base URL")
        return
    
    # Build API credentials
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
    
    # Save to database
    try:
        # Start with placeholder field mappings
        placeholder_mappings = {
            "_status": "pending_file_upload",
            "_created_at": str(datetime.now()),
            "_description": "Configuration created, awaiting file upload for field mapping"
        }
        
        config_id = db_manager.save_brokerage_configuration(
            brokerage_name=brokerage_key,
            configuration_name=config_name,
            field_mappings=placeholder_mappings,
            api_credentials=api_credentials,
            file_headers=None,
            description=config_description,
            auth_type=auth_type,
            bearer_token=save_bearer_token,
            processing_mode='manual'
        )
        
        st.success(f"✅ Configuration '{config_name}' saved successfully!")
        
        # Clear form and close
        st.session_state.config_form_state = {
            'config_name': '',
            'config_description': '',
            'api_base_url': 'https://api.prod.goaugment.com',
            'api_key': '',
            'auth_type': 'api_key',
            'bearer_token': ''
        }
        st.session_state.show_config_form = False
        
        # Set the new configuration as selected
        saved_config = {
            'id': config_id,
            'name': config_name,
            'description': config_description,
            'api_credentials': api_credentials,
            'auth_type': auth_type,
            'bearer_token': save_bearer_token,
            'field_mappings': placeholder_mappings
        }
        st.session_state.selected_config = saved_config
        
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Failed to save configuration: {str(e)}")
        logger.error(f"Configuration save error: {e}")


def render_field_mapping_interface(df: pd.DataFrame, processor: UnifiedLoadProcessor, saved_mappings: Dict[str, str] = None):
    """Render field mapping interface"""
    st.subheader("🔗 Field Mapping")
    
    # Get suggested mappings or use saved mappings
    csv_columns = df.columns.tolist()
    if saved_mappings:
        # Filter saved mappings to only include fields that exist in current CSV
        current_mapping = {col: saved_mappings.get(col, "") for col in csv_columns if col in saved_mappings}
        suggested_mapping = current_mapping
    else:
        suggested_mapping = processor.get_suggested_field_mapping(csv_columns)
    
    # Get API schema for validation
    api_schema = get_full_api_schema()
    api_fields = list(api_schema.keys())
    
    # Create mapping interface
    field_mapping = {}
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**CSV Columns**")
        if saved_mappings:
            st.info("ℹ️ Using saved field mappings - modify as needed")
        
        for csv_col in csv_columns:
            suggested_api_field = suggested_mapping.get(csv_col, "")
            
            # Find index of suggested field
            try:
                default_index = api_fields.index(suggested_api_field) if suggested_api_field in api_fields else 0
            except (ValueError, IndexError):
                default_index = 0
            
            mapped_field = st.selectbox(
                f"Map '{csv_col}' to:",
                [""] + api_fields,
                index=default_index + 1 if suggested_api_field else 0,
                key=f"mapping_{csv_col}",
                help=f"Current mapping: {suggested_api_field}" if suggested_api_field else "No mapping selected"
            )
            
            if mapped_field:
                field_mapping[csv_col] = mapped_field
    
    with col2:
        st.markdown("**Mapping Preview**")
        if field_mapping:
            mapping_df = pd.DataFrame([
                {"CSV Column": csv_col, "API Field": api_field}
                for csv_col, api_field in field_mapping.items()
            ])
            st.dataframe(mapping_df, use_container_width=True)
            
            # Show mapping validation
            required_fields = ['loadNumber', 'mode', 'equipment', 'route']
            mapped_required = [field for field in required_fields if field in field_mapping.values()]
            missing_required = [field for field in required_fields if field not in field_mapping.values()]
            
            if missing_required:
                st.warning(f"⚠️ Missing required fields: {', '.join(missing_required)}")
            else:
                st.success("✅ All required fields mapped")
        else:
            st.info("Configure field mappings on the left")
    
    return field_mapping


def render_processing_results(result):
    """Render processing results"""
    st.subheader("📊 Processing Results")
    
    # Summary metrics
    if result.summary:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Rows", result.summary.get('total_rows', 0))
        
        with col2:
            ff2api_success = result.summary.get('ff2api_success', 0)
            total_rows = result.summary.get('total_rows', 0)
            st.metric("FF2API Success", f"{ff2api_success}/{total_rows}")
        
        with col3:
            if result.load_id_mappings:
                load_ids = result.summary.get('load_ids_retrieved', 0)
                st.metric("Load IDs Retrieved", load_ids)
        
        with col4:
            if result.enriched_data:
                enriched = result.summary.get('rows_enriched', 0)
                st.metric("Rows Enriched", enriched)
    
    # Error display
    if result.errors:
        st.error("Processing Errors:")
        for error in result.errors[:5]:  # Show first 5 errors
            st.error(error)
    
    # Download results
    if result.enriched_data:
        st.subheader("📥 Download Results")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Download CSV", use_container_width=True):
                csv_data = pd.DataFrame(result.enriched_data).to_csv(index=False)
                st.download_button(
                    "📄 Download CSV", 
                    csv_data, 
                    "unified_results.csv", 
                    "text/csv",
                    use_container_width=True
                )
        
        with col2:
            if st.button("Download Excel", use_container_width=True):
                # Excel download logic here
                st.info("Excel download functionality")
        
        with col3:
            if st.button("Download JSON", use_container_width=True):
                json_data = json.dumps(result.enriched_data, indent=2, default=str)
                st.download_button(
                    "📄 Download JSON", 
                    json_data, 
                    "unified_results.json", 
                    "application/json",
                    use_container_width=True
                )


def main():
    """Main unified application"""
    # Check authentication
    if not st.session_state.get('authenticated', False):
        show_login_page()
        return
    
    # Set page config
    st.set_page_config(
        page_title="Unified Load Processing",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load custom CSS
    load_custom_css()
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Render main header
    st.title("⚡ Unified Load Processing")
    st.markdown("Complete freight load processing with field mapping, enrichment, and delivery")
    
    # Processing mode selection
    render_processing_mode_selection()
    
    # Initialize database manager for sidebar
    db_manager = DatabaseManager()
    
    # Always render sidebar first (progressive disclosure happens inside)
    render_enhanced_sidebar(None, db_manager)
    
    # Check if brokerage is configured (progressive disclosure like original FF2API)
    if 'brokerage_name' not in st.session_state:
        st.info("👈 Please select or create a brokerage in the sidebar to continue")
        return
    
    if 'selected_config' not in st.session_state:
        st.info("👈 Please configure your API credentials in the sidebar to continue")
        return
    
    # Get processing mode and brokerage
    processing_mode = st.session_state.get('processing_mode', 'manual')
    brokerage_key = st.session_state.get('brokerage_name')
    
    # Initialize processor - use try/catch for better error handling
    processor = None
    try:
        # Build configuration
        config = {
            'brokerage_key': brokerage_key,
            'api_timeout': 30,
            'retry_count': 3,
            'enrichment': {'sources': []},
            'postback': {'handlers': []}
        }
        
        # Add enrichment config if enabled
        if processing_mode == 'endtoend':
            enrichment_config = st.session_state.get('enrichment_config', {})
            if enrichment_config.get('tracking_enabled'):
                config['enrichment']['sources'].append({
                    'type': 'tracking_api',
                    'pro_column': 'PRO',
                    'carrier_column': 'carrier'
                })
            
            if enrichment_config.get('snowflake_enabled'):
                config['enrichment']['sources'].append({
                    'type': 'snowflake_augment',
                    'database': 'AUGMENT_DW',
                    'schema': 'MARTS',
                    'enrichments': ['tracking', 'customer'],
                    'use_load_ids': True
                })
            
            # Add postback config
            postback_config = st.session_state.get('postback_config', {})
            if postback_config:
                output_format = postback_config.get('output_format', 'CSV').lower()
                config['postback']['handlers'].append({
                    'type': output_format,
                    'output_path': f'/tmp/unified_results.{output_format}'
                })
                
                if postback_config.get('send_email') and postback_config.get('email_recipient'):
                    config['postback']['handlers'].append({
                        'type': 'email',
                        'recipient': postback_config['email_recipient'],
                        'subject': 'Unified Load Processing Results'
                    })
        
        # Initialize processor
        processor = UnifiedLoadProcessor(config, processing_mode)
        
    except Exception as e:
        logger.error(f"Error initializing processor: {e}")
        st.error(f"Configuration Error: {str(e)}")
        st.info("Please check your credentials and try again.")
        return
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Email processing dashboard (show if there's email automation activity)
        try:
            # Check both shared storage and session state for email processing activity
            email_jobs_exist = False
            
            # First check shared storage
            try:
                from shared_storage_bridge import shared_storage
                active_jobs = shared_storage.get_active_jobs(brokerage_key)
                completed_jobs = shared_storage.get_completed_jobs(brokerage_key)
                recent_activity = shared_storage.has_recent_activity(brokerage_key, minutes=60)
                
                if active_jobs or completed_jobs or recent_activity:
                    email_jobs_exist = True
                    logger.info(f"Found email processing activity for {brokerage_key} in shared storage")
            except ImportError:
                logger.debug("Shared storage not available, checking session state")
            
            # Fallback to session state check
            if not email_jobs_exist:
                email_jobs_exist = (
                    'email_processing_jobs' in st.session_state and 
                    st.session_state.email_processing_jobs.get(brokerage_key, [])
                )
                if email_jobs_exist:
                    logger.info(f"Found email processing activity for {brokerage_key} in session state")
            
            # Also check email processing metadata (from logs)
            if not email_jobs_exist:
                email_jobs_exist = (
                    'email_processing_metadata' in st.session_state and
                    len([item for item in st.session_state.email_processing_metadata 
                         if item.get('brokerage_key') == brokerage_key]) > 0
                )
                if email_jobs_exist:
                    logger.info(f"Found email processing metadata for {brokerage_key} in session state")
            
            # Always show the dashboard section - it will display appropriate content
            if email_jobs_exist or True:  # Show dashboard even if no activity to display status
                render_email_processing_dashboard(brokerage_key)
                st.markdown("---")
                
        except Exception as e:
            logger.error(f"Error checking email processing jobs: {e}")
            # Show dashboard anyway in case of errors
            try:
                render_email_processing_dashboard(brokerage_key)
                st.markdown("---")
            except:
                pass
        
        # File upload section
        st.subheader("📁 File Upload")
        
        uploaded_file = st.file_uploader(
            "Choose CSV or Excel file",
            type=["csv", "xlsx", "xls"],
            help="Upload freight data file for processing"
        )
        
        if uploaded_file is not None:
            # Load and validate file
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
                
                st.success(f"File loaded: {len(df)} rows, {len(df.columns)} columns")
                
                # Data preview
                with st.expander("📊 Data Preview", expanded=True):
                    st.dataframe(df.head(10), use_container_width=True)
                
                # Field mapping (if not automated mode)
                if processor and not processor.mode_config.auto_process:
                    # Check if we have a selected configuration with field mappings
                    selected_config = st.session_state.get('selected_config')
                    if selected_config and selected_config.get('field_mappings'):
                        saved_mappings = selected_config['field_mappings']
                        if saved_mappings.get('_status') != 'pending_file_upload':
                            # Use saved mappings as default
                            field_mapping = render_field_mapping_interface(df, processor, saved_mappings)
                        else:
                            # Configuration exists but needs field mapping
                            field_mapping = render_field_mapping_interface(df, processor)
                            # Update the configuration with new field mappings if they've changed
                            if field_mapping and field_mapping != saved_mappings:
                                try:
                                    updated_config = selected_config.copy()
                                    updated_config['field_mappings'] = field_mapping
                                    st.session_state.selected_config = updated_config
                                    # Save the updated mapping to database
                                    db_manager.save_brokerage_configuration(
                                        brokerage_name=brokerage_key,
                                        configuration_name=selected_config['name'],
                                        field_mappings=field_mapping,
                                        api_credentials=selected_config['api_credentials'],
                                        description=selected_config.get('description', ''),
                                        auth_type=selected_config.get('auth_type', 'api_key'),
                                        bearer_token=selected_config.get('bearer_token'),
                                        processing_mode=processing_mode
                                    )
                                except Exception as e:
                                    logger.error(f"Error updating field mappings: {e}")
                    else:
                        field_mapping = render_field_mapping_interface(df, processor)
                elif processor:
                    # Use saved configuration or auto-detect
                    selected_config = st.session_state.get('selected_config')
                    if selected_config and selected_config.get('field_mappings'):
                        field_mapping = selected_config['field_mappings']
                    else:
                        field_mapping = processor.get_suggested_field_mapping(df.columns.tolist())
                else:
                    st.error("Processor not available - please refresh the page")
                    return
                
                # Processing section
                st.markdown("---")
                st.subheader("⚙️ Processing")
                
                if field_mapping:
                    # Get API credentials from selected configuration
                    selected_config = st.session_state.get('selected_config')
                    if selected_config and selected_config.get('api_credentials'):
                        api_config = {
                            'base_url': selected_config['api_credentials'].get('base_url', 'https://api.prod.goaugment.com'),
                            'auth_type': selected_config.get('auth_type', 'api_key')
                        }
                        
                        # Add appropriate credential based on auth type
                        if selected_config.get('auth_type') == 'bearer_token':
                            api_config['bearer_token'] = selected_config.get('bearer_token')
                        else:
                            api_config['api_key'] = selected_config['api_credentials'].get('api_key')
                        
                        if st.button("🚀 Process Data", type="primary", use_container_width=True):
                            with st.spinner("Processing data..."):
                                # Execute unified workflow
                                result = processor.process_unified_workflow(df, field_mapping, api_config)
                                
                                # Store result in session state
                                st.session_state.processing_result = result
                                
                                # Display results
                                render_processing_results(result)
                    else:
                        st.error("❌ No configuration selected with API credentials")
                        st.info("Please select a configuration or create a new one with API credentials")
                else:
                    st.warning("⚠️ Please configure field mappings to proceed")
                
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")
    
    with col2:
        # Status and information panel
        st.subheader("ℹ️ Status")
        
        # Credential status
        cred_status = credential_manager.validate_credentials(brokerage_key)
        
        if cred_status.api_available:
            st.success("✅ API credentials configured")
        else:
            st.error("❌ API credentials missing")
        
        if processor and processor.mode_config.show_enrichment:
            if cred_status.tracking_api_available:
                st.success("✅ Tracking API available")
            else:
                st.warning("⚠️ Tracking API not configured")
        
        if processor and processor.mode_config.show_postback:
            if cred_status.email_available:
                st.success("✅ Email delivery available")
            else:
                st.warning("⚠️ Email delivery not configured")
        
        # Email automation activity sidebar
        render_email_activity_sidebar(brokerage_key)
        
        # Processing statistics
        if 'processing_result' in st.session_state:
            result = st.session_state.processing_result
            st.markdown("---")
            st.subheader("📈 Latest Results")
            st.metric("Success Rate", f"{result.summary.get('ff2api_success', 0)}/{result.summary.get('total_rows', 0)}")
            
            if result.errors:
                st.error(f"{len(result.errors)} errors occurred")
        
        # Background Email Service Controls
        st.markdown("---")
        st.subheader("🔄 Background Service")
        try:
            from background_service_manager import background_service_manager
            # Show compact service status
            status = background_service_manager.get_service_status()
            if status['service_running']:
                st.success(f"🟢 Service Running ({status['active_configurations_count']} configs)")
            else:
                st.warning("🟡 Service Stopped")
            
            # Quick service controls
            col1, col2 = st.columns(2)
            with col1:
                if st.button("▶️ Start", disabled=status['service_running'], use_container_width=True):
                    background_service_manager.start_service()
                    st.rerun()
            with col2:
                if st.button("⏹️ Stop", disabled=not status['service_running'], use_container_width=True):
                    background_service_manager.stop_service()
                    st.rerun()
        except ImportError:
            st.info("🔄 Background Service Not Available")
        
        # Email automation processing history (additional display)
        if 'email_processing_metadata' in st.session_state:
            email_files = st.session_state.email_processing_metadata
            if email_files:
                st.markdown("---")
                st.subheader("📧 Recent Email Processing")
                recent_files = sorted(email_files, key=lambda x: x['processed_time'], reverse=True)[:2]
                
                for file_info in recent_files:
                    with st.expander(f"📄 {file_info['filename']}"):
                        st.write(f"**Time:** {file_info['processed_time'].strftime('%H:%M')}")
                        st.write(f"**Source:** {file_info.get('email_source', 'Unknown')}")
                        st.write(f"**Records:** {file_info.get('record_count', 0)}")
                        st.write(f"**Automated:** ✅" if file_info.get('was_email_automated') else "Manual")
    
    # Logout option
    with st.sidebar:
        st.markdown("---")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()


if __name__ == "__main__":
    main()