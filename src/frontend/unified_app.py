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
            correct_password = "admin123"
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
    mode_config = UnifiedLoadProcessor.PROCESSING_MODES.get(mode)
    if mode_config:
        st.info(f"**{mode_config.name}**: {mode_config.description}")


def render_enhanced_sidebar(processor: UnifiedLoadProcessor, db_manager: DatabaseManager):
    """Render enhanced sidebar with processing options"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Brokerage selection
        available_brokerages = credential_manager.get_available_brokerages()
        if available_brokerages:
            brokerage_key = st.selectbox(
                "🏢 Brokerage", 
                available_brokerages, 
                index=0,
                key="brokerage_selection"
            )
        else:
            brokerage_key = st.text_input(
                "🏢 Brokerage Key", 
                value="augment-brokerage",
                key="brokerage_input"
            )
            st.warning("⚠️ No configured brokerages found")
        
        st.session_state.selected_brokerage = brokerage_key
        
        # Configuration management
        st.markdown("---")
        st.subheader("💾 Saved Configurations")
        
        saved_configs = db_manager.get_configurations_for_company(brokerage_key)
        if saved_configs:
            config_names = [f"{cfg[2]} (v{cfg[7]})" for cfg in saved_configs]
            selected_config = st.selectbox(
                "Select Configuration:",
                ["None"] + config_names,
                key="config_selection"
            )
            
            if selected_config != "None":
                st.session_state.selected_config = selected_config
        else:
            st.info("No saved configurations found")
        
        # Mode-specific options
        mode_config = processor.get_processing_mode_config()
        
        if mode_config.show_enrichment:
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
        
        if mode_config.show_postback:
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
        if processor.processing_mode == 'endtoend':
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


def render_field_mapping_interface(df: pd.DataFrame, processor: UnifiedLoadProcessor):
    """Render field mapping interface"""
    st.subheader("🔗 Field Mapping")
    
    # Get suggested mappings
    csv_columns = df.columns.tolist()
    suggested_mapping = processor.get_suggested_field_mapping(csv_columns)
    
    # Get API schema for validation
    api_schema = get_full_api_schema()
    api_fields = list(api_schema.keys())
    
    # Create mapping interface
    field_mapping = {}
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**CSV Columns**")
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
                key=f"mapping_{csv_col}"
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
    
    # Initialize processor based on selected mode
    processing_mode = st.session_state.get('processing_mode', 'manual')
    brokerage_key = st.session_state.get('selected_brokerage', 'augment-brokerage')
    
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
    
    # Render sidebar
    render_enhanced_sidebar(processor, db_manager)
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
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
                if not processor.mode_config.auto_process:
                    field_mapping = render_field_mapping_interface(df, processor)
                else:
                    # Use saved configuration or auto-detect
                    field_mapping = processor.get_suggested_field_mapping(df.columns.tolist())
                
                # Processing section
                st.markdown("---")
                st.subheader("⚙️ Processing")
                
                if field_mapping:
                    # Get API credentials
                    brokerage_creds = credential_manager.get_brokerage_api_key(brokerage_key)
                    if brokerage_creds:
                        api_config = {
                            'base_url': 'https://load.prod.goaugment.com/unstable/loads',
                            'api_key': brokerage_creds
                        }
                        
                        if st.button("🚀 Process Data", type="primary", use_container_width=True):
                            with st.spinner("Processing data..."):
                                # Execute unified workflow
                                result = processor.process_unified_workflow(df, field_mapping, api_config)
                                
                                # Store result in session state
                                st.session_state.processing_result = result
                                
                                # Display results
                                render_processing_results(result)
                    else:
                        st.error("❌ API credentials not configured for this brokerage")
                        st.info("Please configure API credentials in the credential manager")
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
        
        if processor.mode_config.show_enrichment:
            if cred_status.tracking_api_available:
                st.success("✅ Tracking API available")
            else:
                st.warning("⚠️ Tracking API not configured")
        
        if processor.mode_config.show_postback:
            if cred_status.email_available:
                st.success("✅ Email delivery available")
            else:
                st.warning("⚠️ Email delivery not configured")
        
        # Processing statistics
        if 'processing_result' in st.session_state:
            result = st.session_state.processing_result
            st.markdown("---")
            st.subheader("📈 Latest Results")
            st.metric("Success Rate", f"{result.summary.get('ff2api_success', 0)}/{result.summary.get('total_rows', 0)}")
            
            if result.errors:
                st.error(f"{len(result.errors)} errors occurred")
    
    # Logout option
    with st.sidebar:
        st.markdown("---")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()


if __name__ == "__main__":
    main()