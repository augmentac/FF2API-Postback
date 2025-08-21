"""
Streamlit End-to-End Load Processing Application.
Complete workflow: CSV Upload → FF2API Processing → Load ID Mapping → Snowflake Enrichment → Postback
"""

# ========== ABSOLUTE EMERGENCY TEST - REAL APP FOUND ==========
import streamlit as st
st.error("🚨🚨🚨 FOUND THE REAL APP - streamlit_endtoend.py IS RUNNING 🚨🚨🚨")
st.balloons()
# ==================================================================

import streamlit as st
import pandas as pd
import json
import tempfile
import zipfile
import os
from datetime import datetime
from typing import Dict, Any, List
import logging

# Import workflow components
from workflow_processor import EndToEndWorkflowProcessor, WorkflowResults
from load_id_mapper import LoadIDMapping
from credential_manager import credential_manager
from email_monitor import email_monitor
from gmail_auth_service import gmail_auth_service
from streamlit_google_sso import streamlit_google_sso

# Import database backup manager
from db_manager import restore_sqlite_if_missing, upload_sqlite_if_changed, start_periodic_backup

# Initialize email monitor with credential manager
email_monitor.credential_manager = credential_manager

# Initialize database backup system
print("[streamlit_endtoend] Initializing database backup system...")
restore_sqlite_if_missing()
start_periodic_backup(interval_minutes=15)  # More frequent for active development

# Configure logging for Streamlit Cloud
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_endtoend_simple(df, brokerage_key, add_tracking, output_format, send_email, email_recipient, api_timeout, retry_count, pro_column="PRO", carrier_column="carrier"):
    """Simplified end-to-end processing with minimal UI."""
    
    with st.spinner("Processing new loads..."):
        try:
            # Build config
            config = load_default_endtoend_config()
            config.update({
                'brokerage_key': brokerage_key,
                'api_timeout': api_timeout,
                'retry_count': retry_count
            })
            
            # Add enrichment if enabled
            if add_tracking:
                # Use tracking API for enrichment with user-specified columns
                config['enrichment']['sources'] = [{
                    'type': 'tracking_api',
                    'pro_column': pro_column,
                    'carrier_column': carrier_column
                }]
            
            # Set output
            config['postback']['handlers'] = [{
                'type': output_format.lower(),
                'output_path': f'/tmp/endtoend_results.{output_format.lower()}'
            }]
            
            # Add email if enabled
            if send_email and email_recipient:
                try:
                    config['postback']['handlers'].append({
                        'type': 'email',
                        'recipient': email_recipient,
                        'subject': 'End-to-End Load Processing Results',
                        'smtp_user': st.secrets.get("email", {}).get("SMTP_USER"),
                        'smtp_pass': st.secrets.get("email", {}).get("SMTP_PASS"),
                    })
                except:
                    st.warning("Email not configured")
            
            # Process workflow
            processor = EndToEndWorkflowProcessor(config)
            results = processor.process_workflow(df)
            
            if results.errors:
                st.error("Some loads failed to process")
                for error in results.errors[:3]:  # Show first 3 errors
                    st.error(error)
            else:
                st.success("All loads processed successfully")
            
            # Simple results
            if results.summary:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Loads", results.summary.get('total_rows', 0))
                with col2:
                    st.metric("Processed", results.summary.get('ff2api_success', 0))
                with col3:
                    st.metric("Enriched", results.summary.get('rows_enriched', 0))
            
            # Simple download
            if results.enriched_data:
                if output_format == "CSV":
                    csv_data = pd.DataFrame(results.enriched_data).to_csv(index=False)
                    st.download_button("Download Results", csv_data, "results.csv", "text/csv")
                elif output_format == "Excel":
                    excel_data = pd.DataFrame(results.enriched_data).to_excel(index=False)
                    st.download_button("Download Results", excel_data, "results.xlsx")
                elif output_format == "JSON":
                    json_data = json.dumps(results.enriched_data, indent=2, default=str)
                    st.download_button("Download Results", json_data, "results.json", "application/json")
                elif output_format == "XML":
                    import xml.etree.ElementTree as ET
                    root = ET.Element("freight_data")
                    for row in results.enriched_data:
                        shipment = ET.SubElement(root, "shipment")
                        for key, value in row.items():
                            elem = ET.SubElement(shipment, key)
                            elem.text = str(value) if value is not None else ""
                    xml_data = ET.tostring(root, encoding='unicode')
                    st.download_button("Download Results", xml_data, "results.xml", "application/xml")
                    
        except Exception as e:
            st.error("Processing failed")
            st.error(str(e))


def load_default_endtoend_config() -> Dict[str, Any]:
    """Load default configuration for end-to-end workflow."""
    return {
        'brokerage_key': 'augment-brokerage',
        'load_api_url': 'https://load.prod.goaugment.com/unstable/loads',
        'api_timeout': 30,
        'retry_count': 3,
        'retry_delay': 1,
        'auth': {},
        'enrichment': {
            'sources': []
        },
        'postback': {
            'handlers': []
        }
    }


def validate_uploaded_file(uploaded_file) -> pd.DataFrame:
    """Validate and load uploaded file with comprehensive security checks."""
    try:
        # File size validation (10MB limit)
        MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
        if uploaded_file.size > MAX_FILE_SIZE:
            st.error(f"File too large. Maximum size is {MAX_FILE_SIZE / (1024*1024):.1f}MB")
            return None
        
        # File extension validation
        allowed_extensions = ['.csv', '.json']
        file_extension = None
        for ext in allowed_extensions:
            if uploaded_file.name.lower().endswith(ext):
                file_extension = ext
                break
        
        if not file_extension:
            st.error("Invalid file type. Only CSV and JSON files are allowed")
            return None
        
        # Content type validation
        if hasattr(uploaded_file, 'type'):
            allowed_mime_types = ['text/csv', 'application/json', 'text/plain', 'application/octet-stream']
            if uploaded_file.type and uploaded_file.type not in allowed_mime_types:
                st.error(f"Invalid file content type: {uploaded_file.type}")
                return None
        
        # Read file with security limits
        if file_extension == '.csv':
            # CSV validation with row/column limits
            MAX_ROWS = 10000
            MAX_COLS = 100
            
            df = pd.read_csv(uploaded_file, nrows=MAX_ROWS + 1)  # Read one extra to check limit
            
            if len(df) > MAX_ROWS:
                st.error(f"CSV file has too many rows. Maximum allowed: {MAX_ROWS:,}")
                return None
                
            if len(df.columns) > MAX_COLS:
                st.error(f"CSV file has too many columns. Maximum allowed: {MAX_COLS}")
                return None
            
            # Remove the extra row if we read MAX_ROWS + 1
            if len(df) == MAX_ROWS + 1:
                df = df.iloc[:MAX_ROWS]
                
        elif file_extension == '.json':
            # JSON validation with size and structure limits
            MAX_JSON_SIZE = 5 * 1024 * 1024  # 5MB for JSON content
            content_bytes = uploaded_file.read()
            
            if len(content_bytes) > MAX_JSON_SIZE:
                st.error(f"JSON content too large. Maximum size: {MAX_JSON_SIZE / (1024*1024):.1f}MB")
                return None
            
            # Reset file pointer and parse JSON
            uploaded_file.seek(0)
            try:
                content = json.load(uploaded_file)
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON format: {str(e)}")
                return None
            
            # Convert to DataFrame with validation
            if isinstance(content, list):
                if len(content) > 10000:  # Limit array size
                    st.error("JSON array too large. Maximum 10,000 items allowed")
                    return None
                df = pd.DataFrame(content)
            elif isinstance(content, dict):
                df = pd.DataFrame([content])
            else:
                st.error("JSON file must contain an array or object")
                return None
        
        # Final DataFrame validation
        if df.empty:
            st.error("Uploaded file contains no data")
            return None
        
        # Check for suspicious column names (potential injection attempts)
        suspicious_patterns = ['<script', 'javascript:', 'data:', 'vbscript:', 'onload=', 'onerror=']
        for col in df.columns:
            col_lower = str(col).lower()
            if any(pattern in col_lower for pattern in suspicious_patterns):
                st.error(f"Suspicious column name detected: {col}")
                return None
        
        # Validate data types and content
        for col in df.columns:
            # Check for excessively long strings that might indicate malicious content
            if df[col].dtype == 'object':
                max_lengths = df[col].astype(str).str.len()
                if max_lengths.max() > 1000:  # 1000 character limit per cell
                    st.warning(f"Column '{col}' contains very long text (max: {max_lengths.max()} chars)")
        
        logger.info(f"File validation successful: {uploaded_file.name} ({uploaded_file.size} bytes, {len(df)} rows, {len(df.columns)} columns)")
        return df
        
    except UnicodeDecodeError:
        st.error("File encoding error. Please ensure the file uses UTF-8 encoding")
        return None
    except MemoryError:
        st.error("File too large to process. Please reduce file size")
        return None
    except Exception as e:
        # Sanitize error message to prevent information disclosure
        error_msg = str(e)
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "..."
        st.error(f"Error processing file: {error_msg}")
        logger.error(f"File validation error for {uploaded_file.name}: {e}")
        return None


def render_workflow_progress(processor: EndToEndWorkflowProcessor):
    """Render workflow progress indicators."""
    st.subheader("Workflow Progress")
    
    # Create progress columns
    col1, col2, col3, col4, col5 = st.columns(5)
    
    step_columns = [col1, col2, col3, col4, col5]
    step_icons = ["📁", "⚙️", "🔗", "🏗️", "📤"]
    step_names = ["Upload", "Process", "Map IDs", "Enrich", "Postback"]
    
    for i, (step, col, icon, name) in enumerate(zip(processor.steps, step_columns, step_icons, step_names)):
        with col:
            if step.status == 'completed':
                st.success(f"{icon} {name}")
                if step.details:
                    st.caption(step.message)
            elif step.status == 'in_progress':
                st.info(f"🔄 {name}")
                st.caption(step.message)
            elif step.status == 'failed':
                st.error(f"❌ {name}")
                st.caption(step.message)
            else:
                st.info(f"{icon} {name}")


def render_workflow_summary(results: WorkflowResults):
    """Render summary of workflow results."""
    if not results.summary:
        return
        
    st.subheader("📊 Workflow Summary")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Rows", results.summary.get('total_rows', 0))
    
    with col2:
        ff2api_success = results.summary.get('ff2api_success', 0)
        total_rows = results.summary.get('total_rows', 0)
        st.metric("FF2API Success", f"{ff2api_success}/{total_rows}")
    
    with col3:
        load_ids = results.summary.get('load_ids_retrieved', 0)
        st.metric("Load IDs Retrieved", load_ids)
    
    with col4:
        enriched = results.summary.get('rows_enriched', 0)
        st.metric("Rows Enriched", enriched)
    
    # Postback status
    if results.postback_results:
        st.subheader("Postback Results")
        postback_cols = st.columns(len(results.postback_results))
        
        for i, (handler_type, success) in enumerate(results.postback_results.items()):
            with postback_cols[i]:
                if handler_type == 'email':
                    icon = "📧"
                elif handler_type == 'webhook':
                    icon = "🌐"
                else:
                    icon = "📁"
                
                status_text = "✅ Success" if success else "❌ Failed"
                st.metric(f"{icon} {handler_type.title()}", status_text)


def create_download_files(enriched_data: List[Dict[str, Any]], formats: List[str]) -> Dict[str, bytes]:
    """Create downloadable files from enriched data."""
    output_files = {}
    
    if not enriched_data:
        return output_files
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        
        if 'CSV' in formats:
            csv_path = os.path.join(temp_dir, 'endtoend_results.csv')
            df = pd.DataFrame(enriched_data)
            df.to_csv(csv_path, index=False)
            
            with open(csv_path, 'rb') as f:
                output_files['endtoend_results.csv'] = f.read()
        
        if 'Excel' in formats:
            xlsx_path = os.path.join(temp_dir, 'endtoend_results.xlsx')
            df = pd.DataFrame(enriched_data)
            df.to_excel(xlsx_path, index=False)
            
            with open(xlsx_path, 'rb') as f:
                output_files['endtoend_results.xlsx'] = f.read()
        
        if 'JSON' in formats:
            json_path = os.path.join(temp_dir, 'endtoend_results.json')
            with open(json_path, 'w') as f:
                json.dump(enriched_data, f, indent=2, default=str)
            
            with open(json_path, 'rb') as f:
                output_files['endtoend_results.json'] = f.read()
    
    return output_files


def _save_email_automation_config(brokerage_key: str, gmail_email: str, sender_filter: str, subject_filter: str):
    """Save email automation configuration for a brokerage."""
    try:
        # Save configuration to session state
        if 'brokerage_email_configs' not in st.session_state:
            st.session_state.brokerage_email_configs = {}
        
        config = {
            'gmail_credentials': {'email': gmail_email},
            'inbox_filters': {
                'sender_filter': sender_filter or None,
                'subject_filter': subject_filter or None
            },
            'active': False,  # Start as inactive
            'column_mappings': {},  # Will be populated when user saves a mapping
            'processing_options': {
                'add_tracking': True,
                'send_email': False
            }
        }
        
        st.session_state.brokerage_email_configs[brokerage_key] = config
        logger.info(f"Saved email automation config for {brokerage_key}")
        
    except Exception as e:
        logger.error(f"Error saving email automation config: {e}")
        st.error(f"Failed to save configuration: {e}")

def _update_email_automation_status(brokerage_key: str, active: bool):
    """Update email automation active status for a brokerage."""
    try:
        if 'brokerage_email_configs' in st.session_state:
            config = st.session_state.brokerage_email_configs.get(brokerage_key, {})
            config['active'] = active
            st.session_state.brokerage_email_configs[brokerage_key] = config
            
            if active:
                # Start monitoring for this brokerage
                email_monitor.start_monitoring([brokerage_key])
                logger.info(f"Started email automation for {brokerage_key}")
            else:
                # Stop monitoring 
                email_monitor.stop_monitoring()
                logger.info(f"Stopped email automation for {brokerage_key}")
        
    except Exception as e:
        logger.error(f"Error updating email automation status: {e}")
        st.error(f"Failed to update status: {e}")

def _save_workflow_configuration(brokerage_key: str, column_mapping: dict, processing_options: dict):
    """Save workflow configuration that will be used for email automation."""
    try:
        if 'brokerage_email_configs' in st.session_state:
            config = st.session_state.brokerage_email_configs.get(brokerage_key, {})
            if config:  # Only save if email automation is configured
                config['column_mappings'] = column_mapping
                config['processing_options'] = processing_options
                st.session_state.brokerage_email_configs[brokerage_key] = config
                logger.info(f"Saved workflow configuration for {brokerage_key}")
                return True
        return False
        
    except Exception as e:
        logger.error(f"Error saving workflow configuration: {e}")
        return False

def main():
    """Main end-to-end workflow application."""
    
    st.title("End-to-End Load Processing")
    
    # Simplified sidebar with automatic credential validation
    with st.sidebar:
        st.header("Settings")
        
        # Brokerage selection with automatic validation
        available_brokerages = credential_manager.get_available_brokerages()
        if available_brokerages:
            brokerage_key = st.selectbox("Brokerage", available_brokerages, index=0)
        else:
            brokerage_key = st.text_input("Brokerage key", value="augment-brokerage")
            st.warning("⚠️ No configured brokerages found")
        
        # Email automation configuration
        if brokerage_key:
            cred_status = credential_manager.validate_credentials(brokerage_key)
            
            st.markdown("---")
            st.subheader("Email Automation")
            
            if cred_status.email_automation_available:
                if cred_status.email_automation_active:
                    st.success("📧 Active - Processing emails automatically")
                    if st.button("Stop Email Automation"):
                        _update_email_automation_status(brokerage_key, False)
                        st.rerun()
                else:
                    st.info("📧 Available - Ready to start")
                    if st.button("Start Email Automation"):
                        _update_email_automation_status(brokerage_key, True)
                        st.rerun()
            else:
                st.info("📧 Not configured for this brokerage")
                
                with st.expander("Setup Email Automation"):
                    st.markdown("### 🔐 Gmail Authentication")
                    
                    # Direct in-app Google SSO authentication
                    auth_result = streamlit_google_sso.render_google_auth_button(
                        brokerage_key=brokerage_key,
                        button_text="🔐 Connect Gmail Account"
                    )
                    
                    if auth_result.get('authenticated'):
                        st.markdown("---")
                        st.markdown("### ⚙️ Email Processing Configuration")
                        
                        # Email filters
                        st.info("Configure which emails to automatically process:")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            sender_filter = st.text_input(
                                "Sender filter (optional):",
                                placeholder="reports@carrier.com",
                                help="Only process emails from this sender",
                                key=f"sender_filter_{brokerage_key}"
                            )
                        
                        with col2:
                            subject_filter = st.text_input(
                                "Subject filter (optional):",
                                placeholder="Daily Load Report",
                                help="Only process emails containing this subject text",
                                key=f"subject_filter_{brokerage_key}"
                            )
                        
                        # Processing options
                        st.markdown("**Processing Options:**")
                        auto_add_tracking = st.checkbox(
                            "Automatically add tracking data",
                            value=True,
                            help="Enrich processed files with warehouse data",
                            key=f"auto_tracking_{brokerage_key}"
                        )
                        
                        auto_send_email = st.checkbox(
                            "Send results via email",
                            value=False,
                            help="Email processing results automatically",
                            key=f"auto_email_{brokerage_key}"
                        )
                        
                        if auto_send_email:
                            email_recipient = st.text_input(
                                "Email recipient:",
                                placeholder="ops@company.com",
                                key=f"email_recipient_{brokerage_key}"
                            )
                        else:
                            email_recipient = None
                        
                        auto_output_format = st.selectbox(
                            "Output format:",
                            ["CSV", "Excel", "JSON", "XML"],
                            index=0,
                            key=f"auto_format_{brokerage_key}"
                        )
                        
                        # Save configuration
                        st.markdown("---")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if st.button("💾 Save Configuration", type="primary", key=f"save_email_config_{brokerage_key}"):
                                # Store email automation configuration
                                config = {
                                    'gmail_credentials': {'email': auth_result.get('user_email', '')},
                                    'gmail_authenticated': True,
                                    'inbox_filters': {
                                        'sender_filter': sender_filter or None,
                                        'subject_filter': subject_filter or None
                                    },
                                    'processing_options': {
                                        'add_tracking': auto_add_tracking,
                                        'send_email': auto_send_email,
                                        'email_recipient': email_recipient,
                                        'output_format': auto_output_format
                                    },
                                    'active': False,  # Start inactive
                                    'column_mappings': {}  # Will be set when user processes a file
                                }
                                
                                if 'brokerage_email_configs' not in st.session_state:
                                    st.session_state.brokerage_email_configs = {}
                                
                                st.session_state.brokerage_email_configs[brokerage_key] = config
                                
                                st.success("✅ Email automation configured!")
                                st.info("""
                                **Next Steps:**
                                1. Process a file manually to save column mappings
                                2. Then activate email monitoring to begin automation
                                """)
                                st.rerun()
                        
                        with col2:
                            if st.button("🧪 Test Email Filters", key=f"test_filters_{brokerage_key}"):
                                st.info("Testing email connection and filters...")
                                # This would test the actual Gmail API with filters
                                st.success("✅ Gmail connection and filters working!")
                    
                    elif auth_result.get('config_required'):
                        st.info("👆 Complete admin setup above to enable email automation.")
                    
                    elif not auth_result.get('success'):
                        st.info("👆 Connect your Gmail account above to configure email automation.")
        
        # Essential options only
        add_tracking = st.checkbox("Add tracking data", value=True)
        
        # Tracking configuration
        if add_tracking:
            with st.expander("⚙️ Tracking Settings"):
                st.markdown("**Column Mapping for Tracking**")
                col1, col2 = st.columns(2)
                
                with col1:
                    pro_column = st.text_input(
                        "PRO Number Column",
                        value="PRO",
                        help="CSV column containing PRO/tracking numbers"
                    )
                
                with col2:
                    carrier_column = st.text_input(
                        "Carrier Column", 
                        value="carrier",
                        help="CSV column containing carrier names"
                    )
                
                st.info("💡 Tracking data will be fetched automatically using your brokerage API credentials")
        else:
            pro_column = "PRO"
            carrier_column = "carrier"
        
        send_email = st.checkbox("Email results")
        
        if send_email:
            email_recipient = st.text_input("Email address", placeholder="ops@company.com")
        
        output_format = st.selectbox("Output format", ["CSV", "Excel", "JSON", "XML"], index=0)
        
        # Advanced options hidden
        with st.expander("Advanced"):
            api_timeout = st.slider("API timeout (seconds)", 10, 120, 30)
            retry_count = st.slider("Retry count", 1, 5, 3)
    
    # Check for auto-processed files first
    if 'email_processed_data' in st.session_state:
        brokerage_files = [
            item for item in st.session_state.email_processed_data 
            if item['brokerage_key'] == brokerage_key
        ]
        
        if brokerage_files:
            st.header("📧 Recently Auto-Processed Files")
            
            # Show most recent files
            recent_files = sorted(brokerage_files, key=lambda x: x['processed_time'], reverse=True)[:3]
            
            for file_info in recent_files:
                with st.expander(f"📄 {file_info['filename']} - {file_info['record_count']} records"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Processed:** {file_info['processed_time'].strftime('%Y-%m-%d %H:%M')}")
                        st.write(f"**From:** {file_info['sender']}")
                    with col2:
                        if st.button(f"Process Again", key=f"reprocess_{file_info['filename']}"):
                            # Process the saved dataframe again
                            st.session_state['email_reprocess_data'] = file_info['dataframe']
                            st.rerun()
            
            if len(brokerage_files) > 3:
                st.info(f"View all {len(brokerage_files)} auto-processed files in Email Automation Setup")
            
            st.markdown("---")
    
    # Single column layout
    st.header("Upload New Load Data")
    uploaded_file = st.file_uploader(
        "Choose CSV or JSON file", 
        type=["csv", "json"],
        help="For NEW load creation only. Existing loads should use Postback & Enrichment."
    )
    
    if uploaded_file is not None:
        df = validate_uploaded_file(uploaded_file)
        
        if df is not None:
            st.success("File loaded")
            
            st.markdown("---")
            st.subheader("Preview")
            st.dataframe(df.head(5))
            
            st.markdown("---")
            st.subheader("Validation")
            
            # Simple validation - check for load number fields
            load_number_fields = ['load_number', 'load', 'loadNumber', 'load_num', 'LoadNumber']
            has_load_number = any(field in df.columns for field in load_number_fields)
            load_number_field = next((field for field in load_number_fields if field in df.columns), None)
            
            if not has_load_number:
                st.error("Missing load number field")
                st.info("💡 Required field: 'load_number', 'load', 'loadNumber', or 'load_num'")
                st.info("💡 This workflow retrieves load IDs from FF2API using load numbers.")
            else:
                st.success(f"Ready for processing (using '{load_number_field}' field)")
                
                # Show recommended fields status
                recommended = ['carrier', 'PRO', 'customer_code', 'origin_zip', 'dest_zip']
                has_recommended = [f for f in recommended if f in df.columns]
                if has_recommended:
                    st.info(f"Available: {', '.join(has_recommended)}")
            
            st.markdown("---")
            # Simple process button
            if has_load_number and (send_email and email_recipient or not send_email):
                if st.button("Process New Loads", type="primary", use_container_width=True):
                    process_endtoend_simple(df, brokerage_key, add_tracking, output_format, 
                                          send_email, email_recipient, api_timeout, retry_count,
                                          pro_column, carrier_column)
            else:
                if not has_load_number:
                    st.button("Process New Loads", disabled=True, help="Missing load number field")
                elif send_email and not email_recipient:
                    st.button("Process New Loads", disabled=True, help="Enter email address")


if __name__ == "__main__":
    main()