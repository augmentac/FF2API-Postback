"""
Streamlit End-to-End Load Processing Application.
Complete workflow: CSV Upload → FF2API Processing → Load ID Mapping → Snowflake Enrichment → Postback
"""

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
# Initialize email monitor with credential manager
email_monitor.credential_manager = credential_manager

# Configure logging for Streamlit Cloud
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def process_endtoend_simple(df, brokerage_key, add_tracking, output_format, send_email, email_recipient, snowflake_options, api_timeout, retry_count):
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
                sf_enrichments = []
                if "Load Tracking" in snowflake_options:
                    sf_enrichments.append("tracking")
                if "Customer Info" in snowflake_options:
                    sf_enrichments.append("customer")
                if "Carrier Details" in snowflake_options:
                    sf_enrichments.append("carrier")
                if "Lane Performance" in snowflake_options:
                    sf_enrichments.append("lane")
                
                config['enrichment']['sources'] = [{
                    'type': 'snowflake_augment',
                    'database': 'AUGMENT_DW',
                    'schema': 'MARTS',
                    'enrichments': sf_enrichments,
                    'use_load_ids': True
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
    """Validate and load uploaded file."""
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith('.json'):
            content = json.load(uploaded_file)
            if isinstance(content, list):
                df = pd.DataFrame(content)
            elif isinstance(content, dict):
                df = pd.DataFrame([content])
            else:
                st.error("JSON file must contain an array or object")
                return None
        else:
            st.error("Please upload a CSV or JSON file")
            return None
            
        if df.empty:
            st.error("Uploaded file is empty")
            return None
            
        return df
        
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
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
                    st.write("To enable automatic email processing:")
                    
                    gmail_email = st.text_input("Gmail address for monitoring", placeholder="reports@yourcompany.com")
                    sender_filter = st.text_input("Sender email filter (optional)", placeholder="carrier@company.com")
                    subject_filter = st.text_input("Subject filter (optional)", placeholder="Daily Load Report")
                    
                    if st.button("Save Email Configuration"):
                        if gmail_email:
                            _save_email_automation_config(brokerage_key, gmail_email, sender_filter, subject_filter)
                            st.success("Email automation configured! You can now start monitoring.")
                            st.rerun()
                        else:
                            st.error("Gmail address is required")
        
        # Essential options only
        add_tracking = st.checkbox("Add warehouse data", value=True)
        send_email = st.checkbox("Email results")
        
        if send_email:
            email_recipient = st.text_input("Email address", placeholder="ops@company.com")
        
        output_format = st.selectbox("Output format", ["CSV", "Excel", "JSON", "XML"], index=0)
        
        # Advanced options hidden
        with st.expander("Advanced"):
            api_timeout = st.slider("API timeout (seconds)", 10, 120, 30)
            retry_count = st.slider("Retry count", 1, 5, 3)
            if add_tracking:
                snowflake_options = st.multiselect(
                    "Warehouse data:",
                    ["Load Tracking", "Customer Info", "Carrier Details", "Lane Performance"],
                    default=["Load Tracking", "Customer Info"]
                )
            else:
                snowflake_options = []
    
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
            
            # Simple validation
            has_load_id = 'load_id' in df.columns
            
            if not has_load_id:
                st.error("Missing 'load_id' field")
                st.info("💡 This workflow creates NEW loads. For existing loads, use 'Postback & Enrichment System'.")
            else:
                st.success("Ready for processing")
                
                # Show recommended fields status
                recommended = ['carrier', 'PRO', 'customer_code', 'origin_zip', 'dest_zip']
                has_recommended = [f for f in recommended if f in df.columns]
                if has_recommended:
                    st.info(f"Available: {', '.join(has_recommended)}")
            
            st.markdown("---")
            # Simple process button
            if has_load_id and (send_email and email_recipient or not send_email):
                if st.button("Process New Loads", type="primary", use_container_width=True):
                    process_endtoend_simple(df, brokerage_key, add_tracking, output_format, 
                                          send_email, email_recipient, snowflake_options,
                                          api_timeout, retry_count)
            else:
                if not has_load_id:
                    st.button("Process New Loads", disabled=True, help="Missing 'load_id' field")
                elif send_email and not email_recipient:
                    st.button("Process New Loads", disabled=True, help="Enter email address")


if __name__ == "__main__":
    main()