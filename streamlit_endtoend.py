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

# Configure logging for Streamlit Cloud
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


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


def main():
    """Main end-to-end workflow application."""
    
    st.title("🔄 End-to-End Load Processing")
    st.markdown("**Complete workflow:** Upload CSV → Process Loads → Retrieve Load IDs → Enrich with Warehouse Data → Postback Results")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # FF2API / Load Processing Settings
        st.subheader("Load Processing Settings")
        brokerage_key = st.text_input(
            "Brokerage Key", 
            value="augment-brokerage",
            help="Brokerage identifier for load API calls"
        )
        
        api_timeout = st.slider("API Timeout (seconds)", 10, 120, 30)
        retry_count = st.slider("Retry Count", 1, 5, 3)
        
        # Enrichment Settings
        st.subheader("Enrichment Settings")
        
        # Mock tracking
        enable_mock_tracking = st.checkbox("Enable Mock Tracking", value=False)
        if enable_mock_tracking:
            max_events = st.slider("Max Mock Events", 1, 10, 5)
        
        # Snowflake enrichment
        enable_snowflake = st.checkbox("Enable Snowflake Enrichment", value=True)
        if enable_snowflake:
            st.info("🏗️ Uses load IDs for warehouse data lookups")
            
            snowflake_options = st.multiselect(
                "Select Enrichments:",
                [
                    "📍 Load Tracking Data",
                    "👤 Customer Information", 
                    "🚚 Carrier Details",
                    "🛣️ Lane Performance"
                ],
                default=["📍 Load Tracking Data", "👤 Customer Information"]
            )
        
        # Postback Settings
        st.subheader("Postback Settings")
        
        # Output formats
        output_formats = st.multiselect(
            "Output Formats:",
            ["CSV", "Excel", "JSON"],
            default=["CSV", "Excel"]
        )
        
        # Email settings
        enable_email = st.checkbox("Send Results via Email")
        if enable_email:
            email_recipient = st.text_input("Recipient Email", placeholder="ops@company.com")
            email_subject = st.text_input("Email Subject", value="End-to-End Load Processing Results")
        
        # Advanced settings
        with st.expander("Advanced Settings"):
            log_level = st.selectbox("Log Level", ["INFO", "DEBUG", "WARNING", "ERROR"])
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("📁 Upload Load Data")
        uploaded_file = st.file_uploader(
            "Choose CSV or JSON file",
            type=["csv", "json"],
            help="Upload NEW load data for end-to-end processing. REQUIRED: load_id field for FF2API processing."
        )
        
        if uploaded_file is not None:
            # Validate and display data
            df = validate_uploaded_file(uploaded_file)
            
            if df is not None:
                st.success(f"✅ File loaded: {len(df)} rows")
                
                # Data preview
                st.subheader("Data Preview")
                st.dataframe(df.head(10))
                
                # Field validation
                required_fields = ['load_id']
                recommended_fields = ['carrier', 'PRO', 'customer_code', 'origin_zip', 'dest_zip']
                
                has_required = all(field in df.columns for field in required_fields)
                has_recommended = [field for field in recommended_fields if field in df.columns]
                missing_recommended = [field for field in recommended_fields if field not in df.columns]
                
                if not has_required:
                    st.error(f"❌ Missing required fields for END-TO-END processing: {[f for f in required_fields if f not in df.columns]}")
                    st.info("💡 **Note**: End-to-End workflow requires 'load_id' for new load creation. For EXISTING loads, use the 'Postback & Enrichment System' instead.")
                else:
                    st.success("✅ Required fields present for end-to-end processing")
                
                if missing_recommended:
                    st.warning(f"⚠️ Missing recommended fields: {missing_recommended}")
                if has_recommended:
                    st.info(f"✅ Available fields: {has_recommended}")
    
    with col2:
        st.header("🚀 Process Workflow")
        
        if uploaded_file is not None and df is not None:
            
            # Process button
            if st.button("🚀 Start End-to-End Processing", type="primary", use_container_width=True):
                
                # Validation
                if not has_required:
                    st.error("Cannot process: missing required fields for end-to-end processing")
                    st.info("💡 This workflow is for creating NEW loads. For existing loads, use 'Postback & Enrichment System'.")
                    st.stop()
                
                if not output_formats and not enable_email:
                    st.error("Select at least one output format or enable email")
                    st.stop()
                
                if enable_email and not email_recipient:
                    st.error("Enter recipient email address")
                    st.stop()
                
                # Build configuration
                config = load_default_endtoend_config()
                config.update({
                    'brokerage_key': brokerage_key,
                    'api_timeout': api_timeout,
                    'retry_count': retry_count,
                    'retry_delay': 1
                })
                
                # Configure enrichment
                enrichment_sources = []
                
                if enable_mock_tracking:
                    enrichment_sources.append({
                        'type': 'mock_tracking',
                        'generate_events': True,
                        'max_events': max_events
                    })
                
                if enable_snowflake and snowflake_options:
                    sf_enrichments = []
                    if "📍 Load Tracking Data" in snowflake_options:
                        sf_enrichments.append("tracking")
                    if "👤 Customer Information" in snowflake_options:
                        sf_enrichments.append("customer")
                    if "🚚 Carrier Details" in snowflake_options:
                        sf_enrichments.append("carrier")
                    if "🛣️ Lane Performance" in snowflake_options:
                        sf_enrichments.append("lane")
                    
                    enrichment_sources.append({
                        'type': 'snowflake_augment',
                        'database': 'AUGMENT_DW',
                        'schema': 'MARTS',
                        'enrichments': sf_enrichments,
                        'use_load_ids': True  # Flag for load ID-based enrichment
                    })
                
                config['enrichment']['sources'] = enrichment_sources
                
                # Configure postback
                postback_handlers = []
                
                if enable_email and email_recipient:
                    try:
                        postback_handlers.append({
                            'type': 'email',
                            'recipient': email_recipient,
                            'subject': email_subject,
                            'smtp_user': st.secrets.get("email", {}).get("SMTP_USER"),
                            'smtp_pass': st.secrets.get("email", {}).get("SMTP_PASS"),
                        })
                    except:
                        st.warning("Email credentials not configured")
                
                config['postback']['handlers'] = postback_handlers
                
                # Initialize workflow processor
                processor = EndToEndWorkflowProcessor(config)
                
                # Create progress containers
                progress_container = st.container()
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(progress: float, message: str):
                    progress_bar.progress(progress)
                    status_text.text(message)
                
                # Render initial progress
                with progress_container:
                    render_workflow_progress(processor)
                
                # Process workflow
                try:
                    results = processor.process_workflow(df, update_progress)
                    
                    # Store results in session state
                    st.session_state.workflow_results = results
                    st.session_state.output_formats = output_formats
                    
                    # Update progress display
                    with progress_container:
                        render_workflow_progress(processor)
                    
                    # Show completion message
                    if results.errors:
                        st.error(f"❌ Workflow completed with {len(results.errors)} errors")
                        for error in results.errors:
                            st.error(error)
                    else:
                        st.success("✅ End-to-end workflow completed successfully!")
                    
                except Exception as e:
                    st.error(f"❌ Workflow failed: {str(e)}")
                    logger.error(f"Workflow error: {e}")
    
    # Results section
    if 'workflow_results' in st.session_state:
        results = st.session_state.workflow_results
        
        # Render summary
        render_workflow_summary(results)
        
        # Enhanced data preview
        if results.enriched_data:
            st.subheader("Enhanced Data Preview")
            enhanced_df = pd.DataFrame(results.enriched_data)
            st.dataframe(enhanced_df.head(10))
            
            # Show new columns added
            original_cols = set(results.csv_data[0].keys()) if results.csv_data else set()
            enhanced_cols = set(enhanced_df.columns)
            new_cols = enhanced_cols - original_cols
            
            if new_cols:
                st.info(f"**New columns added:** {', '.join(sorted(new_cols))}")
        
        # Download section
        if 'output_formats' in st.session_state and st.session_state.output_formats:
            st.subheader("📥 Download Results")
            
            try:
                output_files = create_download_files(results.enriched_data, st.session_state.output_formats)
                
                if output_files:
                    cols = st.columns(len(output_files))
                    for i, (filename, file_data) in enumerate(output_files.items()):
                        with cols[i]:
                            st.download_button(
                                label=f"Download {filename}",
                                data=file_data,
                                file_name=filename,
                                mime="application/octet-stream"
                            )
                    
                    # ZIP download if multiple files
                    if len(output_files) > 1:
                        zip_buffer = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
                        try:
                            with zipfile.ZipFile(zip_buffer.name, 'w') as zf:
                                for filename, file_data in output_files.items():
                                    zf.writestr(filename, file_data)
                            
                            with open(zip_buffer.name, 'rb') as f:
                                zip_data = f.read()
                            
                            st.download_button(
                                label="📦 Download All Files (ZIP)",
                                data=zip_data,
                                file_name=f"endtoend_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip"
                            )
                        finally:
                            os.unlink(zip_buffer.name)
            except Exception as e:
                st.error(f"Error creating download files: {e}")


if __name__ == "__main__":
    main()