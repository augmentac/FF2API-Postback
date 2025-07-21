"""
Streamlit interface for the Postback and Enrichment system.
Designed for Streamlit Cloud deployment.
"""

import streamlit as st
import pandas as pd
import json
import yaml
import os
import sys
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, Any, List
import logging

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import our postback and enrichment modules
try:
    from enrichment.manager import EnrichmentManager
    from postback.router import PostbackRouter
except ImportError as e:
    st.error(f"❌ Failed to import postback modules: {e}")
    st.error("Please ensure the enrichment/ and postback/ directories are present in the deployment.")
    st.stop()

# Configure logging for Streamlit Cloud
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def load_default_config() -> Dict[str, Any]:
    """Load default configuration for Streamlit deployment."""
    return {
        'postback': {
            'handlers': [
                {
                    'type': 'csv',
                    'output_path': './outputs/postback.csv'
                },
                {
                    'type': 'xlsx', 
                    'output_path': './outputs/postback.xlsx'
                },
                {
                    'type': 'json',
                    'output_path': './outputs/postback.json',
                    'append_mode': False
                },
                {
                    'type': 'xml',
                    'output_path': './outputs/postback.xml',
                    'root_element': 'freight_data',
                    'row_element': 'shipment'
                }
            ]
        },
        'enrichment': {
            'sources': [
                {
                    'type': 'mock_tracking',
                    'generate_events': True,
                    'max_events': 5
                }
            ]
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

def create_output_files(enriched_rows: List[Dict[str, Any]], enabled_handlers: List[str]) -> Dict[str, bytes]:
    """Create output files and return them as bytes for download."""
    output_files = {}
    
    # Create temporary directory for outputs
    with tempfile.TemporaryDirectory() as temp_dir:
        # Configure handlers based on selection
        handler_configs = []
        
        if 'CSV' in enabled_handlers:
            handler_configs.append({
                'type': 'csv',
                'output_path': os.path.join(temp_dir, 'postback.csv')
            })
            
        if 'Excel (XLSX)' in enabled_handlers:
            handler_configs.append({
                'type': 'xlsx',
                'output_path': os.path.join(temp_dir, 'postback.xlsx')
            })
            
        if 'JSON' in enabled_handlers:
            handler_configs.append({
                'type': 'json',
                'output_path': os.path.join(temp_dir, 'postback.json')
            })
            
        if 'XML' in enabled_handlers:
            handler_configs.append({
                'type': 'xml',
                'output_path': os.path.join(temp_dir, 'postback.xml'),
                'root_element': 'freight_data',
                'row_element': 'shipment'
            })
        
        # Create and run postback router
        router = PostbackRouter(handler_configs)
        results = router.post_all(enriched_rows)
        
        # Read generated files into memory
        for config in handler_configs:
            file_path = config['output_path']
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    filename = os.path.basename(file_path)
                    output_files[filename] = f.read()
                    
    return output_files

def main():
    """Main Streamlit app for postback system."""
    
    st.set_page_config(
        page_title="FF2API - Postback & Enrichment",
        page_icon="🚚",
        layout="wide"
    )
    
    st.title("🚚 FF2API - Postback & Enrichment System")
    st.markdown("Upload freight data, enrich it with tracking information, and export in multiple formats.")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Enrichment settings
        st.subheader("Enrichment Settings")
        enable_tracking = st.checkbox("Enable Mock Tracking Enrichment", value=True)
        if enable_tracking:
            max_events = st.slider("Max Tracking Events per Load", 1, 10, 5)
        
        # Output format selection
        st.subheader("Output Formats")
        output_formats = st.multiselect(
            "Select output formats:",
            options=["CSV", "Excel (XLSX)", "JSON", "XML"],
            default=["CSV", "JSON"]
        )
        
        # Advanced settings
        st.subheader("Advanced Settings")
        log_level = st.selectbox("Log Level", ["INFO", "DEBUG", "WARNING", "ERROR"])
        
        # Webhook configuration (optional)
        st.subheader("Webhook Settings (Optional)")
        enable_webhook = st.checkbox("Enable Webhook")
        if enable_webhook:
            webhook_url = st.text_input("Webhook URL", placeholder="https://example.com/webhook")
            webhook_timeout = st.number_input("Timeout (seconds)", value=30, min_value=5, max_value=120)
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("📁 Upload Data")
        uploaded_file = st.file_uploader(
            "Choose a CSV or JSON file",
            type=["csv", "json"],
            help="Upload your freight data file. Required fields: load_id, carrier, PRO"
        )
        
        if uploaded_file is not None:
            # Validate and display uploaded data
            df = validate_uploaded_file(uploaded_file)
            
            if df is not None:
                st.success(f"✅ File loaded successfully! {len(df)} rows found.")
                
                # Show data preview
                st.subheader("Data Preview")
                st.dataframe(df.head(10))
                
                # Check for required columns
                required_cols = ['load_id', 'carrier', 'PRO']
                missing_cols = [col for col in required_cols if col not in df.columns]
                
                if missing_cols:
                    st.warning(f"⚠️ Missing recommended columns: {', '.join(missing_cols)}")
                    st.info("The system will work but enrichment may be limited.")
    
    with col2:
        st.header("🔄 Processing")
        
        if uploaded_file is not None and df is not None:
            
            # Process button
            if st.button("🚀 Process & Enrich Data", type="primary", use_container_width=True):
                
                if not output_formats:
                    st.error("Please select at least one output format.")
                    st.stop()
                
                # Show progress
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Step 1: Prepare configuration
                    status_text.text("Preparing configuration...")
                    progress_bar.progress(10)
                    
                    config = load_default_config()
                    
                    # Update enrichment config
                    if enable_tracking:
                        config['enrichment']['sources'][0]['max_events'] = max_events
                    else:
                        config['enrichment']['sources'] = []
                    
                    # Add webhook if enabled
                    if enable_webhook and webhook_url:
                        config['postback']['handlers'].append({
                            'type': 'webhook',
                            'url': webhook_url,
                            'timeout': webhook_timeout
                        })
                    
                    # Step 2: Initialize enrichment
                    status_text.text("Initializing enrichment system...")
                    progress_bar.progress(25)
                    
                    enrichment_manager = EnrichmentManager(config['enrichment']['sources'])
                    
                    # Step 3: Enrich data
                    status_text.text("Enriching data...")
                    progress_bar.progress(50)
                    
                    rows = df.to_dict('records')
                    enriched_rows = enrichment_manager.enrich_rows(rows)
                    
                    # Step 4: Generate outputs
                    status_text.text("Generating output files...")
                    progress_bar.progress(75)
                    
                    output_files = create_output_files(enriched_rows, output_formats)
                    
                    # Step 5: Complete
                    status_text.text("Processing complete!")
                    progress_bar.progress(100)
                    
                    # Store results in session state
                    st.session_state.enriched_data = enriched_rows
                    st.session_state.output_files = output_files
                    
                    st.success(f"✅ Successfully processed {len(enriched_rows)} records!")
                    
                except Exception as e:
                    st.error(f"❌ Processing failed: {str(e)}")
                    logger.error(f"Processing error: {e}")
    
    # Results section
    if 'enriched_data' in st.session_state and 'output_files' in st.session_state:
        st.header("📊 Results")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", len(st.session_state.enriched_data))
        
        with col2:
            enriched_count = sum(1 for row in st.session_state.enriched_data if 'enrichment_timestamp' in row)
            st.metric("Enriched Records", enriched_count)
        
        with col3:
            st.metric("Output Files", len(st.session_state.output_files))
        
        with col4:
            tracking_count = sum(1 for row in st.session_state.enriched_data if row.get('tracking_events_count', 0) > 0)
            st.metric("With Tracking", tracking_count)
        
        # Preview enriched data
        st.subheader("Enriched Data Preview")
        enriched_df = pd.DataFrame(st.session_state.enriched_data)
        st.dataframe(enriched_df.head(10))
        
        # Download section
        st.subheader("📥 Download Results")
        
        # Individual file downloads
        cols = st.columns(len(st.session_state.output_files))
        for i, (filename, file_data) in enumerate(st.session_state.output_files.items()):
            with cols[i % len(cols)]:
                st.download_button(
                    label=f"Download {filename}",
                    data=file_data,
                    file_name=filename,
                    mime="application/octet-stream"
                )
        
        # Bulk download as ZIP
        if len(st.session_state.output_files) > 1:
            st.markdown("---")
            
            # Create ZIP file
            zip_buffer = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
            try:
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for filename, file_data in st.session_state.output_files.items():
                        zip_file.writestr(filename, file_data)
                
                zip_buffer.seek(0)
                with open(zip_buffer.name, 'rb') as f:
                    zip_data = f.read()
                
                st.download_button(
                    label="📦 Download All Files (ZIP)",
                    data=zip_data,
                    file_name=f"postback_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip"
                )
            finally:
                os.unlink(zip_buffer.name)

if __name__ == "__main__":
    main()