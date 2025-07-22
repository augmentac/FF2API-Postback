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

def auto_detect_column_mappings(csv_columns: List[str]) -> Dict[str, str]:
    """
    Smart auto-detection of column mappings based on common patterns.
    Returns a dictionary of {system_field: csv_column} mappings.
    """
    mappings = {}
    
    # Define mapping patterns (case-insensitive)
    mapping_patterns = {
        'load_id': [
            'bol #', 'bol_number', 'bol', 'bill_of_lading',
            'load_id', 'load_number', 'shipment_id', 'reference'
        ],
        'pro_number': [
            'carrier pro#', 'pro_number', 'pro #', 'pro', 'tracking_number',
            'carrier_pro', 'pronumber', 'tracking', 'waybill'
        ],
        'carrier': [
            'carrier name', 'carrier', 'carrier_name', 'scac', 'carrier_code',
            'transportation_provider', 'shipper'
        ],
        'customer_code': [
            'customer name', 'customer_code', 'customer', 'client_name',
            'acct/customer#', 'account', 'customer_id', 'client'
        ],
        'origin_zip': [
            'origin zip', 'origin_zip', 'pickup_zip', 'from_zip',
            'origin_postal', 'ship_from_zip', 'pickup_postal'
        ],
        'dest_zip': [
            'destination zip', 'dest_zip', 'delivery_zip', 'to_zip',
            'destination_postal', 'ship_to_zip', 'delivery_postal'
        ]
    }
    
    # Convert CSV columns to lowercase for matching
    csv_lower = {col.lower(): col for col in csv_columns}
    
    # Find best matches for each system field
    for system_field, patterns in mapping_patterns.items():
        best_match = None
        
        # Look for exact matches first
        for pattern in patterns:
            if pattern in csv_lower:
                best_match = csv_lower[pattern]
                break
        
        # If no exact match, look for partial matches
        if not best_match:
            for pattern in patterns:
                for csv_col_lower, csv_col_original in csv_lower.items():
                    if pattern in csv_col_lower or csv_col_lower in pattern:
                        best_match = csv_col_original
                        break
                if best_match:
                    break
        
        if best_match:
            mappings[system_field] = best_match
    
    return mappings


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
    
    # If no file formats selected, return empty (email-only scenario)
    if not enabled_handlers:
        return output_files
    
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
        
        # Only create router if we have handlers
        if handler_configs:
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
        
        # Mock tracking enrichment
        enable_tracking = st.checkbox("Enable Mock Tracking Enrichment", value=False)
        if enable_tracking:
            max_events = st.slider("Max Tracking Events per Load", 1, 10, 5)
        
        # Snowflake enrichment
        enable_snowflake = st.checkbox("Enable Snowflake Data Enrichment", value=True)
        if enable_snowflake:
            st.info("🏗️ Uses GoAugment DBT models for real data enrichment")
            
            snowflake_options = st.multiselect(
                "Select Data to Add:",
                [
                    "📍 Latest Tracking Status", 
                    "👤 Customer Information",
                    "🚚 Carrier Details", 
                    "🛣️ Lane Performance (90 days)"
                ],
                default=["📍 Latest Tracking Status", "👤 Customer Information"],
                help="Choose which additional data columns to add from your internal database"
            )
            
            if snowflake_options:
                st.caption("**Will add columns:**")
                if "📍 Latest Tracking Status" in snowflake_options:
                    st.caption("• sf_tracking_status, sf_last_scan_location, sf_estimated_delivery")
                if "👤 Customer Information" in snowflake_options:
                    st.caption("• sf_customer_name, sf_account_manager, sf_payment_terms")
                if "🚚 Carrier Details" in snowflake_options:
                    st.caption("• sf_carrier_name, sf_carrier_otp, sf_service_levels")
                if "🛣️ Lane Performance (90 days)" in snowflake_options:
                    st.caption("• sf_avg_transit_days, sf_avg_lane_cost, sf_lane_volume")
        
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
        
        # Email configuration (optional)
        st.subheader("Email Settings (Optional)")
        enable_email = st.checkbox("Send Results via Email")
        if enable_email:
            email_recipient = st.text_input(
                "Recipient Email", 
                placeholder="freight@company.com",
                help="Email address to receive the results"
            )
            email_subject = st.text_input(
                "Email Subject", 
                value="Freight Data Results",
                help="Subject line for the email"
            )
            st.info("📧 Results will be sent as CSV attachment via email")
        
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
                
                # Smart column mapping with auto-detection
                st.subheader("🔗 Smart Column Mapping")
                
                # Auto-detect column mappings
                auto_mappings = auto_detect_column_mappings(list(df.columns))
                
                if auto_mappings:
                    st.success(f"✅ Auto-detected {len(auto_mappings)} field mappings")
                    
                    # Display mapping table
                    mapping_df = pd.DataFrame([
                        {"System Field": system_field.replace('_', ' ').title(), 
                         "CSV Column": csv_col,
                         "Field Type": "🔑 Primary" if system_field in ['load_id', 'pro_number'] else "📝 Optional"}
                        for system_field, csv_col in auto_mappings.items()
                    ])
                    st.dataframe(mapping_df, hide_index=True, use_container_width=True)
                    
                    # Store auto-mappings
                    st.session_state.column_mapping = auto_mappings
                    
                else:
                    st.warning("⚠️ Could not auto-detect field mappings")
                    st.session_state.column_mapping = {}
                
                # Manual override option
                with st.expander("🔧 Adjust Mappings (Optional)"):
                    st.info("Only modify if auto-detection is incorrect")
                    
                    # Available CSV columns for manual override
                    csv_columns = ['-- Not Mapped --'] + list(df.columns)
                    
                    # Manual mapping interface (more compact)
                    manual_mapping = {}
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        manual_mapping['load_id'] = st.selectbox(
                            "Load ID Field:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('load_id', '-- Not Mapped --')) if auto_mappings.get('load_id') in csv_columns else 0
                        )
                        manual_mapping['pro_number'] = st.selectbox(
                            "PRO Number Field:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('pro_number', '-- Not Mapped --')) if auto_mappings.get('pro_number') in csv_columns else 0
                        )
                    
                    with col2:
                        manual_mapping['carrier'] = st.selectbox(
                            "Carrier Field:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('carrier', '-- Not Mapped --')) if auto_mappings.get('carrier') in csv_columns else 0
                        )
                        manual_mapping['customer_code'] = st.selectbox(
                            "Customer Field:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('customer_code', '-- Not Mapped --')) if auto_mappings.get('customer_code') in csv_columns else 0
                        )
                    
                    with col3:
                        manual_mapping['origin_zip'] = st.selectbox(
                            "Origin ZIP:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('origin_zip', '-- Not Mapped --')) if auto_mappings.get('origin_zip') in csv_columns else 0
                        )
                        manual_mapping['dest_zip'] = st.selectbox(
                            "Destination ZIP:", 
                            csv_columns,
                            index=csv_columns.index(auto_mappings.get('dest_zip', '-- Not Mapped --')) if auto_mappings.get('dest_zip') in csv_columns else 0
                        )
                    
                    # Apply manual overrides button
                    if st.button("Apply Manual Mappings"):
                        # Filter out unmapped fields
                        filtered_mapping = {k: v for k, v in manual_mapping.items() if v != '-- Not Mapped --'}
                        st.session_state.column_mapping = filtered_mapping
                        st.success(f"✅ Applied {len(filtered_mapping)} manual mappings")
                        st.rerun()
                
                # Final validation
                current_mappings = getattr(st.session_state, 'column_mapping', {})
                if not current_mappings:
                    st.error("⚠️ No field mappings available. Please check your CSV columns.")
                else:
                    primary_fields = [f for f in ['load_id', 'pro_number', 'carrier'] if f in current_mappings]
                    if primary_fields:
                        st.info(f"🎯 Ready to enrich using: {', '.join([f.replace('_', ' ').title() for f in primary_fields])}")
                    else:
                        st.warning("⚠️ No primary identifier fields mapped. Enrichment may be limited.")
    
    with col2:
        st.header("🔄 Processing")
        
        if uploaded_file is not None and df is not None:
            
            # Process button
            if st.button("🚀 Process & Enrich Data", type="primary", use_container_width=True):
                
                # Validation checks
                if not output_formats and not enable_email:
                    st.error("Please select at least one output format or enable email.")
                    st.stop()
                
                if enable_email and not email_recipient:
                    st.error("Please enter a recipient email address.")
                    st.stop()
                
                # Check column mapping
                current_mappings = getattr(st.session_state, 'column_mapping', {})
                if not current_mappings:
                    st.error("No column mappings detected. Please check your CSV format or use manual mapping.")
                    st.stop()
                    
                # Check for at least one primary identifier
                primary_fields = [f for f in ['load_id', 'pro_number', 'carrier'] if f in current_mappings]
                if not primary_fields:
                    st.error("Please ensure at least one primary identifier field (Load ID, PRO Number, or Carrier) is mapped.")
                    st.stop()
                
                if not enable_tracking and not enable_snowflake:
                    st.warning("⚠️ No enrichment sources selected. Data will be processed without additional enrichment.")
                    
                if enable_snowflake and not snowflake_options:
                    st.error("Please select at least one Snowflake enrichment option or disable Snowflake enrichment.")
                    st.stop()
                
                # Show progress
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Step 1: Prepare configuration
                    status_text.text("Preparing configuration...")
                    progress_bar.progress(10)
                    
                    config = load_default_config()
                    config['workflow_type'] = 'postback'  # Set workflow type for validation
                    
                    # Update enrichment config based on UI selections
                    enrichment_sources = []
                    
                    # Add mock tracking if enabled
                    if enable_tracking:
                        enrichment_sources.append({
                            'type': 'mock_tracking',
                            'generate_events': True,
                            'max_events': max_events
                        })
                    
                    # Add Snowflake enrichment if enabled
                    if enable_snowflake and snowflake_options:
                        # Map UI selections to config values
                        sf_enrichments = []
                        if "📍 Latest Tracking Status" in snowflake_options:
                            sf_enrichments.append("tracking")
                        if "👤 Customer Information" in snowflake_options:
                            sf_enrichments.append("customer")
                        if "🚚 Carrier Details" in snowflake_options:
                            sf_enrichments.append("carrier")
                        if "🛣️ Lane Performance (90 days)" in snowflake_options:
                            sf_enrichments.append("lane")
                        
                        enrichment_sources.append({
                            'type': 'snowflake_augment',
                            'database': 'AUGMENT_DW',
                            'schema': 'MARTS',
                            'enrichments': sf_enrichments
                        })
                    
                    # Update config with selected enrichment sources
                    config['enrichment']['sources'] = enrichment_sources
                    
                    # Add email if enabled
                    if enable_email and email_recipient:
                        # Get email credentials from Streamlit secrets
                        try:
                            email_config = {
                                'type': 'email',
                                'recipient': email_recipient,
                                'subject': email_subject,
                                'smtp_user': st.secrets.get("email", {}).get("SMTP_USER"),
                                'smtp_pass': st.secrets.get("email", {}).get("SMTP_PASS"),
                            }
                            
                            # Check if credentials are available
                            if email_config['smtp_user'] and email_config['smtp_pass']:
                                config['postback']['handlers'].append(email_config)
                            else:
                                st.warning("⚠️ Email credentials not configured. Skipping email delivery.")
                                st.info("Contact administrator to configure email settings.")
                        except Exception:
                            st.warning("⚠️ Email service not available in this deployment.")
                    
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
                    
                    # Step 3: Apply column mapping and enrich data
                    status_text.text("Applying column mapping...")
                    progress_bar.progress(40)
                    
                    # Apply column mapping to standardize field names
                    rows = df.to_dict('records')
                    current_mappings = getattr(st.session_state, 'column_mapping', {})
                    
                    if current_mappings:
                        mapped_rows = []
                        for row in rows:
                            mapped_row = row.copy()  # Keep all original fields
                            
                            # Apply field mappings
                            for system_field, csv_field in current_mappings.items():
                                if csv_field in row and row[csv_field] is not None:
                                    # Map CSV field to system field
                                    mapped_row[system_field] = row[csv_field]
                                    
                                    # For PRO field, also map to standard 'PRO' field name
                                    if system_field == 'pro_number':
                                        mapped_row['PRO'] = row[csv_field]
                                        
                            mapped_rows.append(mapped_row)
                        rows = mapped_rows
                        
                        mapped_fields = list(current_mappings.keys())
                        st.info(f"✅ Applied column mapping to {len(rows)} rows using: {', '.join(mapped_fields)}")
                    else:
                        st.warning("No column mapping applied - using original field names")
                    
                    status_text.text("Enriching data...")
                    progress_bar.progress(50)
                    
                    enriched_rows = enrichment_manager.enrich_rows(rows)
                    
                    # Step 4: Generate outputs and send emails
                    status_text.text("Generating output files and sending emails...")
                    progress_bar.progress(75)
                    
                    # Create file outputs
                    output_files = create_output_files(enriched_rows, output_formats)
                    
                    # Handle all postback operations (including email)
                    postback_results = {}
                    if config['postback']['handlers']:
                        router = PostbackRouter(config['postback']['handlers'])
                        postback_results = router.post_all(enriched_rows)
                    
                    # Step 5: Complete
                    status_text.text("Processing complete!")
                    progress_bar.progress(100)
                    
                    # Store results in session state
                    st.session_state.enriched_data = enriched_rows
                    st.session_state.output_files = output_files
                    st.session_state.postback_results = postback_results
                    
                    # Show success message with details
                    success_msg = f"✅ Successfully processed {len(enriched_rows)} records!"
                    
                    # Add email status to success message
                    if enable_email and 'email' in postback_results:
                        if postback_results['email']:
                            success_msg += f"\n📧 Email sent successfully to {email_recipient}"
                        else:
                            success_msg += f"\n❌ Email delivery failed"
                    
                    st.success(success_msg)
                    
                except Exception as e:
                    st.error(f"❌ Processing failed: {str(e)}")
                    logger.error(f"Processing error: {e}")
    
    # Results section
    if 'enriched_data' in st.session_state:
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
        
        # Show postback results if available
        if 'postback_results' in st.session_state and st.session_state.postback_results:
            st.subheader("Postback Status")
            postback_cols = st.columns(len(st.session_state.postback_results))
            
            for i, (handler_type, success) in enumerate(st.session_state.postback_results.items()):
                with postback_cols[i]:
                    if handler_type == 'email':
                        icon = "📧"
                    elif handler_type == 'webhook':
                        icon = "🌐"
                    else:
                        icon = "📁"
                    
                    status_text = "✅ Success" if success else "❌ Failed"
                    st.metric(f"{icon} {handler_type.title()}", status_text)
        
        # Preview enriched data
        st.subheader("Enriched Data Preview")
        enriched_df = pd.DataFrame(st.session_state.enriched_data)
        st.dataframe(enriched_df.head(10))
        
        # Download section (only show if files were generated)
        if 'output_files' in st.session_state and st.session_state.output_files:
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
        else:
            # If no files were generated but email was sent
            if 'postback_results' in st.session_state and st.session_state.postback_results.get('email'):
                st.info("📧 Results were sent via email. No download files were generated.")

if __name__ == "__main__":
    main()