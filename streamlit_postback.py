"""
Streamlit interface for the Postback and Enrichment system - SIMPLIFIED VERSION.
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
    st.error(f"Failed to import modules: {e}")
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
                {'type': 'csv', 'output_path': './outputs/postback.csv'},
                {'type': 'xlsx', 'output_path': './outputs/postback.xlsx'},
                {'type': 'json', 'output_path': './outputs/postback.json', 'append_mode': False}
            ]
        },
        'enrichment': {
            'sources': [
                {'type': 'mock_tracking', 'generate_events': True, 'max_events': 5}
            ]
        }
    }

def auto_detect_column_mappings(csv_columns: List[str]) -> Dict[str, str]:
    """Smart auto-detection of column mappings based on common patterns."""
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

def process_data_simple(df, add_tracking, output_format, send_email, email_recipient, snowflake_options, enable_webhook, webhook_url):
    """Simplified data processing with minimal UI."""
    
    with st.spinner("Processing..."):
        try:
            # Build simple config
            config = load_default_config()
            config['workflow_type'] = 'postback'
            
            # Add enrichment if enabled
            if add_tracking:
                sf_enrichments = []
                if "Tracking Status" in snowflake_options:
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
                    'enrichments': sf_enrichments
                }]
            
            # Set output format
            config['postback']['handlers'] = [{
                'type': output_format.lower(),
                'output_path': f'/tmp/results.{output_format.lower()}'
            }]
            
            # Add email if enabled
            if send_email and email_recipient:
                try:
                    config['postback']['handlers'].append({
                        'type': 'email',
                        'recipient': email_recipient,
                        'subject': 'Data Processing Results',
                        'smtp_user': st.secrets.get("email", {}).get("SMTP_USER"),
                        'smtp_pass': st.secrets.get("email", {}).get("SMTP_PASS"),
                    })
                except:
                    st.warning("Email not configured")
            
            # Process data
            enrichment_manager = EnrichmentManager(config['enrichment']['sources'])
            postback_router = PostbackRouter(config['postback']['handlers'])
            
            # Apply column mapping
            rows = df.to_dict('records')
            current_mappings = getattr(st.session_state, 'column_mapping', {})
            
            if current_mappings:
                mapped_rows = []
                for row in rows:
                    mapped_row = row.copy()
                    for system_field, csv_field in current_mappings.items():
                        if csv_field in row and row[csv_field] is not None:
                            mapped_row[system_field] = row[csv_field]
                            if system_field == 'pro_number':
                                mapped_row['PRO'] = row[csv_field]
                    mapped_rows.append(mapped_row)
                rows = mapped_rows
            
            # Enrich and send
            enriched_rows = enrichment_manager.enrich_rows(rows)
            postback_router.send_all(enriched_rows)
            
            st.success("Processing complete")
            
            # Simple download
            if output_format == "CSV":
                csv_data = pd.DataFrame(enriched_rows).to_csv(index=False)
                st.download_button("Download CSV", csv_data, "results.csv", "text/csv")
            elif output_format == "Excel":
                import io
                excel_buffer = io.BytesIO()
                pd.DataFrame(enriched_rows).to_excel(excel_buffer, index=False)
                excel_data = excel_buffer.getvalue()
                st.download_button("Download Excel", excel_data, "results.xlsx")
            elif output_format == "JSON":
                json_data = json.dumps(enriched_rows, indent=2, default=str)
                st.download_button("Download JSON", json_data, "results.json", "application/json")
            elif output_format == "XML":
                import xml.etree.ElementTree as ET
                root = ET.Element("freight_data")
                for row in enriched_rows:
                    shipment = ET.SubElement(root, "shipment")
                    for key, value in row.items():
                        elem = ET.SubElement(shipment, key)
                        elem.text = str(value) if value is not None else ""
                xml_data = ET.tostring(root, encoding='unicode')
                st.download_button("Download XML", xml_data, "results.xml", "application/xml")
                
        except Exception as e:
            st.error("Processing failed")
            st.error(str(e))

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

def main():
    """Main Streamlit app for postback system."""
    
    st.set_page_config(
        page_title="FF2API - Postback & Enrichment",
        page_icon="🚚",
        layout="wide"
    )
    
    st.title("Postback & Enrichment")
    
    # Simplified sidebar
    with st.sidebar:
        st.header("Settings")
        
        # Essential options only
        add_tracking = st.checkbox("Add tracking data", value=True)
        send_email = st.checkbox("Email results")
        
        if send_email:
            email_recipient = st.text_input("Email address", placeholder="ops@company.com")
        else:
            email_recipient = None
        
        output_format = st.selectbox("Output format", ["CSV", "Excel", "JSON", "XML"], index=0)
        
        # Advanced options hidden  
        with st.expander("Advanced"):
            if add_tracking:
                snowflake_options = st.multiselect(
                    "Data to add:",
                    ["Tracking Status", "Customer Info", "Carrier Details", "Lane Performance"],
                    default=["Tracking Status", "Customer Info"]
                )
            else:
                snowflake_options = []
            enable_webhook = st.checkbox("Enable webhook")
            if enable_webhook:
                webhook_url = st.text_input("Webhook URL")
            else:
                webhook_url = None
    
    # Single column layout - simple flow
    st.header("Upload File")
    uploaded_file = st.file_uploader("Choose CSV or JSON file", type=["csv", "json"])
    
    if uploaded_file is not None:
        df = validate_uploaded_file(uploaded_file)
        
        if df is not None:
            st.success("File loaded")
            
            st.markdown("---")
            st.subheader("Preview") 
            st.dataframe(df.head(5))
            
            st.markdown("---")
            st.subheader("Field Mapping")
            
            # Simple auto-detection with minimal UI
            auto_mappings = auto_detect_column_mappings(list(df.columns))
            
            if auto_mappings:
                st.success("Data format detected")
                st.session_state.column_mapping = auto_mappings
                
                # Show only if there's a problem
                with st.expander("Adjust field mapping"):
                    csv_columns = ['-- Not Mapped --'] + list(df.columns)
                    manual_mapping = {}
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        manual_mapping['load_id'] = st.selectbox("Load ID field:", csv_columns,
                            index=csv_columns.index(auto_mappings.get('load_id', '-- Not Mapped --')) if auto_mappings.get('load_id') in csv_columns else 0)
                        manual_mapping['carrier'] = st.selectbox("Carrier field:", csv_columns,
                            index=csv_columns.index(auto_mappings.get('carrier', '-- Not Mapped --')) if auto_mappings.get('carrier') in csv_columns else 0)
                    
                    with col2:
                        manual_mapping['pro_number'] = st.selectbox("PRO number field:", csv_columns,
                            index=csv_columns.index(auto_mappings.get('pro_number', '-- Not Mapped --')) if auto_mappings.get('pro_number') in csv_columns else 0)
                        manual_mapping['customer_code'] = st.selectbox("Customer field:", csv_columns,
                            index=csv_columns.index(auto_mappings.get('customer_code', '-- Not Mapped --')) if auto_mappings.get('customer_code') in csv_columns else 0)
                    
                    if st.button("Apply changes"):
                        filtered_mapping = {k: v for k, v in manual_mapping.items() if v != '-- Not Mapped --'}
                        st.session_state.column_mapping = filtered_mapping
                        st.success("Mappings updated")
                        st.rerun()
                        
            else:
                st.warning("Cannot detect data format")
                load_field = st.selectbox("Which column contains load IDs?", df.columns)
                st.session_state.column_mapping = {'load_id': load_field}
            
            st.markdown("---")
            # Simple process button
            ready_to_process = getattr(st.session_state, 'column_mapping', {})
            
            if ready_to_process and (not send_email or email_recipient):
                if st.button("Process Data", type="primary", use_container_width=True):
                    process_data_simple(df, add_tracking, output_format, send_email, email_recipient, 
                                       snowflake_options, enable_webhook, webhook_url)
            else:
                if not ready_to_process:
                    st.button("Process Data", disabled=True, help="Fix data format first")
                elif send_email and not email_recipient:
                    st.button("Process Data", disabled=True, help="Enter email address")

if __name__ == "__main__":
    main()