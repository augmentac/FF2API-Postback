"""
Simple fallback postback system for Streamlit Cloud deployment.
Self-contained version with minimal dependencies.
"""

import streamlit as st
import pandas as pd
import json
import os
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, Any, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def mock_enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Simple mock enrichment function."""
    enriched = row.copy()
    
    # Add mock tracking data if carrier and PRO are present
    if 'carrier' in row and 'PRO' in row:
        enriched.update({
            'tracking_status': 'In Transit',
            'tracking_events_count': 3,
            'last_update': datetime.now().isoformat(),
            'enrichment_source': 'mock_tracking_simple',
            'enrichment_timestamp': datetime.now().isoformat()
        })
    
    return enriched

def create_csv_output(data: List[Dict[str, Any]]) -> bytes:
    """Create CSV output."""
    if not data:
        return b""
    df = pd.DataFrame(data)
    return df.to_csv(index=False).encode('utf-8')

def create_json_output(data: List[Dict[str, Any]]) -> bytes:
    """Create JSON output."""
    return json.dumps(data, indent=2, default=str).encode('utf-8')

def create_excel_output(data: List[Dict[str, Any]]) -> bytes:
    """Create Excel output."""
    if not data:
        return b""
    
    df = pd.DataFrame(data)
    
    # Create Excel file in memory
    output = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    try:
        df.to_excel(output.name, index=False)
        with open(output.name, 'rb') as f:
            excel_bytes = f.read()
        return excel_bytes
    finally:
        os.unlink(output.name)

def main():
    """Simple postback system main function."""
    
    st.title("🚚 Freight Data Enrichment & Export")
    st.markdown("Upload freight data, enrich with mock tracking, and export in multiple formats.")
    
    # File upload
    st.header("📁 Upload Data")
    uploaded_file = st.file_uploader(
        "Choose a CSV or JSON file",
        type=["csv", "json"],
        help="Upload your freight data file"
    )
    
    if uploaded_file is not None:
        try:
            # Load data
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:  # JSON
                content = json.load(uploaded_file)
                if isinstance(content, list):
                    df = pd.DataFrame(content)
                else:
                    df = pd.DataFrame([content])
            
            st.success(f"✅ Loaded {len(df)} rows successfully!")
            
            # Show preview
            st.subheader("Data Preview")
            st.dataframe(df.head())
            
            # Configuration
            st.header("⚙️ Settings")
            col1, col2 = st.columns(2)
            
            with col1:
                enable_enrichment = st.checkbox("Enable Mock Tracking Enrichment", value=True)
            
            with col2:
                output_formats = st.multiselect(
                    "Output Formats",
                    ["CSV", "JSON", "Excel"],
                    default=["CSV", "JSON"]
                )
            
            # Process button
            if st.button("🚀 Process Data", type="primary"):
                if not output_formats:
                    st.error("Please select at least one output format.")
                    return
                
                # Convert to records
                rows = df.to_dict('records')
                
                # Enrich data if enabled
                if enable_enrichment:
                    with st.spinner("Enriching data..."):
                        enriched_rows = [mock_enrich_row(row) for row in rows]
                else:
                    enriched_rows = rows
                
                # Create outputs
                output_files = {}
                
                with st.spinner("Generating output files..."):
                    if "CSV" in output_formats:
                        output_files["enriched_data.csv"] = create_csv_output(enriched_rows)
                    
                    if "JSON" in output_formats:
                        output_files["enriched_data.json"] = create_json_output(enriched_rows)
                    
                    if "Excel" in output_formats:
                        output_files["enriched_data.xlsx"] = create_excel_output(enriched_rows)
                
                st.success(f"✅ Successfully processed {len(enriched_rows)} records!")
                
                # Results section
                st.header("📊 Results")
                
                # Metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", len(enriched_rows))
                with col2:
                    if enable_enrichment:
                        st.metric("Enriched", len(enriched_rows))
                    else:
                        st.metric("Enriched", 0)
                with col3:
                    st.metric("Output Files", len(output_files))
                
                # Preview enriched data
                st.subheader("Enriched Data Preview")
                enriched_df = pd.DataFrame(enriched_rows)
                st.dataframe(enriched_df.head())
                
                # Downloads
                st.subheader("📥 Download Files")
                
                # Individual downloads
                cols = st.columns(len(output_files))
                for i, (filename, file_data) in enumerate(output_files.items()):
                    with cols[i % len(cols)]:
                        st.download_button(
                            label=f"Download {filename}",
                            data=file_data,
                            file_name=filename,
                            mime="application/octet-stream"
                        )
                
                # ZIP download if multiple files
                if len(output_files) > 1:
                    # Create ZIP
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
                            file_name=f"enriched_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                            mime="application/zip"
                        )
                    finally:
                        os.unlink(zip_buffer.name)
                        
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")
            logger.error(f"Processing error: {e}")

if __name__ == "__main__":
    main()