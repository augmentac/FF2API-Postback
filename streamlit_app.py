"""
FF2API - Streamlit Cloud Entry Point

This is the main entry point for Streamlit Cloud deployment.
Provides both the original FF2API functionality and the new Postback & Enrichment system.
"""

import sys
import os
import streamlit as st

# Add src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def main():
    """Main application router."""
    
    # Set up page config
    st.set_page_config(
        page_title="FF2API Platform",
        page_icon="🚚",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Sidebar navigation
    with st.sidebar:
        st.title("🚚 FF2API Platform")
        
        # Navigation options
        page = st.selectbox(
            "Choose Application:",
            ["FF2API - Load Processing", "Postback & Enrichment System"],
            help="Select which part of the FF2API platform to use"
        )
        
        st.markdown("---")
        st.markdown("### About")
        if page == "FF2API - Load Processing":
            st.info("📊 Process and upload freight loads to various APIs with smart mapping capabilities.")
        else:
            st.info("🔄 Enrich freight data with tracking information and export to multiple formats.")
    
    # Route to appropriate application
    if page == "FF2API - Load Processing":
        try:
            from src.frontend.app import main as ff2api_main
            ff2api_main()
        except ImportError as e:
            st.error(f"Failed to load FF2API application: {e}")
            st.info("Please ensure all dependencies are installed.")
    else:
        try:
            from streamlit_postback import main as postback_main
            postback_main()
        except ImportError as e:
            st.error(f"Failed to load Postback application: {e}")
            st.info("Please ensure all postback dependencies are installed.")

# Handle both direct execution and module import
if __name__ == "__main__":
    main()
else:
    # For Streamlit Cloud (when imported as module)
    main() 