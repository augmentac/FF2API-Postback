"""
FF2API - Streamlit Cloud Entry Point

This is the main entry point for Streamlit Cloud deployment.
Provides both the original FF2API functionality and the new Postback & Enrichment system.
"""

import sys
import os
import streamlit as st

# Add current directory and src to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.join(current_dir, 'src'))

# Import credential manager for email automation detection
try:
    from credential_manager import credential_manager
    from email_monitor import email_monitor
    email_monitor.credential_manager = credential_manager
except ImportError as e:
    st.error(f"Failed to import required modules: {e}")
    st.stop()

def main():
    """Main application router."""
    
    # Set up page config (only call once)
    if 'page_config_set' not in st.session_state:
        st.set_page_config(
            page_title="FF2API Platform",
            page_icon="🚚",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        st.session_state.page_config_set = True
    
    # Sidebar navigation
    with st.sidebar:
        st.title("🚚 FF2API Platform")
        
        
        # Build app options dynamically
        app_options = [
            "Unified Load Processing",
            "Postback & Enrichment System"
        ]
        
        # Navigation options
        page = st.selectbox(
            "Choose Application:",
            app_options,
            help="Select which part of the FF2API platform to use"
        )
        
        st.markdown("---")
        st.markdown("### About")
        if page == "Unified Load Processing":
            st.info("Complete load processing with field mapping, enrichment, and delivery options")
        elif page == "Postback & Enrichment System":
            st.info("Enrich existing load data without creating new loads")
    
    # Route to appropriate application
    if page == "Unified Load Processing":
        # Load unified processing system
        try:
            from src.frontend.unified_app import main as unified_main
            unified_main()
        except ImportError:
            st.error("Unified processing system not available")
            st.error("Please ensure all required modules are installed")
        except Exception as e:
            st.error("System error occurred")
            st.error(str(e))
    
    else:  # Postback & Enrichment System
        # Load postback system
        try:
            # Try to import the full postback system
            from streamlit_postback import main as postback_main
            postback_main()
        except ImportError:
            st.warning("Loading simplified version...")
            try:
                from postback_simple import main as simple_main
                simple_main()
            except ImportError:
                st.error("Postback system not available")
        except Exception as e:
            st.error("System error occurred")
            st.error(str(e))

# Handle both direct execution and module import  
if __name__ == "__main__":
    main()
else:
    # For Streamlit Cloud (when imported as module)
    main() 