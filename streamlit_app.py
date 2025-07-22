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
        
        # Navigation options
        page = st.selectbox(
            "Choose Application:",
            [
                "End-to-End Load Processing", 
                "Postback & Enrichment System", 
                "FF2API - Load Processing"
            ],
            help="Select which part of the FF2API platform to use"
        )
        
        st.markdown("---")
        st.markdown("---")
        st.markdown("### About")
        if page == "FF2API - Load Processing":
            st.info("Process loads via API mapping")
        elif page == "Postback & Enrichment System":
            st.info("Enrich existing load data")
        else:  # End-to-End Load Processing
            st.info("Create new loads with enrichment")
    
    # Route to appropriate application
    if page == "End-to-End Load Processing":
        # Load end-to-end workflow system
        try:
            from streamlit_endtoend import main as endtoend_main
            endtoend_main()
        except ImportError:
            st.error("End-to-end system not available")
        except Exception as e:
            st.error("System error occurred")
            st.error(str(e))
    
    elif page == "FF2API - Load Processing":
        try:
            # Try multiple import paths for the original app
            try:
                from src.frontend.app import main as ff2api_main
                ff2api_main()
            except ImportError:
                try:
                    from frontend.app import main as ff2api_main
                    ff2api_main()
                except ImportError:
                    st.error("FF2API system not available")
                    
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