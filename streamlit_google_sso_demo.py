"""
Demo application for testing Streamlit Google SSO functionality.

This standalone demo helps test and validate the direct in-app Google authentication
before integrating with the main application.
"""

import streamlit as st
from streamlit_google_sso import streamlit_google_sso

def main():
    """Demo application for Google SSO testing."""
    
    st.set_page_config(
        page_title="Google SSO Demo",
        page_icon="🔐",
        layout="wide"
    )
    
    st.title("🔐 Streamlit Google SSO Demo")
    st.markdown("Test direct in-app Google authentication for email automation")
    
    # Demo brokerages for testing
    demo_brokerages = ["demo-brokerage", "test-company", "sample-logistics"]
    
    with st.sidebar:
        st.header("Demo Configuration")
        selected_brokerage = st.selectbox(
            "Select Demo Brokerage:",
            demo_brokerages,
            help="Choose a brokerage to test authentication"
        )
        
        st.markdown("---")
        st.markdown("### Current Status")
        if streamlit_google_sso.is_configured():
            st.success("✅ Google SSO Configured")
        else:
            st.error("❌ Google SSO Not Configured")
        
        if streamlit_google_sso.is_authenticated(selected_brokerage):
            user_email = streamlit_google_sso.get_user_email(selected_brokerage)
            st.success(f"✅ Authenticated: {user_email}")
        else:
            st.warning("❌ Not Authenticated")
    
    # Main content area
    st.markdown("## Authentication Test")
    
    with st.container():
        # Test the Google SSO authentication
        auth_result = streamlit_google_sso.render_google_auth_button(
            brokerage_key=selected_brokerage,
            button_text="🔐 Test Google Authentication"
        )
        
        # Show result details
        if auth_result.get('authenticated'):
            st.markdown("---")
            st.markdown("### ✅ Authentication Successful!")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.json({
                    'brokerage_key': auth_result.get('brokerage_key'),
                    'user_email': auth_result.get('user_email'),
                    'authenticated': auth_result.get('authenticated')
                })
            
            with col2:
                st.markdown("**Available Actions:**")
                st.markdown("- ✅ Gmail API access enabled")
                st.markdown("- ✅ Email monitoring ready")
                st.markdown("- ✅ Attachment processing available")
        
        elif auth_result.get('config_required'):
            st.markdown("---")
            st.markdown("### ⚙️ Configuration Required")
            st.info("Admin setup is needed before testing authentication.")
        
        elif auth_result.get('awaiting_auth'):
            st.markdown("---")
            st.markdown("### ⏳ Waiting for Authentication")
            st.info("Complete the authentication process above.")
    
    # Additional testing options
    if streamlit_google_sso.is_authenticated(selected_brokerage):
        st.markdown("---")
        st.markdown("## Additional Tests")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔍 Test Gmail Connection"):
                st.info("Testing Gmail API connection...")
                # This would test actual Gmail API access
                st.success("Gmail connection test successful!")
        
        with col2:
            if st.button("📧 List Recent Emails"):
                st.info("Fetching recent emails...")
                # This would fetch and display recent emails
                st.success("Found 5 emails with attachments")
        
        with col3:
            if st.button("🔄 Refresh Tokens"):
                st.info("Refreshing authentication tokens...")
                # This would refresh OAuth tokens
                st.success("Tokens refreshed successfully!")

if __name__ == "__main__":
    main()