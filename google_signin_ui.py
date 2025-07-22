"""
Google Sign-In UI Components for Streamlit.

Provides reusable UI components for Google Sign-In authentication flow,
including sign-in buttons, authentication status, and callback handling.
"""

import streamlit as st
import logging
from typing import Dict, Any, Optional
from google_signin_auth import google_signin_auth

logger = logging.getLogger(__name__)

class GoogleSignInUI:
    """Google Sign-In UI component manager."""
    
    @staticmethod
    def render_signin_button(brokerage_key: str, user_email_hint: str = None) -> Dict[str, Any]:
        """
        Render Google Sign-In button and handle authentication flow.
        
        Args:
            brokerage_key: Brokerage identifier
            user_email_hint: Optional email hint for better UX
            
        Returns:
            Authentication result dictionary
        """
        try:
            # Check if universal Google Sign-In is configured
            if not google_signin_auth.is_configured():
                st.error("🔧 **Universal Google Sign-In Not Configured**")
                
                with st.expander("Admin Setup Instructions"):
                    instructions = google_signin_auth.get_setup_instructions()
                    
                    st.markdown(f"### {instructions['title']}")
                    st.info(instructions['description'])
                    
                    st.markdown("**Setup Steps:**")
                    for step in instructions['steps']:
                        st.markdown(f"- {step}")
                    
                    st.markdown("**Streamlit Secrets Configuration:**")
                    st.code(instructions['secrets_example'], language='toml')
                    
                    st.markdown("**Benefits:**")
                    for benefit in instructions['benefits']:
                        st.markdown(f"- {benefit}")
                
                return {'success': False, 'message': 'Configuration required'}
            
            # Check current authentication status
            existing_email = google_signin_auth.get_user_email_for_brokerage(brokerage_key)
            
            if existing_email:
                # User is already authenticated
                st.success(f"✅ **Signed in as:** {existing_email}")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🔍 Test Gmail Connection", key=f"test_{brokerage_key}"):
                        with st.spinner("Testing connection..."):
                            test_result = gmail_auth_service.test_credentials(brokerage_key)
                            if test_result['success']:
                                st.success(f"✅ {test_result['message']}")
                                if 'total_messages' in test_result:
                                    st.info(f"📧 Total messages: {test_result['total_messages']}")
                            else:
                                st.error(f"❌ {test_result['message']}")
                
                with col2:
                    if st.button("🔓 Sign Out", key=f"signout_{brokerage_key}"):
                        if google_signin_auth.disconnect_user_from_brokerage(brokerage_key):
                            st.success("Signed out successfully")
                            st.rerun()
                        else:
                            st.error("Failed to sign out")
                
                return {
                    'success': True,
                    'authenticated': True,
                    'user_email': existing_email,
                    'message': 'User already authenticated'
                }
            
            else:
                # User needs to authenticate
                st.info("🔐 **Google Sign-In Required**")
                st.markdown("Sign in with your Google account to enable email automation for this brokerage.")
                
                # Email hint input
                if not user_email_hint:
                    user_email_hint = st.text_input(
                        "Your Gmail address (optional):",
                        placeholder="your-email@gmail.com",
                        help="This will pre-fill the Google Sign-In screen",
                        key=f"email_hint_{brokerage_key}"
                    )
                
                # Sign-In button
                if st.button("🔐 **Sign in with Google**", type="primary", key=f"signin_{brokerage_key}"):
                    auth_result = google_signin_auth.authenticate_user_for_brokerage(
                        brokerage_key, user_email_hint
                    )
                    
                    if auth_result['success']:
                        if auth_result.get('already_authenticated'):
                            st.success(auth_result['message'])
                            st.rerun()
                        else:
                            # Show authentication URL
                            st.markdown("### 🔐 **Complete Authentication**")
                            st.markdown("**Step 1:** Click the link below to sign in with Google:")
                            
                            # Create a prominent link button
                            signin_url = auth_result['signin_url']
                            st.markdown(f"""
                            <a href="{signin_url}" target="_blank" style="
                                display: inline-block;
                                background-color: #4285f4;
                                color: white;
                                padding: 12px 24px;
                                text-decoration: none;
                                border-radius: 8px;
                                font-weight: bold;
                                margin: 10px 0;
                            ">🔗 Open Google Sign-In</a>
                            """, unsafe_allow_html=True)
                            
                            st.markdown("**Step 2:** After signing in, you'll get a code. Enter it below:")
                            
                            # Handle authentication code input
                            auth_code = st.text_input(
                                "Enter the authorization code:",
                                placeholder="Paste the code from Google here",
                                key=f"auth_code_{brokerage_key}"
                            )
                            
                            if auth_code:
                                if st.button("✅ Complete Authentication", key=f"complete_{brokerage_key}"):
                                    # For now, we'll simulate the completion since we don't have the full callback flow
                                    st.info("⏳ **Note:** Full OAuth2 callback flow requires additional setup for automatic code handling.")
                                    st.markdown("""
                                    **For production deployment:**
                                    - Set up OAuth2 callback endpoint
                                    - Configure redirect URI in Google Cloud Console
                                    - Implement automatic code exchange
                                    """)
                    else:
                        st.error(f"❌ {auth_result['message']}")
                
                return {
                    'success': False,
                    'authenticated': False,
                    'message': 'Authentication required'
                }
                
        except Exception as e:
            logger.error(f"Error in Google Sign-In UI: {e}")
            st.error(f"Authentication error: {str(e)}")
            return {'success': False, 'message': str(e)}
    
    @staticmethod
    def render_authentication_status(brokerage_key: str) -> Dict[str, Any]:
        """
        Render compact authentication status indicator.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            Status dictionary
        """
        try:
            user_email = google_signin_auth.get_user_email_for_brokerage(brokerage_key)
            
            if user_email:
                st.success(f"📧 **Gmail Connected:** {user_email}")
                return {
                    'authenticated': True,
                    'user_email': user_email,
                    'status': 'connected'
                }
            else:
                st.warning("📧 **Gmail Not Connected**")
                return {
                    'authenticated': False,
                    'status': 'disconnected'
                }
                
        except Exception as e:
            logger.error(f"Error getting authentication status: {e}")
            st.error("Authentication status error")
            return {
                'authenticated': False,
                'status': 'error',
                'error': str(e)
            }
    
    @staticmethod
    def render_setup_flow(brokerage_key: str) -> Dict[str, Any]:
        """
        Render complete email automation setup flow with Google Sign-In.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            Setup result dictionary
        """
        try:
            st.markdown("### 📧 **Email Automation Setup**")
            
            # Step 1: Authentication
            with st.container():
                st.markdown("#### Step 1: Google Authentication")
                auth_result = GoogleSignInUI.render_signin_button(brokerage_key)
                
                if not auth_result['success'] or not auth_result.get('authenticated'):
                    st.info("👆 Complete Google authentication first to proceed with email automation setup.")
                    return auth_result
            
            st.success("✅ **Authentication Complete!** You can now configure email automation.")
            
            # Step 2: Email Filters Configuration
            with st.container():
                st.markdown("#### Step 2: Email Filters")
                st.info("Configure which emails to monitor for automatic processing.")
                
                sender_filter = st.text_input(
                    "Sender email filter (optional):",
                    placeholder="reports@carrier.com",
                    help="Only process emails from this sender",
                    key=f"sender_filter_{brokerage_key}"
                )
                
                subject_filter = st.text_input(
                    "Subject filter (optional):",
                    placeholder="Daily Load Report",
                    help="Only process emails with this subject pattern", 
                    key=f"subject_filter_{brokerage_key}"
                )
            
            # Step 3: Processing Configuration
            with st.container():
                st.markdown("#### Step 3: Processing Options")
                st.info("These settings will be applied to all automatically processed files.")
                
                auto_add_tracking = st.checkbox(
                    "Add tracking data automatically",
                    value=True,
                    help="Automatically enrich processed data with tracking information",
                    key=f"auto_tracking_{brokerage_key}"
                )
                
                auto_send_email = st.checkbox(
                    "Send processing results via email",
                    value=False,
                    help="Email the results after processing each file",
                    key=f"auto_email_{brokerage_key}"
                )
                
                if auto_send_email:
                    email_recipient = st.text_input(
                        "Email recipient:",
                        placeholder="ops@company.com",
                        key=f"email_recipient_{brokerage_key}"
                    )
                else:
                    email_recipient = None
                
                auto_output_format = st.selectbox(
                    "Output format:",
                    ["CSV", "Excel", "JSON", "XML"],
                    index=0,
                    key=f"output_format_{brokerage_key}"
                )
            
            # Step 4: Save Configuration
            with st.container():
                st.markdown("#### Step 4: Activate Email Automation")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("💾 **Save Configuration**", type="primary", key=f"save_config_{brokerage_key}"):
                        # Save email automation configuration
                        config = {
                            'sender_filter': sender_filter,
                            'subject_filter': subject_filter,
                            'auto_add_tracking': auto_add_tracking,
                            'auto_send_email': auto_send_email,
                            'email_recipient': email_recipient,
                            'auto_output_format': auto_output_format,
                            'active': False  # Start as inactive
                        }
                        
                        # Store configuration
                        if 'brokerage_email_configs' not in st.session_state:
                            st.session_state.brokerage_email_configs = {}
                        
                        user_email = auth_result.get('user_email')
                        st.session_state.brokerage_email_configs[brokerage_key] = {
                            'gmail_credentials': {'email': user_email},
                            'gmail_authenticated': True,
                            'inbox_filters': {
                                'sender_filter': sender_filter or None,
                                'subject_filter': subject_filter or None
                            },
                            'processing_options': {
                                'add_tracking': auto_add_tracking,
                                'send_email': auto_send_email,
                                'email_recipient': email_recipient,
                                'output_format': auto_output_format
                            },
                            'active': False,
                            'column_mappings': {}
                        }
                        
                        st.success("✅ **Configuration Saved!** Email automation is now ready.")
                        st.info("💡 Process a file manually first to configure column mappings, then activate automation.")
                        st.rerun()
                
                with col2:
                    if st.button("🔄 **Test Email Connection**", key=f"test_email_{brokerage_key}"):
                        with st.spinner("Testing Gmail connection..."):
                            # Import here to avoid circular dependency
                            from gmail_auth_service import gmail_auth_service
                            test_result = gmail_auth_service.test_credentials(brokerage_key)
                            
                            if test_result['success']:
                                st.success(f"✅ {test_result['message']}")
                                st.info(f"📧 Connected to: {test_result.get('email', 'N/A')}")
                            else:
                                st.error(f"❌ {test_result['message']}")
            
            return {
                'success': True,
                'setup_complete': True,
                'user_email': auth_result.get('user_email'),
                'brokerage_key': brokerage_key
            }
            
        except Exception as e:
            logger.error(f"Error in setup flow: {e}")
            st.error(f"Setup error: {str(e)}")
            return {'success': False, 'error': str(e)}


# Global UI instance
google_signin_ui = GoogleSignInUI()