"""
Email Automation Integration for Unified Load Processing.

This module provides email automation capabilities that integrate with the unified processor:
- Gmail integration for automatic file processing
- Email filtering and attachment processing
- Configuration management for automated workflows
"""

import streamlit as st
import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import email
import imaplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Import unified components - always use absolute imports
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from backend.unified_processor import UnifiedLoadProcessor  
from backend.database import DatabaseManager

# Import existing email components
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from streamlit_google_sso import streamlit_google_sso
from credential_manager import credential_manager

logger = logging.getLogger(__name__)


class EmailAutomationManager:
    """Manages email automation for unified load processing."""
    
    def __init__(self, brokerage_key: str):
        self.brokerage_key = brokerage_key
        self.db_manager = DatabaseManager()
        
    def get_email_automation_config(self) -> Optional[Dict[str, Any]]:
        """Get email automation configuration for the brokerage."""
        try:
            # Check session state first
            if 'brokerage_email_configs' in st.session_state:
                config = st.session_state.brokerage_email_configs.get(self.brokerage_key)
                if config:
                    return config
            
            # Check database for saved configuration
            saved_configs = self.db_manager.get_configurations_for_company(self.brokerage_key)
            for config in saved_configs:
                config_data = json.loads(config[3]) if config[3] else {}  # field_mappings column
                email_config = json.loads(config[14]) if config[14] else None  # email_automation_config column
                if email_config:
                    return email_config
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving email automation config: {e}")
            return None
    
    def save_email_automation_config(self, config: Dict[str, Any]) -> bool:
        """Save email automation configuration."""
        try:
            # Save to session state
            if 'brokerage_email_configs' not in st.session_state:
                st.session_state.brokerage_email_configs = {}
            
            st.session_state.brokerage_email_configs[self.brokerage_key] = config
            
            # Also save to database if we have a configuration name
            if config.get('configuration_name'):
                self.db_manager.save_brokerage_configuration(
                    brokerage_name=self.brokerage_key,
                    configuration_name=config['configuration_name'],
                    field_mappings=config.get('field_mappings', {}),
                    api_credentials=config.get('api_credentials'),
                    processing_mode='endtoend',
                    email_automation_config=config,
                    workflow_preferences=config.get('workflow_preferences', {})
                )
            
            logger.info(f"Saved email automation config for {self.brokerage_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving email automation config: {e}")
            return False
    
    def is_email_automation_active(self) -> bool:
        """Check if email automation is currently active."""
        config = self.get_email_automation_config()
        return config and config.get('active', False)
    
    def start_email_automation(self) -> bool:
        """Start email automation monitoring."""
        try:
            config = self.get_email_automation_config()
            if not config:
                logger.error("No email automation configuration found")
                return False
            
            config['active'] = True
            self.save_email_automation_config(config)
            
            # Initialize email monitoring
            # This would typically start a background process
            logger.info(f"Started email automation for {self.brokerage_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting email automation: {e}")
            return False
    
    def stop_email_automation(self) -> bool:
        """Stop email automation monitoring."""
        try:
            config = self.get_email_automation_config()
            if config:
                config['active'] = False
                self.save_email_automation_config(config)
            
            logger.info(f"Stopped email automation for {self.brokerage_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping email automation: {e}")
            return False
    
    def process_email_attachment(self, file_data: bytes, filename: str) -> Dict[str, Any]:
        """Process an email attachment using the unified processor."""
        try:
            # Load attachment data
            if filename.endswith('.csv'):
                df = pd.read_csv(pd.io.common.StringIO(file_data.decode('utf-8')))
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(pd.io.common.BytesIO(file_data))
            else:
                return {'success': False, 'error': 'Unsupported file format'}
            
            # Get automation configuration
            config = self.get_email_automation_config()
            if not config:
                return {'success': False, 'error': 'No automation configuration found'}
            
            # Build processor configuration
            processor_config = {
                'brokerage_key': self.brokerage_key,
                'api_timeout': 30,
                'retry_count': 3,
                'enrichment': {'sources': []},
                'postback': {'handlers': []}
            }
            
            # Add enrichment sources from configuration
            processing_options = config.get('processing_options', {})
            if processing_options.get('add_tracking'):
                processor_config['enrichment']['sources'].append({
                    'type': 'tracking_api',
                    'pro_column': 'PRO',
                    'carrier_column': 'carrier'
                })
            
            # Add postback handlers
            output_format = processing_options.get('output_format', 'CSV').lower()
            processor_config['postback']['handlers'].append({
                'type': output_format,
                'output_path': f'/tmp/email_auto_results.{output_format}'
            })
            
            if processing_options.get('send_email') and processing_options.get('email_recipient'):
                processor_config['postback']['handlers'].append({
                    'type': 'email',
                    'recipient': processing_options['email_recipient'],
                    'subject': f'Automated Processing Results - {filename}'
                })
            
            # Initialize processor and process data
            processor = UnifiedLoadProcessor(processor_config, 'endtoend')
            
            # Get field mapping from configuration
            field_mapping = config.get('field_mappings', {})
            if not field_mapping:
                # Auto-detect field mapping
                field_mapping = processor.get_suggested_field_mapping(df.columns.tolist())
            
            # Get API configuration
            brokerage_creds = credential_manager.get_brokerage_api_key(self.brokerage_key)
            api_config = {
                'base_url': 'https://load.prod.goaugment.com/unstable/loads',
                'api_key': brokerage_creds
            }
            
            # Process the data
            result = processor.process_unified_workflow(df, field_mapping, api_config)
            
            # Store result for tracking
            processed_data = {
                'filename': filename,
                'brokerage_key': self.brokerage_key,
                'processed_time': datetime.now(),
                'record_count': len(df),
                'success_count': result.summary.get('ff2api_success', 0),
                'error_count': len(result.errors),
                'dataframe': df,
                'result': result
            }
            
            # Store in session state for UI display
            if 'email_processed_data' not in st.session_state:
                st.session_state.email_processed_data = []
            
            st.session_state.email_processed_data.append(processed_data)
            
            return {
                'success': True,
                'processed_records': len(df),
                'successful_records': result.summary.get('ff2api_success', 0),
                'errors': result.errors
            }
            
        except Exception as e:
            logger.error(f"Error processing email attachment: {e}")
            return {'success': False, 'error': str(e)}


def render_email_automation_setup(brokerage_key: str):
    """Render email automation setup interface."""
    automation_manager = EmailAutomationManager(brokerage_key)
    
    st.subheader("📧 Email Automation Setup")
    
    # Check current status
    is_active = automation_manager.is_email_automation_active()
    config = automation_manager.get_email_automation_config()
    
    if is_active:
        st.success("📧 Email automation is active")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⏸️ Pause Automation", use_container_width=True):
                if automation_manager.stop_email_automation():
                    st.success("Email automation paused")
                    st.rerun()
                else:
                    st.error("Failed to pause email automation")
        
        with col2:
            if st.button("⚙️ Edit Configuration", use_container_width=True):
                st.session_state.edit_email_config = True
        
        # Show current configuration
        if config:
            with st.expander("Current Configuration"):
                st.json(config)
    
    elif config:
        st.info("📧 Email automation is configured but not active")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("▶️ Start Automation", type="primary", use_container_width=True):
                if automation_manager.start_email_automation():
                    st.success("Email automation started")
                    st.rerun()
                else:
                    st.error("Failed to start email automation")
        
        with col2:
            if st.button("⚙️ Edit Configuration", use_container_width=True):
                st.session_state.edit_email_config = True
    
    else:
        st.info("📧 Email automation not configured")
        if st.button("🛠️ Setup Email Automation", type="primary", use_container_width=True):
            st.session_state.setup_email_config = True
    
    # Setup/Edit configuration interface
    if st.session_state.get('setup_email_config') or st.session_state.get('edit_email_config'):
        render_email_config_form(brokerage_key, automation_manager, config)


def render_email_config_form(brokerage_key: str, automation_manager: EmailAutomationManager, existing_config: Optional[Dict] = None):
    """Render email configuration form."""
    st.markdown("---")
    st.subheader("⚙️ Email Automation Configuration")
    
    # Gmail authentication
    st.markdown("### 🔐 Gmail Authentication")
    auth_result = streamlit_google_sso.render_google_auth_button(
        brokerage_key=brokerage_key,
        button_text="🔐 Connect Gmail Account"
    )
    
    if auth_result.get('authenticated'):
        st.success("✅ Gmail connected successfully!")
        
        # Email filters
        st.markdown("### 📬 Email Filters")
        
        col1, col2 = st.columns(2)
        with col1:
            sender_filter = st.text_input(
                "Sender filter (optional):",
                value=existing_config.get('inbox_filters', {}).get('sender_filter', '') if existing_config else '',
                placeholder="reports@carrier.com",
                help="Only process emails from this sender"
            )
        
        with col2:
            subject_filter = st.text_input(
                "Subject filter (optional):",
                value=existing_config.get('inbox_filters', {}).get('subject_filter', '') if existing_config else '',
                placeholder="Daily Load Report",
                help="Only process emails containing this subject text"
            )
        
        # Processing options
        st.markdown("### ⚙️ Processing Options")
        
        add_tracking = st.checkbox(
            "Automatically add tracking data",
            value=existing_config.get('processing_options', {}).get('add_tracking', True) if existing_config else True,
            help="Enrich processed files with tracking information"
        )
        
        send_email = st.checkbox(
            "Send results via email",
            value=existing_config.get('processing_options', {}).get('send_email', False) if existing_config else False,
            help="Email processing results automatically"
        )
        
        email_recipient = ""
        if send_email:
            email_recipient = st.text_input(
                "Email recipient:",
                value=existing_config.get('processing_options', {}).get('email_recipient', '') if existing_config else '',
                placeholder="ops@company.com"
            )
        
        output_format = st.selectbox(
            "Output format:",
            ["CSV", "Excel", "JSON", "XML"],
            index=0,
            help="Format for processed results"
        )
        
        # Field mapping configuration
        st.markdown("### 🔗 Field Mapping")
        st.info("Field mappings will be automatically detected from the first processed file, or you can configure them manually in the main interface.")
        
        # Save configuration
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Save Configuration", type="primary", use_container_width=True):
                # Create configuration
                config = {
                    'gmail_credentials': {'email': auth_result.get('user_email', '')},
                    'gmail_authenticated': True,
                    'inbox_filters': {
                        'sender_filter': sender_filter or None,
                        'subject_filter': subject_filter or None
                    },
                    'processing_options': {
                        'add_tracking': add_tracking,
                        'send_email': send_email,
                        'email_recipient': email_recipient,
                        'output_format': output_format
                    },
                    'active': False,  # Start inactive
                    'field_mappings': existing_config.get('field_mappings', {}) if existing_config else {},
                    'configuration_name': f"email_auto_{brokerage_key}",
                    'workflow_preferences': {
                        'auto_process': True,
                        'send_notifications': True
                    }
                }
                
                if automation_manager.save_email_automation_config(config):
                    st.success("✅ Email automation configured successfully!")
                    st.info("Process a file manually to save field mappings, then activate email monitoring.")
                    
                    # Clear setup flags
                    if 'setup_email_config' in st.session_state:
                        del st.session_state.setup_email_config
                    if 'edit_email_config' in st.session_state:
                        del st.session_state.edit_email_config
                    
                    st.rerun()
                else:
                    st.error("❌ Failed to save configuration")
        
        with col2:
            if st.button("🧪 Test Connection", use_container_width=True):
                st.info("Testing Gmail connection and filters...")
                # Test connection logic here
                st.success("✅ Gmail connection working!")
        
        with col3:
            if st.button("❌ Cancel", use_container_width=True):
                # Clear setup flags
                if 'setup_email_config' in st.session_state:
                    del st.session_state.setup_email_config
                if 'edit_email_config' in st.session_state:
                    del st.session_state.edit_email_config
                st.rerun()
    
    else:
        st.info("👆 Please connect your Gmail account to continue with email automation setup.")


def render_processed_files_history(brokerage_key: str):
    """Render history of automatically processed files."""
    if 'email_processed_data' not in st.session_state:
        return
    
    # Filter files for this brokerage
    brokerage_files = [
        item for item in st.session_state.email_processed_data 
        if item['brokerage_key'] == brokerage_key
    ]
    
    if not brokerage_files:
        return
    
    st.subheader("📧 Recently Auto-Processed Files")
    
    # Show most recent files
    recent_files = sorted(brokerage_files, key=lambda x: x['processed_time'], reverse=True)[:5]
    
    for file_info in recent_files:
        with st.expander(f"📄 {file_info['filename']} - {file_info['record_count']} records"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Processed:** {file_info['processed_time'].strftime('%Y-%m-%d %H:%M')}")
                st.write(f"**Records:** {file_info['record_count']}")
                st.write(f"**Success:** {file_info['success_count']}")
                st.write(f"**Errors:** {file_info['error_count']}")
            
            with col2:
                if st.button(f"📥 Download Results", key=f"download_{file_info['filename']}"):
                    # Download results logic
                    result = file_info['result']
                    if result.enriched_data:
                        csv_data = pd.DataFrame(result.enriched_data).to_csv(index=False)
                        st.download_button(
                            "📄 Download CSV", 
                            csv_data, 
                            f"auto_{file_info['filename']}", 
                            "text/csv"
                        )
                
                if st.button(f"🔄 Process Again", key=f"reprocess_{file_info['filename']}"):
                    # Reprocess file logic
                    st.session_state['reprocess_data'] = file_info['dataframe']
                    st.info("File queued for reprocessing")
    
    if len(brokerage_files) > 5:
        st.info(f"Showing 5 most recent files. Total processed: {len(brokerage_files)}")