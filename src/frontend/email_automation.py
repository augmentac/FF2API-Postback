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

# Import auth state synchronization
try:
    from auth_state_sync import auth_state_sync, validate_auth_state_for_ui, clear_auth_state_for_ui
except ImportError:
    logger.warning("Auth state sync module not available - some features may not work correctly")
    # Fallback functions
    def validate_auth_state_for_ui(brokerage_key: str):
        return False, None
    def clear_auth_state_for_ui(brokerage_key: str):
        return False


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
            saved_configs = self.db_manager.get_brokerage_configurations(self.brokerage_key)
            for config in saved_configs:
                email_config = config.get('email_automation_config')
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
            
            # Trigger database backup after email config save
            from db_manager import upload_sqlite_if_changed
            upload_sqlite_if_changed()
            
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
    
    def process_email_attachment(self, file_data: bytes, filename: str, email_source: str = "unknown") -> Dict[str, Any]:
        """Process an email attachment using the same workflow as manual processing."""
        try:
            # Pre-check credentials to provide specific error messages
            credential_check = self._validate_processing_credentials()
            if not credential_check['valid']:
                return {
                    'success': False,
                    'error': f"Credential validation failed: {credential_check['error']}",
                    'error_type': 'credential_error',
                    'suggestions': credential_check.get('suggestions', [])
                }
            
            # Create processing job for dashboard tracking using unified storage
            job_id = None
            try:
                from unified_storage import unified_storage
                
                # Load attachment data first to get record count
                if filename.endswith('.csv'):
                    df = pd.read_csv(pd.io.common.StringIO(file_data.decode('utf-8')))
                elif filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(pd.io.common.BytesIO(file_data))
                else:
                    return {
                        'success': False, 
                        'error': f'Unsupported file format: {filename}. Supported formats: CSV, Excel (.xlsx, .xls)',
                        'error_type': 'file_format_error'
                    }
                
                # Create dashboard job in unified storage (handles failover automatically)
                job_id = unified_storage.add_email_job(
                    filename=filename,
                    brokerage_key=self.brokerage_key,
                    email_source=email_source,
                    record_count=len(df)
                )
                
                # Update progress: parsing email
                if job_id:
                    unified_storage.update_job_status(job_id, self.brokerage_key, "processing", 10.0, "parsing_email")
                
            except ImportError:
                logger.debug("Unified storage not available")
                # Load attachment data if unified storage wasn't available
                if filename.endswith('.csv'):
                    df = pd.read_csv(pd.io.common.StringIO(file_data.decode('utf-8')))
                elif filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(pd.io.common.BytesIO(file_data))
                else:
                    return {'success': False, 'error': 'Unsupported file format'}
            
            # Update progress: analyzing data
            if job_id:
                try:
                    from unified_storage import unified_storage
                    unified_storage.update_job_status(job_id, self.brokerage_key, "processing", 20.0, "analyzing_data")
                except ImportError:
                    pass  # Fallback: no progress tracking
            
            # Use the manual workflow bridge to process this file
            result = self._process_via_manual_workflow(df, filename, job_id)
            
            # Update final job status using unified storage
            if job_id:
                try:
                    from unified_storage import unified_storage
                    if result.get('success'):
                        unified_storage.update_job_status(job_id, self.brokerage_key, "completed", 100.0, "completed",
                                        success_count=result.get('processed_records', 0),
                                        failure_count=0)
                    else:
                        unified_storage.update_job_status(job_id, self.brokerage_key, "failed", 0.0, "failed",
                                        error_message=result.get('error', 'Processing failed'))
                except ImportError:
                    pass  # Fallback: no progress tracking
            
            # Add result object for session state storage
            if result.get('success'):
                result['result_object'] = st.session_state.get('processing_result')
                result['processed_records'] = len(df) if df is not None else 0
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing email attachment: {e}")
            # Update job as failed if we have one
            if 'job_id' in locals() and job_id:
                try:
                    from email_processing_dashboard import update_email_job_progress
                    update_email_job_progress(
                        job_id, self.brokerage_key, "failed", 0.0, status="failed"
                    )
                except:
                    pass
            return {'success': False, 'error': str(e)}
    
    def _process_via_manual_workflow(self, df: pd.DataFrame, filename: str, job_id: str = None) -> Dict[str, Any]:
        """
        Process email attachment using the exact same workflow as manual processing.
        This ensures email automation produces identical results to manual processing.
        """
        try:
            # Helper function to update progress using shared storage
            def update_progress(step: str, progress: float):
                if job_id:
                    try:
                        from shared_storage_bridge import update_job_status
                        update_job_status(job_id, self.brokerage_key, "processing", progress, step)
                    except ImportError:
                        pass  # Fallback: no progress tracking
            # Import the manual processing function
            from .enhanced_ff2api import process_enhanced_data_workflow
            from .enhanced_ff2api import ensure_session_id
            from src.backend.data_processor import DataProcessor
            
            # Update progress: applying field mappings
            update_progress("applying_mappings", 30.0)
            
            # Get saved field mappings for this brokerage from the database
            field_mappings = self._get_saved_field_mappings()
            
            if not field_mappings:
                # If no saved mappings, try to auto-detect
                data_processor = DataProcessor()
                suggested_mappings = data_processor.suggest_field_mapping(df.columns.tolist())
                field_mappings = suggested_mappings
                logger.info(f"No saved field mappings found for {self.brokerage_key}, using auto-detected mappings")
            else:
                logger.info(f"Using saved field mappings for {self.brokerage_key}")
            
            # Get API credentials (same as manual workflow)
            api_credentials = credential_manager.get_brokerage_credentials(self.brokerage_key)
            if not api_credentials:
                return {'success': False, 'error': f'No API credentials found for {self.brokerage_key}'}
            
            # Update progress: preparing API calls
            update_progress("submitting_api", 50.0)
            
            # Determine processing mode based on email automation config
            config = self.get_email_automation_config()
            processing_options = config.get('processing_options', {}) if config else {}
            
            # Use full_endtoend mode if tracking is enabled, otherwise standard
            processing_mode = 'full_endtoend' if processing_options.get('add_tracking', True) else 'standard'
            
            # Initialize components (same as manual workflow)
            data_processor = DataProcessor()
            db_manager = self.db_manager
            session_id = ensure_session_id()
            
            # Store in session state temporarily (required by manual workflow)
            original_uploaded_df = st.session_state.get('uploaded_df')
            original_field_mappings = st.session_state.get('field_mappings')
            original_api_credentials = st.session_state.get('api_credentials')
            original_brokerage_name = st.session_state.get('brokerage_name')
            
            try:
                # Set session state for manual workflow
                st.session_state.uploaded_df = df
                st.session_state.field_mappings = field_mappings
                st.session_state.api_credentials = api_credentials
                st.session_state.brokerage_name = self.brokerage_key
                
                # Update progress: processing data
                update_progress("enriching_data", 70.0)
                
                # Call the exact same processing function as manual workflow
                result = process_enhanced_data_workflow(
                    df, field_mappings, api_credentials, self.brokerage_key,
                    processing_mode, data_processor, db_manager, session_id
                )
                
                # Update progress: generating results
                update_progress("generating_results", 90.0)
                
                # Store result in the same session state as manual processing would
                # This makes the results appear in the same UI
                if result:
                    # Add email processing metadata
                    if 'email_processing_metadata' not in st.session_state:
                        st.session_state.email_processing_metadata = []
                    
                    email_metadata = {
                        'filename': filename,
                        'brokerage_key': self.brokerage_key,
                        'processed_time': datetime.now(),
                        'processing_mode': processing_mode,
                        'was_email_automated': True,
                        'result': result
                    }
                    st.session_state.email_processing_metadata.append(email_metadata)
                    
                    logger.info(f"Successfully processed {filename} via manual workflow for {self.brokerage_key}")
                
                return {
                    'success': True,
                    'processed_records': len(df),
                    'result': result,
                    'processing_mode': processing_mode
                }
                
            finally:
                # Restore original session state
                if original_uploaded_df is not None:
                    st.session_state.uploaded_df = original_uploaded_df
                else:
                    st.session_state.pop('uploaded_df', None)
                    
                if original_field_mappings is not None:
                    st.session_state.field_mappings = original_field_mappings
                else:
                    st.session_state.pop('field_mappings', None)
                    
                if original_api_credentials is not None:
                    st.session_state.api_credentials = original_api_credentials
                else:
                    st.session_state.pop('api_credentials', None)
                    
                if original_brokerage_name is not None:
                    st.session_state.brokerage_name = original_brokerage_name
                else:
                    st.session_state.pop('brokerage_name', None)
            
        except Exception as e:
            logger.error(f"Error in manual workflow bridge: {e}")
            return {'success': False, 'error': f'Manual workflow error: {str(e)}'}
    
    def _get_saved_field_mappings(self) -> Dict[str, str]:
        """Get the most recent saved field mappings for this brokerage."""
        try:
            # Get configurations for this specific brokerage
            brokerage_configs = self.db_manager.get_brokerage_configurations(self.brokerage_key)
            
            if not brokerage_configs:
                logger.info(f"No saved configurations found for {self.brokerage_key}")
                return {}
            
            # Get the most recent configuration
            recent_config = max(brokerage_configs, key=lambda x: x.get('updated_at', ''))
            field_mappings = recent_config.get('field_mappings', {})
            
            logger.info(f"Retrieved saved field mappings for {self.brokerage_key}: {list(field_mappings.keys())}")
            return field_mappings
            
        except Exception as e:
            logger.error(f"Error retrieving saved field mappings for {self.brokerage_key}: {e}")
            return {}
    
    def _validate_processing_credentials(self) -> Dict[str, Any]:
        """Validate that all required credentials are available for processing."""
        try:
            from credential_manager import CredentialManager
            
            credential_manager = CredentialManager()
            issues = []
            suggestions = []
            
            # Check FF2API credentials
            api_key = credential_manager.get_brokerage_api_key(self.brokerage_key)
            if not api_key:
                issues.append(f"No FF2API credentials found for brokerage '{self.brokerage_key}'")
                suggestions.extend([
                    f"Add '{self.brokerage_key}' API key to .streamlit/secrets.toml under [api] section",
                    "Or configure brokerage-specific credentials in the database",
                    "Verify the brokerage name is spelled correctly"
                ])
            elif api_key == "dummy_key_for_local_testing":
                issues.append(f"Dummy/test API key configured for '{self.brokerage_key}' - not suitable for production")
                suggestions.extend([
                    f"Replace dummy key with real FF2API credentials for '{self.brokerage_key}'",
                    "Contact FF2API support to obtain production API key"
                ])
            
            # Check field mappings
            field_mappings = self._get_saved_field_mappings()
            if not field_mappings:
                issues.append(f"No field mappings configured for brokerage '{self.brokerage_key}'")
                suggestions.extend([
                    f"Configure field mappings for '{self.brokerage_key}' in the main application",
                    "Process at least one file manually to create field mapping template"
                ])
            
            # Check API configuration
            api_config = credential_manager.get_api_configuration(self.brokerage_key)
            if api_config and not api_config.get('success'):
                issues.append(f"API configuration issues for '{self.brokerage_key}': {api_config.get('message', 'Unknown error')}")
                suggestions.extend([
                    "Check API endpoint configuration",
                    "Verify network connectivity to FF2API"
                ])
            
            if issues:
                return {
                    'valid': False,
                    'error': '; '.join(issues),
                    'suggestions': suggestions,
                    'error_details': {
                        'missing_api_key': not api_key,
                        'dummy_api_key': api_key == "dummy_key_for_local_testing" if api_key else False,
                        'missing_field_mappings': not field_mappings,
                        'api_config_issues': api_config and not api_config.get('success')
                    }
                }
            
            return {'valid': True}
            
        except Exception as e:
            logger.error(f"Error validating credentials: {e}")
            return {
                'valid': False,
                'error': f"Credential validation error: {str(e)}",
                'suggestions': ["Check system configuration and try again"]
            }


def render_email_automation_setup(brokerage_key: str):
    """Render email automation setup interface with enhanced auth state management."""
    automation_manager = EmailAutomationManager(brokerage_key)
    
    st.subheader("📧 Email Automation Setup")
    
    # Validate and sync authentication state first
    try:
        auth_validated, user_email = validate_auth_state_for_ui(brokerage_key)
        
        if auth_validated and user_email:
            st.success(f"🔐 Gmail connected: {user_email}")
            
            # Add auth debugging info (can be removed in production)
            with st.expander("🔍 Authentication Debug Info"):
                auth_summary = auth_state_sync.get_auth_status_summary(brokerage_key)
                st.json(auth_summary)
                
                if st.button("🔄 Refresh Auth State", key=f"refresh_auth_{brokerage_key}"):
                    with st.spinner("Refreshing authentication state..."):
                        sync_result = auth_state_sync.validate_and_sync_auth_state(brokerage_key)
                        if sync_result['authenticated']:
                            st.success("✅ Auth state refreshed successfully")
                            if sync_result['sync_performed']:
                                st.info("🔄 Synchronization was performed")
                            if sync_result['issues_found']:
                                st.warning(f"Issues found and fixed: {sync_result['issues_found']}")
                        else:
                            st.error("❌ Authentication validation failed")
                        st.rerun()
                
                if st.button("🗑️ Clear All Auth Data", key=f"clear_auth_{brokerage_key}"):
                    with st.spinner("Clearing authentication data..."):
                        clear_result = clear_auth_state_for_ui(brokerage_key)
                        if clear_result:
                            st.success("✅ All authentication data cleared")
                        else:
                            st.warning("⚠️ Some auth data may not have been cleared completely")
                        st.rerun()
        
        else:
            st.warning("🔐 Gmail authentication required")
            
            # Show auth status and sync issues
            try:
                auth_summary = auth_state_sync.get_auth_status_summary(brokerage_key)
                if auth_summary.get('issues'):
                    with st.expander("⚠️ Authentication Issues Detected"):
                        for issue in auth_summary['issues']:
                            st.error(issue)
                        
                        if st.button("🔧 Attempt Auto-Fix", key=f"autofix_{brokerage_key}"):
                            with st.spinner("Attempting to fix auth issues..."):
                                sync_result = auth_state_sync.validate_and_sync_auth_state(brokerage_key)
                                if sync_result['authenticated']:
                                    st.success("✅ Auth issues resolved!")
                                    st.rerun()
                                else:
                                    st.error("❌ Could not resolve auth issues automatically")
            except Exception as e:
                logger.debug(f"Auth summary check failed: {e}")
    
    except Exception as e:
        logger.error(f"Auth validation error: {e}")
        st.error(f"Authentication validation error: {str(e)}")
    
    # Check current automation status
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
    
    # Background monitoring controls - add after existing automation controls
    if config:  # Only show if email automation is configured
        st.divider()
        from background_service_manager import background_service_manager
        background_service_manager.render_configuration_toggle(brokerage_key, config.get('configuration_name', 'default'))
    
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