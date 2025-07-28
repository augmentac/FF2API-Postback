"""
Email Monitor Service for Automatic Load Processing.

Monitors Gmail inboxes for specific brokerages and automatically processes
attachments through the end-to-end load processing workflow.

Features:
- Gmail API integration with OAuth2 authentication
- Brokerage-specific inbox monitoring
- Automatic file attachment processing
- Saved mapping configuration application
- Background monitoring with Streamlit integration
"""

import logging
import time
import json
import base64
import io
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import threading
from pathlib import Path
import requests
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

@dataclass
class EmailAttachment:
    """Represents a processed email attachment."""
    filename: str
    content: bytes
    mime_type: str
    email_id: str
    sender: str
    subject: str
    received_time: datetime

@dataclass
class ProcessingResult:
    """Result of automatic email processing."""
    success: bool
    message: str
    processed_count: int
    error_details: Optional[str] = None
    file_info: Optional[Dict[str, Any]] = None

class EmailMonitorService:
    """Gmail monitoring service for automatic load processing."""
    
    def __init__(self, credential_manager):
        """
        Initialize email monitor service.
        
        Args:
            credential_manager: CredentialManager instance
        """
        self.credential_manager = credential_manager
        self.monitoring_active = False
        self.monitor_thread = None
        self.processing_callbacks = {}
        self.last_check_times = {}
        self.oauth_credentials = {}  # Store OAuth credentials by brokerage
        self.monitored_brokerages = []  # Track actively monitored brokerages
    
    def configure_oauth_monitoring(self, brokerage_key: str, oauth_credentials: Dict[str, Any], email_filters: Dict[str, str]) -> Dict[str, Any]:
        """
        Configure OAuth-based email monitoring for a specific brokerage.
        
        Args:
            brokerage_key: Brokerage identifier
            oauth_credentials: OAuth credentials from session state
            email_filters: Email filtering configuration
            
        Returns:
            Configuration result with success status
        """
        try:
            logger.info(f"Configuring OAuth monitoring for brokerage: {brokerage_key}")
            
            # Validate OAuth credentials
            if not oauth_credentials.get('authenticated') or not oauth_credentials.get('oauth_active'):
                return {
                    'success': False,
                    'message': 'Invalid OAuth credentials - authentication required'
                }
            
            user_email = oauth_credentials.get('user_email')
            if not user_email or user_email == 'user@gmail.com':
                return {
                    'success': False,
                    'message': 'Invalid user email in OAuth credentials'
                }
            
            # Store OAuth credentials for this brokerage
            self.oauth_credentials[brokerage_key] = {
                'user_email': user_email,
                'access_token': oauth_credentials.get('access_token', ''),
                'refresh_token': oauth_credentials.get('refresh_token', ''),
                'brokerage_key': brokerage_key,
                'configured_at': datetime.now().isoformat(),
                'email_filters': email_filters
            }
            
            # Add to monitored brokerages if not already present
            if brokerage_key not in self.monitored_brokerages:
                self.monitored_brokerages.append(brokerage_key)
            
            logger.info(f"OAuth monitoring configured for {brokerage_key} ({user_email})")
            logger.info(f"Email filters: {email_filters}")
            logger.info(f"Monitored brokerages: {self.monitored_brokerages}")
            
            return {
                'success': True,
                'message': f'OAuth monitoring configured for {user_email}',
                'brokerage_key': brokerage_key,
                'user_email': user_email
            }
            
        except Exception as e:
            logger.error(f"Failed to configure OAuth monitoring for {brokerage_key}: {e}")
            return {
                'success': False,
                'message': f'Configuration failed: {str(e)}'
            }
        
    def start_monitoring(self, brokerages: List[str] = None) -> Dict[str, Any]:
        """
        Start background email monitoring for specified brokerages.
        
        Args:
            brokerages: List of brokerage keys to monitor, or None for OAuth-configured ones
            
        Returns:
            Dictionary with success status and details
        """
        try:
            if self.monitoring_active:
                logger.warning("Email monitoring already active")
                return {
                    'success': True,
                    'message': 'Email monitoring already running',
                    'monitored_brokerages': self.monitored_brokerages
                }
                
            # Use OAuth-configured brokerages if no specific list provided
            if brokerages is None:
                brokerages = self.monitored_brokerages
            
            if not brokerages:
                # Check if we have any OAuth credentials
                if self.oauth_credentials:
                    brokerages = list(self.oauth_credentials.keys())
                else:
                    logger.warning("No brokerages configured for OAuth email monitoring")
                    return {
                        'success': False,
                        'message': 'No brokerages configured for OAuth monitoring'
                    }
            
            # Validate that we have OAuth credentials for all brokerages
            missing_oauth = [b for b in brokerages if b not in self.oauth_credentials]
            if missing_oauth:
                return {
                    'success': False,
                    'message': f'Missing OAuth credentials for brokerages: {missing_oauth}'
                }
            
            # Start monitoring thread
            self.monitoring_active = True
            self.monitored_brokerages = brokerages
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop,
                args=(brokerages,),
                daemon=True
            )
            self.monitor_thread.start()
            
            logger.info(f"OAuth email monitoring started for brokerages: {brokerages}")
            return {
                'success': True,
                'message': f'Monitoring started for {len(brokerages)} brokerage(s)',
                'monitored_brokerages': brokerages
            }
            
        except Exception as e:
            logger.error(f"Failed to start email monitoring: {e}")
            self.monitoring_active = False
            return {
                'success': False,
                'message': f'Failed to start monitoring: {str(e)}'
            }
    
    def stop_monitoring(self):
        """Stop background email monitoring."""
        if self.monitoring_active:
            self.monitoring_active = False
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5)
            logger.info("Email monitoring stopped")
    
    def register_processing_callback(self, brokerage_key: str, callback: Callable):
        """
        Register callback function for processing attachments.
        
        Args:
            brokerage_key: Brokerage identifier
            callback: Function to call with (attachment, brokerage_config)
        """
        self.processing_callbacks[brokerage_key] = callback
        logger.info(f"Processing callback registered for {brokerage_key}")
    
    def check_inbox_now(self, brokerage_key: str) -> ProcessingResult:
        """
        Manually check inbox for a specific brokerage using OAuth credentials.
        
        Args:
            brokerage_key: Brokerage to check
            
        Returns:
            ProcessingResult with details
        """
        try:
            # Get OAuth credentials for this brokerage
            oauth_creds = self.oauth_credentials.get(brokerage_key)
            if not oauth_creds:
                return ProcessingResult(
                    success=False,
                    message=f"No OAuth credentials configured for {brokerage_key}",
                    processed_count=0
                )
            
            # Get Gmail API headers using OAuth
            gmail_headers = self._get_gmail_service(brokerage_key)
            if not gmail_headers:
                return ProcessingResult(
                    success=False,
                    message="Failed to connect to Gmail - OAuth token may be expired or invalid. Try disconnecting and reconnecting Gmail.",
                    processed_count=0
                )
            
            # Use email filters from OAuth configuration
            config = {
                'inbox_filters': oauth_creds.get('email_filters', {}),
                'brokerage_key': brokerage_key,
                'user_email': oauth_creds.get('user_email')
            }
            
            # Check for new emails
            attachments = self._check_for_attachments(gmail_headers, config, brokerage_key)
            
            if not attachments:
                return ProcessingResult(
                    success=True,
                    message="No new files found",
                    processed_count=0
                )
            
            # Process attachments
            processed_count = 0
            for attachment in attachments:
                if self._process_attachment(attachment, brokerage_key, config):
                    processed_count += 1
            
            return ProcessingResult(
                success=True,
                message=f"Processed {processed_count} files from {oauth_creds.get('user_email')}",
                processed_count=processed_count
            )
            
        except Exception as e:
            logger.error(f"Error checking inbox for {brokerage_key}: {e}")
            return ProcessingResult(
                success=False,
                message=f"Error: {str(e)}",
                processed_count=0,
                error_details=str(e)
            )
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status for OAuth-configured brokerages."""
        oauth_brokerages = list(self.oauth_credentials.keys())
        
        status = {
            'monitoring_active': self.monitoring_active,
            'monitored_brokerages': self.monitored_brokerages,
            'oauth_configured_brokerages': oauth_brokerages,
            'last_check_times': self.last_check_times.copy(),
            'callback_count': len(self.processing_callbacks),
            'oauth_credentials_count': len(self.oauth_credentials)
        }
        
        return status
    
    def _monitor_loop(self, brokerages: List[str]):
        """Main monitoring loop running in background thread."""
        logger.info("Email monitoring loop started")
        
        while self.monitoring_active:
            try:
                for brokerage_key in brokerages:
                    if not self.monitoring_active:
                        break
                        
                    # Check if enough time has passed since last check
                    last_check = self.last_check_times.get(brokerage_key, datetime.min)
                    check_interval = timedelta(minutes=5)  # Check every 5 minutes
                    
                    if datetime.now() - last_check >= check_interval:
                        logger.debug(f"Checking inbox for {brokerage_key}")
                        result = self.check_inbox_now(brokerage_key)
                        self.last_check_times[brokerage_key] = datetime.now()
                        
                        if result.processed_count > 0:
                            logger.info(f"Auto-processed {result.processed_count} files for {brokerage_key}")
                
                # Sleep between monitoring cycles
                if self.monitoring_active:
                    time.sleep(30)  # Check every 30 seconds
                    
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(60)  # Wait longer on error
        
        logger.info("Email monitoring loop stopped")
    
    def _get_email_automation_brokerages(self) -> List[str]:
        """Get list of brokerages with email automation configured."""
        try:
            email_automation = st.secrets.get("email_automation", {})
            brokerages = []
            
            for key in email_automation.keys():
                # Convert normalized key back to brokerage format
                brokerage_key = key.replace('_', '-')
                brokerages.append(brokerage_key)
            
            return brokerages
            
        except Exception as e:
            logger.error(f"Error getting email automation brokerages: {e}")
            return []
    
    def _get_gmail_service(self, brokerage_key: str):
        """
        Get Gmail API access for a brokerage using OAuth credentials.
        
        Args:
            brokerage_key: Brokerage identifier
            
        Returns:
            Gmail service headers or None
        """
        try:
            # First try to get OAuth credentials from streamlit_google_sso
            try:
                from streamlit_google_sso import streamlit_google_sso
                auth_data = streamlit_google_sso._get_stored_auth(brokerage_key)
                
                if auth_data and auth_data.get('access_token'):
                    access_token = auth_data.get('access_token')
                    logger.info(f"Using OAuth access token from streamlit_google_sso for {brokerage_key}")
                    
                    # Test the token by making a simple API call
                    test_headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Content-Type': 'application/json'
                    }
                    
                    # Quick test to verify token works
                    test_url = "https://gmail.googleapis.com/gmail/v1/users/me/profile"
                    test_response = requests.get(test_url, headers=test_headers)
                    
                    if test_response.status_code == 200:
                        logger.info("OAuth token verified successfully")
                        return test_headers
                    else:
                        logger.warning(f"OAuth token test failed: {test_response.status_code}")
                        
            except Exception as e:
                logger.warning(f"Could not get OAuth token from streamlit_google_sso: {e}")
            
            # Fallback to stored OAuth credentials in this service
            oauth_creds = self.oauth_credentials.get(brokerage_key)
            if not oauth_creds:
                logger.error(f"No OAuth credentials found for {brokerage_key}")
                return None
            
            access_token = oauth_creds.get('access_token')
            if not access_token:
                logger.error(f"No access token found in stored credentials for {brokerage_key}")
                return None
            
            # Return headers for Gmail API calls
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            logger.info(f"Using stored OAuth access token for {brokerage_key}")
            return headers
            
        except Exception as e:
            logger.error(f"Failed to get Gmail service for {brokerage_key}: {e}")
            return None
    
    def _check_for_attachments(self, gmail_headers: Dict[str, str], config: Dict[str, Any], brokerage_key: str) -> List[EmailAttachment]:
        """
        Check for new email attachments using Gmail API.
        
        Args:
            gmail_headers: Gmail API authorization headers
            config: Email automation configuration
            brokerage_key: Brokerage identifier
            
        Returns:
            List of new attachments to process
        """
        try:
            # Build search query
            query_parts = []
            
            # Add sender filter
            sender_filter = config.get('inbox_filters', {}).get('sender_filter')
            if sender_filter:
                query_parts.append(f"from:{sender_filter}")
            
            # Add subject filter
            subject_filter = config.get('inbox_filters', {}).get('subject_filter')
            if subject_filter:
                query_parts.append(f"subject:{subject_filter}")
            
            # Only look for emails with attachments
            query_parts.append("has:attachment")
            
            # Temporarily remove time restriction for testing
            # query_parts.append("newer_than:7d")
            
            query = " ".join(query_parts)
            logger.info(f"Gmail search query: {query}")
            print(f"DEBUG: Gmail search query: {query}")  # Debug output
            
            # Search for messages
            search_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q={query}"
            response = requests.get(search_url, headers=gmail_headers)
            
            if response.status_code != 200:
                logger.error(f"Gmail search failed: {response.status_code} - {response.text}")
                print(f"DEBUG: Gmail search failed: {response.status_code} - {response.text}")
                return []
            
            search_results = response.json()
            messages = search_results.get('messages', [])
            
            print(f"DEBUG: Found {len(messages)} messages matching search criteria")
            print(f"DEBUG: Full search response: {search_results}")
            logger.info(f"Found {len(messages)} messages matching search criteria")
            
            if not messages:
                logger.info(f"No new emails found for {brokerage_key}")
                print(f"DEBUG: No messages found. Search criteria: {query}")
                print(f"DEBUG: Raw Gmail response: {search_results}")
                
                # Let's also try a simpler search to see if Gmail API is working at all
                simple_query = "has:attachment"
                simple_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages?q={simple_query}&maxResults=5"
                simple_response = requests.get(simple_url, headers=gmail_headers)
                if simple_response.status_code == 200:
                    simple_results = simple_response.json()
                    print(f"DEBUG: Simple search 'has:attachment' found {len(simple_results.get('messages', []))} messages")
                else:
                    print(f"DEBUG: Simple search failed: {simple_response.status_code}")
                
                return []
            
            # Process each message
            attachments = []
            for message in messages[:10]:  # Limit to 10 most recent
                message_attachments = self._process_message_for_attachments(
                    message['id'], gmail_headers, brokerage_key
                )
                attachments.extend(message_attachments)
            
            logger.info(f"Found {len(attachments)} attachments for {brokerage_key}")
            return attachments
            
        except Exception as e:
            logger.error(f"Error checking attachments for {brokerage_key}: {e}")
            return []
    
    def _process_message_for_attachments(self, message_id: str, gmail_headers: Dict[str, str], brokerage_key: str) -> List[EmailAttachment]:
        """Process a single Gmail message for attachments."""
        try:
            # Get message details
            message_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
            response = requests.get(message_url, headers=gmail_headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to get message {message_id}: {response.status_code}")
                return []
            
            message_data = response.json()
            
            # Extract email metadata
            headers = {h['name']: h['value'] for h in message_data.get('payload', {}).get('headers', [])}
            sender = headers.get('From', 'Unknown')
            subject = headers.get('Subject', 'No Subject')
            date_str = headers.get('Date', '')
            
            # Parse date
            try:
                from email.utils import parsedate_to_datetime
                received_time = parsedate_to_datetime(date_str)
            except:
                received_time = datetime.now()
            
            # Find attachments
            attachments = []
            payload = message_data.get('payload', {})
            
            # Check if message has parts (multipart)
            if 'parts' in payload:
                for part in payload['parts']:
                    if part.get('filename') and part.get('body', {}).get('attachmentId'):
                        attachment = self._download_attachment(
                            message_id, part, gmail_headers, sender, subject, received_time
                        )
                        if attachment:
                            attachments.append(attachment)
            
            return attachments
            
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            return []
    
    def _download_attachment(self, message_id: str, part: Dict[str, Any], gmail_headers: Dict[str, str], 
                           sender: str, subject: str, received_time: datetime) -> Optional[EmailAttachment]:
        """Download Gmail attachment."""
        try:
            filename = part.get('filename', 'unknown')
            attachment_id = part.get('body', {}).get('attachmentId')
            mime_type = part.get('mimeType', 'application/octet-stream')
            
            if not attachment_id:
                return None
            
            # Process CSV, JSON, and Excel files
            excel_mime_types = [
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # .xlsx
                'application/vnd.ms-excel'  # .xls
            ]
            supported_extensions = ('.csv', '.json', '.xlsx', '.xls')
            
            if not (mime_type in ['text/csv', 'application/csv', 'application/json'] + excel_mime_types or 
                   filename.lower().endswith(supported_extensions)):
                logger.info(f"Skipping unsupported attachment: {filename} (type: {mime_type})")
                return None
            
            # Download attachment
            attachment_url = f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/attachments/{attachment_id}"
            response = requests.get(attachment_url, headers=gmail_headers)
            
            if response.status_code != 200:
                logger.error(f"Failed to download attachment {filename}: {response.status_code}")
                return None
            
            attachment_data = response.json()
            content = base64.urlsafe_b64decode(attachment_data['data'])
            
            return EmailAttachment(
                filename=filename,
                content=content,
                mime_type=mime_type,
                email_id=message_id,
                sender=sender,
                subject=subject,
                received_time=received_time
            )
            
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None
    
    def _process_attachment(self, attachment: EmailAttachment, brokerage_key: str, config: Dict[str, Any]) -> bool:
        """
        Process a single email attachment.
        
        Args:
            attachment: EmailAttachment to process
            brokerage_key: Brokerage identifier
            config: Email automation configuration
            
        Returns:
            True if processing succeeded
        """
        try:
            # Check if callback is registered
            callback = self.processing_callbacks.get(brokerage_key)
            if callback:
                # Use registered callback
                return callback(attachment, config)
            else:
                # Default processing
                return self._default_process_attachment(attachment, brokerage_key, config)
                
        except Exception as e:
            logger.error(f"Error processing attachment {attachment.filename} for {brokerage_key}: {e}")
            return False
    
    def _default_process_attachment(self, attachment: EmailAttachment, brokerage_key: str, config: Dict[str, Any]) -> bool:
        """
        Default attachment processing logic.
        
        Args:
            attachment: EmailAttachment to process
            brokerage_key: Brokerage identifier
            config: Email automation configuration
            
        Returns:
            True if processing succeeded
        """
        try:
            logger.info(f"Processing attachment: {attachment.filename} from {attachment.sender}")
            
            # Parse file content
            if attachment.mime_type in ['text/csv', 'application/csv']:
                df = pd.read_csv(io.BytesIO(attachment.content))
            elif attachment.mime_type == 'application/json':
                data = json.loads(attachment.content.decode('utf-8'))
                df = pd.DataFrame(data) if isinstance(data, list) else pd.json_normalize(data)
            elif attachment.mime_type in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel'] or attachment.filename.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(attachment.content), engine='openpyxl' if attachment.filename.lower().endswith('.xlsx') else None)
            else:
                logger.warning(f"Unsupported file type: {attachment.mime_type}")
                return False
            
            # Apply saved mappings if configured
            mapping_config = config.get('column_mappings', {})
            if mapping_config:
                df = self._apply_column_mappings(df, mapping_config)
            
            # Store processed data for pickup by main application
            self._store_processed_data(df, attachment, brokerage_key, config)
            
            logger.info(f"Successfully processed {attachment.filename} - {len(df)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error in default processing for {attachment.filename}: {e}")
            return False
    
    def _apply_column_mappings(self, df: pd.DataFrame, mapping_config: Dict[str, str]) -> pd.DataFrame:
        """Apply saved column mappings to dataframe."""
        try:
            # Rename columns based on saved mappings
            df = df.rename(columns=mapping_config)
            logger.debug(f"Applied column mappings: {mapping_config}")
            return df
            
        except Exception as e:
            logger.error(f"Error applying column mappings: {e}")
            return df
    
    def _store_processed_data(self, df: pd.DataFrame, attachment: EmailAttachment, brokerage_key: str, config: Dict[str, Any]):
        """Store processed data for main application pickup."""
        try:
            # Only access session state if available (not during import)
            import streamlit as st
            if hasattr(st, 'session_state') and st.session_state is not None:
                # Use Streamlit session state to store processed data
                if 'email_processed_data' not in st.session_state:
                    st.session_state.email_processed_data = []
                
                processed_item = {
                    'brokerage_key': brokerage_key,
                    'filename': attachment.filename,
                    'sender': attachment.sender,
                    'subject': attachment.subject,
                    'received_time': attachment.received_time,
                    'processed_time': datetime.now(),
                    'dataframe': df,
                    'record_count': len(df)
                }
                
                st.session_state.email_processed_data.append(processed_item)
                
                # Keep only recent items (last 50)
                if len(st.session_state.email_processed_data) > 50:
                    st.session_state.email_processed_data = st.session_state.email_processed_data[-50:]
                
                logger.info(f"Stored processed data for {attachment.filename}")
            else:
                # Session state not available - store in memory for now
                logger.info(f"Session state not available - processed data for {attachment.filename} not stored")
            
        except Exception as e:
            logger.error(f"Error storing processed data: {e}")


# Global instance for application use
email_monitor = EmailMonitorService(None)  # Will be initialized with credential_manager