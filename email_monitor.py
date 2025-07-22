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
        
    def start_monitoring(self, brokerages: List[str] = None) -> bool:
        """
        Start background email monitoring for specified brokerages.
        
        Args:
            brokerages: List of brokerage keys to monitor, or None for all configured
            
        Returns:
            True if monitoring started successfully
        """
        try:
            if self.monitoring_active:
                logger.warning("Email monitoring already active")
                return True
                
            # Get list of brokerages to monitor
            if brokerages is None:
                brokerages = self._get_email_automation_brokerages()
            
            if not brokerages:
                logger.warning("No brokerages configured for email automation")
                return False
            
            # Start monitoring thread
            self.monitoring_active = True
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop,
                args=(brokerages,),
                daemon=True
            )
            self.monitor_thread.start()
            
            logger.info(f"Email monitoring started for brokerages: {brokerages}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start email monitoring: {e}")
            self.monitoring_active = False
            return False
    
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
        Manually check inbox for a specific brokerage.
        
        Args:
            brokerage_key: Brokerage to check
            
        Returns:
            ProcessingResult with details
        """
        try:
            config = self.credential_manager._get_email_automation_config(brokerage_key)
            if not config:
                return ProcessingResult(
                    success=False,
                    message=f"No email automation configured for {brokerage_key}",
                    processed_count=0
                )
            
            # Get Gmail service
            service = self._get_gmail_service(config['gmail_credentials'])
            if not service:
                return ProcessingResult(
                    success=False,
                    message="Failed to connect to Gmail",
                    processed_count=0
                )
            
            # Check for new emails
            attachments = self._check_for_attachments(service, config, brokerage_key)
            
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
                message=f"Processed {processed_count} files",
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
        """Get current monitoring status for all brokerages."""
        brokerages = self._get_email_automation_brokerages()
        
        status = {
            'monitoring_active': self.monitoring_active,
            'monitored_brokerages': brokerages,
            'last_check_times': self.last_check_times.copy(),
            'callback_count': len(self.processing_callbacks)
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
    
    def _get_gmail_service(self, gmail_credentials: Dict[str, Any]):
        """
        Create Gmail API service instance.
        
        Args:
            gmail_credentials: OAuth2 credentials for Gmail API
            
        Returns:
            Gmail service instance or None
        """
        try:
            # This would normally use Google API client library
            # For now, return a mock service to establish the interface
            logger.warning("Gmail service creation not yet implemented - using mock")
            return {"mock": True}
            
        except Exception as e:
            logger.error(f"Failed to create Gmail service: {e}")
            return None
    
    def _check_for_attachments(self, service, config: Dict[str, Any], brokerage_key: str) -> List[EmailAttachment]:
        """
        Check for new email attachments matching criteria.
        
        Args:
            service: Gmail service instance
            config: Email automation configuration
            brokerage_key: Brokerage identifier
            
        Returns:
            List of new attachments to process
        """
        try:
            # This would normally use Gmail API to search for emails
            # For now, return empty list to establish the interface
            logger.debug(f"Checking attachments for {brokerage_key} (mock implementation)")
            return []
            
        except Exception as e:
            logger.error(f"Error checking attachments for {brokerage_key}: {e}")
            return []
    
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
            
        except Exception as e:
            logger.error(f"Error storing processed data: {e}")


# Global instance for application use
email_monitor = EmailMonitorService(None)  # Will be initialized with credential_manager