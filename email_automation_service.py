"""
Background Email Automation Service

Provides continuous background monitoring and processing of email automation
configurations without requiring active UI sessions.

Features:
- Continuous monitoring of enabled configurations
- Thread-safe operation
- Automatic service account OAuth handling
- Error recovery and retry logic
- Service state persistence
- Integration with existing email processing pipeline
"""

import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import signal
import sys
import os

from src.backend.database import DatabaseManager
from email_monitor import email_monitor, EmailAutomationConfig
from credential_manager import credential_manager

logger = logging.getLogger(__name__)

class EmailAutomationService:
    """Background service for continuous email automation monitoring"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.running_configs = {}  # Track active monitoring per config
        self.service_thread = None
        self.stop_event = threading.Event()
        self.is_running = False
        
        # Service settings
        self.default_check_interval = 300  # 5 minutes
        self.retry_delay = 60  # 1 minute retry on errors
        self.max_retries = 3
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def start(self):
        """Start the background email automation service"""
        if self.is_running:
            logger.warning("Email automation service is already running")
            return
        
        logger.info("Starting email automation service...")
        self.stop_event.clear()
        self.is_running = True
        
        # Start service in background thread
        self.service_thread = threading.Thread(
            target=self._service_loop,
            name="EmailAutomationService",
            daemon=True
        )
        self.service_thread.start()
        
        logger.info("Email automation service started successfully")
    
    def stop(self):
        """Stop the background email automation service"""
        if not self.is_running:
            logger.info("Email automation service is not running")
            return
        
        logger.info("Stopping email automation service...")
        self.stop_event.set()
        self.is_running = False
        
        # Wait for service thread to finish
        if self.service_thread and self.service_thread.is_alive():
            self.service_thread.join(timeout=10)
            if self.service_thread.is_alive():
                logger.warning("Service thread did not stop gracefully")
        
        # Update all monitored configs to inactive status
        self._update_all_configs_status('stopped')
        
        logger.info("Email automation service stopped")
    
    def is_service_running(self) -> bool:
        """Check if the service is currently running"""
        return self.is_running and self.service_thread and self.service_thread.is_alive()
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get comprehensive service status information"""
        active_configs = self.db_manager.get_background_monitoring_configs()
        
        return {
            'service_running': self.is_service_running(),
            'active_configurations_count': len(active_configs),
            'active_configurations': active_configs,
            'service_thread_alive': self.service_thread.is_alive() if self.service_thread else False,
            'last_check_times': {
                f"{config['brokerage_name']}/{config['configuration_name']}": config['last_background_check']
                for config in active_configs
            }
        }
    
    def enable_monitoring(self, brokerage_name: str, configuration_name: str, 
                         check_interval_minutes: int = 5) -> bool:
        """Enable background monitoring for a configuration"""
        try:
            # Update database to enable monitoring
            self.db_manager.update_background_monitoring(
                brokerage_name, configuration_name, True, check_interval_minutes
            )
            
            # If service is running, it will pick up this config on next cycle
            logger.info(f"Enabled background monitoring for {brokerage_name}/{configuration_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error enabling monitoring for {brokerage_name}/{configuration_name}: {e}")
            return False
    
    def disable_monitoring(self, brokerage_name: str, configuration_name: str) -> bool:
        """Disable background monitoring for a configuration"""
        try:
            # Update database to disable monitoring
            self.db_manager.update_background_monitoring(
                brokerage_name, configuration_name, False
            )
            
            # Update status to inactive
            self.db_manager.update_background_check_timestamp(
                brokerage_name, configuration_name, 'inactive'
            )
            
            logger.info(f"Disabled background monitoring for {brokerage_name}/{configuration_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error disabling monitoring for {brokerage_name}/{configuration_name}: {e}")
            return False
    
    def _service_loop(self):
        """Main service loop - runs continuously in background thread"""
        logger.info("Email automation service loop started")
        
        while not self.stop_event.is_set():
            try:
                # Get all configurations with monitoring enabled
                active_configs = self.db_manager.get_background_monitoring_configs()
                
                if not active_configs:
                    logger.debug("No active monitoring configurations found")
                    self._sleep_with_interrupt(30)  # Check every 30 seconds when no configs
                    continue
                
                # Process each active configuration
                for config in active_configs:
                    if self.stop_event.is_set():
                        break
                    
                    self._process_configuration(config)
                
                # Wait before next cycle (use minimum interval from all configs)
                if active_configs:
                    min_interval = min(config['check_interval_minutes'] for config in active_configs)
                    wait_time = min_interval * 60  # Convert to seconds
                else:
                    wait_time = self.default_check_interval
                
                self._sleep_with_interrupt(wait_time)
                
            except Exception as e:
                logger.error(f"Error in service loop: {e}")
                self._sleep_with_interrupt(self.retry_delay)
        
        logger.info("Email automation service loop stopped")
    
    def _process_configuration(self, config: Dict[str, Any]):
        """Process a single configuration for background email monitoring"""
        brokerage_name = config['brokerage_name']
        config_name = config['configuration_name']
        config_key = f"{brokerage_name}/{config_name}"
        
        try:
            # Check if enough time has passed since last check
            last_check = config.get('last_background_check')
            check_interval = config.get('check_interval_minutes', self.default_check_interval // 60)
            
            if last_check:
                try:
                    last_check_time = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                    time_since_check = datetime.now() - last_check_time.replace(tzinfo=None)
                    
                    if time_since_check.total_seconds() < (check_interval * 60):
                        logger.debug(f"Skipping {config_key} - not enough time elapsed")
                        return
                except (ValueError, TypeError) as e:
                    logger.debug(f"Could not parse last check time for {config_key}: {e}")
            
            logger.info(f"Processing background email check for {config_key}")
            
            # Update status to active and timestamp
            self.db_manager.update_background_check_timestamp(
                brokerage_name, config_name, 'active'
            )
            
            # Create EmailAutomationConfig for processing
            automation_config = self._create_automation_config(config)
            if not automation_config:
                logger.error(f"Could not create automation config for {config_key}")
                self.db_manager.update_background_check_timestamp(
                    brokerage_name, config_name, 'error'
                )
                return
            
            # Process emails using existing automated method
            result = email_monitor.process_brokerage_emails_automated(automation_config)
            
            # Log results
            if result.get('success'):
                logger.info(f"Background processing completed for {config_key}: "
                           f"processed {result.get('processed_count', 0)}/{result.get('total_attachments', 0)} attachments")
            else:
                logger.warning(f"Background processing failed for {config_key}: {result.get('error', 'Unknown error')}")
            
        except Exception as e:
            logger.error(f"Error processing configuration {config_key}: {e}")
            self.db_manager.update_background_check_timestamp(
                brokerage_name, config_name, 'error'
            )
    
    def _create_automation_config(self, config: Dict[str, Any]) -> Optional[EmailAutomationConfig]:
        """Create EmailAutomationConfig from database configuration"""
        try:
            brokerage_name = config['brokerage_name']
            
            # Get email automation settings
            email_config = config.get('email_automation_config', {})
            if not email_config:
                logger.error(f"No email automation config found for {brokerage_name}")
                return None
            
            # Determine folder name from config
            folder_name = email_config.get('folder_name', 'INBOX')
            
            # Create automation config object
            automation_config = EmailAutomationConfig(
                brokerage_key=brokerage_name,
                folder_name=folder_name,
                service_account_oauth=config.get('service_account_oauth'),
                check_interval_minutes=config.get('check_interval_minutes', 5)
            )
            
            return automation_config
            
        except Exception as e:
            logger.error(f"Error creating automation config: {e}")
            return None
    
    def _sleep_with_interrupt(self, seconds: int):
        """Sleep for specified seconds, but wake up if stop event is set"""
        self.stop_event.wait(seconds)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def _update_all_configs_status(self, status: str):
        """Update status for all configurations being monitored"""
        try:
            active_configs = self.db_manager.get_background_monitoring_configs()
            for config in active_configs:
                self.db_manager.update_background_check_timestamp(
                    config['brokerage_name'], 
                    config['configuration_name'], 
                    status
                )
        except Exception as e:
            logger.error(f"Error updating all config statuses: {e}")


# Global service instance
background_email_service = EmailAutomationService()


def main():
    """Run the service as a standalone script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/email_automation_service.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Suppress expected Streamlit context warnings in background mode
    logging.getLogger('streamlit.runtime.scriptrunner_utils.script_run_context').setLevel(logging.CRITICAL)
    logging.getLogger('streamlit.runtime.scriptrunner_utils').setLevel(logging.CRITICAL)
    logging.getLogger('streamlit.runtime').setLevel(logging.CRITICAL)
    
    logger.info("Starting Email Automation Service as standalone application")
    
    try:
        # Start the service
        background_email_service.start()
        
        # Keep the main thread alive
        while background_email_service.is_service_running():
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        background_email_service.stop()
        logger.info("Email Automation Service stopped")


if __name__ == "__main__":
    main()