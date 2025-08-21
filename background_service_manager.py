"""
Background Service Manager

Provides UI integration and management controls for the background email automation service.
This module acts as the bridge between the Streamlit UI and the background service,
providing controls, status monitoring, and configuration management.

Features:
- Service start/stop controls
- Configuration toggle management
- Status monitoring and display
- Service health checks
- Integration with existing UI components
"""

import streamlit as st
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd

from email_automation_service import background_email_service
from src.backend.database import DatabaseManager
from service_account_oauth import service_oauth_manager

logger = logging.getLogger(__name__)

class BackgroundServiceManager:
    """Manages background email automation service from the UI"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.service = background_email_service
        
        # Auto-start service if configurations are enabled
        self._auto_start_service()
    
    def render_service_control_panel(self):
        """Render the main service control panel in Streamlit UI"""
        st.subheader("🔄 Background Email Automation Service")
        
        # Service status display
        status = self.get_service_status()
        self._render_service_status(status)
        
        # Service controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🚀 Start Service", disabled=status['service_running']):
                self.start_service()
                st.rerun()
        
        with col2:
            if st.button("⏹️ Stop Service", disabled=not status['service_running']):
                self.stop_service()
                st.rerun()
        
        with col3:
            if st.button("🔄 Refresh Status"):
                st.rerun()
        
        # Active configurations display
        if status['active_configurations']:
            st.write("### Active Background Monitoring Configurations")
            self._render_active_configurations(status['active_configurations'])
        else:
            st.info("No configurations currently enabled for background monitoring")
    
    def render_configuration_toggle(self, brokerage_name: str, configuration_name: str):
        """Render background monitoring toggle for a specific configuration"""
        # Get current monitoring status
        is_enabled = self._is_monitoring_enabled(brokerage_name, configuration_name)
        
        # Create unique key for this toggle
        toggle_key = f"bg_monitor_{brokerage_name}_{configuration_name}"
        
        # Monitoring toggle
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.write("**Background Email Monitoring**")
            st.write("Automatically process emails when they arrive")
        
        with col2:
            new_enabled = st.toggle(
                "Enable",
                value=is_enabled,
                key=toggle_key
            )
        
        # Handle toggle change
        if new_enabled != is_enabled:
            self._toggle_monitoring(brokerage_name, configuration_name, new_enabled)
            if new_enabled:
                st.success(f"Background monitoring enabled for {brokerage_name}")
            else:
                st.info(f"Background monitoring disabled for {brokerage_name}")
            st.rerun()
        
        # Show monitoring settings if enabled
        if new_enabled:
            self._render_monitoring_settings(brokerage_name, configuration_name)
    
    def render_monitoring_status_widget(self):
        """Render a compact monitoring status widget"""
        status = self.get_service_status()
        
        # Status indicator
        if status['service_running']:
            st.success(f"🟢 Background Service Active ({status['active_configurations_count']} configs)")
        else:
            st.warning("🟡 Background Service Stopped")
        
        # Quick stats in expandable section
        with st.expander("Service Details"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Service Status", "Running" if status['service_running'] else "Stopped")
                st.metric("Active Configs", status['active_configurations_count'])
            
            with col2:
                if status['last_check_times']:
                    latest_check = max(status['last_check_times'].values())
                    if latest_check:
                        st.metric("Last Check", self._format_time_ago(latest_check))
                    else:
                        st.metric("Last Check", "Never")
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get comprehensive service status"""
        return self.service.get_service_status()
    
    def start_service(self) -> bool:
        """Start the background service"""
        try:
            self.service.start()
            st.success("Background email automation service started")
            logger.info("Background service started via UI")
            return True
        except Exception as e:
            st.error(f"Failed to start service: {e}")
            logger.error(f"Failed to start background service: {e}")
            return False
    
    def stop_service(self) -> bool:
        """Stop the background service"""
        try:
            self.service.stop()
            st.success("Background email automation service stopped")
            logger.info("Background service stopped via UI")
            return True
        except Exception as e:
            st.error(f"Failed to stop service: {e}")
            logger.error(f"Failed to stop background service: {e}")
            return False
    
    def _render_service_status(self, status: Dict[str, Any]):
        """Render service status information"""
        # Main status
        if status['service_running']:
            st.success("🟢 Service is running")
        else:
            st.warning("🟡 Service is stopped")
        
        # Status metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Active Configurations",
                status['active_configurations_count'],
                help="Number of configurations with background monitoring enabled"
            )
        
        with col2:
            service_health = "Healthy" if status['service_thread_alive'] else "Unhealthy"
            st.metric(
                "Service Health",
                service_health,
                help="Background service thread status"
            )
        
        with col3:
            if status['last_check_times']:
                latest_check = max(status['last_check_times'].values())
                if latest_check:
                    time_ago = self._format_time_ago(latest_check)
                    st.metric("Last Activity", time_ago)
                else:
                    st.metric("Last Activity", "Never")
            else:
                st.metric("Last Activity", "None")
    
    def _render_active_configurations(self, configs: List[Dict[str, Any]]):
        """Render table of active configurations"""
        if not configs:
            return
        
        # Create DataFrame for display
        display_data = []
        for config in configs:
            last_check = config.get('last_background_check')
            display_data.append({
                'Brokerage': config['brokerage_name'],
                'Configuration': config['configuration_name'],
                'Interval (min)': config['check_interval_minutes'],
                'Last Check': self._format_time_ago(last_check) if last_check else 'Never',
                'Status': config.get('background_service_status', 'inactive').title()
            })
        
        df = pd.DataFrame(display_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    
    def _render_monitoring_settings(self, brokerage_name: str, configuration_name: str):
        """Render monitoring settings for a configuration"""
        with st.expander("⚙️ Monitoring Settings"):
            # Check interval setting
            interval_key = f"interval_{brokerage_name}_{configuration_name}"
            current_interval = self._get_check_interval(brokerage_name, configuration_name)
            
            new_interval = st.slider(
                "Check Interval (minutes)",
                min_value=1,
                max_value=60,
                value=current_interval,
                key=interval_key,
                help="How often to check for new emails"
            )
            
            if new_interval != current_interval:
                self._update_check_interval(brokerage_name, configuration_name, new_interval)
                st.success(f"Check interval updated to {new_interval} minutes")
                st.rerun()
            
            # Service account setup
            self._render_service_account_setup(brokerage_name, configuration_name)
    
    def _render_service_account_setup(self, brokerage_name: str, configuration_name: str):
        """Render service account OAuth setup"""
        st.write("**Service Account Setup**")
        
        # Check if service account is configured
        has_service_account = self._has_service_account(brokerage_name, configuration_name)
        
        if has_service_account:
            st.success("✅ Service account configured")
            if st.button("🔄 Reconfigure Service Account", key=f"reconfig_{brokerage_name}_{configuration_name}"):
                # Clear existing service account
                self._clear_service_account(brokerage_name, configuration_name)
                st.rerun()
        else:
            st.warning("⚠️ Service account not configured - using fallback authentication")
            
            if st.button("🔧 Setup Service Account", key=f"setup_{brokerage_name}_{configuration_name}"):
                self._show_service_account_setup_modal(brokerage_name, configuration_name)
    
    def _show_service_account_setup_modal(self, brokerage_name: str, configuration_name: str):
        """Show service account setup instructions"""
        setup_info = service_oauth_manager.setup_service_account_flow(brokerage_name)
        
        st.info("**Service Account Setup Instructions**")
        
        for i, instruction in enumerate(setup_info['instructions'], 1):
            st.write(f"{instruction}")
        
        st.write("**Next Steps:**")
        for step in setup_info['next_steps']:
            st.write(f"- {step}")
        
        with st.expander("Security Notes"):
            for note in setup_info['security_notes']:
                st.write(f"- {note}")
    
    def _is_monitoring_enabled(self, brokerage_name: str, configuration_name: str) -> bool:
        """Check if background monitoring is enabled for a configuration"""
        try:
            configs = self.db_manager.get_background_monitoring_configs()
            for config in configs:
                if (config['brokerage_name'] == brokerage_name and 
                    config['configuration_name'] == configuration_name):
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking monitoring status: {e}")
            return False
    
    def _toggle_monitoring(self, brokerage_name: str, configuration_name: str, enabled: bool):
        """Toggle background monitoring for a configuration"""
        try:
            if enabled:
                self.service.enable_monitoring(brokerage_name, configuration_name)
                # Auto-start service if not running
                if not self.service.is_service_running():
                    self.service.start()
            else:
                self.service.disable_monitoring(brokerage_name, configuration_name)
        except Exception as e:
            logger.error(f"Error toggling monitoring: {e}")
            st.error(f"Failed to update monitoring setting: {e}")
    
    def _get_check_interval(self, brokerage_name: str, configuration_name: str) -> int:
        """Get check interval for a configuration"""
        try:
            configs = self.db_manager.get_background_monitoring_configs()
            for config in configs:
                if (config['brokerage_name'] == brokerage_name and 
                    config['configuration_name'] == configuration_name):
                    return config.get('check_interval_minutes', 5)
            return 5  # Default
        except Exception as e:
            logger.error(f"Error getting check interval: {e}")
            return 5
    
    def _update_check_interval(self, brokerage_name: str, configuration_name: str, interval: int):
        """Update check interval for a configuration"""
        try:
            self.db_manager.update_background_monitoring(
                brokerage_name, configuration_name, True, interval
            )
        except Exception as e:
            logger.error(f"Error updating check interval: {e}")
            st.error(f"Failed to update check interval: {e}")
    
    def _has_service_account(self, brokerage_name: str, configuration_name: str) -> bool:
        """Check if configuration has service account setup"""
        try:
            configs = self.db_manager.get_background_monitoring_configs()
            for config in configs:
                if (config['brokerage_name'] == brokerage_name and 
                    config['configuration_name'] == configuration_name):
                    return bool(config.get('service_account_oauth'))
            return False
        except Exception as e:
            logger.error(f"Error checking service account: {e}")
            return False
    
    def _clear_service_account(self, brokerage_name: str, configuration_name: str):
        """Clear service account configuration"""
        try:
            self.db_manager.save_service_account_oauth(
                brokerage_name, configuration_name, None
            )
        except Exception as e:
            logger.error(f"Error clearing service account: {e}")
    
    def _auto_start_service(self):
        """Auto-start service if configurations are enabled"""
        try:
            active_configs = self.db_manager.get_background_monitoring_configs()
            if active_configs and not self.service.is_service_running():
                logger.info("Auto-starting background service for enabled configurations")
                self.service.start()
        except Exception as e:
            logger.error(f"Error auto-starting service: {e}")
    
    def _format_time_ago(self, timestamp_str: Optional[str]) -> str:
        """Format timestamp as 'X minutes ago' style string"""
        if not timestamp_str:
            return "Never"
        
        try:
            # Handle various timestamp formats
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            if timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=None)
            
            now = datetime.now()
            delta = now - timestamp
            
            if delta.days > 0:
                return f"{delta.days} days ago"
            elif delta.seconds > 3600:
                hours = delta.seconds // 3600
                return f"{hours} hours ago"
            elif delta.seconds > 60:
                minutes = delta.seconds // 60
                return f"{minutes} minutes ago"
            else:
                return "Just now"
                
        except (ValueError, TypeError):
            return "Unknown"


# Global background service manager
background_service_manager = BackgroundServiceManager()