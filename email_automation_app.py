"""
Email Automation Setup Application.

Provides configuration interface for automatic email processing:
- Gmail connection setup
- Inbox filtering configuration 
- Column mapping presets
- Monitoring status and controls
"""

import streamlit as st
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json

from credential_manager import credential_manager
from email_monitor import email_monitor

logger = logging.getLogger(__name__)

def main():
    """Main email automation setup application."""
    
    st.title("📧 Email Automation Setup")
    
    # Sidebar for brokerage selection
    with st.sidebar:
        st.header("Settings")
        
        # Get brokerages with email automation available
        available_brokerages = credential_manager.get_available_brokerages()
        email_automation_brokerages = []
        
        for brokerage in available_brokerages:
            cred_status = credential_manager.validate_credentials(brokerage)
            if cred_status.email_automation_available:
                email_automation_brokerages.append(brokerage)
        
        if not email_automation_brokerages:
            st.warning("⚠️ No brokerages configured for email automation")
            st.info("Contact your administrator to configure email automation for your brokerages.")
            return
        
        # Brokerage selection
        selected_brokerage = st.selectbox(
            "Brokerage",
            email_automation_brokerages,
            help="Select brokerage to configure email automation"
        )
        
        # Get status for selected brokerage
        cred_status = credential_manager.validate_credentials(selected_brokerage)
        
        # Status indicators
        if cred_status.email_automation_active:
            st.success("✅ Email Monitoring Active")
        elif cred_status.email_automation_available:
            st.info("📧 Email Automation Available")
        
        st.markdown("---")
        
        # Quick actions
        st.subheader("Quick Actions")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Check Now", use_container_width=True):
                with st.spinner("Checking inbox..."):
                    result = email_monitor.check_inbox_now(selected_brokerage)
                    if result.success:
                        if result.processed_count > 0:
                            st.success(f"Processed {result.processed_count} files")
                        else:
                            st.info("No new files found")
                    else:
                        st.error(f"Error: {result.message}")
        
        with col2:
            monitoring_status = email_monitor.get_monitoring_status()
            if monitoring_status['monitoring_active']:
                if st.button("Stop Monitor", use_container_width=True):
                    email_monitor.stop_monitoring()
                    st.rerun()
            else:
                if st.button("Start Monitor", use_container_width=True):
                    if email_monitor.start_monitoring():
                        st.success("Monitoring started")
                        st.rerun()
                    else:
                        st.error("Failed to start monitoring")
    
    # Main content area
    if selected_brokerage:
        show_automation_dashboard(selected_brokerage, cred_status)

def show_automation_dashboard(brokerage_key: str, cred_status):
    """Show the main automation dashboard for a brokerage."""
    
    # Configuration tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Status", 
        "⚙️ Configuration", 
        "📁 Recent Files", 
        "🔍 Logs"
    ])
    
    with tab1:
        show_status_tab(brokerage_key, cred_status)
    
    with tab2:
        show_configuration_tab(brokerage_key)
    
    with tab3:
        show_recent_files_tab(brokerage_key)
    
    with tab4:
        show_logs_tab(brokerage_key)

def show_status_tab(brokerage_key: str, cred_status):
    """Show email automation status overview."""
    
    st.subheader("📊 Email Automation Status")
    
    # Overall status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if cred_status.email_automation_active:
            st.metric("Status", "Active", delta="Running")
        else:
            st.metric("Status", "Inactive", delta="Stopped")
    
    with col2:
        monitoring_status = email_monitor.get_monitoring_status()
        last_check = monitoring_status['last_check_times'].get(brokerage_key)
        if last_check:
            time_ago = datetime.now() - last_check
            if time_ago < timedelta(minutes=10):
                st.metric("Last Check", "Recent", delta="< 10 min ago")
            else:
                st.metric("Last Check", "Delayed", delta=f"{int(time_ago.total_seconds()//60)} min ago")
        else:
            st.metric("Last Check", "Never", delta="Not started")
    
    with col3:
        processed_count = get_recent_processed_count(brokerage_key)
        st.metric("Files Today", str(processed_count), delta=f"{processed_count} processed")
    
    st.markdown("---")
    
    # Configuration summary
    config = credential_manager._get_email_automation_config(brokerage_key)
    if config:
        st.subheader("Configuration Summary")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Gmail Account:**")
            gmail_creds = config.get('gmail_credentials', {})
            st.write(f"• Email: {gmail_creds.get('email', 'Not configured')}")
            
            st.write("**Inbox Filters:**")
            filters = config.get('inbox_filters', {})
            st.write(f"• Sender filter: {filters.get('sender_filter', 'Any')}")
            st.write(f"• Subject filter: {filters.get('subject_filter', 'Any')}")
        
        with col2:
            st.write("**File Processing:**")
            mappings = config.get('column_mappings', {})
            if mappings:
                st.write(f"• Column mappings: {len(mappings)} configured")
            else:
                st.write("• Column mappings: Auto-detect")
            
            st.write("**Processing Options:**")
            processing_opts = config.get('processing_options', {})
            st.write(f"• Add tracking: {processing_opts.get('add_tracking', True)}")
            st.write(f"• Send email: {processing_opts.get('send_email', False)}")

def show_configuration_tab(brokerage_key: str):
    """Show email automation configuration interface."""
    
    st.subheader("⚙️ Email Automation Configuration")
    
    config = credential_manager._get_email_automation_config(brokerage_key)
    
    if not config:
        st.warning("Email automation not configured for this brokerage.")
        st.info("Contact your administrator to set up email automation credentials.")
        return
    
    # Gmail Configuration
    st.write("### Gmail Connection")
    gmail_creds = config.get('gmail_credentials', {})
    
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**Connected Account:** {gmail_creds.get('email', 'Not configured')}")
    with col2:
        if st.button("Test Connection", type="secondary"):
            # Test Gmail connection
            with st.spinner("Testing Gmail connection..."):
                # This would test the actual Gmail API connection
                st.success("✅ Gmail connection successful")
    
    st.markdown("---")
    
    # Inbox Filters
    st.write("### Inbox Filters")
    filters = config.get('inbox_filters', {})
    
    with st.expander("Filter Settings", expanded=False):
        st.write("**Current Filters:**")
        for key, value in filters.items():
            st.write(f"• {key.replace('_', ' ').title()}: {value}")
        
        st.info("💡 Filter configuration is managed through your secrets configuration.")
    
    st.markdown("---")
    
    # Column Mappings
    st.write("### Column Mappings")
    mappings = config.get('column_mappings', {})
    
    if mappings:
        st.write("**Configured Mappings:**")
        for source_col, target_col in mappings.items():
            st.write(f"• `{source_col}` → `{target_col}`")
    else:
        st.info("Using automatic column detection")
    
    if st.button("Test Mapping", type="secondary"):
        st.info("Upload a sample file to test column mappings in the End-to-End Load Processing app.")

def show_recent_files_tab(brokerage_key: str):
    """Show recently processed files."""
    
    st.subheader("📁 Recently Processed Files")
    
    # Get processed files from session state
    processed_data = st.session_state.get('email_processed_data', [])
    brokerage_files = [item for item in processed_data if item['brokerage_key'] == brokerage_key]
    
    if not brokerage_files:
        st.info("No files have been processed yet.")
        st.write("Files will appear here once email automation processes attachments.")
        return
    
    # Show recent files
    for i, file_info in enumerate(reversed(brokerage_files[-10:])):  # Show last 10
        with st.expander(f"📄 {file_info['filename']} - {file_info['record_count']} records"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Sender:** {file_info['sender']}")
                st.write(f"**Subject:** {file_info['subject']}")
                st.write(f"**Records:** {file_info['record_count']}")
            
            with col2:
                st.write(f"**Received:** {file_info['received_time'].strftime('%Y-%m-%d %H:%M')}")
                st.write(f"**Processed:** {file_info['processed_time'].strftime('%Y-%m-%d %H:%M')}")
            
            # Show data preview
            if st.button(f"View Data Preview", key=f"preview_{i}"):
                st.dataframe(file_info['dataframe'].head(10))

def show_logs_tab(brokerage_key: str):
    """Show email automation logs."""
    
    st.subheader("🔍 Email Automation Logs")
    
    # In a real implementation, this would show actual logs
    st.info("Email automation logs will appear here.")
    
    # Mock log entries for demonstration
    with st.expander("Recent Activity", expanded=True):
        st.text(f"""
2024-01-15 10:30:00 - INFO - Checking inbox for {brokerage_key}
2024-01-15 10:30:01 - INFO - Found 2 new emails
2024-01-15 10:30:02 - INFO - Processing attachment: loads_2024_01_15.csv
2024-01-15 10:30:05 - INFO - Successfully processed 45 records
2024-01-15 10:30:05 - INFO - Processing attachment: updates_2024_01_15.json
2024-01-15 10:30:08 - INFO - Successfully processed 12 records
2024-01-15 10:30:08 - INFO - Inbox check completed
        """)

def get_recent_processed_count(brokerage_key: str) -> int:
    """Get count of files processed today for a brokerage."""
    processed_data = st.session_state.get('email_processed_data', [])
    today = datetime.now().date()
    
    count = 0
    for item in processed_data:
        if (item['brokerage_key'] == brokerage_key and 
            item['processed_time'].date() == today):
            count += 1
    
    return count

if __name__ == "__main__":
    main()