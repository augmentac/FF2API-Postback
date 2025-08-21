"""
Email Processing Activity Dashboard

This module provides real-time visibility into email automation processing,
mirroring the exact same interface and components as manual upload processing.

Key Features:
- Live processing queue display
- Real-time progress indicators
- Results display matching manual upload UI
- Background process status monitoring
"""

import streamlit as st
import pandas as pd
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import time
import threading
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class EmailProcessingJob:
    """Represents an email processing job with status tracking."""
    job_id: str
    filename: str
    brokerage_key: str
    email_source: str
    file_size: int
    record_count: int
    started_at: datetime
    current_step: str = "queued"
    progress_percent: float = 0.0
    status: str = "pending"  # pending, processing, completed, failed
    error_message: str = ""
    success_count: int = 0
    failure_count: int = 0
    processing_time: float = 0.0
    result_data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['started_at'] = self.started_at.isoformat()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EmailProcessingJob':
        """Create from dictionary."""
        data['started_at'] = datetime.fromisoformat(data['started_at'])
        return cls(**data)

class EmailProcessingDashboard:
    """Manages email processing activity dashboard and real-time updates."""
    
    def __init__(self):
        """Initialize the dashboard."""
        self.processing_steps = [
            "📧 Parsing email attachment",
            "📊 Analyzing data structure", 
            "🔗 Applying field mappings",
            "⚡ Submitting to API",
            "🔍 Enriching data",
            "📁 Generating results"
        ]
        
    def render_email_processing_dashboard(self, brokerage_key: str):
        """Render the complete email processing dashboard."""
        st.subheader("📧 Email Processing Activity")
        
        # Get current processing jobs first to check for immediate alerts
        active_jobs, completed_jobs = self._get_processing_jobs(brokerage_key)
        
        # Display real-time error alerts first (most prominent)
        self._render_real_time_error_alerts(brokerage_key, active_jobs, completed_jobs)
        
        # Display any system errors
        self._render_system_errors(brokerage_key)
        
        # Render active processing section
        if active_jobs:
            self._render_active_processing_section(active_jobs)
        
        # Render processing queue
        self._render_processing_queue(brokerage_key)
        
        # Render recent results (mirroring manual upload results)
        if completed_jobs:
            self._render_recent_results(completed_jobs)
            
        # Auto-refresh for real-time updates
        self._setup_auto_refresh()
        
        # Show enhanced dashboard controls
        self._render_dashboard_controls(brokerage_key)
    
    def _render_real_time_error_alerts(self, brokerage_key: str, active_jobs: List[EmailProcessingJob], completed_jobs: List[EmailProcessingJob]):
        """Render real-time error alerts for immediate attention."""
        alerts = []
        
        # Check for recent failures (last 10 minutes)
        recent_failures = []
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        
        for job in completed_jobs:
            if (job.status == "failed" and 
                hasattr(job, 'started_at') and 
                job.started_at > ten_minutes_ago):
                recent_failures.append(job)
        
        # Check for stuck processing jobs (running > 5 minutes)
        stuck_jobs = []
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        
        for job in active_jobs:
            if (job.status == "processing" and 
                hasattr(job, 'started_at') and 
                job.started_at < five_minutes_ago):
                stuck_jobs.append(job)
        
        # Generate alerts
        if recent_failures:
            alerts.append({
                'type': 'error',
                'title': f'🚨 {len(recent_failures)} Recent Processing Failure(s)',
                'message': f'Email processing failed for files: {", ".join([job.filename for job in recent_failures[:3]])}',
                'action': 'Check error details below and verify brokerage settings'
            })
        
        if stuck_jobs:
            alerts.append({
                'type': 'warning', 
                'title': f'⏱️ {len(stuck_jobs)} Job(s) Taking Too Long',
                'message': f'Processing has been running for over 5 minutes: {", ".join([job.filename for job in stuck_jobs[:2]])}',
                'action': 'Check system resources or restart processing if needed'
            })
        
        # Check for high failure rate
        if completed_jobs:
            recent_completed = [job for job in completed_jobs if hasattr(job, 'started_at') and job.started_at > ten_minutes_ago]
            if recent_completed:
                failed_count = len([job for job in recent_completed if job.status == "failed"])
                failure_rate = (failed_count / len(recent_completed)) * 100
                
                if failure_rate > 50 and len(recent_completed) >= 3:
                    alerts.append({
                        'type': 'error',
                        'title': f'📊 High Failure Rate Detected ({failure_rate:.0f}%)',
                        'message': f'{failed_count} out of {len(recent_completed)} recent jobs failed',
                        'action': 'Check FF2API credentials and field mappings immediately'
                    })
        
        # Render alerts
        if alerts:
            for alert in alerts:
                if alert['type'] == 'error':
                    st.error(f"**{alert['title']}**\n\n{alert['message']}\n\n💡 **Action Required:** {alert['action']}")
                elif alert['type'] == 'warning':
                    st.warning(f"**{alert['title']}**\n\n{alert['message']}\n\n💡 **Suggestion:** {alert['action']}")
                
                # Add dismiss button for each alert
                if st.button(f"✖️ Dismiss Alert", key=f"dismiss_{hash(alert['title'])}"):
                    # Store dismissed alerts in session state
                    if 'dismissed_alerts' not in st.session_state:
                        st.session_state.dismissed_alerts = set()
                    st.session_state.dismissed_alerts.add(alert['title'])
                    st.rerun()
    
    def _render_system_errors(self, brokerage_key: str):
        """Render system errors and issues."""
        try:
            import streamlit as st
            if hasattr(st, 'session_state') and 'dashboard_errors' in st.session_state:
                # Filter errors for this brokerage
                brokerage_errors = [
                    error for error in st.session_state.dashboard_errors 
                    if error.get('brokerage_key') == brokerage_key
                ]
                
                if brokerage_errors:
                    # Only show recent errors (last hour)
                    recent_errors = []
                    one_hour_ago = datetime.now() - timedelta(hours=1)
                    
                    for error in brokerage_errors[-5:]:  # Last 5 errors
                        try:
                            error_time = datetime.fromisoformat(error['timestamp'])
                            if error_time > one_hour_ago:
                                recent_errors.append(error)
                        except (ValueError, KeyError):
                            # Include errors with invalid timestamps
                            recent_errors.append(error)
                    
                    if recent_errors:
                        with st.expander("⚠️ System Issues", expanded=len(recent_errors) > 0):
                            for error in recent_errors:
                                error_type = error.get('error_type', 'unknown')
                                message = error.get('message', 'Unknown error')
                                timestamp = error.get('timestamp', 'Unknown time')
                                
                                if error_type == 'critical_system_error':
                                    st.error(f"🚨 **Critical Error** ({timestamp}): {message}")
                                    st.info("💡 **Suggestion**: Try refreshing the page. If the error persists, contact support.")
                                elif error_type == 'data_retrieval_failure':
                                    st.warning(f"⚠️ **Data Issue** ({timestamp}): {message}")
                                    st.info("💡 **Suggestion**: Check if background processing is running and try refreshing.")
                                else:
                                    st.warning(f"⚠️ **System Issue** ({timestamp}): {message}")
                            
                            # Add clear errors button
                            if st.button("🧹 Clear Error History", key=f"clear_errors_{brokerage_key}"):
                                st.session_state.dashboard_errors = [
                                    e for e in st.session_state.dashboard_errors 
                                    if e.get('brokerage_key') != brokerage_key
                                ]
                                st.rerun()
                
        except Exception as e:
            logger.error(f"Error rendering system errors: {e}")
    
    def _convert_storage_job_to_dashboard_job(self, storage_job) -> Optional[EmailProcessingJob]:
        """Convert storage job format to dashboard job format."""
        try:
            # Handle both dataclass and dict formats
            if hasattr(storage_job, 'job_id'):
                # Dataclass format (from shared storage)
                return EmailProcessingJob(
                    job_id=storage_job.job_id,
                    filename=storage_job.filename,
                    brokerage_key=storage_job.brokerage_key,
                    email_source=storage_job.email_source,
                    file_size=0,  # Not stored in unified storage
                    record_count=storage_job.record_count,
                    started_at=datetime.fromisoformat(storage_job.started_at) if isinstance(storage_job.started_at, str) else storage_job.started_at,
                    current_step=storage_job.current_step,
                    progress_percent=storage_job.progress_percent,
                    status=storage_job.status,
                    error_message=storage_job.error_message,
                    success_count=storage_job.success_count,
                    failure_count=storage_job.failure_count,
                    processing_time=storage_job.processing_time,
                    result_data=getattr(storage_job, 'result_data', None)
                )
            else:
                # Dict format (from session state)
                return EmailProcessingJob(
                    job_id=storage_job.get('job_id', ''),
                    filename=storage_job.get('filename', ''),
                    brokerage_key=storage_job.get('brokerage_key', ''),
                    email_source=storage_job.get('email_source', ''),
                    file_size=storage_job.get('file_size', 0),
                    record_count=storage_job.get('record_count', 0),
                    started_at=datetime.fromisoformat(storage_job.get('started_at', datetime.now().isoformat())) if isinstance(storage_job.get('started_at'), str) else storage_job.get('started_at', datetime.now()),
                    current_step=storage_job.get('current_step', 'queued'),
                    progress_percent=storage_job.get('progress_percent', 0.0),
                    status=storage_job.get('status', 'pending'),
                    error_message=storage_job.get('error_message', ''),
                    success_count=storage_job.get('success_count', 0),
                    failure_count=storage_job.get('failure_count', 0),
                    processing_time=storage_job.get('processing_time', 0.0),
                    result_data=storage_job.get('result_data')
                )
        except Exception as e:
            logger.error(f"Error converting storage job to dashboard job: {e}")
            return None
    
    def _add_storage_health_error(self, brokerage_key: str, system_name: str, error_message: str):
        """Add storage health error to UI error tracking."""
        try:
            import streamlit as st
            if hasattr(st, 'session_state'):
                if 'dashboard_errors' not in st.session_state:
                    st.session_state.dashboard_errors = []
                st.session_state.dashboard_errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'error_type': 'storage_health_issue',
                    'message': f"{system_name} storage issue: {error_message}",
                    'brokerage_key': brokerage_key
                })
        except Exception as e:
            logger.error(f"Error tracking storage health error: {e}")
    
    def _convert_canonical_job_to_dashboard_job(self, canonical_job) -> Optional[EmailProcessingJob]:
        """Convert canonical EmailJob to dashboard EmailProcessingJob format."""
        try:
            from data_models import DataModelConverter
            return DataModelConverter.to_dashboard_job(canonical_job)
        except Exception as e:
            logger.error(f"Error converting canonical job to dashboard job: {e}")
            return None
    
    def _render_active_processing_section(self, active_jobs: List[EmailProcessingJob]):
        """Render active processing jobs with same UI as manual upload."""
        st.markdown("### 🔄 Currently Processing")
        
        for job in active_jobs:
            if job.status == "processing":
                # Use the same components as manual upload
                self._render_processing_job_card(job)
    
    def _render_processing_job_card(self, job: EmailProcessingJob):
        """Render individual processing job card matching manual upload UI."""
        with st.container():
            # File info header (same as manual upload)
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.markdown(f"**📧 {job.filename}**")
                st.caption(f"From: {job.email_source}")
            
            with col2:
                st.metric("Records", job.record_count)
            
            with col3:
                processing_time = (datetime.now() - job.started_at).total_seconds()
                st.metric("Time", f"{processing_time:.1f}s")
            
            # Progress display (same component as manual upload)
            self._render_email_processing_progress(job)
            
            # Live data preview if available
            if job.result_data and job.result_data.get('preview_data'):
                self._render_email_data_preview(job.result_data['preview_data'])
    
    def _render_email_processing_progress(self, job: EmailProcessingJob):
        """Render enhanced processing progress with detailed visualization."""
        # Determine current step index
        step_index = self._get_step_index_from_status(job.current_step)
        total_steps = len(self.processing_steps)
        progress = (step_index + 1) / total_steps if step_index >= 0 else job.progress_percent / 100
        
        # Enhanced progress visualization
        if step_index >= 0 and step_index < total_steps:
            # Main progress bar
            st.info(f"⚡ Processing: {self.processing_steps[step_index]} ({step_index + 1}/{total_steps})")
            st.progress(progress)
            
            # Detailed step-by-step visualization
            self._render_step_by_step_progress(step_index, job)
            
        else:
            # Fallback for unknown steps
            st.info(f"⚡ Processing: {job.current_step}")
            st.progress(job.progress_percent / 100)
        
        # Show timing information
        self._render_progress_timing(job)
    
    def _render_step_by_step_progress(self, current_step_index: int, job: EmailProcessingJob):
        """Render detailed step-by-step progress visualization."""
        try:
            # Define step details with icons, descriptions, and typical durations
            step_details = [
                {"icon": "📧", "name": "Parsing Email", "desc": "Extracting attachment data", "est_time": "2-5s"},
                {"icon": "🔍", "name": "Analyzing Data", "desc": "Validating columns and data types", "est_time": "5-10s"},
                {"icon": "🔗", "name": "Applying Mappings", "desc": "Mapping fields to FF2API format", "est_time": "10-20s"},
                {"icon": "⚡", "name": "Submitting to API", "desc": "Sending data to FreightForwarder2", "est_time": "30-60s"},
                {"icon": "🌟", "name": "Enriching Data", "desc": "Adding tracking and analytics", "est_time": "10-30s"},
                {"icon": "📁", "name": "Generating Results", "desc": "Creating output files", "est_time": "5-15s"}
            ]
            
            # Create columns for each step
            cols = st.columns(len(step_details))
            
            for i, (col, step_detail) in enumerate(zip(cols, step_details)):
                with col:
                    if i < current_step_index:
                        # Completed step
                        st.success(f"✅ {step_detail['icon']}")
                        st.caption(f"**{step_detail['name']}**")
                        st.caption("✅ Completed")
                    elif i == current_step_index:
                        # Current step
                        st.info(f"🔄 {step_detail['icon']}")
                        st.caption(f"**{step_detail['name']}**")
                        st.caption(f"🔄 {step_detail['desc']}")
                        
                        # Show more detailed progress for current step
                        if hasattr(job, 'progress_percent'):
                            step_progress = (job.progress_percent % (100/len(step_details))) / (100/len(step_details))
                            st.progress(step_progress)
                            st.caption(f"⏱️ Est: {step_detail['est_time']}")
                    else:
                        # Pending step
                        st.empty()
                        st.caption(f"⏳ {step_detail['icon']}")
                        st.caption(f"**{step_detail['name']}**")
                        st.caption("⏳ Pending")
            
            # Overall progress summary
            st.markdown(f"""
            <div style="text-align: center; padding: 10px; background-color: #f0f2f6; border-radius: 5px; margin-top: 10px;">
                <strong>Progress: {current_step_index + 1} of {len(step_details)} steps completed</strong><br/>
                <small>Currently: {step_details[current_step_index]['desc']}</small>
            </div>
            """, unsafe_allow_html=True)
            
        except Exception as e:
            logger.error(f"Error rendering step-by-step progress: {e}")
            # Fallback to simple progress
            st.info("🔄 Processing in progress...")
    
    def _render_progress_timing(self, job: EmailProcessingJob):
        """Render timing information for the processing job."""
        try:
            # Calculate timing metrics
            if hasattr(job, 'started_at') and job.started_at:
                elapsed = (datetime.now() - job.started_at).total_seconds()
                
                # Create timing display
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if elapsed < 60:
                        st.metric("⏱️ Elapsed", f"{elapsed:.0f}s")
                    else:
                        st.metric("⏱️ Elapsed", f"{elapsed/60:.1f}m")
                
                with col2:
                    # Estimate remaining time based on current step
                    step_index = self._get_step_index_from_status(job.current_step)
                    if step_index >= 0:
                        # Rough estimates for remaining time per step
                        remaining_estimates = [120, 90, 60, 30, 15, 5]  # seconds per remaining step
                        if step_index < len(remaining_estimates):
                            estimated_remaining = sum(remaining_estimates[step_index:])
                            if estimated_remaining < 60:
                                st.metric("⏳ Est. Remaining", f"{estimated_remaining}s")
                            else:
                                st.metric("⏳ Est. Remaining", f"{estimated_remaining/60:.1f}m")
                        else:
                            st.metric("⏳ Est. Remaining", "Almost done!")
                    else:
                        st.metric("⏳ Est. Remaining", "Calculating...")
                
                with col3:
                    # Show records being processed
                    if hasattr(job, 'record_count') and job.record_count > 0:
                        st.metric("📊 Records", f"{job.record_count:,}")
                    else:
                        st.metric("📊 Records", "Counting...")
                
                # Performance indicator
                if elapsed > 0:
                    if elapsed < 30:
                        st.success("🚀 Processing quickly")
                    elif elapsed < 120:
                        st.info("⚡ Processing normally") 
                    elif elapsed < 300:
                        st.warning("🐌 Processing slowly")
                    else:
                        st.error("⚠️ Processing taking longer than expected")
            
        except Exception as e:
            logger.debug(f"Error rendering progress timing: {e}")
    
    def _render_email_data_preview(self, preview_data: Dict[str, Any]):
        """Render data preview using same component as manual upload."""
        try:
            # Convert preview data to DataFrame
            if 'columns' in preview_data and 'sample_rows' in preview_data:
                df = pd.DataFrame(preview_data['sample_rows'], columns=preview_data['columns'])
                
                # Use the same data preview card as manual upload
                from src.frontend.ui_components import create_data_preview_card
                create_data_preview_card(df)
        except Exception as e:
            logger.debug(f"Error rendering email data preview: {e}")
    
    def _render_processing_queue(self, brokerage_key: str):
        """Render email processing queue status."""
        # Get queue status
        queue_status = self._get_queue_status(brokerage_key)
        
        if queue_status['total'] > 0:
            st.markdown("### 📋 Processing Queue")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total", queue_status['total'])
            
            with col2:
                st.metric("Processing", queue_status['processing'])
            
            with col3:
                st.metric("Queued", queue_status['queued'])
                
            with col4:
                st.metric("Completed Today", queue_status['completed_today'])
                
            # Queue health indicator
            if queue_status['processing'] > 0:
                st.success("📧 Email automation is actively processing files")
            elif queue_status['queued'] > 0:
                st.info("⏳ Files are queued for processing")
            else:
                st.info("✅ No files currently in queue")
    
    def _render_recent_results(self, completed_jobs: List[EmailProcessingJob]):
        """Render recent processing results using same format as manual upload."""
        st.markdown("### 📈 Recent Email Processing Results")
        
        # Separate failed and successful jobs
        recent_jobs = sorted(completed_jobs, key=lambda x: x.started_at, reverse=True)[:10]
        failed_jobs = [job for job in recent_jobs if job.status == "failed"]
        successful_jobs = [job for job in recent_jobs if job.status == "completed"]
        
        # Show failed jobs prominently first
        if failed_jobs:
            st.markdown("#### 🚨 Failed Processing Jobs")
            st.error(f"⚠️ **{len(failed_jobs)} job(s) failed processing** - Check details below for troubleshooting")
            
            for job in failed_jobs[:3]:  # Show up to 3 most recent failures
                self._render_failed_job_card(job)
        
        # Show successful jobs
        if successful_jobs:
            if failed_jobs:
                st.markdown("#### ✅ Successful Processing Jobs")
            
            for job in successful_jobs[:5]:  # Show up to 5 successful jobs
                self._render_completed_job_card(job)
    
    def _render_completed_job_card(self, job: EmailProcessingJob):
        """Render completed job card matching manual upload results display."""
        # Determine status color and icon
        if job.status == "completed":
            status_color = "success" if job.failure_count == 0 else "warning"
            icon = "✅" if job.failure_count == 0 else "⚠️"
        else:
            status_color = "error"
            icon = "❌"
        
        with st.expander(f"{icon} {job.filename} - {job.success_count}/{job.record_count} successful"):
            
            # Results summary using same component as manual upload
            if job.status == "completed":
                from src.frontend.ui_components import create_results_summary_card
                create_results_summary_card(job.success_count, job.failure_count, job.processing_time)
            
            # Job details
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Source:** 📧 {job.email_source}")
                st.write(f"**Started:** {job.started_at.strftime('%H:%M:%S')}")
                st.write(f"**Duration:** {job.processing_time:.1f}s")
                
            with col2:
                st.write(f"**Records:** {job.record_count}")
                st.write(f"**Success:** {job.success_count}")
                st.write(f"**Errors:** {job.failure_count}")
            
            # Error details if any
            if job.error_message:
                st.error(f"**Error:** {job.error_message}")
            
            # Download results (same as manual upload)
            if job.result_data and job.result_data.get('enriched_data'):
                self._render_download_buttons(job)
    
    def _render_failed_job_card(self, job: EmailProcessingJob):
        """Render failed job card with detailed error information."""
        with st.expander(f"❌ {job.filename} - FAILED", expanded=True):
            
            # Prominent error display
            st.error(f"**Processing Failed:** {job.error_message}")
            
            # Job details
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Source:** 📧 {job.email_source}")
                st.write(f"**Started:** {job.started_at.strftime('%H:%M:%S')}")
                st.write(f"**Duration:** {job.processing_time:.1f}s")
                
            with col2:
                st.write(f"**Records:** {job.record_count}")
                st.write(f"**Success:** {job.success_count}")
                st.write(f"**Errors:** {job.failure_count}")
            
            # Detailed error analysis
            st.markdown("**🔍 Error Analysis:**")
            error_msg = job.error_message.lower()
            
            if "ff2api" in error_msg or "401" in error_msg:
                st.warning("""
                **FF2API Authentication Issue:**
                - Check FF2API credentials for this brokerage
                - Verify API endpoint configuration
                - Ensure brokerage has proper API access permissions
                """)
            elif "mapping" in error_msg or "field" in error_msg:
                st.info("""
                **Field Mapping Issue:**
                - Check field mappings for this brokerage
                - Verify column names in uploaded file
                - Update field mappings if data structure changed
                """)
            elif "unsupported file type" in error_msg:
                st.info("""
                **File Type Issue:**
                - Supported formats: CSV, Excel (.xlsx, .xls), JSON
                - Check if file is corrupted or in unsupported format
                """)
            else:
                st.warning("""
                **General Processing Error:**
                - Check logs for detailed error information
                - Contact support if issue persists
                """)
            
            # Suggested actions
            st.markdown("**🛠️ Suggested Actions:**")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button(f"🔄 Retry Processing", key=f"retry_{job.job_id}"):
                    st.info("Manual retry functionality - reprocess this file through manual upload")
            
            with col2:
                if st.button(f"📋 Copy Error Details", key=f"copy_{job.job_id}"):
                    st.code(f"File: {job.filename}\nError: {job.error_message}\nTime: {job.started_at}")
            
            with col3:
                if st.button(f"🔧 Check Settings", key=f"settings_{job.job_id}"):
                    st.info("Check brokerage field mappings and API credentials in main settings")
    
    def _render_download_buttons(self, job: EmailProcessingJob):
        """Render download buttons matching manual upload interface."""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button(f"📄 Download CSV", key=f"csv_{job.job_id}"):
                csv_data = self._generate_csv_download(job.result_data['enriched_data'])
                st.download_button(
                    "📄 Download CSV", 
                    csv_data, 
                    f"email_auto_{job.filename}.csv", 
                    "text/csv",
                    key=f"csv_dl_{job.job_id}"
                )
        
        with col2:
            if st.button(f"📊 Download Excel", key=f"excel_{job.job_id}"):
                st.info("Excel download functionality")  # Placeholder
                
        with col3:
            if st.button(f"📋 Download JSON", key=f"json_{job.job_id}"):
                json_data = json.dumps(job.result_data['enriched_data'], indent=2, default=str)
                st.download_button(
                    "📋 Download JSON", 
                    json_data, 
                    f"email_auto_{job.filename}.json", 
                    "application/json",
                    key=f"json_dl_{job.job_id}"
                )
    
    def _get_processing_jobs(self, brokerage_key: str) -> Tuple[List[EmailProcessingJob], List[EmailProcessingJob]]:
        """Get active and completed processing jobs using unified storage."""
        try:
            # Use unified storage for automatic failover and health monitoring
            from unified_storage import unified_storage
            
            # Get jobs through unified storage (handles failover automatically)
            active_storage_jobs = unified_storage.get_active_jobs(brokerage_key)
            completed_storage_jobs = unified_storage.get_completed_jobs(brokerage_key)
            
            # Convert to EmailProcessingJob format
            active_jobs = []
            completed_jobs = []
            
            # Process active jobs (already canonical EmailJob objects)
            for canonical_job in active_storage_jobs:
                dashboard_job = self._convert_canonical_job_to_dashboard_job(canonical_job)
                if dashboard_job:
                    active_jobs.append(dashboard_job)
            
            # Process completed jobs (already canonical EmailJob objects)
            for canonical_job in completed_storage_jobs:
                dashboard_job = self._convert_canonical_job_to_dashboard_job(canonical_job)
                if dashboard_job:
                    completed_jobs.append(dashboard_job)
            
            # Check storage health and surface any issues
            health_status = unified_storage.get_storage_health()
            for system_name, status in health_status.items():
                if not status.is_available and status.last_error:
                    self._add_storage_health_error(brokerage_key, system_name, status.last_error)
            
            return active_jobs, completed_jobs
            
        except Exception as e:
            logger.critical(f"Unified storage failed completely: {e}")
            # Surface critical errors to UI
            import streamlit as st
            if hasattr(st, 'session_state'):
                if 'dashboard_errors' not in st.session_state:
                    st.session_state.dashboard_errors = []
                st.session_state.dashboard_errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'error_type': 'storage_system_failure',
                    'message': f"All storage systems failed: {str(e)}",
                    'brokerage_key': brokerage_key
                })
            return [], []
    
    def _get_jobs_from_storage(self, brokerage_key: str) -> List[EmailProcessingJob]:
        """Get jobs from session state storage."""
        try:
            # Check session state for email processing jobs
            if 'email_processing_jobs' not in st.session_state:
                st.session_state.email_processing_jobs = {}
            
            brokerage_jobs = st.session_state.email_processing_jobs.get(brokerage_key, [])
            
            # Convert dictionaries back to EmailProcessingJob objects
            jobs = []
            for job_data in brokerage_jobs:
                try:
                    if isinstance(job_data, dict):
                        job = EmailProcessingJob.from_dict(job_data)
                    else:
                        job = job_data
                    jobs.append(job)
                except Exception as e:
                    logger.debug(f"Error deserializing job: {e}")
            
            return jobs
            
        except Exception as e:
            logger.error(f"Error getting jobs from session state storage: {e}")
            # Surface error to UI
            import streamlit as st
            if hasattr(st, 'session_state'):
                if 'dashboard_errors' not in st.session_state:
                    st.session_state.dashboard_errors = []
                st.session_state.dashboard_errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'error_type': 'session_storage_error',
                    'message': f"Cannot retrieve jobs from session storage: {str(e)}",
                    'brokerage_key': brokerage_key
                })
            return []
    
    def _get_queue_status(self, brokerage_key: str) -> Dict[str, int]:
        """Get processing queue status using unified storage."""
        try:
            from unified_storage import unified_storage
            
            # Get stats through unified storage (handles failover automatically)
            stats = unified_storage.get_processing_stats(brokerage_key)
            
            return {
                'total': stats.get('total', 0),
                'processing': stats.get('processing', 0),
                'queued': stats.get('pending', 0),  # 'pending' in storage = 'queued' in UI
                'completed_today': stats.get('completed_today', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting queue status from unified storage: {e}")
            # Surface error to UI
            import streamlit as st
            if hasattr(st, 'session_state'):
                if 'dashboard_errors' not in st.session_state:
                    st.session_state.dashboard_errors = []
                st.session_state.dashboard_errors.append({
                    'timestamp': datetime.now().isoformat(),
                    'error_type': 'queue_status_error',
                    'message': f"Cannot retrieve queue status: {str(e)}",
                    'brokerage_key': brokerage_key
                })
            # Return zeros but user will see the error in UI
            return {'total': 0, 'processing': 0, 'queued': 0, 'completed_today': 0}
    
    def _get_step_index_from_status(self, current_step: str) -> int:
        """Map current step status to step index."""
        step_mapping = {
            "parsing_email": 0,
            "analyzing_data": 1,
            "applying_mappings": 2,
            "submitting_api": 3,
            "enriching_data": 4,
            "generating_results": 5
        }
        
        return step_mapping.get(current_step, -1)
    
    def _generate_csv_download(self, enriched_data: List[Dict[str, Any]]) -> str:
        """Generate CSV data for download."""
        try:
            df = pd.DataFrame(enriched_data)
            return df.to_csv(index=False)
        except Exception as e:
            logger.error(f"Error generating CSV: {e}")
            return "Error generating CSV data"
    
    def _setup_auto_refresh(self):
        """Setup auto-refresh for real-time updates."""
        # Check if there are active jobs that need monitoring
        try:
            brokerage_key = st.session_state.get('brokerage_name', '')
            active_jobs, _ = self._get_processing_jobs(brokerage_key)
            
            if active_jobs:
                # Show real-time refresh indicator
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button("🔄 Refresh", key="email_refresh"):
                        st.rerun()
                
                # Use Streamlit-native auto-refresh with proper timing
                if 'last_email_refresh' not in st.session_state:
                    st.session_state.last_email_refresh = time.time()
                
                current_time = time.time()
                time_since_refresh = current_time - st.session_state.last_email_refresh
                
                # Show refresh countdown
                refresh_interval = 5  # 5 seconds for active jobs
                time_until_refresh = max(0, refresh_interval - time_since_refresh)
                
                if time_until_refresh > 0:
                    st.info(f"⏱️ Auto-refreshing in {time_until_refresh:.0f}s")
                else:
                    st.success("🔄 Refreshing...")
                    st.session_state.last_email_refresh = current_time
                    time.sleep(0.1)  # Small delay to prevent rapid refresh
                    st.rerun()
                    
        except Exception as e:
            logger.debug(f"Error in auto-refresh setup: {e}")
    
    def _render_dashboard_controls(self, brokerage_key: str):
        """Render enhanced dashboard controls and status indicators."""
        try:
            st.markdown("---")
            st.markdown("### 📊 Dashboard Status")
            
            # Get system health
            from unified_storage import unified_storage
            health_status = unified_storage.get_storage_health()
            
            # Create status columns
            col1, col2, col3, col4 = st.columns(4)
            
            # System health indicator
            with col1:
                all_systems_healthy = all(status.is_available for status in health_status.values())
                if all_systems_healthy:
                    st.metric("System Status", "🟢 Online", delta="All systems operational")
                else:
                    failed_systems = [name for name, status in health_status.items() if not status.is_available]
                    st.metric("System Status", "🔴 Issues", delta=f"{len(failed_systems)} system(s) down", delta_color="inverse")
            
            # Data freshness indicator
            with col2:
                last_refresh = st.session_state.get('last_email_refresh', 0)
                if last_refresh > 0:
                    refresh_age = time.time() - last_refresh
                    if refresh_age < 10:
                        st.metric("Data Freshness", "🟢 Fresh", delta=f"{refresh_age:.0f}s ago")
                    elif refresh_age < 60:
                        st.metric("Data Freshness", "🟡 Recent", delta=f"{refresh_age:.0f}s ago")
                    else:
                        st.metric("Data Freshness", "🔴 Stale", delta=f"{refresh_age/60:.0f}m ago", delta_color="inverse")
                else:
                    st.metric("Data Freshness", "❓ Unknown", delta="Never refreshed")
            
            # Active jobs count
            with col3:
                try:
                    active_jobs, _ = self._get_processing_jobs(brokerage_key)
                    active_count = len([job for job in active_jobs if job.status == "processing"])
                    pending_count = len([job for job in active_jobs if job.status == "pending"])
                    
                    if active_count > 0:
                        st.metric("Active Jobs", f"🔄 {active_count}", delta=f"{pending_count} queued")
                    elif pending_count > 0:
                        st.metric("Active Jobs", f"⏸️ {pending_count}", delta="queued")
                    else:
                        st.metric("Active Jobs", "✅ 0", delta="idle")
                except Exception as e:
                    st.metric("Active Jobs", "❌ Error", delta=str(e)[:20])
            
            # Storage performance
            with col4:
                if health_status:
                    avg_performance = sum(status.performance_ms for status in health_status.values() if status.is_available)
                    healthy_count = sum(1 for status in health_status.values() if status.is_available)
                    
                    if healthy_count > 0:
                        avg_performance = avg_performance / healthy_count
                        if avg_performance < 100:
                            st.metric("Storage Speed", f"🚀 {avg_performance:.0f}ms", delta="fast")
                        elif avg_performance < 500:
                            st.metric("Storage Speed", f"⚡ {avg_performance:.0f}ms", delta="normal")
                        else:
                            st.metric("Storage Speed", f"🐌 {avg_performance:.0f}ms", delta="slow", delta_color="inverse")
                    else:
                        st.metric("Storage Speed", "❌ N/A", delta="systems down")
                else:
                    st.metric("Storage Speed", "❓ Unknown", delta="no data")
            
            # Advanced controls
            with st.expander("🛠️ Advanced Controls", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**System Actions**")
                    if st.button("🔄 Force Full Refresh", help="Force a complete data refresh"):
                        # Clear all cached data
                        for key in list(st.session_state.keys()):
                            if 'email' in key or 'refresh' in key:
                                del st.session_state[key]
                        st.rerun()
                    
                    if st.button("🧹 Clear Error History", help="Clear all error notifications"):
                        if 'dashboard_errors' in st.session_state:
                            st.session_state.dashboard_errors = []
                        st.success("Error history cleared")
                    
                    if st.button("💾 Migrate Session Data", help="Migrate session data to shared storage"):
                        try:
                            unified_storage.migrate_session_to_shared(brokerage_key)
                            st.success("Session data migrated successfully")
                        except Exception as e:
                            st.error(f"Migration failed: {e}")
                
                with col2:
                    st.markdown("**Debug Information**")
                    
                    # Show detailed health status
                    for system_name, status in health_status.items():
                        status_color = "🟢" if status.is_available else "🔴"
                        st.write(f"{status_color} **{system_name}**: {status.performance_ms:.0f}ms")
                        if not status.is_available and status.last_error:
                            st.caption(f"❌ {status.last_error}")
                    
                    # Show session state size
                    session_size = len(str(st.session_state))
                    st.write(f"📊 **Session Size**: {session_size:,} chars")
                    
                    # Show brokerage key info
                    from brokerage_key_utils import BrokerageKeyManager
                    normalized_key = BrokerageKeyManager.normalize(brokerage_key)
                    st.write(f"🏢 **Brokerage**: `{normalized_key}`")
        
        except Exception as e:
            logger.error(f"Error rendering dashboard controls: {e}")
            st.error(f"Dashboard controls error: {e}")
    
    def add_processing_job(self, job: EmailProcessingJob):
        """Add a new processing job to the dashboard."""
        try:
            if 'email_processing_jobs' not in st.session_state:
                st.session_state.email_processing_jobs = {}
            
            if job.brokerage_key not in st.session_state.email_processing_jobs:
                st.session_state.email_processing_jobs[job.brokerage_key] = []
            
            # Add job as dictionary for session state compatibility
            st.session_state.email_processing_jobs[job.brokerage_key].append(job.to_dict())
            
            logger.info(f"Added email processing job: {job.job_id}")
            
        except Exception as e:
            logger.error(f"Error adding processing job: {e}")
    
    def update_job_progress(self, job_id: str, brokerage_key: str, progress_data: Dict[str, Any]):
        """Update job progress in real-time."""
        try:
            if 'email_processing_jobs' not in st.session_state:
                return
            
            jobs = st.session_state.email_processing_jobs.get(brokerage_key, [])
            
            # Find and update the job
            for i, job_data in enumerate(jobs):
                if job_data.get('job_id') == job_id:
                    # Update job data
                    job_data.update(progress_data)
                    
                    # If this is a completion update, calculate final metrics
                    if progress_data.get('status') == 'completed':
                        job_data['processing_time'] = (datetime.now() - datetime.fromisoformat(job_data['started_at'])).total_seconds()
                        
                        # Store result data for downloads
                        if 'processing_result' in st.session_state:
                            result = st.session_state.processing_result
                            if hasattr(result, 'summary'):
                                job_data['success_count'] = result.summary.get('ff2api_success', 0)
                                job_data['failure_count'] = result.summary.get('total_rows', 0) - job_data['success_count']
                            
                            # Store enriched data for downloads
                            if hasattr(result, 'enriched_data'):
                                job_data['result_data'] = {
                                    'enriched_data': result.enriched_data.to_dict('records') if hasattr(result.enriched_data, 'to_dict') else result.enriched_data,
                                    'summary': result.summary,
                                    'errors': result.errors if hasattr(result, 'errors') else []
                                }
                    
                    jobs[i] = job_data
                    st.session_state.email_processing_jobs[brokerage_key] = jobs
                    
                    # Force UI refresh for real-time updates
                    if progress_data.get('status') in ['processing', 'completed', 'failed']:
                        st.session_state['email_dashboard_refresh'] = datetime.now().timestamp()
                    
                    break
                    
        except Exception as e:
            logger.error(f"Error updating job progress: {e}")
    
    def render_email_activity_sidebar(self, brokerage_key: str):
        """Render compact email activity display for sidebar."""
        active_jobs, completed_jobs = self._get_processing_jobs(brokerage_key)
        
        if active_jobs or completed_jobs:
            st.markdown("---")
            st.subheader("📧 Email Activity")
            
            # Active processing indicator
            if active_jobs:
                for job in active_jobs[:1]:  # Show only first active job in sidebar
                    st.info(f"🔄 Processing: {job.filename}")
                    st.progress(job.progress_percent / 100)
            
            # Recent completions
            if completed_jobs:
                recent = completed_jobs[0]  # Most recent
                success_rate = recent.success_count / recent.record_count * 100 if recent.record_count > 0 else 0
                
                if success_rate == 100:
                    st.success(f"✅ {recent.filename}: {recent.success_count} records")
                else:
                    st.warning(f"⚠️ {recent.filename}: {recent.success_count}/{recent.record_count}")


# Global instance
email_processing_dashboard = EmailProcessingDashboard()

# Helper functions for easy integration
def render_email_processing_dashboard(brokerage_key: str):
    """Render the email processing dashboard."""
    email_processing_dashboard.render_email_processing_dashboard(brokerage_key)

def render_email_activity_sidebar(brokerage_key: str):
    """Render email activity in sidebar."""
    email_processing_dashboard.render_email_activity_sidebar(brokerage_key)

def add_email_processing_job(filename: str, brokerage_key: str, email_source: str, 
                           record_count: int, file_size: int = 0) -> str:
    """Add a new email processing job."""
    job_id = f"email_{int(datetime.now().timestamp())}_{filename.replace('.', '_')}"
    
    job = EmailProcessingJob(
        job_id=job_id,
        filename=filename,
        brokerage_key=brokerage_key,
        email_source=email_source,
        file_size=file_size,
        record_count=record_count,
        started_at=datetime.now()
    )
    
    email_processing_dashboard.add_processing_job(job)
    return job_id

def update_email_job_progress(job_id: str, brokerage_key: str, step: str, 
                            progress: float, status: str = "processing"):
    """Update email job progress."""
    progress_data = {
        'current_step': step,
        'progress_percent': progress,
        'status': status
    }
    
    email_processing_dashboard.update_job_progress(job_id, brokerage_key, progress_data)