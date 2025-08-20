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
        
        # Get current processing jobs
        active_jobs, completed_jobs = self._get_processing_jobs(brokerage_key)
        
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
        """Render processing progress using same UI as manual upload."""
        # Determine current step index
        step_index = self._get_step_index_from_status(job.current_step)
        total_steps = len(self.processing_steps)
        progress = (step_index + 1) / total_steps if step_index >= 0 else job.progress_percent / 100
        
        # Use same format as manual processing
        if step_index >= 0 and step_index < total_steps:
            st.info(f"⚡ Processing: {self.processing_steps[step_index]} ({step_index + 1}/{total_steps})")
            st.progress(progress)
        else:
            st.info(f"⚡ Processing: {job.current_step}")
            st.progress(job.progress_percent / 100)
    
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
        
        # Show last 5 completed jobs
        recent_jobs = sorted(completed_jobs, key=lambda x: x.started_at, reverse=True)[:5]
        
        for job in recent_jobs:
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
        """Get active and completed processing jobs from shared storage."""
        try:
            # Try to get jobs from shared storage first
            from shared_storage_bridge import shared_storage
            
            # Convert shared storage jobs to EmailProcessingJob format
            active_jobs = []
            completed_jobs = []
            
            # Get active jobs from shared storage
            shared_active = shared_storage.get_active_jobs(brokerage_key)
            for shared_job in shared_active:
                job = EmailProcessingJob(
                    job_id=shared_job.job_id,
                    filename=shared_job.filename,
                    brokerage_key=shared_job.brokerage_key,
                    email_source=shared_job.email_source,
                    file_size=0,  # Not stored in shared storage
                    record_count=shared_job.record_count,
                    started_at=datetime.fromisoformat(shared_job.started_at),
                    current_step=shared_job.current_step,
                    progress_percent=shared_job.progress_percent,
                    status=shared_job.status,
                    error_message=shared_job.error_message,
                    success_count=shared_job.success_count,
                    failure_count=shared_job.failure_count,
                    processing_time=shared_job.processing_time,
                    result_data=shared_job.result_data
                )
                active_jobs.append(job)
            
            # Get completed jobs from shared storage
            shared_completed = shared_storage.get_completed_jobs(brokerage_key)
            for shared_job in shared_completed:
                job = EmailProcessingJob(
                    job_id=shared_job.job_id,
                    filename=shared_job.filename,
                    brokerage_key=shared_job.brokerage_key,
                    email_source=shared_job.email_source,
                    file_size=0,  # Not stored in shared storage
                    record_count=shared_job.record_count,
                    started_at=datetime.fromisoformat(shared_job.started_at),
                    current_step=shared_job.current_step,
                    progress_percent=shared_job.progress_percent,
                    status=shared_job.status,
                    error_message=shared_job.error_message,
                    success_count=shared_job.success_count,
                    failure_count=shared_job.failure_count,
                    processing_time=shared_job.processing_time,
                    result_data=shared_job.result_data
                )
                completed_jobs.append(job)
            
            return active_jobs, completed_jobs
            
        except ImportError:
            logger.debug("Shared storage not available, falling back to session state")
            # Fallback to session state method
            all_jobs = self._get_jobs_from_storage(brokerage_key)
            active_jobs = [job for job in all_jobs if job.status in ["pending", "processing"]]
            completed_jobs = [job for job in all_jobs if job.status in ["completed", "failed"]]
            return active_jobs, completed_jobs
        
        except Exception as e:
            logger.error(f"Error getting processing jobs from shared storage: {e}")
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
            logger.error(f"Error getting jobs from storage: {e}")
            return []
    
    def _get_queue_status(self, brokerage_key: str) -> Dict[str, int]:
        """Get processing queue status from shared storage."""
        try:
            # Try to get stats from shared storage first
            from shared_storage_bridge import shared_storage
            
            stats = shared_storage.get_processing_stats(brokerage_key)
            return {
                'total': stats.get('total', 0),
                'processing': stats.get('processing', 0),
                'queued': stats.get('pending', 0),  # 'pending' in shared storage = 'queued' in UI
                'completed_today': stats.get('completed_today', 0)
            }
            
        except ImportError:
            logger.debug("Shared storage not available, falling back to session state")
            # Fallback to session state method
            jobs = self._get_jobs_from_storage(brokerage_key)
            
            # Count jobs by status
            total = len(jobs)
            processing = len([j for j in jobs if j.status == "processing"])
            queued = len([j for j in jobs if j.status == "pending"]) 
            
            # Count completed today
            today = datetime.now().date()
            completed_today = len([
                j for j in jobs 
                if j.status == "completed" and j.started_at.date() == today
            ])
            
            return {
                'total': total,
                'processing': processing,
                'queued': queued,
                'completed_today': completed_today
            }
            
        except Exception as e:
            logger.error(f"Error getting queue status from shared storage: {e}")
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
                
                # Auto-refresh every 3 seconds using JavaScript
                st.markdown("""
                <script>
                setTimeout(function(){
                    window.parent.document.querySelector('[data-testid="stApp"]').dispatchEvent(
                        new KeyboardEvent('keydown', {key: 'F5', keyCode: 116})
                    );
                }, 3000);
                </script>
                """, unsafe_allow_html=True)
                
                # Alternative: Show refresh timer
                refresh_time = st.session_state.get('email_dashboard_refresh', time.time())
                elapsed = time.time() - refresh_time
                
                if elapsed > 3:  # Auto refresh every 3 seconds
                    st.rerun()
                    
        except Exception as e:
            logger.debug(f"Error in auto-refresh setup: {e}")
    
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