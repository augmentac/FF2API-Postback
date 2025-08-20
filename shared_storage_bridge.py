"""
Shared Storage Bridge for Background-to-UI Communication

This module provides a robust bridge between background email processing
and the main UI, solving the session state isolation problem.

Features:
- File-based storage for cross-thread communication
- Thread-safe operations
- Automatic cleanup of old data  
- JSON serialization for complex data structures
- Real-time UI updates via shared storage polling
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import fcntl  # For file locking on Unix systems
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class EmailProcessingJobStatus:
    """Represents an email processing job for UI display."""
    job_id: str
    filename: str
    brokerage_key: str
    email_source: str
    status: str  # pending, processing, completed, failed
    progress_percent: float
    current_step: str
    started_at: str  # ISO format
    completed_at: Optional[str] = None
    record_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    processing_time: float = 0.0
    error_message: str = ""
    result_data: Optional[Dict[str, Any]] = None

@dataclass 
class EmailProcessingResult:
    """Represents completed email processing result."""
    filename: str
    brokerage_key: str
    email_source: str
    subject: str
    processed_time: str  # ISO format
    processing_mode: str
    was_email_automated: bool
    record_count: int
    success: bool
    result_summary: Optional[Dict[str, Any]] = None
    download_links: Optional[Dict[str, str]] = None

class SharedStorageBridge:
    """Thread-safe shared storage bridge for background-to-UI communication."""
    
    def __init__(self, storage_dir: str = ".streamlit_shared"):
        """Initialize shared storage bridge."""
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        
        # Storage files
        self.jobs_file = self.storage_dir / "email_jobs.json"
        self.results_file = self.storage_dir / "email_results.json" 
        self.metadata_file = self.storage_dir / "processing_metadata.json"
        
        # Thread locks
        self._jobs_lock = threading.RLock()
        self._results_lock = threading.RLock()
        self._metadata_lock = threading.RLock()
        
        # Initialize storage files
        self._initialize_storage_files()
        
        logger.info(f"Shared storage bridge initialized at: {self.storage_dir}")
    
    def _initialize_storage_files(self):
        """Initialize storage files if they don't exist."""
        if not self.jobs_file.exists():
            self._write_json_file(self.jobs_file, {}, self._jobs_lock)
        if not self.results_file.exists():
            self._write_json_file(self.results_file, {}, self._results_lock)
        if not self.metadata_file.exists():
            self._write_json_file(self.metadata_file, {}, self._metadata_lock)
    
    def _read_json_file(self, file_path: Path, lock: threading.RLock) -> Dict[str, Any]:
        """Thread-safe JSON file reading with file locking."""
        with lock:
            try:
                if not file_path.exists():
                    return {}
                
                with open(file_path, 'r') as f:
                    # Use file locking on Unix systems
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                        data = json.load(f)
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                        return data
                    except (AttributeError, OSError):
                        # Fallback for Windows or when fcntl not available
                        return json.load(f)
                        
            except (json.JSONDecodeError, FileNotFoundError, PermissionError) as e:
                logger.warning(f"Error reading {file_path}: {e}")
                return {}
    
    def _write_json_file(self, file_path: Path, data: Dict[str, Any], lock: threading.RLock):
        """Thread-safe JSON file writing with file locking."""
        with lock:
            try:
                with open(file_path, 'w') as f:
                    # Use file locking on Unix systems
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                        json.dump(data, f, indent=2, default=str)
                        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    except (AttributeError, OSError):
                        # Fallback for Windows or when fcntl not available
                        json.dump(data, f, indent=2, default=str)
                        
            except (PermissionError, OSError) as e:
                logger.error(f"Error writing {file_path}: {e}")
    
    def add_processing_job(self, job: EmailProcessingJobStatus):
        """Add a new email processing job."""
        try:
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            
            # Initialize brokerage jobs list if needed
            if job.brokerage_key not in jobs_data:
                jobs_data[job.brokerage_key] = []
            
            # Add job
            job_dict = asdict(job)
            jobs_data[job.brokerage_key].append(job_dict)
            
            # Keep only recent jobs (last 20 per brokerage)
            jobs_data[job.brokerage_key] = jobs_data[job.brokerage_key][-20:]
            
            self._write_json_file(self.jobs_file, jobs_data, self._jobs_lock)
            
            logger.info(f"Added processing job to shared storage: {job.job_id}")
            
        except Exception as e:
            logger.error(f"Error adding processing job: {e}")
    
    def update_job_progress(self, job_id: str, brokerage_key: str, **updates):
        """Update progress for an existing job."""
        try:
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            
            if brokerage_key in jobs_data:
                for i, job in enumerate(jobs_data[brokerage_key]):
                    if job.get('job_id') == job_id:
                        # Update job with new data
                        jobs_data[brokerage_key][i].update(updates)
                        
                        # Update timestamps
                        if updates.get('status') == 'completed':
                            jobs_data[brokerage_key][i]['completed_at'] = datetime.now().isoformat()
                            jobs_data[brokerage_key][i]['processing_time'] = (
                                datetime.now() - datetime.fromisoformat(job['started_at'])
                            ).total_seconds()
                        
                        self._write_json_file(self.jobs_file, jobs_data, self._jobs_lock)
                        logger.debug(f"Updated job progress: {job_id}")
                        return
            
            logger.warning(f"Job not found for progress update: {job_id}")
            
        except Exception as e:
            logger.error(f"Error updating job progress: {e}")
    
    def add_processing_result(self, result: EmailProcessingResult):
        """Add a completed processing result."""
        try:
            results_data = self._read_json_file(self.results_file, self._results_lock)
            
            # Initialize brokerage results list if needed
            if result.brokerage_key not in results_data:
                results_data[result.brokerage_key] = []
            
            # Add result
            result_dict = asdict(result)
            results_data[result.brokerage_key].append(result_dict)
            
            # Keep only recent results (last 50 per brokerage)
            results_data[result.brokerage_key] = results_data[result.brokerage_key][-50:]
            
            self._write_json_file(self.results_file, results_data, self._results_lock)
            
            logger.info(f"Added processing result to shared storage: {result.filename}")
            
        except Exception as e:
            logger.error(f"Error adding processing result: {e}")
    
    def get_active_jobs(self, brokerage_key: str) -> List[EmailProcessingJobStatus]:
        """Get active (pending/processing) jobs for a brokerage."""
        try:
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            brokerage_jobs = jobs_data.get(brokerage_key, [])
            
            # Filter for active jobs and convert back to dataclass
            active_jobs = []
            for job_dict in brokerage_jobs:
                if job_dict.get('status') in ['pending', 'processing']:
                    try:
                        job = EmailProcessingJobStatus(**job_dict)
                        active_jobs.append(job)
                    except Exception as e:
                        logger.debug(f"Error deserializing job: {e}")
            
            return active_jobs
            
        except Exception as e:
            logger.error(f"Error getting active jobs: {e}")
            return []
    
    def get_completed_jobs(self, brokerage_key: str, limit: int = 10) -> List[EmailProcessingJobStatus]:
        """Get recently completed jobs for a brokerage."""
        try:
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            brokerage_jobs = jobs_data.get(brokerage_key, [])
            
            # Filter for completed jobs and convert back to dataclass
            completed_jobs = []
            for job_dict in brokerage_jobs:
                if job_dict.get('status') in ['completed', 'failed']:
                    try:
                        job = EmailProcessingJobStatus(**job_dict)
                        completed_jobs.append(job)
                    except Exception as e:
                        logger.debug(f"Error deserializing job: {e}")
            
            # Sort by completion time and limit
            completed_jobs.sort(key=lambda x: x.completed_at or x.started_at, reverse=True)
            return completed_jobs[:limit]
            
        except Exception as e:
            logger.error(f"Error getting completed jobs: {e}")
            return []
    
    def get_recent_results(self, brokerage_key: str, limit: int = 5) -> List[EmailProcessingResult]:
        """Get recent processing results for UI display."""
        try:
            results_data = self._read_json_file(self.results_file, self._results_lock)
            brokerage_results = results_data.get(brokerage_key, [])
            
            # Convert back to dataclass and sort by processed time
            recent_results = []
            for result_dict in brokerage_results:
                try:
                    result = EmailProcessingResult(**result_dict)
                    recent_results.append(result)
                except Exception as e:
                    logger.debug(f"Error deserializing result: {e}")
            
            # Sort by processed time and limit
            recent_results.sort(key=lambda x: x.processed_time, reverse=True)
            return recent_results[:limit]
            
        except Exception as e:
            logger.error(f"Error getting recent results: {e}")
            return []
    
    def get_processing_stats(self, brokerage_key: str) -> Dict[str, int]:
        """Get processing statistics for a brokerage."""
        try:
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            brokerage_jobs = jobs_data.get(brokerage_key, [])
            
            # Count jobs by status
            stats = {
                'total': len(brokerage_jobs),
                'pending': 0,
                'processing': 0,
                'completed': 0,
                'failed': 0
            }
            
            for job in brokerage_jobs:
                status = job.get('status', 'unknown')
                if status in stats:
                    stats[status] += 1
            
            # Count completed today
            today = datetime.now().date()
            completed_today = 0
            for job in brokerage_jobs:
                if job.get('status') == 'completed' and job.get('completed_at'):
                    try:
                        completed_date = datetime.fromisoformat(job['completed_at']).date()
                        if completed_date == today:
                            completed_today += 1
                    except:
                        pass
            
            stats['completed_today'] = completed_today
            return stats
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {'total': 0, 'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0, 'completed_today': 0}
    
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Clean up old data from shared storage."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Clean up jobs
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            cleaned_jobs = {}
            
            for brokerage_key, jobs in jobs_data.items():
                cleaned_jobs[brokerage_key] = []
                for job in jobs:
                    try:
                        job_date = datetime.fromisoformat(job.get('started_at', ''))
                        if job_date > cutoff_date:
                            cleaned_jobs[brokerage_key].append(job)
                    except:
                        # Keep jobs with invalid dates
                        cleaned_jobs[brokerage_key].append(job)
            
            self._write_json_file(self.jobs_file, cleaned_jobs, self._jobs_lock)
            
            # Clean up results
            results_data = self._read_json_file(self.results_file, self._results_lock)
            cleaned_results = {}
            
            for brokerage_key, results in results_data.items():
                cleaned_results[brokerage_key] = []
                for result in results:
                    try:
                        result_date = datetime.fromisoformat(result.get('processed_time', ''))
                        if result_date > cutoff_date:
                            cleaned_results[brokerage_key].append(result)
                    except:
                        # Keep results with invalid dates
                        cleaned_results[brokerage_key].append(result)
            
            self._write_json_file(self.results_file, cleaned_results, self._results_lock)
            
            logger.info(f"Cleaned up shared storage data older than {days_to_keep} days")
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
    
    def has_recent_activity(self, brokerage_key: str, minutes: int = 5) -> bool:
        """Check if there has been recent email processing activity."""
        try:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            
            # Check for recent jobs
            jobs_data = self._read_json_file(self.jobs_file, self._jobs_lock)
            brokerage_jobs = jobs_data.get(brokerage_key, [])
            
            for job in brokerage_jobs:
                try:
                    job_time = datetime.fromisoformat(job.get('started_at', ''))
                    if job_time > cutoff_time:
                        return True
                except:
                    pass
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking recent activity: {e}")
            return False


# Global shared storage instance
shared_storage = SharedStorageBridge()

# Convenience functions for easy usage
def add_email_job(filename: str, brokerage_key: str, email_source: str, record_count: int = 0) -> str:
    """Add a new email processing job to shared storage."""
    job_id = f"email_{int(datetime.now().timestamp())}_{filename.replace('.', '_')}"
    
    job = EmailProcessingJobStatus(
        job_id=job_id,
        filename=filename,
        brokerage_key=brokerage_key,
        email_source=email_source,
        status='pending',
        progress_percent=0.0,
        current_step='queued',
        started_at=datetime.now().isoformat(),
        record_count=record_count
    )
    
    shared_storage.add_processing_job(job)
    return job_id

def update_job_status(job_id: str, brokerage_key: str, status: str, progress: float = None, step: str = None, **kwargs):
    """Update job status in shared storage."""
    updates = {'status': status}
    if progress is not None:
        updates['progress_percent'] = progress
    if step is not None:
        updates['current_step'] = step
    updates.update(kwargs)
    
    shared_storage.update_job_progress(job_id, brokerage_key, **updates)

def add_email_result(filename: str, brokerage_key: str, email_source: str, success: bool, record_count: int = 0, **kwargs):
    """Add an email processing result to shared storage."""
    result = EmailProcessingResult(
        filename=filename,
        brokerage_key=brokerage_key,
        email_source=email_source,
        subject=kwargs.get('subject', ''),
        processed_time=datetime.now().isoformat(),
        processing_mode='email_automation',
        was_email_automated=True,
        record_count=record_count,
        success=success,
        result_summary=kwargs.get('result_summary'),
        download_links=kwargs.get('download_links')
    )
    
    shared_storage.add_processing_result(result)

def get_email_processing_data(brokerage_key: str) -> Dict[str, Any]:
    """Get comprehensive email processing data for UI display."""
    return {
        'active_jobs': shared_storage.get_active_jobs(brokerage_key),
        'completed_jobs': shared_storage.get_completed_jobs(brokerage_key),
        'recent_results': shared_storage.get_recent_results(brokerage_key),
        'stats': shared_storage.get_processing_stats(brokerage_key),
        'has_recent_activity': shared_storage.has_recent_activity(brokerage_key)
    }