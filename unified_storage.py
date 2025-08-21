"""
Unified Storage Manager

Provides a unified interface over SharedStorageBridge (file-based) and SessionState (memory-based)
with clear hierarchy, automatic failover, and health monitoring.

Architecture:
- Primary: SharedStorageBridge (persistent across sessions, background-to-UI communication)
- Fallback: SessionState (same-session only, UI state management) 
- Health Monitoring: Tracks storage system availability and performance
- Automatic Migration: Moves data from session state to shared storage when possible

Features:
- Transparent failover between storage systems
- Storage health monitoring and reporting
- Automatic data migration and consolidation
- Consistent API across all storage operations
- Error recovery and resilience
"""

import logging
import streamlit as st
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import threading
import time
from brokerage_key_utils import normalize_brokerage_key
from data_models import EmailJob, ProcessingResult, DataModelConverter, create_email_job

logger = logging.getLogger(__name__)

@dataclass
class StorageHealthStatus:
    """Represents the health status of a storage system."""
    system_name: str
    is_available: bool
    last_check: datetime
    error_count: int
    last_error: Optional[str]
    performance_ms: float
    data_count: int
    

class UnifiedStorageManager:
    """
    Unified storage manager with automatic failover and health monitoring.
    
    Provides a consistent interface over multiple storage backends with
    intelligent routing, failover, and data synchronization.
    """
    
    def __init__(self, enable_session_fallback: bool = True):
        """Initialize unified storage manager."""
        self.enable_session_fallback = enable_session_fallback
        self.health_status = {}
        self.last_health_check = datetime.min
        self.health_check_interval = timedelta(minutes=1)
        self.lock = threading.RLock()
        
        # Initialize storage systems
        self._init_shared_storage()
        self._init_session_storage()
        
        logger.info("Unified storage manager initialized")
    
    def _init_shared_storage(self):
        """Initialize shared storage system."""
        try:
            from shared_storage_bridge import shared_storage
            self.shared_storage = shared_storage
            self.shared_storage_available = True
            logger.info("Shared storage system initialized")
        except Exception as e:
            logger.error(f"Failed to initialize shared storage: {e}")
            self.shared_storage = None
            self.shared_storage_available = False
    
    def _init_session_storage(self):
        """Initialize session state storage."""
        try:
            # Session state is always available when Streamlit is running
            self.session_storage_available = hasattr(st, 'session_state')
            if self.session_storage_available:
                logger.info("Session state storage initialized")
            else:
                logger.warning("Session state not available (not running in Streamlit)")
        except Exception as e:
            logger.error(f"Error checking session state availability: {e}")
            self.session_storage_available = False
    
    def _check_storage_health(self, force_check: bool = False):
        """Check health of all storage systems."""
        now = datetime.now()
        if not force_check and now - self.last_health_check < self.health_check_interval:
            return
        
        with self.lock:
            self.last_health_check = now
            
            # Check shared storage health
            if self.shared_storage:
                shared_status = self._check_shared_storage_health()
                self.health_status['shared_storage'] = shared_status
            
            # Check session storage health  
            if self.session_storage_available:
                session_status = self._check_session_storage_health()
                self.health_status['session_storage'] = session_status
    
    def _check_shared_storage_health(self) -> StorageHealthStatus:
        """Check shared storage system health."""
        start_time = time.time()
        error_count = 0
        last_error = None
        data_count = 0
        is_available = False
        
        try:
            # Test basic operation
            test_jobs = self.shared_storage.get_active_jobs('health-check')
            data_count = len(test_jobs)
            is_available = True
        except Exception as e:
            error_count = 1
            last_error = str(e)
            logger.debug(f"Shared storage health check failed: {e}")
        
        performance_ms = (time.time() - start_time) * 1000
        
        return StorageHealthStatus(
            system_name='shared_storage',
            is_available=is_available,
            last_check=datetime.now(),
            error_count=error_count,
            last_error=last_error,
            performance_ms=performance_ms,
            data_count=data_count
        )
    
    def _check_session_storage_health(self) -> StorageHealthStatus:
        """Check session state storage health."""
        start_time = time.time()
        error_count = 0
        last_error = None
        data_count = 0
        is_available = False
        
        try:
            # Test basic operation
            if hasattr(st, 'session_state'):
                data_count = len(getattr(st.session_state, 'email_processing_jobs', {}))
                is_available = True
            else:
                error_count = 1
                last_error = "Session state not available"
        except Exception as e:
            error_count = 1
            last_error = str(e)
            logger.debug(f"Session storage health check failed: {e}")
        
        performance_ms = (time.time() - start_time) * 1000
        
        return StorageHealthStatus(
            system_name='session_storage',
            is_available=is_available,
            last_check=datetime.now(),
            error_count=error_count,
            last_error=last_error,
            performance_ms=performance_ms,
            data_count=data_count
        )
    
    def get_storage_health(self) -> Dict[str, StorageHealthStatus]:
        """Get current storage health status."""
        self._check_storage_health()
        return self.health_status.copy()
    
    def add_email_job(self, filename: str, brokerage_key: str, email_source: str, record_count: int = 0) -> Optional[str]:
        """
        Add a new email processing job with automatic failover.
        
        Args:
            filename: Name of the file being processed
            brokerage_key: Brokerage identifier (will be normalized)
            email_source: Email address that sent the file
            record_count: Number of records in the file
            
        Returns:
            Job ID if successful, None if failed
        """
        self._check_storage_health()
        
        # Normalize brokerage key
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        # Try shared storage first (primary)
        if self.shared_storage_available and self.shared_storage:
            try:
                from shared_storage_bridge import add_email_job
                job_id = add_email_job(filename, normalized_key, email_source, record_count)
                logger.info(f"Job {job_id} added to shared storage (primary)")
                return job_id
            except Exception as e:
                logger.warning(f"Shared storage failed, attempting fallback: {e}")
                self.shared_storage_available = False
        
        # Fallback to session state
        if self.enable_session_fallback and self.session_storage_available:
            try:
                job_id = self._add_job_to_session_state(filename, normalized_key, email_source, record_count)
                logger.info(f"Job {job_id} added to session storage (fallback)")
                return job_id
            except Exception as e:
                logger.error(f"Session storage fallback also failed: {e}")
        
        logger.error(f"Failed to add job {filename} to any storage system")
        return None
    
    def update_job_status(self, job_id: str, brokerage_key: str, status: str, 
                         progress: float = None, step: str = None, **kwargs) -> bool:
        """
        Update job status with automatic failover.
        
        Args:
            job_id: Job identifier
            brokerage_key: Brokerage identifier (will be normalized)
            status: New status ('pending', 'processing', 'completed', 'failed')
            progress: Progress percentage (0-100)
            step: Current processing step
            **kwargs: Additional update fields
            
        Returns:
            True if update successful, False otherwise
        """
        self._check_storage_health()
        
        # Normalize brokerage key
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        # Try shared storage first (primary)
        if self.shared_storage_available and self.shared_storage:
            try:
                from shared_storage_bridge import update_job_status
                update_job_status(job_id, normalized_key, status, progress, step, **kwargs)
                logger.debug(f"Job {job_id} updated in shared storage (primary)")
                return True
            except Exception as e:
                logger.warning(f"Shared storage update failed, attempting fallback: {e}")
                self.shared_storage_available = False
        
        # Fallback to session state
        if self.enable_session_fallback and self.session_storage_available:
            try:
                success = self._update_job_in_session_state(job_id, normalized_key, status, progress, step, **kwargs)
                if success:
                    logger.debug(f"Job {job_id} updated in session storage (fallback)")
                    return True
            except Exception as e:
                logger.error(f"Session storage update fallback also failed: {e}")
        
        logger.error(f"Failed to update job {job_id} in any storage system")
        return False
    
    def get_active_jobs(self, brokerage_key: str) -> List[EmailJob]:
        """
        Get active jobs with automatic failover.
        
        Args:
            brokerage_key: Brokerage identifier (will be normalized)
            
        Returns:
            List of active EmailJob objects (canonical format)
        """
        self._check_storage_health()
        
        # Normalize brokerage key
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        # Try shared storage first (primary)
        if self.shared_storage_available and self.shared_storage:
            try:
                raw_jobs = self.shared_storage.get_active_jobs(normalized_key)
                # Convert to canonical format
                canonical_jobs = []
                for raw_job in raw_jobs:
                    try:
                        canonical_job = DataModelConverter.from_shared_storage_job(raw_job)
                        canonical_jobs.append(canonical_job)
                    except Exception as e:
                        logger.error(f"Error converting shared storage job: {e}")
                
                logger.debug(f"Retrieved {len(canonical_jobs)} active jobs from shared storage (primary)")
                return canonical_jobs
            except Exception as e:
                logger.warning(f"Shared storage retrieval failed, attempting fallback: {e}")
                self.shared_storage_available = False
        
        # Fallback to session state
        if self.enable_session_fallback and self.session_storage_available:
            try:
                raw_jobs = self._get_active_jobs_from_session_state(normalized_key)
                # Convert to canonical format
                canonical_jobs = []
                for raw_job in raw_jobs:
                    try:
                        canonical_job = DataModelConverter.from_session_state_job(raw_job)
                        canonical_jobs.append(canonical_job)
                    except Exception as e:
                        logger.error(f"Error converting session state job: {e}")
                
                logger.debug(f"Retrieved {len(canonical_jobs)} active jobs from session storage (fallback)")
                return canonical_jobs
            except Exception as e:
                logger.error(f"Session storage retrieval fallback also failed: {e}")
        
        logger.warning(f"Failed to retrieve active jobs for {normalized_key} from any storage system")
        return []
    
    def get_completed_jobs(self, brokerage_key: str, limit: int = 10) -> List[EmailJob]:
        """
        Get completed jobs with automatic failover.
        
        Args:
            brokerage_key: Brokerage identifier (will be normalized)
            limit: Maximum number of jobs to return
            
        Returns:
            List of completed EmailJob objects (canonical format)
        """
        self._check_storage_health()
        
        # Normalize brokerage key
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        # Try shared storage first (primary)
        if self.shared_storage_available and self.shared_storage:
            try:
                raw_jobs = self.shared_storage.get_completed_jobs(normalized_key, limit)
                # Convert to canonical format
                canonical_jobs = []
                for raw_job in raw_jobs:
                    try:
                        canonical_job = DataModelConverter.from_shared_storage_job(raw_job)
                        canonical_jobs.append(canonical_job)
                    except Exception as e:
                        logger.error(f"Error converting shared storage job: {e}")
                
                logger.debug(f"Retrieved {len(canonical_jobs)} completed jobs from shared storage (primary)")
                return canonical_jobs
            except Exception as e:
                logger.warning(f"Shared storage retrieval failed, attempting fallover: {e}")
                self.shared_storage_available = False
        
        # Fallback to session state
        if self.enable_session_fallback and self.session_storage_available:
            try:
                raw_jobs = self._get_completed_jobs_from_session_state(normalized_key, limit)
                # Convert to canonical format
                canonical_jobs = []
                for raw_job in raw_jobs:
                    try:
                        canonical_job = DataModelConverter.from_session_state_job(raw_job)
                        canonical_jobs.append(canonical_job)
                    except Exception as e:
                        logger.error(f"Error converting session state job: {e}")
                
                logger.debug(f"Retrieved {len(canonical_jobs)} completed jobs from session storage (fallback)")
                return canonical_jobs
            except Exception as e:
                logger.error(f"Session storage retrieval fallback also failed: {e}")
        
        logger.warning(f"Failed to retrieve completed jobs for {normalized_key} from any storage system")
        return []
    
    def get_processing_stats(self, brokerage_key: str) -> Dict[str, int]:
        """
        Get processing statistics with automatic failover.
        
        Args:
            brokerage_key: Brokerage identifier (will be normalized)
            
        Returns:
            Dictionary with processing statistics
        """
        self._check_storage_health()
        
        # Normalize brokerage key
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        # Try shared storage first (primary)
        if self.shared_storage_available and self.shared_storage:
            try:
                stats = self.shared_storage.get_processing_stats(normalized_key)
                logger.debug(f"Retrieved processing stats from shared storage (primary)")
                return stats
            except Exception as e:
                logger.warning(f"Shared storage stats retrieval failed, attempting fallback: {e}")
                self.shared_storage_available = False
        
        # Fallback to session state
        if self.enable_session_fallback and self.session_storage_available:
            try:
                stats = self._get_stats_from_session_state(normalized_key)
                logger.debug(f"Retrieved processing stats from session storage (fallback)")
                return stats
            except Exception as e:
                logger.error(f"Session storage stats retrieval fallback also failed: {e}")
        
        logger.warning(f"Failed to retrieve processing stats for {normalized_key} from any storage system")
        return {'total': 0, 'pending': 0, 'processing': 0, 'completed': 0, 'failed': 0, 'completed_today': 0}
    
    def migrate_session_to_shared(self, brokerage_key: str) -> bool:
        """
        Migrate data from session storage to shared storage.
        
        Args:
            brokerage_key: Brokerage identifier (will be normalized)
            
        Returns:
            True if migration successful, False otherwise
        """
        if not self.shared_storage_available or not self.session_storage_available:
            return False
        
        normalized_key = normalize_brokerage_key(brokerage_key)
        
        try:
            # Get all session jobs
            session_jobs = self._get_all_jobs_from_session_state(normalized_key)
            
            if not session_jobs:
                logger.debug(f"No session jobs to migrate for {normalized_key}")
                return True
            
            # Migrate each job to shared storage
            migrated_count = 0
            for job in session_jobs:
                try:
                    # Convert to shared storage format and add
                    from shared_storage_bridge import EmailProcessingJobStatus
                    shared_job = EmailProcessingJobStatus(
                        job_id=job.get('job_id', ''),
                        filename=job.get('filename', ''),
                        brokerage_key=normalized_key,
                        email_source=job.get('email_source', ''),
                        status=job.get('status', 'pending'),
                        progress_percent=job.get('progress_percent', 0.0),
                        current_step=job.get('current_step', 'queued'),
                        started_at=job.get('started_at', datetime.now().isoformat()),
                        completed_at=job.get('completed_at'),
                        record_count=job.get('record_count', 0),
                        success_count=job.get('success_count', 0),
                        failure_count=job.get('failure_count', 0),
                        processing_time=job.get('processing_time', 0.0),
                        error_message=job.get('error_message', ''),
                        result_data=job.get('result_data')
                    )
                    
                    self.shared_storage.add_processing_job(shared_job)
                    migrated_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to migrate job {job.get('job_id')}: {e}")
            
            if migrated_count > 0:
                logger.info(f"Migrated {migrated_count} jobs from session to shared storage for {normalized_key}")
                # Clear session storage after successful migration
                self._clear_session_jobs(normalized_key)
            
            return migrated_count > 0
            
        except Exception as e:
            logger.error(f"Migration failed for {normalized_key}: {e}")
            return False
    
    # Session state implementation methods
    
    def _add_job_to_session_state(self, filename: str, brokerage_key: str, email_source: str, record_count: int) -> str:
        """Add job to session state storage."""
        job_id = f"session_{int(datetime.now().timestamp())}_{filename.replace('.', '_')}"
        
        if 'email_processing_jobs' not in st.session_state:
            st.session_state.email_processing_jobs = {}
        
        if brokerage_key not in st.session_state.email_processing_jobs:
            st.session_state.email_processing_jobs[brokerage_key] = []
        
        job_data = {
            'job_id': job_id,
            'filename': filename,
            'brokerage_key': brokerage_key,
            'email_source': email_source,
            'status': 'pending',
            'progress_percent': 0.0,
            'current_step': 'queued',
            'started_at': datetime.now().isoformat(),
            'record_count': record_count
        }
        
        st.session_state.email_processing_jobs[brokerage_key].append(job_data)
        
        # Keep only recent jobs (last 20)
        st.session_state.email_processing_jobs[brokerage_key] = st.session_state.email_processing_jobs[brokerage_key][-20:]
        
        return job_id
    
    def _update_job_in_session_state(self, job_id: str, brokerage_key: str, status: str, 
                                    progress: float = None, step: str = None, **kwargs) -> bool:
        """Update job in session state storage."""
        if 'email_processing_jobs' not in st.session_state:
            return False
        
        jobs = st.session_state.email_processing_jobs.get(brokerage_key, [])
        
        for i, job in enumerate(jobs):
            if job.get('job_id') == job_id:
                # Update job data
                job['status'] = status
                if progress is not None:
                    job['progress_percent'] = progress
                if step is not None:
                    job['current_step'] = step
                
                # Add any additional updates
                job.update(kwargs)
                
                # Update timestamps
                if status == 'completed':
                    job['completed_at'] = datetime.now().isoformat()
                    job['processing_time'] = (datetime.now() - datetime.fromisoformat(job['started_at'])).total_seconds()
                
                st.session_state.email_processing_jobs[brokerage_key][i] = job
                return True
        
        return False
    
    def _get_active_jobs_from_session_state(self, brokerage_key: str) -> List[Dict[str, Any]]:
        """Get active jobs from session state storage."""
        if 'email_processing_jobs' not in st.session_state:
            return []
        
        jobs = st.session_state.email_processing_jobs.get(brokerage_key, [])
        return [job for job in jobs if job.get('status') in ['pending', 'processing']]
    
    def _get_completed_jobs_from_session_state(self, brokerage_key: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get completed jobs from session state storage."""
        if 'email_processing_jobs' not in st.session_state:
            return []
        
        jobs = st.session_state.email_processing_jobs.get(brokerage_key, [])
        completed_jobs = [job for job in jobs if job.get('status') in ['completed', 'failed']]
        
        # Sort by completion time and limit
        completed_jobs.sort(key=lambda x: x.get('completed_at', x.get('started_at', '')), reverse=True)
        return completed_jobs[:limit]
    
    def _get_all_jobs_from_session_state(self, brokerage_key: str) -> List[Dict[str, Any]]:
        """Get all jobs from session state storage."""
        if 'email_processing_jobs' not in st.session_state:
            return []
        
        return st.session_state.email_processing_jobs.get(brokerage_key, [])
    
    def _get_stats_from_session_state(self, brokerage_key: str) -> Dict[str, int]:
        """Get processing statistics from session state storage."""
        jobs = self._get_all_jobs_from_session_state(brokerage_key)
        
        stats = {
            'total': len(jobs),
            'pending': 0,
            'processing': 0,
            'completed': 0,
            'failed': 0
        }
        
        for job in jobs:
            status = job.get('status', 'unknown')
            if status in stats:
                stats[status] += 1
        
        # Count completed today
        today = datetime.now().date()
        completed_today = 0
        for job in jobs:
            if job.get('status') == 'completed' and job.get('completed_at'):
                try:
                    completed_date = datetime.fromisoformat(job['completed_at']).date()
                    if completed_date == today:
                        completed_today += 1
                except:
                    pass
        
        stats['completed_today'] = completed_today
        return stats
    
    def _clear_session_jobs(self, brokerage_key: str):
        """Clear session jobs for a brokerage."""
        if 'email_processing_jobs' in st.session_state:
            st.session_state.email_processing_jobs.pop(brokerage_key, None)


# Global unified storage instance
unified_storage = UnifiedStorageManager()


# Convenience functions for backward compatibility
def add_unified_job(filename: str, brokerage_key: str, email_source: str, record_count: int = 0) -> Optional[str]:
    """Add job using unified storage."""
    return unified_storage.add_email_job(filename, brokerage_key, email_source, record_count)


def update_unified_job(job_id: str, brokerage_key: str, status: str, progress: float = None, step: str = None, **kwargs) -> bool:
    """Update job using unified storage."""
    return unified_storage.update_job_status(job_id, brokerage_key, status, progress, step, **kwargs)


def get_unified_active_jobs(brokerage_key: str) -> List[EmailJob]:
    """Get active jobs using unified storage."""
    return unified_storage.get_active_jobs(brokerage_key)


def get_unified_completed_jobs(brokerage_key: str, limit: int = 10) -> List[EmailJob]:
    """Get completed jobs using unified storage."""
    return unified_storage.get_completed_jobs(brokerage_key, limit)


def get_unified_stats(brokerage_key: str) -> Dict[str, int]:
    """Get processing stats using unified storage."""
    return unified_storage.get_processing_stats(brokerage_key)
