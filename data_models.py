"""
Canonical Data Models for FF2API Tool

Provides standardized data models and conversion utilities to ensure consistency
across all storage systems (SharedStorage, SessionState, UI components).

Features:
- Canonical data models for all major entities
- Automatic validation and type coercion
- Conversion utilities between different formats
- Schema versioning and migration support
- JSON serialization/deserialization
- Error handling and data repair

Models:
- EmailJob: Email processing job data
- ProcessingResult: Processing result data
- JobStatus: Job status and progress data
- BrokerageConfig: Brokerage configuration data
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from enum import Enum
import json
import uuid
from brokerage_key_utils import normalize_brokerage_key

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Standardized job status values."""
    PENDING = "pending"
    PROCESSING = "processing" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProcessingStep(str, Enum):
    """Standardized processing step values."""
    QUEUED = "queued"
    PARSING_EMAIL = "parsing_email"
    ANALYZING_DATA = "analyzing_data"
    APPLYING_MAPPINGS = "applying_mappings"
    SUBMITTING_API = "submitting_api"
    ENRICHING_DATA = "enriching_data"
    GENERATING_RESULTS = "generating_results"


@dataclass
class EmailJob:
    """
    Canonical email processing job model.
    
    This is the single source of truth for email job data across all systems.
    All other job formats should be converted to/from this model.
    """
    job_id: str
    filename: str
    brokerage_key: str
    email_source: str
    status: JobStatus = JobStatus.PENDING
    current_step: ProcessingStep = ProcessingStep.QUEUED
    progress_percent: float = 0.0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # File and data info
    file_size: int = 0
    mime_type: str = ""
    record_count: int = 0
    
    # Processing results
    success_count: int = 0
    failure_count: int = 0
    processing_time: float = 0.0
    
    # Error handling
    error_message: str = ""
    error_details: Optional[Dict[str, Any]] = None
    
    # Result data
    result_data: Optional[Dict[str, Any]] = None
    
    # Metadata
    schema_version: str = "1.0"
    
    def __post_init__(self):
        """Validate and normalize data after initialization."""
        # Normalize brokerage key
        self.brokerage_key = normalize_brokerage_key(self.brokerage_key)
        
        # Generate job_id if not provided
        if not self.job_id:
            timestamp = int(self.created_at.timestamp())
            safe_filename = self.filename.replace('.', '_').replace(' ', '_')
            self.job_id = f"{self.brokerage_key}_{timestamp}_{safe_filename}"[:50]
        
        # Ensure enums
        if isinstance(self.status, str):
            try:
                self.status = JobStatus(self.status)
            except ValueError:
                logger.warning(f"Invalid status '{self.status}', defaulting to PENDING")
                self.status = JobStatus.PENDING
        
        if isinstance(self.current_step, str):
            try:
                self.current_step = ProcessingStep(self.current_step)
            except ValueError:
                logger.warning(f"Invalid step '{self.current_step}', defaulting to QUEUED")
                self.current_step = ProcessingStep.QUEUED
        
        # Validate progress
        self.progress_percent = max(0.0, min(100.0, self.progress_percent))
    
    @property
    def is_active(self) -> bool:
        """Check if job is currently active (pending or processing)."""
        return self.status in [JobStatus.PENDING, JobStatus.PROCESSING]
    
    @property
    def is_complete(self) -> bool:
        """Check if job is complete (completed, failed, or cancelled)."""
        return self.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
    
    @property
    def duration_seconds(self) -> float:
        """Calculate job duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        elif self.started_at:
            return (datetime.now() - self.started_at).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization."""
        data = asdict(self)
        
        # Convert enums to strings
        data['status'] = self.status.value
        data['current_step'] = self.current_step.value
        
        # Convert datetimes to ISO strings
        for field_name in ['created_at', 'started_at', 'completed_at']:
            if data[field_name]:
                data[field_name] = data[field_name].isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EmailJob':
        """Create instance from dictionary with proper deserialization."""
        # Convert datetime strings back to datetime objects
        for field_name in ['created_at', 'started_at', 'completed_at']:
            if data.get(field_name):
                try:
                    data[field_name] = datetime.fromisoformat(data[field_name])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid datetime for {field_name}: {data.get(field_name)}")
                    if field_name == 'created_at':
                        data[field_name] = datetime.now()
                    else:
                        data[field_name] = None
        
        # Handle missing fields with defaults
        defaults = {
            'job_id': '',
            'filename': 'unknown',
            'brokerage_key': '',
            'email_source': 'unknown',
            'status': JobStatus.PENDING,
            'current_step': ProcessingStep.QUEUED,
            'progress_percent': 0.0,
            'file_size': 0,
            'mime_type': '',
            'record_count': 0,
            'success_count': 0,
            'failure_count': 0,
            'processing_time': 0.0,
            'error_message': '',
            'schema_version': '1.0'
        }
        
        # Apply defaults for missing fields
        for key, default_value in defaults.items():
            if key not in data:
                data[key] = default_value
        
        return cls(**data)
    
    def update_progress(self, progress: float, step: ProcessingStep = None, **kwargs):
        """Update job progress and status."""
        self.progress_percent = max(0.0, min(100.0, progress))
        
        if step:
            self.current_step = step
        
        # Auto-update status based on progress
        if self.progress_percent > 0 and self.status == JobStatus.PENDING:
            self.status = JobStatus.PROCESSING
            if not self.started_at:
                self.started_at = datetime.now()
        
        # Update other fields
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def mark_completed(self, success_count: int = None, failure_count: int = None, result_data: Dict[str, Any] = None):
        """Mark job as completed."""
        self.status = JobStatus.COMPLETED
        self.progress_percent = 100.0
        self.completed_at = datetime.now()
        
        if success_count is not None:
            self.success_count = success_count
        
        if failure_count is not None:
            self.failure_count = failure_count
        
        if result_data:
            self.result_data = result_data
        
        # Calculate processing time
        if self.started_at:
            self.processing_time = (self.completed_at - self.started_at).total_seconds()
    
    def mark_failed(self, error_message: str, error_details: Dict[str, Any] = None):
        """Mark job as failed."""
        self.status = JobStatus.FAILED
        self.completed_at = datetime.now()
        self.error_message = error_message
        
        if error_details:
            self.error_details = error_details
        
        # Calculate processing time
        if self.started_at:
            self.processing_time = (self.completed_at - self.started_at).total_seconds()


@dataclass
class ProcessingResult:
    """
    Canonical processing result model.
    
    Stores the result of processing an email attachment.
    """
    result_id: str
    job_id: str
    brokerage_key: str
    filename: str
    processed_at: datetime = field(default_factory=datetime.now)
    
    # Processing details
    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    processing_time_seconds: float = 0.0
    
    # Result files and data
    output_files: List[Dict[str, str]] = field(default_factory=list)  # [{"type": "csv", "path": "...", "size": 123}]
    summary_data: Optional[Dict[str, Any]] = None
    
    # Status and errors
    overall_status: JobStatus = JobStatus.COMPLETED
    error_summary: str = ""
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    schema_version: str = "1.0"
    
    def __post_init__(self):
        """Validate and normalize data after initialization."""
        # Normalize brokerage key
        self.brokerage_key = normalize_brokerage_key(self.brokerage_key)
        
        # Generate result_id if not provided
        if not self.result_id:
            timestamp = int(self.processed_at.timestamp())
            safe_filename = self.filename.replace('.', '_').replace(' ', '_')
            self.result_id = f"result_{self.brokerage_key}_{timestamp}_{safe_filename}"[:50]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization."""
        data = asdict(self)
        
        # Convert enums to strings
        data['overall_status'] = self.overall_status.value
        
        # Convert datetime to ISO string
        if data['processed_at']:
            data['processed_at'] = data['processed_at'].isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessingResult':
        """Create instance from dictionary with proper deserialization."""
        # Convert datetime string back to datetime object
        if data.get('processed_at'):
            try:
                data['processed_at'] = datetime.fromisoformat(data['processed_at'])
            except (ValueError, TypeError):
                logger.warning(f"Invalid datetime for processed_at: {data.get('processed_at')}")
                data['processed_at'] = datetime.now()
        
        # Handle missing fields with defaults
        defaults = {
            'result_id': '',
            'job_id': '',
            'brokerage_key': '',
            'filename': 'unknown',
            'records_processed': 0,
            'records_successful': 0,
            'records_failed': 0,
            'processing_time_seconds': 0.0,
            'output_files': [],
            'overall_status': JobStatus.COMPLETED,
            'error_summary': '',
            'warnings': [],
            'schema_version': '1.0'
        }
        
        # Apply defaults for missing fields
        for key, default_value in defaults.items():
            if key not in data:
                data[key] = default_value
        
        return cls(**data)


class DataModelConverter:
    """Utility class for converting between different data formats."""
    
    @staticmethod
    def from_shared_storage_job(shared_job) -> EmailJob:
        """Convert SharedStorageBridge job format to canonical EmailJob."""
        try:
            return EmailJob(
                job_id=shared_job.job_id,
                filename=shared_job.filename,
                brokerage_key=shared_job.brokerage_key,
                email_source=shared_job.email_source,
                status=JobStatus(shared_job.status),
                current_step=ProcessingStep(shared_job.current_step) if shared_job.current_step else ProcessingStep.QUEUED,
                progress_percent=shared_job.progress_percent,
                created_at=datetime.fromisoformat(shared_job.started_at) if isinstance(shared_job.started_at, str) else shared_job.started_at,
                started_at=datetime.fromisoformat(shared_job.started_at) if isinstance(shared_job.started_at, str) else shared_job.started_at,
                completed_at=datetime.fromisoformat(shared_job.completed_at) if shared_job.completed_at and isinstance(shared_job.completed_at, str) else shared_job.completed_at,
                record_count=shared_job.record_count,
                success_count=shared_job.success_count,
                failure_count=shared_job.failure_count,
                processing_time=shared_job.processing_time,
                error_message=shared_job.error_message or "",
                result_data=shared_job.result_data
            )
        except Exception as e:
            logger.error(f"Error converting shared storage job: {e}")
            # Return a minimal valid job
            return EmailJob(
                job_id=getattr(shared_job, 'job_id', 'unknown'),
                filename=getattr(shared_job, 'filename', 'unknown'),
                brokerage_key=normalize_brokerage_key(getattr(shared_job, 'brokerage_key', '')),
                email_source=getattr(shared_job, 'email_source', 'unknown')
            )
    
    @staticmethod
    def to_shared_storage_job(email_job: EmailJob):
        """Convert canonical EmailJob to SharedStorageBridge format."""
        try:
            from shared_storage_bridge import EmailProcessingJobStatus
            
            return EmailProcessingJobStatus(
                job_id=email_job.job_id,
                filename=email_job.filename,
                brokerage_key=email_job.brokerage_key,
                email_source=email_job.email_source,
                status=email_job.status.value,
                progress_percent=email_job.progress_percent,
                current_step=email_job.current_step.value,
                started_at=email_job.started_at.isoformat() if email_job.started_at else email_job.created_at.isoformat(),
                completed_at=email_job.completed_at.isoformat() if email_job.completed_at else None,
                record_count=email_job.record_count,
                success_count=email_job.success_count,
                failure_count=email_job.failure_count,
                processing_time=email_job.processing_time,
                error_message=email_job.error_message,
                result_data=email_job.result_data
            )
        except Exception as e:
            logger.error(f"Error converting to shared storage job: {e}")
            raise
    
    @staticmethod
    def from_session_state_job(session_job: Dict[str, Any]) -> EmailJob:
        """Convert session state job format to canonical EmailJob."""
        return EmailJob.from_dict(session_job)
    
    @staticmethod
    def to_session_state_job(email_job: EmailJob) -> Dict[str, Any]:
        """Convert canonical EmailJob to session state format."""
        return email_job.to_dict()
    
    @staticmethod
    def from_dashboard_job(dashboard_job) -> EmailJob:
        """Convert dashboard job format to canonical EmailJob."""
        try:
            return EmailJob(
                job_id=dashboard_job.job_id,
                filename=dashboard_job.filename,
                brokerage_key=dashboard_job.brokerage_key,
                email_source=dashboard_job.email_source,
                status=JobStatus(dashboard_job.status),
                current_step=ProcessingStep(dashboard_job.current_step) if dashboard_job.current_step else ProcessingStep.QUEUED,
                progress_percent=dashboard_job.progress_percent,
                created_at=dashboard_job.started_at,  # Dashboard uses started_at as created_at
                started_at=dashboard_job.started_at,
                file_size=dashboard_job.file_size,
                record_count=dashboard_job.record_count,
                success_count=dashboard_job.success_count,
                failure_count=dashboard_job.failure_count,
                processing_time=dashboard_job.processing_time,
                error_message=dashboard_job.error_message or "",
                result_data=dashboard_job.result_data
            )
        except Exception as e:
            logger.error(f"Error converting dashboard job: {e}")
            # Return a minimal valid job
            return EmailJob(
                job_id=getattr(dashboard_job, 'job_id', 'unknown'),
                filename=getattr(dashboard_job, 'filename', 'unknown'),
                brokerage_key=normalize_brokerage_key(getattr(dashboard_job, 'brokerage_key', '')),
                email_source=getattr(dashboard_job, 'email_source', 'unknown')
            )
    
    @staticmethod
    def to_dashboard_job(email_job: EmailJob):
        """Convert canonical EmailJob to dashboard job format."""
        try:
            from email_processing_dashboard import EmailProcessingJob
            
            return EmailProcessingJob(
                job_id=email_job.job_id,
                filename=email_job.filename,
                brokerage_key=email_job.brokerage_key,
                email_source=email_job.email_source,
                file_size=email_job.file_size,
                record_count=email_job.record_count,
                started_at=email_job.started_at or email_job.created_at,
                current_step=email_job.current_step.value,
                progress_percent=email_job.progress_percent,
                status=email_job.status.value,
                error_message=email_job.error_message,
                success_count=email_job.success_count,
                failure_count=email_job.failure_count,
                processing_time=email_job.processing_time,
                result_data=email_job.result_data
            )
        except Exception as e:
            logger.error(f"Error converting to dashboard job: {e}")
            raise


class DataValidator:
    """Utility class for validating and repairing data."""
    
    @staticmethod
    def validate_email_job(job_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate email job data.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Required fields
        required_fields = ['job_id', 'filename', 'brokerage_key', 'email_source']
        for field in required_fields:
            if not job_data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Status validation
        status = job_data.get('status')
        if status and status not in [s.value for s in JobStatus]:
            errors.append(f"Invalid status: {status}")
        
        # Progress validation
        progress = job_data.get('progress_percent', 0)
        try:
            progress = float(progress)
            if progress < 0 or progress > 100:
                errors.append(f"Invalid progress: {progress} (must be 0-100)")
        except (ValueError, TypeError):
            errors.append(f"Invalid progress type: {progress}")
        
        # Datetime validation
        for field in ['created_at', 'started_at', 'completed_at']:
            value = job_data.get(field)
            if value:
                try:
                    if isinstance(value, str):
                        datetime.fromisoformat(value)
                    elif not isinstance(value, datetime):
                        errors.append(f"Invalid datetime format for {field}: {value}")
                except (ValueError, TypeError):
                    errors.append(f"Invalid datetime format for {field}: {value}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def repair_email_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Repair and normalize email job data.
        
        Returns:
            Repaired job data dictionary
        """
        repaired = job_data.copy()
        
        # Fix missing required fields
        if not repaired.get('job_id'):
            timestamp = int(datetime.now().timestamp())
            filename = repaired.get('filename', 'unknown').replace('.', '_')
            repaired['job_id'] = f"repaired_{timestamp}_{filename}"[:50]
        
        if not repaired.get('filename'):
            repaired['filename'] = f"unknown_file_{repaired['job_id']}"
        
        if not repaired.get('brokerage_key'):
            repaired['brokerage_key'] = 'unknown-brokerage'
        else:
            repaired['brokerage_key'] = normalize_brokerage_key(repaired['brokerage_key'])
        
        if not repaired.get('email_source'):
            repaired['email_source'] = 'unknown@unknown.com'
        
        # Fix status
        status = repaired.get('status', JobStatus.PENDING.value)
        if status not in [s.value for s in JobStatus]:
            logger.warning(f"Invalid status '{status}', defaulting to PENDING")
            repaired['status'] = JobStatus.PENDING.value
        
        # Fix progress
        try:
            progress = float(repaired.get('progress_percent', 0))
            repaired['progress_percent'] = max(0.0, min(100.0, progress))
        except (ValueError, TypeError):
            repaired['progress_percent'] = 0.0
        
        # Fix datetime fields
        for field in ['created_at', 'started_at', 'completed_at']:
            value = repaired.get(field)
            if value:
                try:
                    if isinstance(value, str):
                        datetime.fromisoformat(value)  # Validate
                    elif isinstance(value, datetime):
                        repaired[field] = value.isoformat()  # Convert to string
                    else:
                        logger.warning(f"Invalid datetime for {field}: {value}")
                        if field == 'created_at':
                            repaired[field] = datetime.now().isoformat()
                        else:
                            repaired[field] = None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid datetime for {field}: {value}")
                    if field == 'created_at':
                        repaired[field] = datetime.now().isoformat()
                    else:
                        repaired[field] = None
        
        # Ensure created_at exists
        if not repaired.get('created_at'):
            repaired['created_at'] = datetime.now().isoformat()
        
        # Fix numeric fields
        for field in ['record_count', 'success_count', 'failure_count', 'processing_time', 'file_size']:
            try:
                repaired[field] = max(0, int(repaired.get(field, 0)))
            except (ValueError, TypeError):
                repaired[field] = 0
        
        # Fix string fields
        for field in ['error_message']:
            value = repaired.get(field)
            if value is None:
                repaired[field] = ''
            else:
                repaired[field] = str(value)
        
        return repaired


# Convenience functions
def create_email_job(filename: str, brokerage_key: str, email_source: str, **kwargs) -> EmailJob:
    """Create a new EmailJob with proper validation."""
    job_data = {
        'job_id': kwargs.get('job_id', ''),
        'filename': filename,
        'brokerage_key': brokerage_key,
        'email_source': email_source,
        **kwargs
    }
    
    # Validate and repair if needed
    is_valid, errors = DataValidator.validate_email_job(job_data)
    if not is_valid:
        logger.warning(f"Job data validation errors: {errors}")
        job_data = DataValidator.repair_email_job(job_data)
    
    return EmailJob.from_dict(job_data)


def convert_any_job_to_canonical(job_data: Any, source_format: str) -> EmailJob:
    """
    Convert any job format to canonical EmailJob.
    
    Args:
        job_data: Job data in any format
        source_format: Source format ('shared_storage', 'session_state', 'dashboard', 'dict')
        
    Returns:
        Canonical EmailJob instance
    """
    try:
        if source_format == 'shared_storage':
            return DataModelConverter.from_shared_storage_job(job_data)
        elif source_format == 'session_state':
            return DataModelConverter.from_session_state_job(job_data)
        elif source_format == 'dashboard':
            return DataModelConverter.from_dashboard_job(job_data)
        elif source_format == 'dict':
            return EmailJob.from_dict(job_data)
        else:
            logger.error(f"Unknown source format: {source_format}")
            # Try to auto-detect format
            if hasattr(job_data, 'job_id') and hasattr(job_data, 'started_at'):
                return DataModelConverter.from_shared_storage_job(job_data)
            elif isinstance(job_data, dict):
                return EmailJob.from_dict(job_data)
            else:
                raise ValueError(f"Cannot convert job data from format: {source_format}")
    except Exception as e:
        logger.error(f"Error converting job data: {e}")
        # Return a minimal valid job as fallback
        return EmailJob(
            job_id='conversion_error',
            filename='unknown',
            brokerage_key='unknown',
            email_source='unknown',
            error_message=f"Conversion error: {str(e)}"
        )
