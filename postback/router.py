"""Postback router for dispatching to multiple handlers."""

from typing import List, Dict, Any, Type
import logging
from .base import PostbackHandler
from .csv_handler import CSVPostbackHandler
from .xlsx_handler import XLSXPostbackHandler
from .json_handler import JSONPostbackHandler
from .xml_handler import XMLPostbackHandler
from .webhook_handler import WebhookPostbackHandler
from .email_handler import EmailPostbackHandler

logger = logging.getLogger(__name__)


class PostbackRouter:
    """Routes postback requests to configured handlers."""
    
    HANDLER_TYPES: Dict[str, Type[PostbackHandler]] = {
        'csv': CSVPostbackHandler,
        'xlsx': XLSXPostbackHandler,
        'json': JSONPostbackHandler,
        'xml': XMLPostbackHandler,
        'webhook': WebhookPostbackHandler,
        'email': EmailPostbackHandler,
    }
    
    def __init__(self, handler_configs: List[Dict[str, Any]]):
        """Initialize router with handler configurations.
        
        Args:
            handler_configs: List of handler configuration dictionaries
        """
        self.handlers = []
        self._initialize_handlers(handler_configs)
        
    def _initialize_handlers(self, handler_configs: List[Dict[str, Any]]):
        """Initialize handlers from configuration."""
        for config in handler_configs:
            handler_type = config.get('type')
            
            if handler_type not in self.HANDLER_TYPES:
                logger.error(f"Unknown handler type: {handler_type}")
                continue
                
            try:
                handler_class = self.HANDLER_TYPES[handler_type]
                handler = handler_class(config)
                
                if handler.validate_config():
                    self.handlers.append(handler)
                    logger.info(f"Initialized {handler_type} handler")
                else:
                    logger.error(f"Invalid configuration for {handler_type} handler")
                    
            except Exception as e:
                logger.error(f"Failed to initialize {handler_type} handler: {e}")
                
    def post_all(self, rows: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Post data to all configured handlers.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            Dictionary mapping handler type to success status
        """
        results = {}
        
        if not rows:
            logger.warning("No rows to post")
            return results
            
        logger.info(f"Posting {len(rows)} rows to {len(self.handlers)} handlers")
        
        for handler in self.handlers:
            handler_type = handler.__class__.__name__.replace('PostbackHandler', '').lower()
            
            try:
                success = handler.post(rows)
                results[handler_type] = success
                
                if success:
                    logger.info(f"Successfully posted to {handler_type} handler")
                else:
                    logger.error(f"Failed to post to {handler_type} handler")
                    
            except Exception as e:
                logger.error(f"Error posting to {handler_type} handler: {e}")
                results[handler_type] = False
                
        return results
        
    def get_handler_count(self) -> int:
        """Get number of active handlers."""
        return len(self.handlers)
    
    def export_enriched_data(self, enriched_df, export_format: str = 'csv', 
                            filename_prefix: str = 'enriched_data') -> Dict[str, Any]:
        """
        Export enriched dataset to specified format.
        
        This function creates downloadable files from the enriched dataset for user download.
        It bypasses the normal postback workflow to provide immediate file generation.
        
        Args:
            enriched_df: pandas DataFrame with enriched data
            export_format: Format to export ('csv', 'xlsx', 'json')
            filename_prefix: Prefix for the generated filename
            
        Returns:
            Dictionary with export results including file path, success status, and metadata
        """
        import pandas as pd
        from datetime import datetime
        import tempfile
        import os
        
        try:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename_prefix}_{timestamp}.{export_format}"
            
            # Create temporary file for export
            temp_dir = tempfile.gettempdir()
            file_path = os.path.join(temp_dir, filename)
            
            logger.info(f"Exporting enriched data to {export_format.upper()} format: {filename}")
            
            # Export based on format
            if export_format.lower() == 'csv':
                enriched_df.to_csv(file_path, index=False)
                
            elif export_format.lower() == 'xlsx':
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    enriched_df.to_excel(writer, sheet_name='Enriched Data', index=False)
                    
                    # Add metadata sheet
                    metadata_df = pd.DataFrame({
                        'Export Info': [
                            'Export Timestamp',
                            'Total Rows',
                            'Total Columns',
                            'Processing Status Summary'
                        ],
                        'Value': [
                            datetime.now().isoformat(),
                            len(enriched_df),
                            len(enriched_df.columns),
                            f"Processed: {len(enriched_df[enriched_df.get('processing_status', '') == 'processed'])}"
                        ]
                    })
                    metadata_df.to_excel(writer, sheet_name='Export Metadata', index=False)
                    
            elif export_format.lower() == 'json':
                enriched_df.to_json(file_path, orient='records', indent=2)
                
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            # Get file size
            file_size = os.path.getsize(file_path)
            
            # Generate export summary
            export_summary = {
                'success': True,
                'file_path': file_path,
                'filename': filename,
                'format': export_format.upper(),
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'total_rows': len(enriched_df),
                'total_columns': len(enriched_df.columns),
                'export_timestamp': datetime.now().isoformat(),
                'columns': list(enriched_df.columns)
            }
            
            # Add processing statistics if available
            if 'processing_status' in enriched_df.columns:
                status_counts = enriched_df['processing_status'].value_counts().to_dict()
                export_summary['processing_statistics'] = status_counts
            
            if 'ff2api_success' in enriched_df.columns:
                success_count = enriched_df['ff2api_success'].sum() if enriched_df['ff2api_success'].dtype == 'bool' else len(enriched_df[enriched_df['ff2api_success'] == True])
                export_summary['ff2api_success_count'] = success_count
                export_summary['ff2api_success_rate'] = f"{(success_count / len(enriched_df) * 100):.1f}%"
            
            logger.info(f"Successfully exported {len(enriched_df)} rows to {filename} ({export_summary['file_size_mb']} MB)")
            
            return export_summary
            
        except Exception as e:
            logger.error(f"Error exporting enriched data: {e}")
            return {
                'success': False,
                'error': str(e),
                'export_timestamp': datetime.now().isoformat()
            }