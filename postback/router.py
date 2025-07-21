"""Postback router for dispatching to multiple handlers."""

from typing import List, Dict, Any, Type
import logging
from .base import PostbackHandler
from .csv_handler import CSVPostbackHandler
from .xlsx_handler import XLSXPostbackHandler
from .json_handler import JSONPostbackHandler
from .xml_handler import XMLPostbackHandler
from .webhook_handler import WebhookPostbackHandler

logger = logging.getLogger(__name__)


class PostbackRouter:
    """Routes postback requests to configured handlers."""
    
    HANDLER_TYPES: Dict[str, Type[PostbackHandler]] = {
        'csv': CSVPostbackHandler,
        'xlsx': XLSXPostbackHandler,
        'json': JSONPostbackHandler,
        'xml': XMLPostbackHandler,
        'webhook': WebhookPostbackHandler,
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