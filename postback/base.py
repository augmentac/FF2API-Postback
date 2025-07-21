"""Base classes for postback handlers."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class PostbackHandler(ABC):
    """Abstract base class for postback handlers."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize handler with configuration.
        
        Args:
            config: Handler-specific configuration
        """
        self.config = config
        
    @abstractmethod
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Post enriched data rows.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    def validate_config(self) -> bool:
        """Validate handler configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return True