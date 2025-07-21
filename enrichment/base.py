"""Base classes for enrichment sources."""

from abc import ABC, abstractmethod
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class EnrichmentSource(ABC):
    """Abstract base class for enrichment sources."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize enrichment source with configuration.
        
        Args:
            config: Source-specific configuration
        """
        self.config = config
        
    @abstractmethod
    def enrich(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a data row with additional information.
        
        Args:
            row: Original data row dictionary
            
        Returns:
            Enriched data row dictionary (should include original data)
        """
        pass
    
    def validate_config(self) -> bool:
        """Validate enrichment source configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return True
    
    def is_applicable(self, row: Dict[str, Any]) -> bool:
        """Check if this enrichment source is applicable to the given row.
        
        Args:
            row: Data row to check
            
        Returns:
            True if enrichment should be applied, False otherwise
        """
        return True