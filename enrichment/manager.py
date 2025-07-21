"""Enrichment manager for coordinating multiple enrichment sources."""

from typing import List, Dict, Any, Type
import logging
from .base import EnrichmentSource
from .mock_tracking import MockTrackingEnrichmentSource

logger = logging.getLogger(__name__)


class EnrichmentManager:
    """Manages multiple enrichment sources and applies them to data rows."""
    
    SOURCE_TYPES: Dict[str, Type[EnrichmentSource]] = {
        'mock_tracking': MockTrackingEnrichmentSource,
    }
    
    def __init__(self, source_configs: List[Dict[str, Any]]):
        """Initialize manager with enrichment source configurations.
        
        Args:
            source_configs: List of source configuration dictionaries
        """
        self.sources = []
        self._initialize_sources(source_configs)
        
    def _initialize_sources(self, source_configs: List[Dict[str, Any]]):
        """Initialize enrichment sources from configuration."""
        for config in source_configs:
            source_type = config.get('type')
            
            if source_type not in self.SOURCE_TYPES:
                logger.error(f"Unknown enrichment source type: {source_type}")
                continue
                
            try:
                source_class = self.SOURCE_TYPES[source_type]
                source = source_class(config)
                
                if source.validate_config():
                    self.sources.append(source)
                    logger.info(f"Initialized {source_type} enrichment source")
                else:
                    logger.error(f"Invalid configuration for {source_type} enrichment source")
                    
            except Exception as e:
                logger.error(f"Failed to initialize {source_type} enrichment source: {e}")
                
    def enrich_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single data row using all applicable sources.
        
        Args:
            row: Original data row dictionary
            
        Returns:
            Enriched data row dictionary
        """
        enriched_row = row.copy()
        
        for source in self.sources:
            try:
                if source.is_applicable(enriched_row):
                    enriched_row = source.enrich(enriched_row)
                    
            except Exception as e:
                source_type = source.__class__.__name__
                logger.error(f"Error in {source_type} enrichment: {e}")
                # Add error information to the row
                enriched_row[f'{source_type}_error'] = str(e)
                
        return enriched_row
        
    def enrich_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich multiple data rows.
        
        Args:
            rows: List of original data row dictionaries
            
        Returns:
            List of enriched data row dictionaries
        """
        if not rows:
            logger.warning("No rows to enrich")
            return rows
            
        logger.info(f"Enriching {len(rows)} rows with {len(self.sources)} sources")
        
        enriched_rows = []
        for i, row in enumerate(rows):
            try:
                enriched_row = self.enrich_row(row)
                enriched_rows.append(enriched_row)
                
                if (i + 1) % 100 == 0:  # Log progress every 100 rows
                    logger.info(f"Enriched {i + 1}/{len(rows)} rows")
                    
            except Exception as e:
                logger.error(f"Failed to enrich row {i}: {e}")
                # Include original row with error information
                error_row = row.copy()
                error_row['enrichment_error'] = str(e)
                enriched_rows.append(error_row)
                
        logger.info(f"Successfully enriched {len(enriched_rows)} rows")
        return enriched_rows
        
    def get_source_count(self) -> int:
        """Get number of active enrichment sources."""
        return len(self.sources)
        
    def get_source_types(self) -> List[str]:
        """Get list of active enrichment source types."""
        return [source.__class__.__name__ for source in self.sources]