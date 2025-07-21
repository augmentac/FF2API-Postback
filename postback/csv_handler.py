"""CSV postback handler."""

import csv
import os
from typing import List, Dict, Any
import logging
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class CSVPostbackHandler(PostbackHandler):
    """Handler that appends enriched rows to a CSV file."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_path = config.get('output_path', './outputs/postback.csv')
        
    def validate_config(self) -> bool:
        """Validate CSV handler configuration."""
        if not self.output_path:
            logger.error("CSV handler missing output_path")
            return False
        return True
        
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Append enriched rows to CSV file.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to write to CSV")
            return True
            
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Check if file exists to determine if we need headers
            file_exists = os.path.exists(self.output_path)
            
            with open(self.output_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = rows[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                    
                writer.writerows(rows)
                
            logger.info(f"Successfully wrote {len(rows)} rows to {self.output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write CSV: {e}")
            return False