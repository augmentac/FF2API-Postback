"""JSON postback handler."""

import json
import os
from typing import List, Dict, Any
import logging
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class JSONPostbackHandler(PostbackHandler):
    """Handler that writes enriched rows to a JSON file."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_path = config.get('output_path', './outputs/postback.json')
        self.append_mode = config.get('append_mode', False)
        
    def validate_config(self) -> bool:
        """Validate JSON handler configuration."""
        if not self.output_path:
            logger.error("JSON handler missing output_path")
            return False
        return True
        
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Write enriched rows to JSON file.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to write to JSON")
            return True
            
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            if self.append_mode and os.path.exists(self.output_path):
                # Load existing data and append
                with open(self.output_path, 'r', encoding='utf-8') as f:
                    try:
                        existing_data = json.load(f)
                        if isinstance(existing_data, list):
                            combined_data = existing_data + rows
                        else:
                            combined_data = [existing_data] + rows
                    except json.JSONDecodeError:
                        logger.warning("Existing JSON file is invalid, overwriting")
                        combined_data = rows
            else:
                combined_data = rows
            
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, default=str, ensure_ascii=False)
                
            logger.info(f"Successfully wrote {len(rows)} rows to {self.output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write JSON: {e}")
            return False