"""XML postback handler."""

import os
from typing import List, Dict, Any
import logging
from xml.etree.ElementTree import Element, SubElement, ElementTree
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class XMLPostbackHandler(PostbackHandler):
    """Handler that writes enriched rows to an XML file."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_path = config.get('output_path', './outputs/postback.xml')
        self.root_element = config.get('root_element', 'data')
        self.row_element = config.get('row_element', 'row')
        
    def validate_config(self) -> bool:
        """Validate XML handler configuration."""
        if not self.output_path:
            logger.error("XML handler missing output_path")
            return False
        return True
        
    def _sanitize_element_name(self, name: str) -> str:
        """Sanitize field name to be valid XML element name."""
        # Replace invalid characters with underscores
        sanitized = ''.join(c if c.isalnum() or c in '-_' else '_' for c in str(name))
        # Ensure it starts with a letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = '_' + sanitized
        return sanitized or 'field'
        
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Write enriched rows to XML file.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to write to XML")
            return True
            
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Create XML structure
            root = Element(self.root_element)
            
            for row_data in rows:
                row_elem = SubElement(root, self.row_element)
                
                for key, value in row_data.items():
                    field_elem = SubElement(row_elem, self._sanitize_element_name(key))
                    field_elem.text = str(value) if value is not None else ''
            
            # Write to file
            tree = ElementTree(root)
            tree.write(self.output_path, encoding='utf-8', xml_declaration=True)
            
            logger.info(f"Successfully wrote {len(rows)} rows to {self.output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write XML: {e}")
            return False