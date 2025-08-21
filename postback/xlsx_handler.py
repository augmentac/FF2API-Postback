"""Excel (XLSX) postback handler."""

import os
from typing import List, Dict, Any
import logging
from openpyxl import Workbook, load_workbook
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class XLSXPostbackHandler(PostbackHandler):
    """Handler that writes enriched rows to an Excel file."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_path = config.get('output_path', './outputs/postback.xlsx')
        self.sheet_name = config.get('sheet_name', 'Enriched_Data')
        
    def validate_config(self) -> bool:
        """Validate XLSX handler configuration."""
        if not self.output_path:
            logger.error("XLSX handler missing output_path")
            return False
        return True
        
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Write enriched rows to Excel file.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to write to XLSX")
            return True
            
        # Filter out empty dictionaries and validate data
        valid_rows = [row for row in rows if row and isinstance(row, dict) and any(row.values())]
        
        if not valid_rows:
            logger.warning("No valid data rows to write to XLSX (all rows were empty or invalid)")
            return True
            
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Load existing workbook or create new one
            if os.path.exists(self.output_path):
                wb = load_workbook(self.output_path)
                if self.sheet_name in wb.sheetnames:
                    ws = wb[self.sheet_name]
                    # Find next empty row
                    next_row = ws.max_row + 1
                else:
                    ws = wb.create_sheet(self.sheet_name)
                    next_row = 1
            else:
                wb = Workbook()
                ws = wb.active
                ws.title = self.sheet_name
                next_row = 1
            
            # Write headers if this is the first row
            if next_row == 1:
                headers = list(valid_rows[0].keys())
                for col, header in enumerate(headers, 1):
                    ws.cell(row=1, column=col, value=header)
                next_row = 2
            
            # Write data rows
            for row_data in valid_rows:
                for col, key in enumerate(row_data.keys(), 1):
                    value = row_data[key]
                    # Handle None values and complex objects
                    if value is None:
                        value = ""
                    elif isinstance(value, (dict, list)):
                        value = str(value)
                    ws.cell(row=next_row, column=col, value=value)
                next_row += 1
                
            wb.save(self.output_path)
            logger.info(f"Successfully wrote {len(valid_rows)} rows to {self.output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write XLSX: {e}")
            return False