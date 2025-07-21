"""Webhook postback handler."""

import json
from typing import List, Dict, Any
import logging
import requests
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class WebhookPostbackHandler(PostbackHandler):
    """Handler that sends enriched rows to a webhook endpoint."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.url = config.get('url')
        self.timeout = config.get('timeout', 30)
        self.headers = config.get('headers', {'Content-Type': 'application/json'})
        self.batch_size = config.get('batch_size', 100)  # Send in batches
        self.retry_count = config.get('retry_count', 3)
        
    def validate_config(self) -> bool:
        """Validate webhook handler configuration."""
        if not self.url:
            logger.error("Webhook handler missing url")
            return False
        return True
        
    def _send_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Send a batch of rows to the webhook."""
        payload = {
            'data': batch,
            'count': len(batch)
        }
        
        for attempt in range(self.retry_count):
            try:
                response = requests.post(
                    self.url,
                    json=payload,
                    headers=self.headers,
                    timeout=self.timeout
                )
                
                if response.status_code in (200, 201, 202):
                    logger.info(f"Successfully sent batch of {len(batch)} rows to webhook")
                    return True
                else:
                    logger.warning(f"Webhook returned status {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Webhook timeout on attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Webhook request failed on attempt {attempt + 1}: {e}")
                
            if attempt < self.retry_count - 1:
                logger.info(f"Retrying webhook request in 2 seconds...")
                import time
                time.sleep(2)
        
        logger.error(f"Failed to send batch to webhook after {self.retry_count} attempts")
        return False
        
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Send enriched rows to webhook endpoint.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to send to webhook")
            return True
            
        try:
            # Send data in batches
            success = True
            for i in range(0, len(rows), self.batch_size):
                batch = rows[i:i + self.batch_size]
                if not self._send_batch(batch):
                    success = False
                    
            if success:
                logger.info(f"Successfully sent all {len(rows)} rows to webhook")
            else:
                logger.error("Some batches failed to send to webhook")
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to send to webhook: {e}")
            return False