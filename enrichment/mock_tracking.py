"""Mock tracking enrichment source."""

from typing import Dict, Any
import logging
from datetime import datetime, timedelta
import random
from .base import EnrichmentSource

logger = logging.getLogger(__name__)


class MockTrackingEnrichmentSource(EnrichmentSource):
    """Enriches data with mock tracking events for carrier + PRO pairs."""
    
    TRACKING_STATUSES = [
        "Picked Up",
        "In Transit",
        "At Terminal",
        "Out for Delivery",
        "Delivered",
        "Delivery Exception",
        "Returned to Shipper"
    ]
    
    TRACKING_LOCATIONS = [
        "Chicago, IL",
        "Atlanta, GA",
        "Los Angeles, CA",
        "Dallas, TX",
        "New York, NY",
        "Phoenix, AZ",
        "Philadelphia, PA",
        "Houston, TX",
        "San Antonio, TX",
        "San Diego, CA"
    ]
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.generate_events = config.get('generate_events', True)
        self.max_events = config.get('max_events', 5)
        
    def validate_config(self) -> bool:
        """Validate mock tracking configuration."""
        return True
        
    def is_applicable(self, row: Dict[str, Any]) -> bool:
        """Check if row has required fields for tracking enrichment."""
        return 'carrier' in row and 'PRO' in row
        
    def _generate_tracking_events(self, carrier: str, pro: str) -> list:
        """Generate mock tracking events for a shipment."""
        events = []
        base_date = datetime.now() - timedelta(days=random.randint(1, 7))
        
        num_events = random.randint(2, self.max_events)
        
        for i in range(num_events):
            event_date = base_date + timedelta(hours=random.randint(6, 48))
            status = random.choice(self.TRACKING_STATUSES)
            location = random.choice(self.TRACKING_LOCATIONS)
            
            events.append({
                'timestamp': event_date.isoformat(),
                'status': status,
                'location': location,
                'description': f"{status} at {location}"
            })
            
        # Sort events by timestamp
        events.sort(key=lambda x: x['timestamp'])
        return events
        
    def _get_current_status(self, events: list) -> str:
        """Get current status from tracking events."""
        if not events:
            return "Unknown"
        return events[-1]['status']
        
    def enrich(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich row with mock tracking information.
        
        Args:
            row: Original data row
            
        Returns:
            Row enriched with tracking data
        """
        # Create copy to avoid modifying original
        enriched_row = row.copy()
        
        try:
            carrier = row.get('carrier', 'Unknown')
            pro = row.get('PRO', 'Unknown')
            
            if self.generate_events and carrier != 'Unknown' and pro != 'Unknown':
                # Generate mock tracking events
                tracking_events = self._generate_tracking_events(carrier, pro)
                current_status = self._get_current_status(tracking_events)
                
                # Add enrichment fields
                enriched_row.update({
                    'tracking_status': current_status,
                    'tracking_events_count': len(tracking_events),
                    'tracking_events': tracking_events,
                    'last_update': tracking_events[-1]['timestamp'] if tracking_events else None,
                    'enrichment_source': 'mock_tracking',
                    'enrichment_timestamp': datetime.now().isoformat()
                })
                
                logger.debug(f"Enriched {carrier} {pro} with {len(tracking_events)} tracking events")
            else:
                # Add basic enrichment fields
                enriched_row.update({
                    'tracking_status': 'No Data Available',
                    'tracking_events_count': 0,
                    'tracking_events': [],
                    'last_update': None,
                    'enrichment_source': 'mock_tracking',
                    'enrichment_timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            logger.error(f"Error enriching row with mock tracking: {e}")
            # Add error information
            enriched_row.update({
                'enrichment_error': str(e),
                'enrichment_source': 'mock_tracking',
                'enrichment_timestamp': datetime.now().isoformat()
            })
            
        return enriched_row