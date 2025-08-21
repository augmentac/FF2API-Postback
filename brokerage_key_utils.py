"""
Brokerage Key Management Utilities

Provides centralized normalization and handling of brokerage keys to ensure 
consistency across the entire application. Handles the various formats:
- augment-brokerage (standard format)
- augment_brokerage (underscore format) 
- eshipping
- variations in case and spacing

Features:
- Key normalization to standard format
- Backward compatibility search 
- Migration utilities
- Validation functions
"""

import logging
from typing import Dict, Any, List, Optional, Set
import re

logger = logging.getLogger(__name__)


class BrokerageKeyManager:
    """Centralized brokerage key management and normalization."""
    
    # Standard format patterns
    STANDARD_FORMAT_PATTERN = re.compile(r'^[a-z][a-z0-9]*(-[a-z0-9]+)*$')
    
    # Common variations mapping
    KNOWN_VARIATIONS = {
        'augment-brokerage': ['augment_brokerage', 'Augment-Brokerage', 'AUGMENT_BROKERAGE', 'augment brokerage'],
        'eshipping': ['e-shipping', 'E-Shipping', 'E_SHIPPING', 'eShipping'],
        'test-brokerage': ['test_brokerage', 'Test-Brokerage', 'TEST_BROKERAGE', 'test brokerage']
    }
    
    @staticmethod
    def normalize(key: str) -> str:
        """
        Convert any brokerage key to standard format.
        
        Standard format: lowercase, hyphen-separated words
        Examples: 'augment-brokerage', 'eshipping', 'my-company-freight'
        
        Args:
            key: Raw brokerage key in any format
            
        Returns:
            Normalized key in standard format
        """
        if not key or not isinstance(key, str):
            return ""
        
        # Remove leading/trailing whitespace
        normalized = key.strip()
        
        # Convert to lowercase
        normalized = normalized.lower()
        
        # Replace spaces and underscores with hyphens
        normalized = re.sub(r'[_\s]+', '-', normalized)
        
        # Remove any non-alphanumeric characters except hyphens
        normalized = re.sub(r'[^a-z0-9-]', '', normalized)
        
        # Remove multiple consecutive hyphens
        normalized = re.sub(r'-+', '-', normalized)
        
        # Remove leading/trailing hyphens
        normalized = normalized.strip('-')
        
        return normalized
    
    @staticmethod
    def get_all_variations(key: str) -> List[str]:
        """
        Get all possible variations of a brokerage key for backwards compatibility.
        
        Args:
            key: Brokerage key in any format
            
        Returns:
            List of all possible variations, with normalized version first
        """
        normalized = BrokerageKeyManager.normalize(key)
        variations = [normalized]
        
        if not normalized:
            return variations
        
        # Add known variations if this is a known key
        for standard_key, known_vars in BrokerageKeyManager.KNOWN_VARIATIONS.items():
            if normalized == standard_key:
                variations.extend(known_vars)
                break
        
        # Generate common variations
        # Underscore version
        underscore_version = normalized.replace('-', '_')
        if underscore_version != normalized:
            variations.append(underscore_version)
        
        # Title case versions
        title_case = normalized.replace('-', ' ').title()
        variations.append(title_case)
        variations.append(title_case.replace(' ', '-'))
        variations.append(title_case.replace(' ', '_'))
        
        # Upper case versions
        variations.append(normalized.upper())
        variations.append(underscore_version.upper())
        
        # Original key if it's different
        if key.strip() not in variations:
            variations.append(key.strip())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variations = []
        for var in variations:
            if var and var not in seen:
                seen.add(var)
                unique_variations.append(var)
        
        return unique_variations
    
    @staticmethod
    def search_in_dict(storage_dict: Dict[str, Any], target_key: str) -> Optional[Any]:
        """
        Search for a value in a dictionary using all possible key variations.
        
        Args:
            storage_dict: Dictionary to search in
            target_key: Key to search for (any format)
            
        Returns:
            Value if found, None if not found
        """
        if not storage_dict or not target_key:
            return None
        
        # Try all variations
        for variant in BrokerageKeyManager.get_all_variations(target_key):
            if variant in storage_dict:
                logger.debug(f"Found brokerage data using key variation: {variant}")
                return storage_dict[variant]
        
        return None
    
    @staticmethod
    def search_all_in_dict(storage_dict: Dict[str, Any], target_key: str) -> Dict[str, Any]:
        """
        Search for all matching entries in a dictionary using key variations.
        
        Args:
            storage_dict: Dictionary to search in
            target_key: Key to search for (any format)
            
        Returns:
            Dictionary with all found entries {key_variation: value}
        """
        results = {}
        if not storage_dict or not target_key:
            return results
        
        # Search for all variations
        for variant in BrokerageKeyManager.get_all_variations(target_key):
            if variant in storage_dict:
                results[variant] = storage_dict[variant]
        
        return results
    
    @staticmethod
    def consolidate_dict_entries(storage_dict: Dict[str, Any], target_key: str, 
                                merge_function: callable = None) -> Dict[str, Any]:
        """
        Consolidate multiple entries for the same brokerage under the standard key.
        
        Args:
            storage_dict: Dictionary to consolidate
            target_key: Brokerage key to consolidate
            merge_function: Function to merge multiple values (optional)
            
        Returns:
            Updated dictionary with consolidated entries
        """
        if not storage_dict or not target_key:
            return storage_dict
        
        normalized_key = BrokerageKeyManager.normalize(target_key)
        if not normalized_key:
            return storage_dict
        
        # Find all entries for this brokerage
        all_entries = BrokerageKeyManager.search_all_in_dict(storage_dict, target_key)
        
        if len(all_entries) <= 1:
            # Nothing to consolidate
            return storage_dict
        
        logger.info(f"Consolidating {len(all_entries)} entries for brokerage: {normalized_key}")
        
        # Consolidate entries
        if merge_function:
            # Use custom merge function
            consolidated_value = merge_function(list(all_entries.values()))
        else:
            # Default: use the first non-empty value
            consolidated_value = next(
                (value for value in all_entries.values() if value), 
                None
            )
        
        # Remove old entries
        for old_key in all_entries.keys():
            if old_key != normalized_key:
                storage_dict.pop(old_key, None)
        
        # Set consolidated entry under normalized key
        if consolidated_value is not None:
            storage_dict[normalized_key] = consolidated_value
        
        return storage_dict
    
    @staticmethod
    def validate_key(key: str) -> bool:
        """
        Validate if a key follows the standard format.
        
        Args:
            key: Key to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not key or not isinstance(key, str):
            return False
        
        return bool(BrokerageKeyManager.STANDARD_FORMAT_PATTERN.match(key))
    
    @staticmethod
    def migrate_storage_keys(storage_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate all keys in a storage dictionary to standard format.
        
        Args:
            storage_dict: Dictionary with potentially inconsistent keys
            
        Returns:
            Dictionary with all keys in standard format
        """
        if not storage_dict:
            return storage_dict
        
        migrated = {}
        migration_count = 0
        
        for old_key, value in storage_dict.items():
            normalized_key = BrokerageKeyManager.normalize(old_key)
            
            if normalized_key != old_key:
                migration_count += 1
                logger.debug(f"Migrating key: {old_key} -> {normalized_key}")
            
            # Handle key collisions by merging
            if normalized_key in migrated:
                logger.warning(f"Key collision during migration: {normalized_key}")
                # For now, keep the first value - could be enhanced with merge logic
                continue
            
            migrated[normalized_key] = value
        
        if migration_count > 0:
            logger.info(f"Migrated {migration_count} brokerage keys to standard format")
        
        return migrated
    
    @staticmethod
    def get_migration_report(storage_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a report on what keys would be migrated.
        
        Args:
            storage_dict: Dictionary to analyze
            
        Returns:
            Migration report with statistics and mapping
        """
        report = {
            'total_keys': len(storage_dict),
            'keys_needing_migration': 0,
            'migration_mapping': {},
            'potential_collisions': [],
            'invalid_keys': []
        }
        
        if not storage_dict:
            return report
        
        seen_normalized = set()
        
        for key in storage_dict.keys():
            normalized = BrokerageKeyManager.normalize(key)
            
            # Check if key needs migration
            if key != normalized:
                report['keys_needing_migration'] += 1
                report['migration_mapping'][key] = normalized
                
                # Check for potential collisions
                if normalized in seen_normalized:
                    report['potential_collisions'].append(normalized)
                
                seen_normalized.add(normalized)
            
            # Check if key is valid
            if not BrokerageKeyManager.validate_key(normalized):
                report['invalid_keys'].append(key)
        
        return report


# Convenience functions for common operations
def normalize_brokerage_key(key: str) -> str:
    """Convenience function for key normalization."""
    return BrokerageKeyManager.normalize(key)


def find_brokerage_data(storage_dict: Dict[str, Any], brokerage_key: str) -> Optional[Any]:
    """Convenience function for finding brokerage data with key variations."""
    return BrokerageKeyManager.search_in_dict(storage_dict, brokerage_key)


def consolidate_brokerage_data(storage_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function for consolidating all brokerage data."""
    # Get all unique normalized keys
    all_normalized_keys = set()
    for key in storage_dict.keys():
        normalized = BrokerageKeyManager.normalize(key)
        if normalized:
            all_normalized_keys.add(normalized)
    
    # Consolidate each brokerage
    consolidated = storage_dict.copy()
    for normalized_key in all_normalized_keys:
        consolidated = BrokerageKeyManager.consolidate_dict_entries(
            consolidated, normalized_key
        )
    
    return consolidated
