#!/usr/bin/env python3
"""
Postback and enrichment CLI tool for ff2api-tool.

This script loads data from CSV or JSON files, enriches it using configured
sources, and then posts the enriched data using configured postback handlers.
"""

import argparse
import json
import logging
import os
import sys
from typing import List, Dict, Any

import pandas as pd
import yaml

# Import our custom modules
from enrichment.manager import EnrichmentManager
from postback.router import PostbackRouter


def setup_logging(log_level: str = 'INFO'):
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('postback.log')
        ]
    )


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logging.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config from {config_path}: {e}")
        raise


def load_input_data(input_path: str) -> List[Dict[str, Any]]:
    """Load input data from CSV or JSON file."""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    file_ext = os.path.splitext(input_path)[1].lower()
    
    try:
        if file_ext == '.csv':
            df = pd.read_csv(input_path)
            rows = df.to_dict('records')
            logging.info(f"Loaded {len(rows)} rows from CSV file: {input_path}")
            
        elif file_ext == '.json':
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = [data]
            else:
                raise ValueError("JSON file must contain an array or object")
                
            logging.info(f"Loaded {len(rows)} rows from JSON file: {input_path}")
            
        else:
            raise ValueError(f"Unsupported file format: {file_ext}. Use .csv or .json")
            
        return rows
        
    except Exception as e:
        logging.error(f"Failed to load input data from {input_path}: {e}")
        raise


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration structure."""
    required_sections = ['postback', 'enrichment']
    
    for section in required_sections:
        if section not in config:
            logging.error(f"Missing required config section: {section}")
            return False
    
    if 'handlers' not in config['postback']:
        logging.error("Missing 'handlers' in postback config")
        return False
        
    if 'sources' not in config['enrichment']:
        logging.error("Missing 'sources' in enrichment config")
        return False
    
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Enrich and post freight data using configurable handlers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --input processed_loads.csv --config config.yml
  %(prog)s --input data.json --config config.yml --log-level DEBUG
  %(prog)s --input loads.csv --config config.yml --dry-run
        """
    )
    
    parser.add_argument(
        '--input',
        required=True,
        help='Path to input data file (CSV or JSON)'
    )
    
    parser.add_argument(
        '--config',
        required=True,
        help='Path to configuration YAML file'
    )
    
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform enrichment but skip postback handlers'
    )
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        if not validate_config(config):
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        # Load input data
        rows = load_input_data(args.input)
        
        if not rows:
            logger.warning("No data rows found in input file")
            sys.exit(0)
        
        # Initialize enrichment manager
        enrichment_config = config['enrichment']['sources']
        enrichment_manager = EnrichmentManager(enrichment_config)
        
        logger.info(f"Initialized {enrichment_manager.get_source_count()} enrichment sources: "
                   f"{', '.join(enrichment_manager.get_source_types())}")
        
        # Enrich data
        logger.info("Starting data enrichment...")
        enriched_rows = enrichment_manager.enrich_rows(rows)
        
        if not enriched_rows:
            logger.error("No enriched data to process")
            sys.exit(1)
        
        logger.info(f"Enrichment completed. Processing {len(enriched_rows)} enriched rows.")
        
        if args.dry_run:
            logger.info("Dry run mode - skipping postback handlers")
            logger.info("Sample enriched row:")
            if enriched_rows:
                sample_row = {k: v for k, v in list(enriched_rows[0].items())[:5]}
                for key, value in sample_row.items():
                    logger.info(f"  {key}: {value}")
            sys.exit(0)
        
        # Initialize postback router
        postback_config = config['postback']['handlers']
        postback_router = PostbackRouter(postback_config)
        
        logger.info(f"Initialized {postback_router.get_handler_count()} postback handlers")
        
        # Post enriched data
        logger.info("Starting postback operations...")
        results = postback_router.post_all(enriched_rows)
        
        # Report results
        logger.info("Postback Results:")
        success_count = 0
        for handler_type, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"  {handler_type}: {status}")
            if success:
                success_count += 1
        
        total_handlers = len(results)
        logger.info(f"Overall: {success_count}/{total_handlers} handlers succeeded")
        
        if success_count == 0:
            logger.error("All postback handlers failed")
            sys.exit(1)
        elif success_count < total_handlers:
            logger.warning("Some postback handlers failed")
            sys.exit(2)
        else:
            logger.info("All postback handlers succeeded")
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if args.log_level == 'DEBUG':
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()