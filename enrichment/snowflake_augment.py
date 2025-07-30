"""GoAugment Snowflake enrichment source."""

import streamlit as st
from typing import Dict, Any, List
import logging
from datetime import datetime
from .base import EnrichmentSource

logger = logging.getLogger(__name__)

# Try to import Snowflake connector, fall back gracefully if not available
try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    logger.warning("snowflake-connector-python not available. Snowflake enrichment will be disabled.")


class SnowflakeAugmentEnrichmentSource(EnrichmentSource):
    """Enriches data using GoAugment DBT models in Snowflake."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.database = config.get('database', 'AUGMENT_DW')
        self.schema = config.get('schema', 'MARTS')
        self.enabled_enrichments = config.get('enrichments', [])
        self.use_load_ids = config.get('use_load_ids', False)  # Flag for load ID-based enrichment
        self.brokerage_key = config.get('brokerage_key')  # NEW: Brokerage context for filtering
        self._connection = None
        self._connection_error = None
        
        # Store global Snowflake credentials from config
        self.snowflake_creds = {
            'account': config.get('account'),
            'user': config.get('user'), 
            'password': config.get('password'),
            'warehouse': config.get('warehouse', 'COMPUTE_WH'),
            'role': config.get('role')
        }
        
        if not SNOWFLAKE_AVAILABLE:
            logger.error("Snowflake connector not available")
            self._connection_error = "Snowflake connector not installed"
    
    def validate_config(self) -> bool:
        """Validate Snowflake enrichment configuration."""
        if not SNOWFLAKE_AVAILABLE:
            logger.error("Snowflake connector not available")
            return False
            
        try:
            # Test connection
            conn = self._get_connection()
            if conn:
                conn.close()
                return True
            return False
        except Exception as e:
            logger.error(f"Snowflake configuration validation failed: {e}")
            return False
    
    def _get_connection(self):
        """Get Snowflake connection using global credentials with brokerage context."""
        if self._connection_error:
            return None
            
        if not self._connection:
            try:
                # Use global credentials passed from credential manager
                if all(self.snowflake_creds.get(key) for key in ['account', 'user', 'password']):
                    self._connection = snowflake.connector.connect(
                        account=self.snowflake_creds['account'],
                        user=self.snowflake_creds['user'],
                        password=self.snowflake_creds['password'],
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.snowflake_creds['warehouse'],
                        role=self.snowflake_creds.get('role')
                    )
                    
                    # Set session parameters for brokerage context if available
                    if self.brokerage_key:
                        cursor = self._connection.cursor()
                        try:
                            cursor.execute("ALTER SESSION SET BROKERAGE_CONTEXT = %s", (self.brokerage_key,))
                            logger.info(f"Set brokerage context: {self.brokerage_key}")
                        except Exception as e:
                            logger.warning(f"Could not set brokerage context: {e}")
                        finally:
                            cursor.close()
                    
                    logger.info(f"Snowflake connection established for brokerage: {self.brokerage_key}")
                else:
                    self._connection_error = "Snowflake global credentials not provided"
                    logger.error(self._connection_error)
                    return None
                    
            except Exception as e:
                self._connection_error = f"Failed to connect to Snowflake: {str(e)}"
                logger.error(self._connection_error)
                return None
                
        return self._connection
    
    def is_applicable(self, row: Dict[str, Any]) -> bool:
        """Check if Snowflake enrichment is applicable to the row."""
        if self._connection_error:
            return False
        
        # If using load IDs, check for internal_load_id
        if self.use_load_ids:
            return bool(row.get('internal_load_id'))
            
        # Check for various identifier fields that can be used for lookups
        identifier_fields = [
            'PRO', 'pro_number', 'Carrier Pro#',  # PRO number variants
            'load_id', 'bol_number', 'BOL #',     # BOL/Load ID variants
            'customer_code', 'Customer Name',      # Customer variants
            'carrier', 'Carrier Name'              # Carrier variants
        ]
        
        # Return True if we have at least one identifier field
        has_identifier = any(row.get(field) for field in identifier_fields)
        
        # Also check for enrichment-specific requirements
        if 'tracking' in self.enabled_enrichments and any(row.get(field) for field in ['PRO', 'pro_number', 'Carrier Pro#']):
            return True
        if 'customer' in self.enabled_enrichments and any(row.get(field) for field in ['customer_code', 'Customer Name']):
            return True
        if 'carrier' in self.enabled_enrichments and any(row.get(field) for field in ['carrier', 'Carrier Name']):
            return True
        if 'lane' in self.enabled_enrichments and (row.get('origin_zip') or row.get('Origin Zip')) and (row.get('dest_zip') or row.get('Destination Zip')):
            return True
            
        return has_identifier
    
    def enrich(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich row with GoAugment Snowflake data."""
        enriched = row.copy()
        
        # Add enrichment metadata
        enriched['sf_enrichment_timestamp'] = datetime.now().isoformat()
        enriched['sf_enrichment_source'] = 'snowflake_augment'
        
        # If there's a connection error, add error info and return
        if self._connection_error:
            enriched['sf_enrichment_error'] = self._connection_error
            return enriched
        
        connection = self._get_connection()
        if not connection:
            enriched['sf_enrichment_error'] = "Unable to connect to Snowflake"
            return enriched
        
        try:
            # Use load ID-based enrichment if enabled and internal_load_id is available
            if self.use_load_ids and row.get('internal_load_id'):
                load_data = self._get_load_data_by_id(connection, row['internal_load_id'])
                enriched.update(load_data)
            else:
                # Traditional field-based enrichment with flexible field matching
                # Add tracking data if any PRO field exists and tracking enrichment enabled
                if 'tracking' in self.enabled_enrichments:
                    pro_number = (row.get('PRO') or row.get('pro_number') or 
                                row.get('Carrier Pro#') or row.get('carrier_pro'))
                    if pro_number:
                        tracking_data = self._get_tracking_data(connection, str(pro_number))
                        enriched.update(tracking_data)
                
                # Add customer data if any customer field exists and customer enrichment enabled
                if 'customer' in self.enabled_enrichments:
                    customer_id = (row.get('customer_code') or row.get('Customer Name') or
                                 row.get('customer_name') or row.get('Acct/Customer#'))
                    if customer_id:
                        customer_data = self._get_customer_data(connection, str(customer_id))
                        enriched.update(customer_data)
                    
                # Add carrier data if any carrier field exists and carrier enrichment enabled
                if 'carrier' in self.enabled_enrichments:
                    carrier_id = (row.get('carrier') or row.get('Carrier Name') or
                                row.get('carrier_name') or row.get('carrier_code'))
                    if carrier_id:
                        carrier_data = self._get_carrier_data(connection, str(carrier_id))
                        enriched.update(carrier_data)
                    
                # Add lane performance if any origin/dest fields exist and lane enrichment enabled  
                if 'lane' in self.enabled_enrichments:
                    origin_zip = (row.get('origin_zip') or row.get('Origin Zip') or 
                                row.get('origin_postal_code'))
                    dest_zip = (row.get('dest_zip') or row.get('Destination Zip') or
                              row.get('dest_postal_code'))
                    carrier_for_lane = (row.get('carrier') or row.get('Carrier Name'))
                    
                    if origin_zip and dest_zip:
                        lane_data = self._get_lane_data(connection, str(origin_zip), str(dest_zip), 
                                                      str(carrier_for_lane) if carrier_for_lane else None)
                        enriched.update(lane_data)
                
        except Exception as e:
            logger.error(f"Snowflake enrichment error for row: {e}")
            enriched['sf_enrichment_error'] = str(e)
            
        return enriched
    
    def _execute_query(self, connection, query: str, params: List = None) -> List[tuple]:
        """Execute a query and return results."""
        cursor = connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def _get_tracking_data(self, connection, pro_number: str) -> Dict[str, Any]:
        """Get latest tracking data for a PRO number with brokerage filtering."""
        query = f"""
        SELECT 
            current_status,
            scan_location,
            scan_datetime,
            estimated_delivery_date
        FROM {self.database}.{self.schema}.fct_tracking_events
        WHERE pro_number = %s"""
        
        # Add brokerage filtering if context is available
        params = [pro_number]
        if self.brokerage_key:
            query += " AND brokerage_id = %s"
            params.append(self.brokerage_key)
        
        query += " ORDER BY scan_datetime DESC LIMIT 1"
        
        try:
            results = self._execute_query(connection, query, params)
            
            if results:
                result = results[0]
                return {
                    'sf_tracking_status': result[0] if result[0] else 'Unknown',
                    'sf_last_scan_location': result[1] if result[1] else 'Unknown',
                    'sf_last_scan_time': result[2].isoformat() if result[2] else None,
                    'sf_estimated_delivery': result[3].isoformat() if result[3] else None
                }
            else:
                return {
                    'sf_tracking_status': 'No Data',
                    'sf_last_scan_location': 'No Data',
                    'sf_last_scan_time': None,
                    'sf_estimated_delivery': None
                }
        except Exception as e:
            logger.error(f"Error getting tracking data for PRO {pro_number}: {e}")
            return {'sf_tracking_error': str(e)}
    
    def _get_customer_data(self, connection, customer_code: str) -> Dict[str, Any]:
        """Get customer information with brokerage filtering."""
        query = f"""
        SELECT 
            customer_name,
            account_manager,
            payment_terms,
            customer_tier
        FROM {self.database}.{self.schema}.dim_customers
        WHERE customer_code = %s"""
        
        # Add brokerage filtering if context is available
        params = [customer_code]
        if self.brokerage_key:
            query += " AND brokerage_id = %s"
            params.append(self.brokerage_key)
        
        try:
            results = self._execute_query(connection, query, params)
            
            if results:
                result = results[0]
                return {
                    'sf_customer_name': result[0] if result[0] else 'Unknown',
                    'sf_account_manager': result[1] if result[1] else 'Unassigned',
                    'sf_payment_terms': result[2] if result[2] else 'Unknown',
                    'sf_customer_tier': result[3] if result[3] else 'Standard'
                }
            else:
                return {
                    'sf_customer_name': 'Not Found',
                    'sf_account_manager': 'Unknown',
                    'sf_payment_terms': 'Unknown',
                    'sf_customer_tier': 'Unknown'
                }
        except Exception as e:
            logger.error(f"Error getting customer data for {customer_code}: {e}")
            return {'sf_customer_error': str(e)}
    
    def _get_carrier_data(self, connection, carrier_code: str) -> Dict[str, Any]:
        """Get carrier information."""
        query = f"""
        SELECT 
            carrier_name,
            on_time_percentage,
            service_levels
        FROM {self.database}.{self.schema}.dim_carriers  
        WHERE carrier_code = %s
        """
        
        try:
            results = self._execute_query(connection, query, [carrier_code])
            
            if results:
                result = results[0]
                return {
                    'sf_carrier_name': result[0] if result[0] else carrier_code,
                    'sf_carrier_otp': float(result[1]) if result[1] else 0.0,
                    'sf_service_levels': result[2] if result[2] else 'Unknown'
                }
            else:
                return {
                    'sf_carrier_name': carrier_code,
                    'sf_carrier_otp': 0.0,
                    'sf_service_levels': 'Unknown'
                }
        except Exception as e:
            logger.error(f"Error getting carrier data for {carrier_code}: {e}")
            return {'sf_carrier_error': str(e)}
    
    def _get_lane_data(self, connection, origin_zip: str, dest_zip: str, carrier_code: str = None) -> Dict[str, Any]:
        """Get lane performance data."""
        base_query = f"""
        SELECT 
            AVG(transit_days) as avg_transit_days,
            AVG(total_cost) as avg_lane_cost,
            COUNT(*) as lane_volume
        FROM {self.database}.{self.schema}.fct_shipments
        WHERE origin_zip = %s 
          AND dest_zip = %s
        """
        
        params = [origin_zip, dest_zip]
        
        if carrier_code:
            base_query += " AND carrier_code = %s"
            params.append(carrier_code)
            
        base_query += " AND ship_date >= CURRENT_DATE - 90"
        
        try:
            results = self._execute_query(connection, base_query, params)
            
            if results:
                result = results[0]
                return {
                    'sf_avg_transit_days': float(result[0]) if result[0] else 0.0,
                    'sf_avg_lane_cost': float(result[1]) if result[1] else 0.0,
                    'sf_lane_volume': int(result[2]) if result[2] else 0
                }
            else:
                return {
                    'sf_avg_transit_days': 0.0,
                    'sf_avg_lane_cost': 0.0,
                    'sf_lane_volume': 0
                }
        except Exception as e:
            logger.error(f"Error getting lane data for {origin_zip}-{dest_zip}: {e}")
            return {'sf_lane_error': str(e)}
    
    def _get_load_data_by_id(self, connection, internal_load_id: str) -> Dict[str, Any]:
        """Get comprehensive load data using internal load ID with brokerage filtering."""
        query = f"""
        SELECT 
            l.current_status as sf_load_status,
            l.pickup_date as sf_pickup_date,
            l.delivery_date as sf_delivery_date,
            l.total_cost as sf_total_cost,
            l.carrier_name as sf_carrier_name,
            c.customer_name as sf_customer_name,
            c.account_manager as sf_account_manager,
            c.payment_terms as sf_payment_terms,
            t.current_status as sf_tracking_status,
            t.scan_location as sf_last_scan_location,
            t.scan_datetime as sf_last_scan_time,
            t.estimated_delivery_date as sf_estimated_delivery
        FROM {self.database}.{self.schema}.dim_loads l
        LEFT JOIN {self.database}.{self.schema}.dim_customers c ON l.customer_id = c.customer_id
        LEFT JOIN {self.database}.{self.schema}.fct_tracking_events t ON l.pro_number = t.pro_number
        WHERE l.internal_load_id = %s"""
        
        # Add brokerage filtering if context is available
        params = [internal_load_id]
        if self.brokerage_key:
            query += " AND l.brokerage_id = %s"
            params.append(self.brokerage_key)
        
        query += " ORDER BY t.scan_datetime DESC LIMIT 1"
        
        try:
            results = self._execute_query(connection, query, params)
            
            if results:
                result = results[0]
                return {
                    'sf_load_status': result[0] if result[0] else 'Unknown',
                    'sf_pickup_date': result[1].isoformat() if result[1] else None,
                    'sf_delivery_date': result[2].isoformat() if result[2] else None,
                    'sf_total_cost': float(result[3]) if result[3] else 0.0,
                    'sf_carrier_name': result[4] if result[4] else 'Unknown',
                    'sf_customer_name': result[5] if result[5] else 'Unknown',
                    'sf_account_manager': result[6] if result[6] else 'Unassigned',
                    'sf_payment_terms': result[7] if result[7] else 'Unknown',
                    'sf_tracking_status': result[8] if result[8] else 'No Data',
                    'sf_last_scan_location': result[9] if result[9] else 'Unknown',
                    'sf_last_scan_time': result[10].isoformat() if result[10] else None,
                    'sf_estimated_delivery': result[11].isoformat() if result[11] else None
                }
            else:
                return {
                    'sf_load_status': 'Not Found',
                    'sf_pickup_date': None,
                    'sf_delivery_date': None,
                    'sf_total_cost': 0.0,
                    'sf_carrier_name': 'Unknown',
                    'sf_customer_name': 'Unknown',
                    'sf_account_manager': 'Unknown',
                    'sf_payment_terms': 'Unknown',
                    'sf_tracking_status': 'No Data',
                    'sf_last_scan_location': 'Unknown',
                    'sf_last_scan_time': None,
                    'sf_estimated_delivery': None
                }
        except Exception as e:
            logger.error(f"Error getting load data for ID {internal_load_id}: {e}")
            return {'sf_load_error': str(e)}
    
    def __del__(self):
        """Clean up connection on object destruction."""
        if self._connection:
            try:
                self._connection.close()
            except:
                pass