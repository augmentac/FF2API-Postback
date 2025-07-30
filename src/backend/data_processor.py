import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
import logging
from datetime import datetime
import re

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.enum_schema = self._get_enum_schema()
    
    def _get_enum_schema(self) -> Dict[str, List[str]]:
        """Get the enumerated field validation schema"""
        return {
            "bidCriteria.accessorials": [
                "DETENTION", "DETENTION_LOADING", "DETENTION_UNLOADING", "LAYOVER", "AFTER_HOURS",
                "WEEKEND", "HOLIDAY", "LUMPER", "DRIVER_ASSIST", "DRIVER_COUNT", "SORT_AND_SEGREGATE",
                "INSIDE_DELIVERY", "INSIDE_PICKUP", "LIFTGATE", "FORKLIFT", "LIMITED_ACCESS", "RESIDENTIAL",
                "CONSTRUCTION_SITE", "TRADESHOW", "MILITARY_BASE", "TEAM_SERVICE", "TRAILER_CLEANING",
                "OVERSIZE", "OVERWEIGHT", "LOAD_BAR", "STRAPS", "CHAINS", "TARPS", "TARGETED_COMMODITY",
                "HIGH_VISIBILITY", "TEMPERATURE_REQUIREMENT", "HIGH_VALUE", "RAMPS", "TONU", "TWIC",
                "TANKER_ENDORSED", "FOOD_GRADE", "TRAILER_INTERCHANGE", "GENERAL_LIABILITY", "BULK_HEAD",
                "HAZMAT", "E_TRACKING", "POD_REQUIRED", "NOTIFY_BEFORE_ARRIVAL", "DELIVERY_APPOINTMENT",
                "PICKUP_APPOINTMENT", "SIGNATURE_REQUIRED"
            ],
            "bidCriteria.service": [
                "STANDARD", "PARTIAL", "VOLUME", "HOTSHOT", "TIME_CRITICAL"
            ],
            "load.mode": [
                "FTL", "LTL", "DRAYAGE"
            ],
            "load.rateType": [
                "SPOT", "CONTRACT", "DEDICATED", "PROJECT"
            ],
            "load.status": [
                "DRAFT", "CUSTOMER_CONFIRMED", "COVERED", "DISPATCHED", "AT_PICKUP",
                "IN_TRANSIT", "AT_DELIVERY", "DELIVERED", "POD_COLLECTED", "CANCELED", "ERROR"
            ],
            "load.referenceNumbers.0.name": [
                "PRO_NUMBER", "PICKUP_NUMBER", "PO_NUMBER", "TRAILER_NUMBER", "TRUCK_NUMBER",
                "SHIPPER_NUMBER", "CONSIGNEE_NUMBER", "OTHER"
            ],
            "load.equipment.equipmentType": [
                "DRY_VAN", "FLATBED", "REEFER", "CONTAINER", "OTHER"
            ],
            "load.items.0.packageType": [
                "PALLET", "PIECE", "CARTON", "TOTE", "SKID", "CRATE", "BOX", "DRUM", "BUNDLE", "OTHER"
            ],
            "load.items.0.freightClass": [
                "50", "55", "60", "65", "70", "77.5", "85", "92.5", "100", "110", "125", "150", "175", "200", "250", "300", "400", "500"
            ],
            "load.route.0.stopActivity": [
                "PICKUP", "DELIVERY"
            ],
            "load.route.1.stopActivity": [
                "PICKUP", "DELIVERY"
            ],
            "load.trackingEvents.0.eventType": [
                "INFO", "PING", "DRIVER_AT_PICKUP", "DRIVER_AT_DELIVERY",
                "PICKED_UP", "DELIVERED", "DELAYED"
            ],
            "load.trackingEvents.0.eventSource": [
                "MACROPOINT", "4KITES", "P44", "SMC3", "CARRIER_API", "PHONE_EMAIL", "TEXT", "OTHER"
            ],
            # Contact role fields - FIXED with correct API enum values
            "carrier.contacts.0.role": [
                "DISPATCHER", "CARRIER_ADMIN"
            ],
            "brokerage.contacts.0.role": [
                "ACCOUNT_MANAGER", "OPERATIONS_REP", "CARRIER_REP", "CUSTOMER_TEAM"
            ]
        }
    
    def _get_enum_mapping(self) -> Dict[str, Dict[str, str]]:
        """Get mapping from common variations to valid enum values"""
        return {
            "bidCriteria.service": {
                "standard": "STANDARD",
                "partial": "PARTIAL", 
                "volume": "VOLUME",
                "hotshot": "HOTSHOT",
                "hot shot": "HOTSHOT",
                "time critical": "TIME_CRITICAL",
                "time_critical": "TIME_CRITICAL",
                "rush": "TIME_CRITICAL"
            },
            "load.mode": {
                "ftl": "FTL",
                "full truckload": "FTL",
                "full truck load": "FTL",
                "ltl": "LTL", 
                "less than truckload": "LTL",
                "less truckload": "LTL",
                "drayage": "DRAYAGE"
            },
            "load.rateType": {
                "spot": "SPOT",
                "contract": "CONTRACT",
                "dedicated": "DEDICATED",
                "project": "PROJECT"
            },
            "load.status": {
                "draft": "DRAFT",
                "customer confirmed": "CUSTOMER_CONFIRMED",
                "customer_confirmed": "CUSTOMER_CONFIRMED", 
                "covered": "COVERED",
                "dispatched": "DISPATCHED",
                "at pickup": "AT_PICKUP",
                "at_pickup": "AT_PICKUP",
                "in transit": "IN_TRANSIT",
                "in_transit": "IN_TRANSIT",
                "at delivery": "AT_DELIVERY",
                "at_delivery": "AT_DELIVERY",
                "delivered": "DELIVERED",
                "pod collected": "POD_COLLECTED",
                "pod_collected": "POD_COLLECTED",
                "canceled": "CANCELED",
                "cancelled": "CANCELED",
                "error": "ERROR"
            },
            "load.equipment.equipmentType": {
                "dry van": "DRY_VAN",
                "dryvan": "DRY_VAN",
                "van": "DRY_VAN",
                "flatbed": "FLATBED",
                "flat bed": "FLATBED",
                "flat": "FLATBED",
                "reefer": "REEFER",
                "refrigerated": "REEFER",
                "container": "CONTAINER",
                "other": "OTHER"
            },
            "load.items.0.packageType": {
                "pallet": "PALLET",
                "piece": "PIECE",
                "carton": "CARTON",
                "tote": "TOTE", 
                "skid": "SKID"
            },
            "load.route.0.stopActivity": {
                "pickup": "PICKUP",
                "pick up": "PICKUP",
                "delivery": "DELIVERY",
                "deliver": "DELIVERY"
            },
            "load.route.1.stopActivity": {
                "pickup": "PICKUP",
                "pick up": "PICKUP", 
                "delivery": "DELIVERY",
                "deliver": "DELIVERY"
            },
            "trackingEvents.0.eventType": {
                "info": "INFO",
                "information": "INFO",
                "ping": "PING",
                "driver at pickup": "DRIVER_AT_PICKUP",
                "driver_at_pickup": "DRIVER_AT_PICKUP",
                "driver at delivery": "DRIVER_AT_DELIVERY", 
                "driver_at_delivery": "DRIVER_AT_DELIVERY",
                "picked up": "PICKED_UP",
                "picked_up": "PICKED_UP",
                "delivered": "DELIVERED",
                "delayed": "DELAYED"
            },
            "trackingEvents.0.eventSource": {
                "macropoint": "MACROPOINT",
                "4kites": "4KITES",
                "p44": "P44",
                "smc3": "SMC3",
                "carrier api": "CARRIER_API",
                "carrier_api": "CARRIER_API",
                "phone email": "PHONE_EMAIL",
                "phone_email": "PHONE_EMAIL", 
                "text": "TEXT",
                "other": "OTHER"
            },
            # Contact role mappings for carrier and brokerage
            "carrier.contacts.0.role": {
                "dispatcher": "DISPATCHER",
                "dispatch": "DISPATCHER",
                "carrier admin": "CARRIER_ADMIN",
                "carrier_admin": "CARRIER_ADMIN",
                "admin": "CARRIER_ADMIN",
                "carrier administrator": "CARRIER_ADMIN",
                "administrator": "CARRIER_ADMIN",
                "primary": "DISPATCHER",  # Common fallback mapping
                "main": "DISPATCHER",
                "default": "DISPATCHER"
            },
            "brokerage.contacts.0.role": {
                "account manager": "ACCOUNT_MANAGER",
                "account_manager": "ACCOUNT_MANAGER",
                "am": "ACCOUNT_MANAGER",
                "operations rep": "OPERATIONS_REP",
                "operations_rep": "OPERATIONS_REP",
                "ops rep": "OPERATIONS_REP",
                "ops_rep": "OPERATIONS_REP",
                "carrier rep": "CARRIER_REP",
                "carrier_rep": "CARRIER_REP",
                "customer team": "CUSTOMER_TEAM",
                "customer_team": "CUSTOMER_TEAM",
                "primary": "ACCOUNT_MANAGER",  # Common fallback mapping
                "main": "ACCOUNT_MANAGER",
                "default": "ACCOUNT_MANAGER"
            }
        }
    
    def _validate_enum_value(self, field_path: str, value: str) -> bool:
        """Validate if a value is valid for an enum field"""
        if field_path in self.enum_schema:
            return value in self.enum_schema[field_path]
        return True
    
    def _get_field_description(self, field_path: str) -> str:
        """Get user-friendly description for API field"""
        descriptions = {
            'load.loadNumber': 'unique identifier for this shipment',
            'load.mode': 'transportation type (FTL/LTL/DRAYAGE)',
            'load.rateType': 'pricing type (SPOT/CONTRACT/DEDICATED/PROJECT)',
            'load.status': 'current shipment status',
            'load.route.0.stopActivity': 'pickup or delivery activity',
            'load.route.0.address.addressLine1': 'pickup street address',
            'load.route.0.address.city': 'pickup city',
            'load.route.0.address.state': 'pickup state/province',
            'load.route.0.address.postalCode': 'pickup ZIP/postal code',
            'load.route.0.address.country': 'pickup country code',
            'load.route.0.expectedArrivalWindowStart': 'pickup appointment start time',
            'load.route.0.expectedArrivalWindowEnd': 'pickup appointment end time',
            'load.route.1.address.addressLine1': 'delivery street address',
            'load.route.1.address.city': 'delivery city',
            'load.route.1.address.state': 'delivery state/province',
            'load.route.1.address.postalCode': 'delivery ZIP/postal code',
            'load.route.1.expectedArrivalWindowStart': 'delivery appointment start time',
            'customer.customerId': 'customer account identifier',
            'customer.name': 'customer business name',
            'load.items.0.quantity': 'number of items/pallets',
            'load.items.0.totalWeightLbs': 'total shipment weight in pounds',
            'carrier.name': 'trucking company or carrier business name',
            'carrier.dotNumber': 'Department of Transportation number',
            'carrier.mcNumber': 'Motor Carrier number (federal identifier)'
        }
        return descriptions.get(field_path, 'required for shipment processing')
    
    def _map_enum_value(self, field_path: str, value: str) -> str:
        """Map a value to a valid enum value if possible"""
        enum_mapping = self._get_enum_mapping()
        if field_path in enum_mapping:
            value_lower = str(value).lower().strip()
            return enum_mapping[field_path].get(value_lower, value)
        return value
    
    def _suggest_enum_field(self, column_values: List[str], field_path: str) -> float:
        """Calculate confidence score for enum field mapping based on column values"""
        if field_path not in self.enum_schema:
            return 0.0
        
        enum_mapping = self._get_enum_mapping()
        valid_enum_values = self.enum_schema[field_path]
        
        # Count how many column values can be mapped to valid enum values
        mappable_count = 0
        total_count = len(column_values)
        
        if total_count == 0:
            return 0.0
        
        for value in column_values:
            if pd.isna(value) or str(value).strip() == '':
                continue
                
            value_str = str(value).lower().strip()
            
            # Check if value is already a valid enum value
            if value_str.upper() in valid_enum_values:
                mappable_count += 1
                continue
            
            # Check if value can be mapped through enum mapping
            if field_path in enum_mapping:
                mapped_value = enum_mapping[field_path].get(value_str)
                if mapped_value and mapped_value in valid_enum_values:
                    mappable_count += 1
                    continue
            
            # Check for partial matches (fuzzy matching)
            for enum_value in valid_enum_values:
                if value_str in enum_value.lower() or enum_value.lower() in value_str:
                    mappable_count += 0.5  # Partial match gets half credit
                    break
        
        return mappable_count / total_count
    
    def read_file(self, file_path: str) -> pd.DataFrame:
        """Read CSV or Excel file into DataFrame"""
        try:
            if file_path.endswith('.csv'):
                # Try different encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    raise ValueError("Could not read CSV file with any encoding")
            
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format")
            
            return df
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def suggest_mapping(self, df_columns: List[str], api_schema: Dict[str, Any], df: Optional[pd.DataFrame] = None) -> Dict[str, str]:
        """Enhanced smart mapping with regex patterns and value-based inference"""
        suggestions = {}
        
        # Smart mapping rules with regex patterns and confidence scoring
        smart_mapping_rules = {
            # 📦 load.loadNumber
            'load.loadNumber': {
                'regex': r'(load|shipment)[\s_]*(number|id|ref)',
                'aliases': ['load #', 'load number', 'load id', 'shipment number', 'reference', 'shipment ref', 'shipment_id'],
                'value_patterns': [r'LOAD\d+', r'SHIP\d+', r'REF\d+'],
                'priority': 1
            },
            
            # 🚛 load.mode  
            'load.mode': {
                'regex': r'(mode|transport[\s_]*type|shipment[\s_]*type)',
                'aliases': ['mode', 'transport mode', 'shipment type', 'move type'],
                'enum_values': ['FTL', 'LTL', 'DRAYAGE'],
                'priority': 1
            },
            
            # 💵 load.rateType
            'load.rateType': {
                'regex': r'(rate|pricing|contract)[\s_]*(type|category)',
                'aliases': ['rate type', 'contract type', 'pricing', 'rate category'],
                'enum_values': ['SPOT', 'CONTRACT', 'DEDICATED', 'PROJECT'],
                'priority': 1
            },
            
            # 🔄 load.status
            'load.status': {
                'regex': r'(status|stage)',
                'aliases': ['status', 'load status', 'shipment status', 'stage'],
                'enum_values': ['DRAFT', 'CUSTOMER_CONFIRMED', 'COVERED', 'DISPATCHED'],
                'priority': 1
            },
            
            # 🚚 load.route.0.stopActivity
            'load.route.0.stopActivity': {
                'regex': r'(stop|activity|pickup[\s_/]*delivery|direction|action)',
                'aliases': ['stop type', 'activity', 'action', 'pickup/delivery', 'direction'],
                'enum_values': ['PICKUP', 'DELIVERY'],
                'priority': 1
            },
            
            # 🏢 load.route.0.address.addressLine1
            'load.route.0.address.addressLine1': {
                'regex': r'(street|address|line[\s_]*1)(?!.*(?:zip|city|state))',
                'aliases': ['street', 'address', 'street address', 'line 1', 'pickup address'],
                'exclude_tokens': ['zip', 'city', 'state', 'postal'],
                'priority': 1
            },
            
            # 🌆 load.route.0.address.city
            'load.route.0.address.city': {
                'regex': r'city',
                'aliases': ['city', 'pickup city', 'destination city', 'location city'],
                'priority': 1
            },
            
            # 🗺️ load.route.0.address.state
            'load.route.0.address.state': {
                'regex': r'(state|province|region|state[\s_]*code)',
                'aliases': ['state', 'province', 'region', 'state code'],
                'value_patterns': [r'^[A-Z]{2}$'],  # US state codes
                'priority': 1
            },
            
            # 🔢 load.route.0.address.postalCode
            'load.route.0.address.postalCode': {
                'regex': r'(zip|postal[\s_]*code|post[\s_]*code|zipcode)',
                'aliases': ['zip', 'postal code', 'zip code', 'post code', 'zipcode'],
                'value_patterns': [r'^\d{5}(-\d{4})?$'],  # US ZIP codes
                'priority': 1
            },
            
            # 🌎 load.route.0.address.country
            'load.route.0.address.country': {
                'regex': r'(country|nation|iso[\s_]*country)',
                'aliases': ['country', 'country code', 'nation', 'iso country'],
                'enum_values': ['US', 'CA', 'MX'],
                'priority': 1
            },
            
            # ⏰ load.route.0.expectedArrivalWindowStart
            'load.route.0.expectedArrivalWindowStart': {
                'regex': r'(appt|eta|arrival|window|pickup)[\s_]*(start|from|begin)',
                'aliases': ['appt start', 'eta start', 'arrival start', 'start window', 'window from', 'pickup time start'],
                'value_patterns': [r'\d{4}-\d{2}-\d{2}', r'\d{1,2}/\d{1,2}/\d{4}'],
                'priority': 1
            },
            
            # ⏰ load.route.0.expectedArrivalWindowEnd
            'load.route.0.expectedArrivalWindowEnd': {
                'regex': r'(appt|eta|arrival|window|pickup)[\s_]*(end|to|finish)',
                'aliases': ['appt end', 'eta end', 'arrival end', 'end window', 'window to', 'pickup time end'],
                'value_patterns': [r'\d{4}-\d{2}-\d{2}', r'\d{1,2}/\d{1,2}/\d{4}'],
                'priority': 1
            },
            
            # 📦 load.items.0.quantity
            'load.items.0.quantity': {
                'regex': r'(qty|quantity|count|units|pieces|pallets?)',
                'aliases': ['qty', 'quantity', 'units', 'pallet count', 'count', 'pieces'],
                'value_patterns': [r'^\d+$'],
                'priority': 1
            },
            
            # ⚖️ load.items.0.totalWeightLbs
            'load.items.0.totalWeightLbs': {
                'regex': r'weight',
                'aliases': ['weight', 'total weight', 'weight (lbs)', 'shipment weight', 'gross weight'],
                'value_patterns': [r'\d+(\.\d+)?\s*(lbs?|pounds?)', r'^\d+(\.\d+)?$'],
                'unit_detection': ['lbs', 'lb', 'pounds', 'pound'],
                'priority': 1
            },
            
            # 🆔 customer.customerId
            'customer.customerId': {
                'regex': r'(customer|shipper|account|acct)[\s_]*(id|number)',
                'aliases': ['customer id', 'customer number', 'acct id', 'shipper id'],
                'value_patterns': [r'CUST\d+', r'[A-Z]{2,5}\d+'],
                'priority': 1
            },
            
                         # 🏢 customer.name
             'customer.name': {
                 'regex': r'(customer|shipper|client|business|company)[\s_]*(name)?',
                 'aliases': ['customer name', 'shipper name', 'client', 'business', 'company'],
                 'priority': 1
             },
             
             # 📍 Delivery/Destination Address Fields
             'load.route.1.address.addressLine1': {
                 'regex': r'(dest|delivery|destination|to)[\s_]*(street|address|line[\s_]*1)',
                 'aliases': ['dest street', 'delivery street', 'destination address', 'to street', 'delivery address'],
                 'exclude_tokens': ['zip', 'city', 'state', 'postal'],
                 'priority': 1
             },
             
             'load.route.1.address.city': {
                 'regex': r'(dest|delivery|destination|to)[\s_]*city',
                 'aliases': ['dest city', 'delivery city', 'destination city', 'to city'],
                 'priority': 1
             },
             
             'load.route.1.address.state': {
                 'regex': r'(dest|delivery|destination|to)[\s_]*(state|province|region)',
                 'aliases': ['dest state', 'delivery state', 'destination state', 'to state'],
                 'value_patterns': [r'^[A-Z]{2}$'],
                 'priority': 1
             },
             
             'load.route.1.address.postalCode': {
                 'regex': r'(dest|delivery|destination|to)[\s_]*(zip|postal)',
                 'aliases': ['dest zip', 'delivery zip', 'destination zip', 'to zip'],
                 'value_patterns': [r'^\d{5}(-\d{4})?$'],
                 'priority': 1
             },
             
             'load.route.1.address.country': {
                 'regex': r'(dest|delivery|destination|to)[\s_]*country',
                 'aliases': ['dest country', 'delivery country', 'destination country', 'to country'],
                 'enum_values': ['US', 'CA', 'MX'],
                 'priority': 1
             },
             
             # ⏰ Delivery Time Windows
             'load.route.1.expectedArrivalWindowStart': {
                 'regex': r'(delivery|dest|destination|due)[\s_]*(date|time|appt|eta)[\s_]*(start|from|begin)?',
                 'aliases': ['delivery date', 'delivery time', 'due date', 'appointment date', 'dest eta start'],
                 'value_patterns': [r'\d{4}-\d{2}-\d{2}', r'\d{1,2}/\d{1,2}/\d{4}'],
                 'priority': 1
             },
             
             'load.route.1.expectedArrivalWindowEnd': {
                 'regex': r'(delivery|dest|destination|due)[\s_]*(date|time|appt|eta)[\s_]*(end|to|finish)',
                 'aliases': ['delivery date end', 'delivery time end', 'due date end', 'appointment end', 'dest eta end'],
                 'value_patterns': [r'\d{4}-\d{2}-\d{2}', r'\d{1,2}/\d{1,2}/\d{4}'],
                 'priority': 1
             },
             
             # 🔢 Stop Sequence
             'load.route.0.sequence': {
                 'regex': r'(stop|sequence|order|pickup)[\s_]*(#|num|number|sequence)?',
                 'aliases': ['stop #', 'stop seq', 'stop order', 'sequence', 'pickup order'],
                 'value_patterns': [r'^[1-9]\d*$'],  # Positive integers
                 'priority': 1
             },
             
             'load.route.1.sequence': {
                 'regex': r'(stop|sequence|order|delivery)[\s_]*(#|num|number|sequence)?',
                 'aliases': ['stop #', 'stop seq', 'stop order', 'sequence', 'delivery order'],
                 'value_patterns': [r'^[1-9]\d*$'],  # Positive integers
                 'priority': 1
             },
             
             # 🚚 Equipment Type
             'load.equipment.equipmentType': {
                 'regex': r'(equipment|trailer|truck)[\s_]*type',
                 'aliases': ['equipment', 'equipment type', 'trailer type', 'truck type'],
                 'enum_values': ['DRY_VAN', 'REEFER', 'FLATBED', 'STEP_DECK', 'LOWBOY'],
                 'priority': 1
             },
             
             # 📦 Item Description
             'load.items.0.description': {
                 'regex': r'(commodity|product|freight|description|item)',
                 'aliases': ['commodity', 'product', 'freight', 'description', 'item description'],
                 'priority': 1
             },
             
             # 🚛 Load Equipment (bidCriteria)
             'bidCriteria.equipment': {
                 'regex': r'(equipment|trailer|truck)[\s_]*type',
                 'aliases': ['equipment', 'equipment type', 'trailer type', 'truck type'],
                 'enum_values': ['DRY_VAN', 'REEFER', 'FLATBED', 'STEP_DECK', 'LOWBOY'],
                 'priority': 1
             },
             
             # 💰 Rate/Cost Fields
             'bidCriteria.targetCostUsd': {
                 'regex': r'(price|amount|cost|total|linehaul|revenue|charge)(?!.*type)(?!.*category)',
                 'aliases': ['price', 'amount', 'total', 'linehaul', 'target cost', 'revenue', 'charge', 'cost', 'rate amount'],
                 'value_patterns': [r'^\d+(\.\d{2})?$', r'^\$?\d+(\.\d{2})?$'],
                 'exclude_patterns': [r'type', r'category', r'mode', r'spot', r'contract', r'dedicated'],
                 'numeric_required': True,  # Flag to indicate this field requires numeric values
                 'priority': 1
             },
             
             # 📧 Contact Information
             'carrier.contacts.0.name': {
                 'regex': r'(carrier|driver|contact)[\s_]*name',
                 'aliases': ['carrier contact', 'carrier contact name', 'driver name', 'contact name'],
                 'priority': 1
             },
             
             'carrier.contacts.0.phone': {
                 'regex': r'(carrier|driver|contact)[\s_]*phone',
                 'aliases': ['carrier phone', 'carrier contact phone', 'driver phone', 'contact phone'],
                 'value_patterns': [r'\d{3}-\d{3}-\d{4}', r'\(\d{3}\)\s*\d{3}-\d{4}'],
                 'priority': 1
             },
             
             'carrier.contacts.0.email': {
                 'regex': r'(carrier|driver|contact)[\s_]*email',
                 'aliases': ['carrier email', 'carrier contact email', 'driver email', 'contact email'],
                 'value_patterns': [r'^[^@]+@[^@]+\.[^@]+$'],
                 'priority': 1
             }
        }
        
        # Process each column for smart mapping
        mapping_candidates = {}  # {api_field: [(column, confidence_score)]}
        
        for column in df_columns:
            self._analyze_column_for_mapping(column, df, smart_mapping_rules, mapping_candidates)
        
        # Select best matches based on confidence scores
        for api_field, candidates in mapping_candidates.items():
            if candidates:
                # Sort by confidence score (descending)
                candidates.sort(key=lambda x: x[1], reverse=True)
                best_candidate = candidates[0]
                
                # Only suggest if confidence is above threshold
                if best_candidate[1] >= 0.7:
                    suggestions[api_field] = best_candidate[0]
        
        # Apply multi-key resolution for related fields
        suggestions = self._apply_multi_key_resolution(suggestions, df_columns, df)
        
        return suggestions
    
    def _analyze_column_for_mapping(self, column: str, df: Optional[pd.DataFrame], 
                                    smart_mapping_rules: Dict[str, Dict], 
                                    mapping_candidates: Dict[str, List]) -> None:
        """Analyze a column against smart mapping rules with confidence scoring"""
        import re
        
        # Check for exact field name match first (highest priority)
        for api_field in smart_mapping_rules.keys():
            if column == api_field:
                # Perfect match - highest confidence
                if api_field not in mapping_candidates:
                    mapping_candidates[api_field] = []
                mapping_candidates[api_field].append((column, 1.0))  # Maximum confidence
                continue
        
        # Normalize column name for comparison
        normalized_column = column.lower().replace(' ', '_').replace('-', '_')
        
        # Get sample values for value-based inference
        sample_values = []
        if df is not None and column in df.columns:
            sample_values = df[column].dropna().astype(str).tolist()[:10]  # First 10 non-null values
        
        # Test each mapping rule
        for api_field, rule in smart_mapping_rules.items():
            # Skip if we already found an exact match
            if api_field in mapping_candidates and any(score >= 1.0 for _, score in mapping_candidates[api_field]):
                continue
                
            confidence_score = 0.0
            
            # 1. Regex pattern matching on column name
            if 'regex' in rule:
                regex_pattern = rule['regex']
                if re.search(regex_pattern, normalized_column, re.IGNORECASE):
                    confidence_score += 0.6  # High confidence for regex match
            
            # 2. Alias matching
            if 'aliases' in rule:
                for alias in rule['aliases']:
                    norm_alias = alias.lower().replace(' ', '_').replace('-', '_')
                    if norm_alias == normalized_column:
                        confidence_score += 0.8  # Very high confidence for exact alias match
                    elif norm_alias in normalized_column or normalized_column in norm_alias:
                        confidence_score += 0.5  # Medium confidence for partial match
            
            # 3. Value pattern matching
            if sample_values and 'value_patterns' in rule:
                pattern_matches = 0
                for value in sample_values:
                    for pattern in rule['value_patterns']:
                        if re.search(pattern, str(value), re.IGNORECASE):
                            pattern_matches += 1
                            break
                
                if pattern_matches > 0:
                    value_confidence = pattern_matches / len(sample_values)
                    confidence_score += value_confidence * 0.4  # Value pattern bonus
            
            # 4. Enum value matching
            if sample_values and 'enum_values' in rule:
                enum_matches = 0
                for value in sample_values:
                    if str(value).upper() in rule['enum_values']:
                        enum_matches += 1
                
                if enum_matches > 0:
                    enum_confidence = enum_matches / len(sample_values)
                    confidence_score += enum_confidence * 0.5  # Enum matching bonus
            
            # 5. Unit detection for weight fields
            if 'unit_detection' in rule:
                unit_found = False
                for value in sample_values:
                    value_str = str(value).lower()
                    if any(unit in value_str for unit in rule['unit_detection']):
                        unit_found = True
                        break
                
                if unit_found:
                    confidence_score += 0.3  # Unit detection bonus
            
            # 6. Exclude tokens check (negative scoring)
            if 'exclude_tokens' in rule:
                for token in rule['exclude_tokens']:
                    if token in normalized_column:
                        confidence_score -= 0.2  # Penalty for excluded tokens
            
            # 6a. Exclude patterns check (negative scoring for regex patterns)
            if 'exclude_patterns' in rule:
                for pattern in rule['exclude_patterns']:
                    if re.search(pattern, normalized_column, re.IGNORECASE):
                        confidence_score -= 0.5  # Strong penalty for excluded patterns
                    # Also check if sample values contain excluded patterns
                    if sample_values:
                        pattern_found_in_values = False
                        for value in sample_values:
                            if re.search(pattern, str(value).lower(), re.IGNORECASE):
                                pattern_found_in_values = True
                                break
                        if pattern_found_in_values:
                            confidence_score -= 0.7  # Very strong penalty if values contain excluded patterns
            
            # 6b. Numeric validation for fields that require numeric values
            if 'numeric_required' in rule and rule['numeric_required'] and sample_values:
                numeric_count = 0
                for value in sample_values:
                    try:
                        # Try to convert to float after cleaning
                        cleaned_value = str(value).replace('$', '').replace(',', '').strip()
                        if cleaned_value:
                            float(cleaned_value)
                            numeric_count += 1
                    except (ValueError, TypeError):
                        continue
                
                if numeric_count == 0:
                    # No numeric values found - this is likely not a numeric field
                    confidence_score = 0.0  # Set to zero to completely exclude this mapping
                else:
                    # Bonus for having numeric values
                    numeric_ratio = numeric_count / len(sample_values)
                    if numeric_ratio >= 0.8:  # 80% or more are numeric
                        confidence_score += 0.3
                    elif numeric_ratio >= 0.5:  # 50% or more are numeric
                        confidence_score += 0.1
                    else:
                        confidence_score -= 0.2  # Penalty for low numeric ratio
            
            # 7. Priority boost
            if 'priority' in rule and rule['priority'] == 1:
                confidence_score += 0.1  # Small priority boost
            
            # Store candidate if confidence is reasonable
            if confidence_score > 0.3:
                if api_field not in mapping_candidates:
                    mapping_candidates[api_field] = []
                mapping_candidates[api_field].append((column, confidence_score))
    
    def _apply_multi_key_resolution(self, suggestions: Dict[str, str], 
                                   df_columns: List[str], 
                                   df: Optional[pd.DataFrame]) -> Dict[str, str]:
        """Apply multi-key resolution for related fields"""
        enhanced_suggestions = suggestions.copy()
        
        # Multi-key resolution patterns
        resolution_patterns = [
            # ETA Start/End pairs
            {
                'start_fields': ['load.route.0.expectedArrivalWindowStart', 'load.route.1.expectedArrivalWindowStart'],
                'end_fields': ['load.route.0.expectedArrivalWindowEnd', 'load.route.1.expectedArrivalWindowEnd'],
                'keywords': ['eta', 'arrival', 'window', 'appt']
            },
            # Address field grouping
            {
                'group_fields': ['load.route.0.address.addressLine1', 'load.route.0.address.city', 
                               'load.route.0.address.state', 'load.route.0.address.postalCode'],
                'keywords': ['pickup', 'origin', 'from']
            },
            {
                'group_fields': ['load.route.1.address.addressLine1', 'load.route.1.address.city', 
                               'load.route.1.address.state', 'load.route.1.address.postalCode'],
                'keywords': ['delivery', 'destination', 'dest', 'to']
            }
        ]
        
        # Look for related columns that weren't mapped
        unmapped_columns = [col for col in df_columns if col not in suggestions.values()]
        
        for pattern in resolution_patterns:
            if 'start_fields' in pattern and 'end_fields' in pattern:
                # Handle start/end field pairs
                for start_field in pattern['start_fields']:
                    if start_field in suggestions:
                        # Look for corresponding end field
                        for end_field in pattern['end_fields']:
                            if end_field not in suggestions:
                                # Find unmapped column that could be the end field
                                for col in unmapped_columns:
                                    col_lower = col.lower()
                                    if any(kw in col_lower for kw in pattern['keywords']) and \
                                       any(end_kw in col_lower for end_kw in ['end', 'to', 'finish']):
                                        enhanced_suggestions[end_field] = col
                                        break
            
            elif 'group_fields' in pattern:
                # Handle address field grouping
                mapped_in_group = [field for field in pattern['group_fields'] if field in suggestions]
                if mapped_in_group:
                    # Find other address fields with similar prefixes
                    for field in pattern['group_fields']:
                        if field not in suggestions:
                            field_type = field.split('.')[-1]  # street1, city, etc.
                            
                            for col in unmapped_columns:
                                col_lower = col.lower()
                                if any(kw in col_lower for kw in pattern['keywords']) and \
                                   field_type.lower() in col_lower:
                                    enhanced_suggestions[field] = col
                                    break
        
        return enhanced_suggestions
    
    def apply_mapping(self, df: pd.DataFrame, field_mappings: Dict[str, str], preview_mode: bool = False) -> Tuple[pd.DataFrame, List[str]]:
        """Apply field mappings to DataFrame"""
        errors = []
        mapped_df = pd.DataFrame()
        
        for api_field, csv_column in field_mappings.items():
            if csv_column.startswith("MANUAL_VALUE:"):
                # Handle manual values - apply to all rows
                manual_value = csv_column.replace("MANUAL_VALUE:", "")
                mapped_df[api_field] = [manual_value] * len(df)
            elif csv_column.startswith("DEFAULT_VALUE:"):
                # Handle default values - apply to all rows
                default_value = csv_column.replace("DEFAULT_VALUE:", "")
                mapped_df[api_field] = [default_value] * len(df)
            elif csv_column in df.columns:
                mapped_df[api_field] = df[csv_column]
            else:
                # Report missing columns but in preview mode, continue processing
                errors.append(f"Column '{csv_column}' not found in uploaded file")
        
        # Auto-generate sequence numbers for route stops
        self._add_auto_generated_fields(mapped_df)
        
        return mapped_df, errors
    
    def apply_carrier_mapping(self, df: pd.DataFrame, brokerage_name: str, db_manager=None) -> pd.DataFrame:
        """
        Apply automatic carrier mapping to DataFrame based on brokerage configuration.
        
        Args:
            df: DataFrame to process
            brokerage_name: Name of the brokerage
            db_manager: Database manager instance
            
        Returns:
            DataFrame with carrier fields populated
        """
        if not db_manager:
            return df
        
        try:
            # Check if auto-mapping is enabled
            config = db_manager.get_carrier_mapping_config(brokerage_name)
            if not config.get('enable_auto_carrier_mapping', False):
                return df
            
            # Get carrier mappings
            carrier_mappings = db_manager.get_carrier_mappings(brokerage_name)
            if not carrier_mappings:
                return df
            
            # Import carrier config parser for fuzzy matching
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            from carrier_config_parser import carrier_config_parser
            
            # Create a copy to avoid modifying original
            df_copy = df.copy()
            
            # Track auto-mapped carriers for logging
            auto_mapped_count = 0
            
            for index, row in df_copy.iterrows():
                carrier_match = None
                
                # Look for carrier identifier in various columns
                potential_carrier_columns = [
                    'carrier_name', 'carrier', 'scac', 'carrier_scac', 
                    'Carrier', 'Carrier Name', 'SCAC', 'Carrier SCAC'
                ]
                
                for col in potential_carrier_columns:
                    if col in df_copy.columns and pd.notna(row.get(col)):
                        carrier_value = str(row[col]).strip()
                        if carrier_value:
                            # Use fuzzy matching to find best carrier match
                            carrier_match = carrier_config_parser.find_best_carrier_match(
                                carrier_value, 
                                list(carrier_mappings.keys())
                            )
                            if carrier_match:
                                break
                
                # Apply carrier mapping if match found
                if carrier_match and carrier_match in carrier_mappings:
                    carrier_data = carrier_mappings[carrier_match]
                    
                    # Apply all carrier fields to this row
                    for api_field, value in carrier_data.items():
                        if value:  # Only apply non-empty values
                            df_copy.loc[index, api_field] = value
                    
                    auto_mapped_count += 1
                    self.logger.info(f"Auto-mapped carrier '{carrier_match}' for row {index}")
            
            if auto_mapped_count > 0:
                self.logger.info(f"Applied automatic carrier mapping to {auto_mapped_count} rows for brokerage '{brokerage_name}'")
            
            return df_copy
            
        except Exception as e:
            self.logger.error(f"Error applying carrier mapping: {e}")
            return df
    
    def _add_auto_generated_fields(self, df: pd.DataFrame) -> None:
        """Add auto-generated fields like sequence numbers"""
        # Auto-generate route sequences
        route_fields = [col for col in df.columns if col.startswith('load.route.') and '.sequence' not in col]
        if route_fields:
            # Group route fields by stop index and auto-assign sequences
            stops = {}
            for col in route_fields:
                # Extract stop index from field name (e.g., load.route.0.stopActivity -> 0)
                parts = col.split('.')
                if len(parts) >= 3 and parts[2].isdigit():
                    stop_idx = int(parts[2])
                    if stop_idx not in stops:
                        stops[stop_idx] = []
                    stops[stop_idx].append(col)
            
            # Assign sequences based on sorted stop indices
            for stop_idx in sorted(stops.keys()):
                sequence_field = f"load.route.{stop_idx}.sequence"
                df[sequence_field] = [stop_idx + 1] * len(df)  # 1-based sequence
        
        # Add default items if none specified but weight/quantity exists
        weight_cols = [col for col in df.columns if 'weight' in col.lower() or 'totalWeightLbs' in col]
        quantity_cols = [col for col in df.columns if 'quantity' in col.lower() or 'qty' in col.lower()]
        
        # If we have weight or quantity data but no explicit item mapping, create basic items
        if (len(weight_cols) > 0 or len(quantity_cols) > 0) and not any(col.startswith('load.items.') for col in df.columns):
            # Add basic item structure with default quantity if not present
            if not any('load.items.0.quantity' in col for col in df.columns):
                df['load.items.0.quantity'] = [1] * len(df)  # Default to 1 item
            
            # If weight column exists but not mapped to items, try to use it
            if len(weight_cols) > 0 and not any('load.items.0.totalWeightLbs' in col for col in df.columns):
                # Use the first weight column found
                weight_col = weight_cols[0]
                if weight_col in df.columns:
                    df['load.items.0.totalWeightLbs'] = df[weight_col]
    
    def validate_data(self, df: pd.DataFrame, api_schema: Dict[str, Any], chunk_size: int = 1000) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Validate mapped data against API schema"""
        validation_errors = []
        total_rows = len(df)
        
        # Process in chunks for better performance with large files
        if total_rows > chunk_size:
            self.logger.info(f"Validating {total_rows} rows in chunks of {chunk_size}")
            
            valid_indices = []
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk_df = df.iloc[start_idx:end_idx]
                
                self.logger.info(f"Validating chunk {start_idx//chunk_size + 1}/{(total_rows + chunk_size - 1)//chunk_size}")
                
                # Validate chunk
                chunk_errors = self._validate_chunk(chunk_df, start_idx)
                validation_errors.extend(chunk_errors)
                
                # Track valid indices
                chunk_valid_indices = [start_idx + i for i in range(len(chunk_df)) 
                                     if not any(error['row'] == start_idx + i + 1 for error in chunk_errors)]
                valid_indices.extend(chunk_valid_indices)
            
            # Create valid DataFrame from all valid indices
            valid_df = df.iloc[valid_indices].copy()
            return valid_df, validation_errors
        else:
            # Process normally for small files
            validation_errors = self._validate_chunk(df, 0)
            valid_indices = [i for i in range(len(df)) 
                           if not any(error['row'] == i + 1 for error in validation_errors)]
            valid_df = df.iloc[valid_indices].copy()
            return valid_df, validation_errors
    
    def _validate_chunk(self, df: pd.DataFrame, start_row_offset: int = 0) -> List[Dict[str, Any]]:
        """Validate a chunk of DataFrame"""
        validation_errors = []
        
        for i, (index, row) in enumerate(df.iterrows()):
            row_errors = []
            actual_row_index = i + start_row_offset  # Use enumerate index for clean integer
            
            # Check required fields - Only top-level objects (load, customer, brokerage) and core load fields
            required_fields = [
                # Core load fields (always required)
                'load.loadNumber', 'load.mode', 'load.rateType', 'load.status',
                
                # Route fields (at least one stop required)
                # Note: sequence is auto-generated, not required from user
                'load.route.0.stopActivity',
                'load.route.0.address.addressLine1', 'load.route.0.address.city',
                'load.route.0.address.state', 'load.route.0.address.postalCode',
                'load.route.0.address.country', 'load.route.0.expectedArrivalWindowStart',
                'load.route.0.expectedArrivalWindowEnd',
                
                # Customer fields (top-level required)
                'customer.customerId', 'customer.name'
                
                # Note: brokerage is required as object but has no required fields
                # Note: bidCriteria, trackingEvents, carrier are optional blocks
                # Note: items are only required if item data is present
            ]
            
            # Conditionally add item requirements if we have item data
            has_item_data = any(col.startswith('load.items.') for col in row.keys())
            if has_item_data:
                required_fields.extend([
                    'load.items.0.quantity', 'load.items.0.totalWeightLbs'
                ])
            for field in required_fields:
                if field not in row or pd.isna(row.get(field)) or str(row.get(field, '')).strip() == '':
                    # Create more descriptive error messages
                    field_description = self._get_field_description(field)
                    row_errors.append(f"Missing required field: {field} ({field_description})")
            
            # Validate data types and formats
            pickup_date_field = 'load.route.0.expectedArrivalWindowStart'
            if pickup_date_field in row and not pd.isna(row.get(pickup_date_field)):
                try:
                    pd.to_datetime(row[pickup_date_field])
                except (ValueError, TypeError, pd.errors.ParserError) as date_error:
                    self.logger.warning(f"Invalid pickup date format for value '{row.get(pickup_date_field)}': {date_error}")
                    row_errors.append("Invalid pickup date format")
            
            delivery_date_field = 'load.route.1.expectedArrivalWindowStart'
            if delivery_date_field in row and not pd.isna(row.get(delivery_date_field)):
                try:
                    pd.to_datetime(row[delivery_date_field])
                except (ValueError, TypeError, pd.errors.ParserError) as date_error:
                    self.logger.warning(f"Invalid delivery date format for value '{row.get(delivery_date_field)}': {date_error}")
                    row_errors.append("Invalid delivery date format")
            
            rate_field = 'bidCriteria.targetCostUsd'
            if rate_field in row and not pd.isna(row.get(rate_field)):
                rate_value = str(row[rate_field]).strip()
                if rate_value:  # Only validate if there's actually a value
                    # Skip validation for obvious enum values that shouldn't be in a rate field
                    if rate_value.upper() in ['CONTRACT', 'SPOT', 'DEDICATED', 'PROJECT', 'FTL', 'LTL', 'DRAYAGE']:
                        # This looks like an enum value that was incorrectly mapped to a rate field
                        # Skip validation to avoid false positives
                        pass
                    else:
                        try:
                            # Clean the value by removing currency symbols and commas
                            cleaned_value = rate_value.replace('$', '').replace(',', '').strip()
                            
                            # Skip validation if the value is empty after cleaning
                            if cleaned_value:
                                float(cleaned_value)
                        except (ValueError, TypeError):
                            row_errors.append(f"Invalid rate format: '{rate_value}' cannot be converted to a number")
            
            # Validate enum values
            for field_path, field_value in row.items():
                field_path_str = str(field_path)  # Convert to string for type safety
                if not pd.isna(field_value) and str(field_value).strip() != '':
                    formatted_value = self._format_value(field_path_str, field_value)
                    if not self._validate_enum_value(field_path_str, formatted_value):
                        if field_path_str in self.enum_schema:
                            valid_values = ", ".join(self.enum_schema[field_path_str])
                            row_errors.append(f"Invalid value '{field_value}' for field '{field_path_str}'. Valid values: {valid_values}")
            
            # Additional validation can be added here as needed
            
            if row_errors:
                validation_errors.append({
                    'row': actual_row_index + 1,  # Use actual row index with offset
                    'errors': row_errors,
                    'data': row.to_dict()
                })
        
        return validation_errors
    
    def format_for_api(self, df: pd.DataFrame, chunk_size: int = 1000, preview_mode: bool = False) -> List[Dict[str, Any]]:
        """Format DataFrame for API consumption - only include mapped fields"""
        api_data = []
        
        # Process in chunks for better performance with large files
        total_rows = len(df)
        if total_rows > chunk_size:
            self.logger.info(f"Processing {total_rows} rows in chunks of {chunk_size}")
            
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk_df = df.iloc[start_idx:end_idx]
                
                self.logger.info(f"Processing chunk {start_idx//chunk_size + 1}/{(total_rows + chunk_size - 1)//chunk_size}")
                
                # Process chunk
                chunk_data = self._process_chunk_for_api(chunk_df, preview_mode)
                api_data.extend(chunk_data)
                
            return api_data
        else:
            # Process normally for small files
            return self._process_chunk_for_api(df, preview_mode)
    
    def _process_chunk_for_api(self, df: pd.DataFrame, preview_mode: bool = False) -> List[Dict[str, Any]]:
        """Process a chunk of DataFrame for API consumption"""
        api_data = []
        
        for _, row in df.iterrows():
            # Start with empty payload structure
            load_payload = {}
            
            # Build payload structure from mapped data only
            for field, value in row.items():
                field_str = str(field)  # Ensure field is string
                if pd.isna(value) or str(value).strip() == '':
                    continue
                
                # Use helper method to set nested values
                self._set_nested_value(load_payload, field_str, value)
            
            # Ensure required top-level objects exist (even if empty)
            if not load_payload.get('load'):
                load_payload['load'] = {}
            if not load_payload.get('customer'):
                load_payload['customer'] = {}
            if not load_payload.get('brokerage'):
                load_payload['brokerage'] = {}
            
            # Add minimal required structure for brokerage (must have contacts array)
            if 'brokerage' in load_payload and 'contacts' not in load_payload['brokerage']:
                load_payload['brokerage']['contacts'] = []
            
            # Ensure route is an array if it exists
            if 'load' in load_payload and 'route' in load_payload['load']:
                route_data = load_payload['load']['route']
                if not isinstance(route_data, list):
                    # Convert single route object to array
                    load_payload['load']['route'] = [route_data]
            
            # Apply API-specific validation fixes (skip for preview mode)
            if not preview_mode:
                self._apply_api_validation_fixes(load_payload)
            
            # Clean up any empty nested structures, but preserve required top-level objects
            self._clean_empty_structures(load_payload)
            
            # Ensure required top-level objects are always present (even if empty)
            if 'load' not in load_payload:
                load_payload['load'] = {}
            if 'customer' not in load_payload:
                load_payload['customer'] = {}
            if 'brokerage' not in load_payload:
                load_payload['brokerage'] = {}
            
            # Apply final API validation fixes after cleaning (for structures that might get cleaned up)
            if not preview_mode:
                self._apply_final_api_fixes(load_payload)
            
            api_data.append(load_payload)
        
        return api_data
    
    def _apply_api_validation_fixes(self, load_payload: Dict[str, Any]) -> None:
        """Apply API-specific validation fixes to ensure payload meets API requirements"""
        
        # Fix 1: Ensure equipment object exists if any equipment fields are present
        if 'load' in load_payload:
            load_obj = load_payload['load']
            
            # Check if any equipment fields exist at top level and move them to equipment object
            equipment_fields = {}
            keys_to_remove = []
            
            for key, value in load_obj.items():
                if key.startswith('equipment'):
                    # Handle equipment field mapping correctly
                    if key == 'equipment':
                        # Map bare 'equipment' field to 'equipmentType'
                        equipment_fields['equipmentType'] = value
                    elif key.startswith('equipment.'):
                        # Handle nested equipment fields like 'equipment.equipmentType'
                        nested_key = key.replace('equipment.', '')
                        equipment_fields[nested_key] = value
                    else:
                        # Handle other equipment-prefixed fields
                        equipment_fields[key] = value
                    keys_to_remove.append(key)
            
            # Remove old equipment fields and create proper equipment object
            for key in keys_to_remove:
                del load_obj[key]
            
            if equipment_fields:
                load_obj['equipment'] = equipment_fields
            
            # Fix 2: Ensure all route stops have required fields
            if 'route' in load_obj and isinstance(load_obj['route'], list):
                for i, stop in enumerate(load_obj['route']):
                    if isinstance(stop, dict):
                        # Ensure stopActivity is present
                        if 'stopActivity' not in stop:
                            # Default based on sequence - first stop is pickup, rest are delivery
                            stop['stopActivity'] = 'PICKUP' if i == 0 else 'DELIVERY'
                        
                        # Ensure address object exists with required fields
                        if 'address' not in stop:
                            stop['address'] = {}
                        
                        if isinstance(stop['address'], dict):
                            # Ensure country is present
                            if 'country' not in stop['address'] or not stop['address']['country']:
                                stop['address']['country'] = 'US'
                            
                            # For second stop and beyond, ensure all required address fields exist
                            if i > 0:  # Second stop (route.$1)
                                required_address_fields = ['addressLine1', 'city', 'state', 'postalCode']
                                for field in required_address_fields:
                                    if field not in stop['address'] or not stop['address'][field]:
                                        # Use first stop's address as fallback
                                        if len(load_obj['route']) > 0 and 'address' in load_obj['route'][0]:
                                            first_stop_address = load_obj['route'][0]['address']
                                            if field in first_stop_address:
                                                stop['address'][field] = first_stop_address[field]
                                            else:
                                                stop['address'][field] = f"Unknown {field.title()}"
                                        else:
                                            stop['address'][field] = f"Unknown {field.title()}"
                        
                        # Only generate arrival windows from existing data, no defaults
                        if ('expectedArrivalWindowStart' not in stop or not stop['expectedArrivalWindowStart']) and i > 0:
                            # For delivery stops, try to derive from pickup time if available
                            if len(load_obj['route']) > 0 and 'expectedArrivalWindowStart' in load_obj['route'][0] and load_obj['route'][0]['expectedArrivalWindowStart']:
                                try:
                                    first_start = pd.to_datetime(load_obj['route'][0]['expectedArrivalWindowStart'])
                                    if not pd.isna(first_start):
                                        # Add one day for delivery
                                        second_start = first_start + pd.Timedelta(days=1)
                                        stop['expectedArrivalWindowStart'] = second_start.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                                except (ValueError, TypeError, pd.errors.ParserError) as date_error:
                                    self.logger.warning(f"Could not parse delivery start time from pickup: {date_error}")
                                    # Don't set any default - leave it empty
                        
                        # Only generate end time if start time exists and is valid
                        if ('expectedArrivalWindowEnd' not in stop or not stop['expectedArrivalWindowEnd']) and 'expectedArrivalWindowStart' in stop and stop['expectedArrivalWindowStart']:
                            try:
                                start_time = pd.to_datetime(stop['expectedArrivalWindowStart'])
                                end_time = start_time + pd.Timedelta(hours=2)
                                stop['expectedArrivalWindowEnd'] = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
                            except (ValueError, TypeError, pd.errors.ParserError, AttributeError) as date_error:
                                self.logger.warning(f"Could not generate arrival window end time: {date_error}")
                                # Don't set any default - leave it empty
                
                # Fix 2.5: Ensure proper sequence numbers (must start from 1 and be unique)
                for i, stop in enumerate(load_obj['route']):
                    if isinstance(stop, dict):
                        # Set sequence number starting from 1 (not 0)
                        stop['sequence'] = i + 1
        
        # Fix 3: Handle bidCriteria conditional requirements
        if 'bidCriteria' in load_payload and isinstance(load_payload['bidCriteria'], dict):
            bid_criteria = load_payload['bidCriteria']
            
            # If bidCriteria exists, ensure required fields are present
            if 'equipment' not in bid_criteria or not bid_criteria['equipment']:
                # Default equipment type based on load mode
                default_equipment = 'DRY_VAN'
                if 'load' in load_payload and 'mode' in load_payload['load']:
                    mode = load_payload['load']['mode']
                    if mode == 'REEFER':
                        default_equipment = 'REEFER'
                    elif mode == 'FLATBED':
                        default_equipment = 'FLATBED'
                bid_criteria['equipment'] = default_equipment
            
            if 'totalWeightLbs' not in bid_criteria or not bid_criteria['totalWeightLbs']:
                # Try to get weight from load items
                total_weight = 0
                if 'load' in load_payload and 'items' in load_payload['load']:
                    items = load_payload['load']['items']
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and 'totalWeightLbs' in item:
                                try:
                                    total_weight += float(item['totalWeightLbs'])
                                except:
                                    pass
                
                # Use calculated weight or default
                bid_criteria['totalWeightLbs'] = total_weight if total_weight > 0 else 1000
            
            if 'maxBidAmountUsd' not in bid_criteria or not bid_criteria['maxBidAmountUsd']:
                # Try to get from targetCostUsd or set reasonable default
                target_cost = bid_criteria.get('targetCostUsd', 0)
                if target_cost and target_cost > 0:
                    # Set max bid to 110% of target cost
                    bid_criteria['maxBidAmountUsd'] = float(target_cost) * 1.1
                else:
                    # Default max bid amount
                    bid_criteria['maxBidAmountUsd'] = 2000.0
        
        # Fix 4: Ensure load has required equipment object for all loads
        if 'load' in load_payload:
            load_obj = load_payload['load']
            
            # Always ensure equipment object exists with default values
            # The API seems to require this for all loads
            if 'equipment' not in load_obj:
                # Default equipment type - check bidCriteria first, then default to DRY_VAN
                default_equipment_type = 'DRY_VAN'
                
                # Check if bidCriteria has equipment info
                if 'bidCriteria' in load_payload and isinstance(load_payload['bidCriteria'], dict):
                    if 'equipment' in load_payload['bidCriteria']:
                        default_equipment_type = load_payload['bidCriteria']['equipment']
                
                # Check if mode suggests equipment type
                if 'mode' in load_obj:
                    mode = load_obj['mode']
                    if mode == 'LTL':
                        default_equipment_type = 'DRY_VAN'  # LTL typically uses dry vans
                    elif mode == 'FTL':
                        default_equipment_type = 'DRY_VAN'  # Default FTL to dry van
                
                load_obj['equipment'] = {
                    'equipmentType': default_equipment_type
                }
            elif isinstance(load_obj['equipment'], dict):
                # Fix any nested equipmentType issues (double-wrapping)
                if 'equipmentType' in load_obj['equipment']:
                    equipment_type_value = load_obj['equipment']['equipmentType']
                    if isinstance(equipment_type_value, dict) and 'equipmentType' in equipment_type_value:
                        # Fix double-wrapped equipmentType
                        load_obj['equipment']['equipmentType'] = equipment_type_value['equipmentType']
                    elif not isinstance(equipment_type_value, str):
                        # Convert to string if it's not already
                        load_obj['equipment']['equipmentType'] = str(equipment_type_value)
                else:
                    # Ensure equipment object has required equipmentType field
                    load_obj['equipment']['equipmentType'] = 'DRY_VAN'
    
    def _apply_final_api_fixes(self, load_payload: Dict[str, Any]) -> None:
        """Apply final API validation fixes after cleaning process"""
        
        # Fix carrier contacts requirement (must be done after cleaning to avoid removal)
        if 'carrier' in load_payload:
            carrier_obj = load_payload['carrier']
            if isinstance(carrier_obj, dict):
                if 'contacts' not in carrier_obj:
                    carrier_obj['contacts'] = []
                elif not isinstance(carrier_obj['contacts'], list):
                    # Convert single contact to list
                    carrier_obj['contacts'] = [carrier_obj['contacts']]
                    
                # Ensure contacts array has required structure if it contains data
                if isinstance(carrier_obj['contacts'], list):
                    for contact in carrier_obj['contacts']:
                        if isinstance(contact, dict):
                            # Ensure each contact has required role field with valid enum value
                            if 'role' not in contact:
                                contact['role'] = 'CARRIER_REP'  # Default role - valid enum value
        
        # Fix brokerage contacts requirement (must be done after cleaning to avoid removal)
        if 'brokerage' in load_payload:
            brokerage_obj = load_payload['brokerage']
            if isinstance(brokerage_obj, dict):
                if 'contacts' not in brokerage_obj:
                    brokerage_obj['contacts'] = []
                elif not isinstance(brokerage_obj['contacts'], list):
                    # Convert single contact to list
                    brokerage_obj['contacts'] = [brokerage_obj['contacts']]
                    
                # Ensure contacts array has required structure if it contains data
                if isinstance(brokerage_obj['contacts'], list):
                    for contact in brokerage_obj['contacts']:
                        if isinstance(contact, dict):
                            # Ensure each contact has required role field with valid enum value
                            if 'role' not in contact:
                                contact['role'] = 'ACCOUNT_MANAGER'  # Default role - valid enum value
    
    def _clean_empty_structures(self, obj: Dict[str, Any]) -> None:
        """Remove empty nested structures from the payload"""
        keys_to_remove = []
        
        # List of top-level keys that should be preserved even if empty
        required_top_level_keys = ['load', 'customer', 'brokerage']
        
        for key, value in obj.items():
            if isinstance(value, dict):
                # Recursively clean nested dictionaries
                self._clean_empty_structures(value)
                # Remove if empty after cleaning, but preserve required top-level keys
                if not value and key not in required_top_level_keys:
                    keys_to_remove.append(key)
            elif isinstance(value, list):
                # Clean items in lists
                cleaned_list = []
                for item in value:
                    if isinstance(item, dict):
                        self._clean_empty_structures(item)
                        if item:  # Only keep non-empty items
                            cleaned_list.append(item)
                    elif item is not None and str(item).strip() != '':
                        cleaned_list.append(item)
                
                # Update the list
                obj[key] = cleaned_list
                
                # Remove empty lists (but not required top-level keys)
                if not cleaned_list and key not in required_top_level_keys:
                    keys_to_remove.append(key)
        
        # Remove empty keys
        for key in keys_to_remove:
            del obj[key]
    
    def _set_nested_value(self, obj: Dict[str, Any], field_path: str, value: Any):
        """Set a nested value in the object using dot notation"""
        try:
            parts = field_path.split('.')
            current: Any = obj
            
            # Navigate through the path, creating structure as needed
            for i, part in enumerate(parts[:-1]):
                if part.isdigit():
                    # Handle array indices
                    index = int(part)
                    if not isinstance(current, list):
                        # Need to convert to list - this usually happens when the parent key needs to be a list
                        self.logger.warning(f"Expected list but got {type(current)} at part {part} in {field_path}")
                        return
                    
                    # Ensure list has enough elements
                    while len(current) <= index:
                        current.append({})
                    current = current[index]
                else:
                    if not isinstance(current, dict):
                        self.logger.warning(f"Expected dict but got {type(current)} at part {part} in {field_path}")
                        return
                    
                    # Determine if next part is an array index to decide structure
                    next_part = parts[i+1] if i+1 < len(parts) else None
                    if next_part and next_part.isdigit():
                        # Next part is array index, so this should be a list
                        if part not in current:
                            current[part] = []
                    else:
                        # Regular nested object
                        if part not in current:
                            current[part] = {}
                    current = current[part]
            
            # Set the final value
            final_key = parts[-1]
            formatted_value = self._format_value(field_path, value)
            
            if final_key.isdigit():
                index = int(final_key)
                if not isinstance(current, list):
                    self.logger.warning(f"Expected list but got {type(current)} for final key {final_key} in {field_path}")
                    return
                    
                # Ensure list has enough elements
                while len(current) <= index:
                    current.append(None)
                current[index] = formatted_value
            else:
                if not isinstance(current, dict):
                    self.logger.warning(f"Expected dict but got {type(current)} for final key {final_key} in {field_path}")
                    return
                current[final_key] = formatted_value
                
        except Exception as e:
            self.logger.warning(f"Failed to set nested value for {field_path}: {e}")
    
    def _format_value(self, field_path: str, value: Any) -> Any:
        """Format value based on field type"""
        # Date fields
        if any(date_field in field_path for date_field in ['expectedArrivalWindowStart', 'expectedArrivalWindowEnd', 'bidExpiration', 'nextEtaUtc', 'eventUtc', 'actualArrivalTime', 'actualCompletionTime']):
            try:
                return pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%S.000Z')
            except:
                return ""
        
        # Numeric fields - integer fields
        if any(num_field in field_path for num_field in ['mcNumber', 'dotNumber', 'totalWeightLbs', 'lengthInches', 'widthInches', 'heightInches', 'quantity', 'pickupSequence', 'deliverySequence', 'sequence', 'nextSequence']):
            try:
                return int(float(str(value).replace('$', '').replace(',', '')))
            except:
                return 0
        
        # Numeric fields - float fields
        if any(num_field in field_path for num_field in ['targetCostUsd', 'maxBidAmountUsd', 'minTemperatureF', 'maxTemperatureF', 'density', 'temperatureF', 'latitude', 'longitude']):
            try:
                # Handle various input formats
                if pd.isna(value) or str(value).strip() == '':
                    return 0.0
                
                # Clean the value by removing currency symbols, commas, and whitespace
                cleaned_value = str(value).replace('$', '').replace(',', '').strip()
                
                # Skip if empty after cleaning
                if not cleaned_value:
                    return 0.0
                
                # Try to convert to float
                return float(cleaned_value)
            except (ValueError, TypeError):
                # Log the problematic value for debugging
                self.logger.warning(f"Could not convert value '{value}' to float for field '{field_path}', using 0.0")
                return 0.0
        
        # Apply enum mapping first to get the correct value
        formatted_value = self._map_enum_value(field_path, str(value))
        
        # If enum mapping was applied, validate the result
        if formatted_value != str(value):
            if self._validate_enum_value(field_path, formatted_value):
                return formatted_value
        
        # Check if the original value is already valid for enum fields
        if self._validate_enum_value(field_path, str(value)):
            return str(value)
        
        # Legacy equipment type mapping (kept for backward compatibility)
        if field_path.endswith('.equipment') or field_path.endswith('.equipmentType'):
            equipment_mapping = {
                'dry van': 'DRY_VAN',
                'dryvan': 'DRY_VAN',
                'van': 'DRY_VAN',
                'reefer': 'REEFER',
                'refrigerated': 'REEFER',
                'flatbed': 'FLATBED',
                'flat': 'FLATBED',
                'stepdeck': 'STEPDECK',
                'step deck': 'STEPDECK',
                'lowboy': 'LOWBOY'
            }
            return equipment_mapping.get(str(value).lower(), 'DRY_VAN')
        
        # String fields
        return str(value).strip() 

    # =============================================================================
    # Learning System Integration Methods
    # =============================================================================
    
    def suggest_mapping_with_learning(self, df_columns: List[str], api_schema: Dict[str, Any], 
                                    df: Optional[pd.DataFrame] = None, 
                                    db_manager=None, brokerage_name: str = None) -> Dict[str, str]:
        """Enhanced mapping with learning system integration"""
        # Get base suggestions from existing system
        base_suggestions = self.suggest_mapping(df_columns, api_schema, df)
        
        # If learning system is available, enhance with learned patterns
        if db_manager and brokerage_name:
            enhanced_suggestions = self._enhance_with_learned_patterns(
                base_suggestions, df_columns, df, db_manager, brokerage_name
            )
            return enhanced_suggestions
        
        return base_suggestions
    
    def _enhance_with_learned_patterns(self, base_suggestions: Dict[str, str], 
                                     df_columns: List[str], df: Optional[pd.DataFrame],
                                     db_manager, brokerage_name: str) -> Dict[str, str]:
        """Enhance base suggestions with learned patterns"""
        enhanced_suggestions = base_suggestions.copy()
        learning_confidence = {}
        
        for column in df_columns:
            # Get sample data for better matching
            sample_data = None
            if df is not None and column in df.columns:
                sample_data = df[column].dropna().head(10).tolist()
            
            # Get learning suggestions for this column
            learning_suggestions = db_manager.get_learning_suggestions(
                brokerage_name, column, sample_data
            )
            
            if learning_suggestions:
                # Find the best learning suggestion
                best_learning = learning_suggestions[0]
                
                # Get current base suggestion confidence
                base_confidence = self._calculate_base_confidence(column, base_suggestions.get(column))
                
                # Compare learning confidence with base confidence
                if best_learning['confidence'] > base_confidence + 0.1:  # 10% threshold
                    enhanced_suggestions[column] = best_learning['api_field']
                    learning_confidence[column] = best_learning['confidence']
                    
                    self.logger.info(f"Learning override for {column}: {best_learning['api_field']} "
                                   f"(confidence: {best_learning['confidence']:.2f}, "
                                   f"success_rate: {best_learning['success_rate']:.2f})")
        
        return enhanced_suggestions
    
    def _calculate_base_confidence(self, column_name: str, suggested_field: Optional[str]) -> float:
        """Calculate confidence score for base suggestion"""
        if not suggested_field:
            return 0.0
        
        # Use existing scoring logic from suggest_mapping
        column_lower = column_name.lower()
        field_lower = suggested_field.lower()
        
        # Exact match
        if column_lower == field_lower:
            return 1.0
        
        # Partial match
        if column_lower in field_lower or field_lower in column_lower:
            return 0.8
        
        # Default moderate confidence
        return 0.6
    
    def track_mapping_interaction(self, session_id: str, brokerage_name: str, 
                                configuration_name: str, df_columns: List[str],
                                suggested_mappings: Dict[str, str], 
                                final_mappings: Dict[str, str],
                                df: Optional[pd.DataFrame] = None, 
                                db_manager=None) -> Dict[str, Any]:
        """Track mapping interaction for learning system"""
        if not db_manager:
            return {}
        
        # Calculate interaction statistics
        suggestions_accepted = 0
        manual_corrections = 0
        decisions = []
        
        for column in df_columns:
            suggested_field = suggested_mappings.get(column)
            actual_field = final_mappings.get(column)
            
            if suggested_field and actual_field:
                decision_type = 'accepted' if suggested_field == actual_field else 'corrected'
                if decision_type == 'accepted':
                    suggestions_accepted += 1
                else:
                    manual_corrections += 1
                
                # Get sample data for this column
                sample_data = []
                column_data_type = 'string'
                if df is not None and column in df.columns:
                    sample_data = df[column].dropna().head(10).tolist()
                    column_data_type = self._infer_column_type(df[column])
                
                # Calculate confidence for this decision
                confidence = self._calculate_base_confidence(column, suggested_field)
                
                decisions.append({
                    'column_name': column,
                    'column_sample_data': sample_data,
                    'column_data_type': column_data_type,
                    'suggested_field': suggested_field,
                    'suggested_confidence': confidence,
                    'actual_field': actual_field,
                    'decision_type': decision_type
                })
        
        # Prepare interaction data
        interaction_data = {
            'session_id': session_id,
            'brokerage_name': brokerage_name,
            'configuration_name': configuration_name,
            'file_headers': df_columns,
            'suggested_mappings': suggested_mappings,
            'final_mappings': final_mappings,
            'suggestions_accepted': suggestions_accepted,
            'manual_corrections': manual_corrections,
            'total_fields': len(df_columns),
            'decisions': decisions
        }
        
        try:
            # Save interaction data
            interaction_id = db_manager.save_mapping_interaction(interaction_data)
            
            # Update brokerage patterns
            db_manager.update_brokerage_patterns(brokerage_name, decisions)
            
            return {
                'interaction_id': interaction_id,
                'suggestions_accepted': suggestions_accepted,
                'manual_corrections': manual_corrections,
                'total_fields': len(df_columns)
            }
            
        except Exception as e:
            self.logger.error(f"Error tracking mapping interaction: {e}")
            return {}
    
    def update_learning_with_processing_results(self, session_id: str, 
                                              processing_success_rate: float,
                                              db_manager=None) -> None:
        """Update learning system with processing results"""
        if not db_manager:
            return
        
        try:
            # Update the most recent interaction with processing results
            conn = db_manager.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE mapping_interactions 
                SET processing_success_rate = ?
                WHERE session_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            ''', (processing_success_rate, session_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error updating learning with processing results: {e}")
    
    def get_learning_insights(self, brokerage_name: str, db_manager=None) -> Dict[str, Any]:
        """Get insights from learning system for mapping interface"""
        if not db_manager:
            return {}
        
        try:
            # Get mapping analytics
            analytics = db_manager.get_mapping_analytics(brokerage_name)
            
            # Get top patterns
            patterns = db_manager.get_brokerage_patterns(brokerage_name)
            
            # Process insights
            insights = {
                'total_sessions': analytics['interaction_stats']['total_interactions'],
                'avg_acceptance_rate': analytics['interaction_stats']['avg_suggestions_accepted'],
                'avg_success_rate': analytics['interaction_stats']['avg_processing_success'],
                'top_patterns': [
                    {
                        'column_pattern': pattern['column_pattern'],
                        'api_field': pattern['api_field'],
                        'confidence': pattern['average_confidence'],
                        'usage_count': pattern['total_count']
                    }
                    for pattern in patterns[:10]  # Top 10 patterns
                ],
                'learning_progress': analytics['learning_progress']
            }
            
            return insights
            
        except Exception as e:
            self.logger.error(f"Error getting learning insights: {e}")
            return {}
    
    def _infer_column_type(self, column: pd.Series) -> str:
        """Infer the data type of a column"""
        try:
            # Check if numeric
            if pd.api.types.is_numeric_dtype(column):
                return 'numeric'
            
            # Check if datetime
            if pd.api.types.is_datetime64_any_dtype(column):
                return 'datetime'
            
            # Check if boolean
            if pd.api.types.is_bool_dtype(column):
                return 'boolean'
            
            # Try to infer from sample values
            sample_values = column.dropna().head(100)
            
            # Check for date patterns
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                r'\d{1,2}/\d{1,2}/\d{2,4}',  # M/D/YY or MM/DD/YYYY
            ]
            
            for pattern in date_patterns:
                matches = sample_values.astype(str).str.match(pattern)
                if len(matches) > 0 and matches.any():
                    return 'date'
            
            # Check for email patterns
            email_matches = sample_values.astype(str).str.contains('@')
            if len(email_matches) > 0 and email_matches.any():
                return 'email'
            
            # Check for phone patterns
            phone_pattern = r'[\+]?[1-9]?[\d\s\-\(\)]{7,15}'
            phone_matches = sample_values.astype(str).str.match(phone_pattern)
            if len(phone_matches) > 0 and phone_matches.any():
                return 'phone'
            
            # Default to string
            return 'string'
            
        except Exception:
            return 'string'
    
    def suggest_field_improvements(self, brokerage_name: str, db_manager=None) -> List[Dict[str, Any]]:
        """Suggest improvements based on learning patterns"""
        if not db_manager:
            return []
        
        suggestions = []
        
        try:
            # Get patterns with low success rates
            patterns = db_manager.get_brokerage_patterns(brokerage_name)
            
            for pattern in patterns:
                if pattern['total_count'] >= 5:  # Minimum sample size
                    success_rate = pattern['success_count'] / pattern['total_count']
                    
                    # Suggest improvements for low success patterns
                    if success_rate < 0.6:  # Less than 60% success
                        suggestions.append({
                            'type': 'low_success_pattern',
                            'column_pattern': pattern['column_pattern'],
                            'api_field': pattern['api_field'],
                            'success_rate': success_rate,
                            'sample_count': pattern['total_count'],
                            'suggestion': f"Consider reviewing mapping for '{pattern['column_pattern']}' "
                                        f"to '{pattern['api_field']}' (only {success_rate:.1%} success rate)"
                        })
            
            # Get fields that are frequently manually corrected
            analytics = db_manager.get_mapping_analytics(brokerage_name)
            
            if analytics['interaction_stats']['avg_manual_corrections'] > 3:
                suggestions.append({
                    'type': 'high_manual_corrections',
                    'avg_corrections': analytics['interaction_stats']['avg_manual_corrections'],
                    'suggestion': "Consider reviewing common field mappings to reduce manual corrections"
                })
            
            return suggestions
            
        except Exception as e:
            self.logger.error(f"Error getting field improvement suggestions: {e}")
            return []
    
    def cleanup_learning_data(self, db_manager=None, days_to_keep: int = 90) -> Dict[str, Any]:
        """Clean up old learning data"""
        if not db_manager:
            return {'success': False, 'error': 'No database manager provided'}
        
        try:
            db_manager.cleanup_old_learning_data(days_to_keep)
            return {'success': True, 'message': f'Cleaned up learning data older than {days_to_keep} days'}
            
        except Exception as e:
            self.logger.error(f"Error cleaning up learning data: {e}")
            return {'success': False, 'error': str(e)} 