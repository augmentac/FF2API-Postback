"""
Carrier Configuration Parser
---------------------------
Parses carrier_config.py and provides utilities for carrier mapping functionality.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# Import the carrier configuration
CARRIER_DETAILS = {
    "Dayton Freight": {
        "mcNumber": 114457,
        "dotNumber": 214457,
        "scac": "DYFT",
        "email": "customer.service@daytonfreight.com",
        "phone": "+18008605102",
        "dispatcher": {
            "externalId": "ext-dyft-1",
            "email": "customer.service@daytonfreight.com",
            "phone": "+18008605102",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "YRC Freight": {
        "mcNumber": 123698,
        "dotNumber": 223698,
        "scac": "YRCW",
        "email": "customersupport@yrcfreight.com",
        "phone": "+18006108898",
        "dispatcher": {
            "externalId": "ext-yrcw-1",
            "email": "customersupport@yrcfreight.com",
            "phone": "+18006108898",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "XPO Logistics": {
        "mcNumber": 142319,
        "dotNumber": 242319,
        "scac": "XPOL",
        "email": "customercare@xpo.com",
        "phone": "+18007552728",
        "dispatcher": {
            "externalId": "ext-xpol-1",
            "email": "customercare@xpo.com",
            "phone": "+18007552728",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Old Dominion": {
        "mcNumber": 107098,
        "dotNumber": 207098,
        "scac": "ODFL",
        "email": "customer.service@odfl.com",
        "phone": "+18002355569",
        "dispatcher": {
            "externalId": "ext-odfl-1",
            "email": "customer.service@odfl.com",
            "phone": "+18002355569",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "ABF Freight": {
        "mcNumber": 100946,
        "dotNumber": 200946,
        "scac": "ABFS",
        "email": "customersolutions@arcb.com",
        "phone": "+18006105544",
        "dispatcher": {
            "externalId": "ext-abfs-1",
            "email": "customersolutions@arcb.com",
            "phone": "+18006105544",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "FedEx Freight": {
        "mcNumber": 121685,
        "dotNumber": 221685,
        "scac": "FXFE",
        "email": "freightcustomerservice@fedex.com",
        "phone": "+18002749099",
        "dispatcher": {
            "externalId": "ext-fxfe-1",
            "email": "freightcustomerservice@fedex.com",
            "phone": "+18002749099",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "UPS Freight": {
        "mcNumber": 109708,
        "dotNumber": 209708,
        "scac": "UPGF",
        "email": "ltlspecialservices@ups.com",
        "phone": "+18003334784",
        "dispatcher": {
            "externalId": "ext-upgf-1",
            "email": "ltlspecialservices@ups.com",
            "phone": "+18003334784",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Estes Express": {
        "mcNumber": 105764,
        "dotNumber": 205764,
        "scac": "EXLA",
        "email": "customercare@estes-express.com",
        "phone": "+18663783748",
        "dispatcher": {
            "externalId": "ext-exla-1",
            "email": "customercare@estes-express.com",
            "phone": "+18663783748",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "R&L Carriers": {
        "mcNumber": 108341,
        "dotNumber": 208341,
        "scac": "RLCA",
        "email": "customerservice@rlcarriers.com",
        "phone": "+18005435589",
        "dispatcher": {
            "externalId": "ext-rlca-1",
            "email": "customerservice@rlcarriers.com",
            "phone": "+18005435589",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "SAIA": {
        "mcNumber": 123897,
        "dotNumber": 223897,
        "scac": "SAIA",
        "email": "customerservice@saia.com",
        "phone": "+18007657242",
        "dispatcher": {
            "externalId": "ext-saia-1",
            "email": "customerservice@saia.com",
            "phone": "+18007657242",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "AAA Cooper / Midwest Motor": {
        "mcNumber": 100843,
        "dotNumber": 200843,
        "scac": "ACAA",
        "phone": "+18006337571",
        "dispatcher": {
            "externalId": "ext-acaa-1",
            "phone": "+18006337571",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "A. Duie Pyle": {
        "mcNumber": 100025,
        "dotNumber": 200025,
        "scac": "ADPI",
        "phone": "+18008438479",
        "dispatcher": {
            "externalId": "ext-adpi-1",
            "phone": "+18008438479",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "ArcBest": {
        "mcNumber": 100883,
        "dotNumber": 200883,
        "scac": "ARCB",
        "phone": "+18792326111",
        "dispatcher": {
            "externalId": "ext-arcb-1",
            "phone": "+18792326111",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Central Transport": {
        "mcNumber": 102032,
        "dotNumber": 202032,
        "scac": "CTII",
        "phone": "+18663787737",
        "dispatcher": {
            "externalId": "ext-ctii-1",
            "phone": "+18663787737",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "First Choice Delivery LLC": {
        "mcNumber": 102258,
        "dotNumber": 202258,
        "scac": "FCDL",
        "dispatcher": {
            "externalId": "ext-fcdl-1",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "FLAGSHIP is a nopro carrier": {
        "mcNumber": 102337,
        "dotNumber": 202337,
        "scac": "FIAN",
        "phone": "+17023461962",
        "dispatcher": {
            "externalId": "ext-fian-1",
            "phone": "+17023461962",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Forward Air": {
        "mcNumber": 102369,
        "dotNumber": 202369,
        "scac": "FARD",
        "phone": "+18886652795",
        "dispatcher": {
            "externalId": "ext-fard-1",
            "phone": "+18886652795",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Holland": {
        "mcNumber": 103004,
        "dotNumber": 203004,
        "scac": "HMES",
        "phone": "+18004567885",
        "dispatcher": {
            "externalId": "ext-hmes-1",
            "phone": "+18004567885",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Pilot Freight Services": {
        "mcNumber": 107873,
        "dotNumber": 207873,
        "scac": "PLFS",
        "dispatcher": {
            "externalId": "ext-plfs-1",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "FedEx Freight Economy": {
        "mcNumber": 121805,
        "dotNumber": 239039,
        "scac": "FXEC",
        "email": "freightcustomerservice@fedex.com",
        "phone": "+18002749099",
        "dispatcher": {
            "externalId": "ext-fxec-1",
            "email": "freightcustomerservice@fedex.com",
            "phone": "+18002749099",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "FedEx Freight Priority": {
        "mcNumber": 12180501,
        "dotNumber": 23903901,
        "scac": "FXPR",
        "email": "freightcustomerservice@fedex.com",
        "phone": "+18002749099",
        "dispatcher": {
            "externalId": "ext-fxpr-1",
            "email": "freightcustomerservice@fedex.com",
            "phone": "+18002749099",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Peninsula Truck Lines Inc": {
        "mcNumber": 113165,
        "dotNumber": 8329,
        "scac": "PENS",
        "email": "custserv@peninsulatruck.com",
        "phone": "+12539292000",
        "dispatcher": {
            "externalId": "ext-pen-1",
            "email": "custserv@peninsulatruck.com",
            "phone": "+12539292000",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "TForce Freight": {
        "mcNumber": 109533,
        "dotNumber": 121058,
        "scac": "TFIN",
        "phone": "+18003337400",
        "dispatcher": {
            "externalId": "ext-tfin-1",
            "phone": "+18003337400",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Southeastern": {
        "mcNumber": 111871,
        "dotNumber": 63419,
        "scac": "SEFL",
        "phone": "+18006377335",
        "dispatcher": {
            "externalId": "ext-sout-1",
            "phone": "+18006377335",
            "email": "customer.service@sefl.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "CrossCountry Freight": {
        "mcNumber": 209657,
        "dotNumber": 313378,
        "scac": "CCYF",
        "phone": "+18005210287",
        "dispatcher": {
            "externalId": "ext-CCYF-1",
            "phone": "+18005210287",
            "email": "cs@shipcc.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Diamond Line Delivery Systems": {
        "mcNumber": 386888,
        "dotNumber": 887550,
        "scac": "DLDS",
        "phone": "+12088887133",
        "dispatcher": {
            "externalId": "ext-DLDS-1",
            "phone": "+12088887133",
            "email": "cs@dlds.company",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Oak Harbor Freight Lines": {
        "mcNumber": 139763,
        "dotNumber": 8314,
        "scac": "OAKH",
        "phone": "+18008588815",
        "dispatcher": {
            "externalId": "ext-OAKH-1",
            "phone": "+18008588815",
            "email": "OHFL@oakh.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Ward Trucking": {
        "mcNumber": 84850,
        "dotNumber": 65916,
        "scac": "WARD",
        "phone": "+18004583625",
        "dispatcher": {
            "externalId": "ext-WARD-1",
            "phone": "+18004583625",
            "email": "cservice@wardtlc.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "Magnum LTL": {
        "mcNumber": 233791,
        "dotNumber": 404961,
        "scac": "MGNM",
        "phone": "+18007268952",
        "dispatcher": {
            "externalId": "ext-MGNM-1",
            "phone": "+18007268952",
            "email": "pickups@magnumlog.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "GLS US Freight": {
        "mcNumber": 147640,
        "dotNumber": 172279,
        "scac": "GLSF",
        "phone": "+18003225555",
        "dispatcher": {
            "externalId": "ext-GLSF-1",
            "phone": "+18003225555",
            "email": "ltlcs@glsus.com",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    },
    "DEFAULT": {
        "mcNumber": 999999,
        "dotNumber": 888888,
        "scac": "UNKN",
        "dispatcher": {
            "externalId": "ext-unknown-1",
            "roles": ["DISPATCHER"],
            "preferredContactMode": "EMAIL"
        }
    }
}

class CarrierConfigParser:
    """Parser and utilities for carrier configuration management."""
    
    def __init__(self):
        self.carrier_details = CARRIER_DETAILS
    
    def convert_to_api_schema_format(self, carrier_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert carrier_config.py format to FF2API schema format.
        
        Args:
            carrier_data: Carrier data from CARRIER_DETAILS
            
        Returns:
            Dictionary with FF2API schema field names
        """
        api_format = {}
        
        # Map basic fields
        api_format['carrier_mc_number'] = str(carrier_data.get('mcNumber', ''))
        api_format['carrier_dot_number'] = str(carrier_data.get('dotNumber', ''))
        api_format['carrier_scac'] = carrier_data.get('scac', '')
        api_format['carrier_email'] = carrier_data.get('email', '')
        api_format['carrier_phone'] = carrier_data.get('phone', '')
        
        # Map dispatcher info
        dispatcher = carrier_data.get('dispatcher', {})
        api_format['carrier_contact_email'] = dispatcher.get('email', carrier_data.get('email', ''))
        api_format['carrier_contact_phone'] = dispatcher.get('phone', carrier_data.get('phone', ''))
        api_format['carrier_contact_name'] = 'Customer Service'  # Default contact name
        
        return api_format
    
    def get_brokerage_template(self, include_carriers: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Generate a template carrier mapping for a new brokerage.
        
        Args:
            include_carriers: List of carrier names to include, or None for all
            
        Returns:
            Dictionary with carrier mappings in API schema format
        """
        template = {}
        
        for carrier_name, carrier_data in self.carrier_details.items():
            # Skip DEFAULT entry for templates
            if carrier_name == "DEFAULT":
                continue
                
            # Filter by included carriers if specified
            if include_carriers and carrier_name not in include_carriers:
                continue
            
            # Convert to API schema format and add carrier name
            api_format = self.convert_to_api_schema_format(carrier_data)
            api_format['carrier_name'] = carrier_name
            
            template[carrier_name] = api_format
        
        return template
    
    def find_best_carrier_match(self, input_value: str, carrier_names: List[str], 
                               threshold: float = 0.6) -> Optional[str]:
        """
        Find the best matching carrier name using fuzzy string matching.
        
        Args:
            input_value: The input carrier name/identifier
            carrier_names: List of available carrier names to match against
            threshold: Minimum similarity score (0.0 to 1.0)
            
        Returns:
            Best matching carrier name or None if no match above threshold
        """
        if not input_value or not carrier_names:
            return None
        
        input_clean = input_value.strip().upper()
        best_match = None
        best_score = 0.0
        
        for carrier_name in carrier_names:
            carrier_clean = carrier_name.strip().upper()
            
            # Direct exact matches
            if input_clean == carrier_clean:
                return carrier_name
            
            # Check if input matches SCAC code
            carrier_data = self.carrier_details.get(carrier_name, {})
            scac = carrier_data.get('scac', '').upper()
            if scac and input_clean == scac:
                return carrier_name
            
            # Fuzzy string matching
            similarity = SequenceMatcher(None, input_clean, carrier_clean).ratio()
            
            # Also check against common abbreviations/variations
            variations = self._get_carrier_variations(carrier_name)
            for variation in variations:
                var_similarity = SequenceMatcher(None, input_clean, variation.upper()).ratio()
                similarity = max(similarity, var_similarity)
            
            if similarity > best_score:
                best_score = similarity
                best_match = carrier_name
        
        return best_match if best_score >= threshold else None
    
    def _get_carrier_variations(self, carrier_name: str) -> List[str]:
        """Get common variations/abbreviations for a carrier name."""
        variations = [carrier_name]
        
        # Add common abbreviations
        abbreviations = {
            "FedEx Freight": ["FEDEX", "FXF", "FXFE"],
            "UPS Freight": ["UPS", "UPGF"],
            "Old Dominion": ["ODFL", "OD"],
            "YRC Freight": ["YRC", "YRCW"],
            "ABF Freight": ["ABF", "ABFS"],
            "XPO Logistics": ["XPO", "XPOL"],
            "Estes Express": ["ESTES", "EXLA"],
            "R&L Carriers": ["R&L", "RL", "RLCA"],
            "Dayton Freight": ["DAYTON", "DYFT"],
            "TForce Freight": ["TFORCE", "TFIN"],
            "Southeastern": ["SEFL", "SE"]
        }
        
        if carrier_name in abbreviations:
            variations.extend(abbreviations[carrier_name])
        
        return variations
    
    def get_carrier_count(self) -> int:
        """Get total number of available carriers (excluding DEFAULT)."""
        return len([k for k in self.carrier_details.keys() if k != "DEFAULT"])
    
    def get_carrier_list(self) -> List[str]:
        """Get list of all available carrier names (excluding DEFAULT)."""
        return [k for k in self.carrier_details.keys() if k != "DEFAULT"]

# Global instance
carrier_config_parser = CarrierConfigParser()