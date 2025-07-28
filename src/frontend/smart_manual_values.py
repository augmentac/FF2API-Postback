"""
Smart Manual Value Interface System

Provides intelligent field handling with enum dropdowns, type-specific inputs,
real-time validation, and comprehensive configuration management.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

# Enhanced enum definitions with descriptions
ENUM_DEFINITIONS = {
    'load.mode': {
        'FTL': 'Full Truckload - Single shipper uses entire truck capacity',
        'LTL': 'Less Than Truckload - Shared truck space with multiple shippers',
        'DRAYAGE': 'Short-distance freight transport, typically to/from ports'
    },
    'load.rateType': {
        'SPOT': 'One-time pricing for immediate shipment',
        'CONTRACT': 'Pre-negotiated rates under contract terms',
        'DEDICATED': 'Exclusive equipment and driver assignment',
        'PROJECT': 'Special project-based pricing arrangement'
    },
    'load.status': {
        'DRAFT': 'Load created but not yet confirmed',
        'CUSTOMER_CONFIRMED': 'Customer has approved the load',
        'COVERED': 'Carrier assigned to the load',
        'DISPATCHED': 'Load dispatched to carrier',
        'AT_PICKUP': 'Driver arrived at pickup location',
        'IN_TRANSIT': 'Load is en route to destination',
        'AT_DELIVERY': 'Driver arrived at delivery location',
        'DELIVERED': 'Load successfully delivered',
        'POD_COLLECTED': 'Proof of delivery collected',
        'CANCELED': 'Load has been canceled',
        'ERROR': 'Error occurred during processing'
    },
    'load.route.0.stopActivity': {
        'PICKUP': 'Pickup stop - collect freight from shipper',
        'DELIVERY': 'Delivery stop - deliver freight to consignee'
    },
    'load.route.1.stopActivity': {
        'PICKUP': 'Pickup stop - collect freight from shipper',
        'DELIVERY': 'Delivery stop - deliver freight to consignee'
    },
    'load.equipment.equipmentType': {
        'DRY_VAN': 'Standard enclosed trailer for dry goods',
        'FLATBED': 'Open platform trailer for oversized cargo',
        'REEFER': 'Refrigerated trailer for temperature-controlled goods',
        'CONTAINER': 'Standardized shipping container',
        'OTHER': 'Other specialized equipment type'
    },
    'bidCriteria.equipment': {
        'DRY_VAN': 'Standard enclosed trailer for dry goods',
        'FLATBED': 'Open platform trailer for oversized cargo',
        'REEFER': 'Refrigerated trailer for temperature-controlled goods',
        'CONTAINER': 'Standardized shipping container',
        'OTHER': 'Other specialized equipment type'
    },
    'bidCriteria.service': {
        'STANDARD': 'Standard shipping service',
        'PARTIAL': 'Partial load service',
        'VOLUME': 'High-volume shipment',
        'HOTSHOT': 'Expedited delivery service',
        'TIME_CRITICAL': 'Time-sensitive delivery'
    },
    'carrier.contacts.0.role': {
        'DISPATCHER': 'Coordinates driver assignments and logistics',
        'CARRIER_ADMIN': 'Administrative contact for carrier operations'
    },
    'brokerage.contacts.0.role': {
        'ACCOUNT_MANAGER': 'Manages customer relationship and account',
        'OPERATIONS_REP': 'Handles operational coordination',
        'CARRIER_REP': 'Manages carrier relationships',
        'CUSTOMER_TEAM': 'Customer service and support team'
    }
}

# Value mapping for common alternatives
VALUE_MAPPINGS = {
    'load.mode': {
        'ftl': 'FTL',
        'full truckload': 'FTL',
        'full': 'FTL',
        'ltl': 'LTL', 
        'less than truckload': 'LTL',
        'partial': 'LTL',
        'drayage': 'DRAYAGE',
        'port': 'DRAYAGE'
    },
    'load.rateType': {
        'spot': 'SPOT',
        'one-time': 'SPOT',
        'contract': 'CONTRACT',
        'contracted': 'CONTRACT',
        'dedicated': 'DEDICATED',
        'project': 'PROJECT'
    },
    'load.equipment.equipmentType': {
        'dry van': 'DRY_VAN',
        'dry': 'DRY_VAN',
        'van': 'DRY_VAN',
        'flatbed': 'FLATBED',
        'flat': 'FLATBED',
        'reefer': 'REEFER',
        'refrigerated': 'REEFER',
        'container': 'CONTAINER',
        'other': 'OTHER'
    }
}

class SmartManualValueInterface:
    """Smart interface for manual value entry with type validation and enum handling"""
    
    def __init__(self, api_schema: Dict[str, Any]):
        self.api_schema = api_schema
        self.session_key_prefix = "smart_manual_"
        
    def get_field_info(self, field_name: str) -> Dict[str, Any]:
        """Get comprehensive field information including type, enum, validation"""
        field_info = self.api_schema.get(field_name, {})
        
        # Add enum descriptions if available
        if field_info.get('enum') and field_name in ENUM_DEFINITIONS:
            field_info['enum_descriptions'] = ENUM_DEFINITIONS[field_name]
        
        # Add value mappings if available
        if field_name in VALUE_MAPPINGS:
            field_info['value_mappings'] = VALUE_MAPPINGS[field_name]
            
        return field_info
    
    def render_manual_value_input(self, field_name: str, current_value: Any = None) -> Tuple[Any, bool]:
        """
        Render appropriate input widget based on field type
        
        Returns:
            Tuple of (value, is_valid)
        """
        field_info = self.get_field_info(field_name)
        field_type = field_info.get('type', 'string')
        is_enum = bool(field_info.get('enum'))
        is_required = field_info.get('required', False)
        
        # Generate unique session key
        session_key = f"{self.session_key_prefix}{field_name}"
        
        # Initialize session state if needed
        if session_key not in st.session_state:
            st.session_state[session_key] = current_value
        
        st.markdown(f"**{field_info.get('description', field_name)}**")
        
        # Show field type and requirements
        type_indicators = []
        if is_enum:
            type_indicators.append("🔽 Dropdown")
        else:
            type_indicators.append(f"📝 {field_type.title()}")
            
        if is_required:
            type_indicators.append("⭐ Required")
            
        st.caption(" • ".join(type_indicators))
        
        value = None
        is_valid = True
        
        if is_enum:
            # Enum field - render dropdown with descriptions
            value, is_valid = self._render_enum_input(field_name, field_info, session_key)
        elif field_type == 'number':
            # Number field - render number input
            value, is_valid = self._render_number_input(field_name, field_info, session_key)
        elif field_type == 'date':
            # Date field - render date input with validation
            value, is_valid = self._render_date_input(field_name, field_info, session_key)
        else:
            # String field - render text input
            value, is_valid = self._render_text_input(field_name, field_info, session_key)
        
        # Show validation status
        if not is_valid:
            st.error("❌ Invalid value - please correct the input above")
        elif value is not None and value != "":
            st.success("✅ Valid value")
            
        return value, is_valid
    
    def _render_enum_input(self, field_name: str, field_info: Dict, session_key: str) -> Tuple[Any, bool]:
        """Render enum dropdown with descriptions"""
        enum_options = field_info.get('enum', [])
        enum_descriptions = field_info.get('enum_descriptions', {})
        
        # Create options with descriptions
        display_options = ["-- Select Option --"]
        option_mapping = {}
        
        for option in enum_options:
            description = enum_descriptions.get(option, '')
            if description:
                display_text = f"{option} - {description}"
            else:
                display_text = option
            display_options.append(display_text)
            option_mapping[display_text] = option
        
        # Find current selection
        current_value = st.session_state.get(session_key)
        current_index = 0
        
        if current_value:
            for i, display_text in enumerate(display_options[1:], 1):
                if option_mapping[display_text] == current_value:
                    current_index = i
                    break
        
        # Render selectbox
        selected_display = st.selectbox(
            "Select value:",
            options=display_options,
            index=current_index,
            key=f"{session_key}_selectbox",
            help=f"Choose from predefined options for {field_name}"
        )
        
        if selected_display == "-- Select Option --":
            value = None
            is_valid = not field_info.get('required', False)
        else:
            value = option_mapping[selected_display]
            is_valid = True
            
            # Show selected option description
            if value in enum_descriptions:
                st.info(f"💡 {enum_descriptions[value]}")
        
        # Update session state
        st.session_state[session_key] = value
        
        return value, is_valid
    
    def _render_number_input(self, field_name: str, field_info: Dict, session_key: str) -> Tuple[Any, bool]:
        """Render number input with validation"""
        current_value = st.session_state.get(session_key)
        
        # Determine if integer or float
        if 'quantity' in field_name.lower() or 'sequence' in field_name.lower():
            step = 1
            value_type = int
        else:
            step = 0.01
            value_type = float
        
        try:
            if current_value is not None:
                default_value = value_type(current_value)
            else:
                default_value = None
        except (ValueError, TypeError):
            default_value = None
        
        # Render number input
        value = st.number_input(
            "Enter numeric value:",
            value=default_value,
            step=step,
            key=f"{session_key}_number",
            help=f"Enter a numeric value for {field_name}"
        )
        
        # Validation
        is_valid = True
        if value is None and field_info.get('required', False):
            is_valid = False
        elif value is not None:
            # Additional validation rules
            if value < 0 and ('weight' in field_name.lower() or 'cost' in field_name.lower()):
                is_valid = False
                st.error("❌ Value cannot be negative")
        
        # Update session state
        st.session_state[session_key] = value
        
        return value, is_valid
    
    def _render_date_input(self, field_name: str, field_info: Dict, session_key: str) -> Tuple[Any, bool]:
        """Render date input with validation"""
        current_value = st.session_state.get(session_key)
        
        # Date format help
        st.caption("📅 Format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD")
        
        # Render text input for date
        date_str = st.text_input(
            "Enter date:",
            value=current_value or "",
            key=f"{session_key}_date",
            placeholder="2024-01-15 10:30:00",
            help="Enter date in ISO format (YYYY-MM-DD HH:MM:SS)"
        )
        
        # Validation
        is_valid = True
        value = date_str
        
        if date_str:
            # Try to parse date
            try:
                # Support multiple date formats
                if len(date_str) == 10:  # YYYY-MM-DD
                    datetime.strptime(date_str, "%Y-%m-%d")
                elif len(date_str) == 19:  # YYYY-MM-DD HH:MM:SS
                    datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                else:
                    raise ValueError("Invalid date format")
                    
                st.success(f"✅ Valid date: {date_str}")
                
            except ValueError:
                is_valid = False
                st.error("❌ Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
                
        elif field_info.get('required', False):
            is_valid = False
        
        # Update session state
        st.session_state[session_key] = value
        
        return value, is_valid
    
    def _render_text_input(self, field_name: str, field_info: Dict, session_key: str) -> Tuple[Any, bool]:
        """Render text input with validation"""
        current_value = st.session_state.get(session_key, "")
        
        # Determine input type based on field name
        if 'email' in field_name.lower():
            input_type = "email"
            placeholder = "user@example.com"
        elif 'phone' in field_name.lower():
            input_type = "phone"
            placeholder = "+1-555-123-4567"
        elif 'address' in field_name.lower():
            input_type = "address"
            placeholder = "123 Main St"
        else:
            input_type = "text"
            placeholder = f"Enter {field_info.get('description', field_name).lower()}"
        
        # Render text input
        value = st.text_input(
            "Enter text value:",
            value=current_value,
            key=f"{session_key}_text",
            placeholder=placeholder,
            help=f"Enter text value for {field_name}"
        )
        
        # Validation
        is_valid = True
        
        if not value and field_info.get('required', False):
            is_valid = False
        elif value:
            # Type-specific validation
            if input_type == "email":
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(email_pattern, value):
                    is_valid = False
                    st.error("❌ Invalid email format")
                    
            elif input_type == "phone":
                # Basic phone validation
                phone_pattern = r'^[\+]?[1-9][\d]{0,15}$'
                clean_phone = re.sub(r'[^\d\+]', '', value)
                if not re.match(phone_pattern, clean_phone):
                    is_valid = False
                    st.error("❌ Invalid phone format")
        
        # Update session state
        st.session_state[session_key] = value
        
        return value, is_valid
    
    def get_all_manual_values(self) -> Dict[str, Any]:
        """Get all current manual values from session state"""
        manual_values = {}
        
        for key, value in st.session_state.items():
            if key.startswith(self.session_key_prefix):
                field_name = key[len(self.session_key_prefix):]
                if value is not None and value != "":
                    manual_values[field_name] = value
        
        return manual_values
    
    def clear_manual_values(self):
        """Clear all manual values from session state"""
        keys_to_remove = [key for key in st.session_state.keys() 
                         if key.startswith(self.session_key_prefix)]
        
        for key in keys_to_remove:
            del st.session_state[key]
    
    def validate_all_manual_values(self) -> Tuple[Dict[str, Any], List[str]]:
        """
        Validate all current manual values
        
        Returns:
            Tuple of (valid_values, error_messages)
        """
        manual_values = self.get_all_manual_values()
        valid_values = {}
        errors = []
        
        for field_name, value in manual_values.items():
            field_info = self.get_field_info(field_name)
            
            # Type validation
            field_type = field_info.get('type', 'string')
            is_enum = bool(field_info.get('enum'))
            
            try:
                if is_enum:
                    if value not in field_info.get('enum', []):
                        # Try value mapping
                        mapped_value = self._map_enum_value(field_name, value)
                        if mapped_value:
                            valid_values[field_name] = mapped_value
                        else:
                            errors.append(f"{field_name}: Invalid enum value '{value}'")
                    else:
                        valid_values[field_name] = value
                        
                elif field_type == 'number':
                    valid_values[field_name] = float(value) if '.' in str(value) else int(value)
                    
                elif field_type == 'date':
                    # Validate date format
                    if len(str(value)) == 10:
                        datetime.strptime(str(value), "%Y-%m-%d")
                    else:
                        datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                    valid_values[field_name] = value
                    
                else:
                    valid_values[field_name] = str(value)
                    
            except (ValueError, TypeError) as e:
                errors.append(f"{field_name}: {str(e)}")
        
        return valid_values, errors
    
    def _map_enum_value(self, field_name: str, value: str) -> Optional[str]:
        """Map alternative values to enum values"""
        if field_name in VALUE_MAPPINGS:
            return VALUE_MAPPINGS[field_name].get(str(value).lower())
        return None

def render_smart_manual_value_interface(api_schema: Dict[str, Any], 
                                      existing_manual_values: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Render the complete smart manual value interface
    
    Returns:
        Dictionary of valid manual values
    """
    interface = SmartManualValueInterface(api_schema)
    
    st.subheader("🎯 Smart Manual Value Entry")
    st.caption("Set values that will be applied to ALL records in your CSV file")
    
    # Initialize existing values in session state
    if existing_manual_values:
        for field_name, value in existing_manual_values.items():
            session_key = f"{interface.session_key_prefix}{field_name}"
            if session_key not in st.session_state:
                st.session_state[session_key] = value
    
    # Group fields by category for better organization
    field_categories = {
        "Load Information": [
            'load.mode', 'load.rateType', 'load.status'
        ],
        "Equipment & Service": [
            'load.equipment.equipmentType', 'bidCriteria.equipment', 'bidCriteria.service'
        ],
        "Contact Roles": [
            'carrier.contacts.0.role', 'brokerage.contacts.0.role'
        ],
        "Route Activities": [
            'load.route.0.stopActivity', 'load.route.1.stopActivity'
        ]
    }
    
    # Render field categories in tabs
    tab_names = list(field_categories.keys())
    tabs = st.tabs(tab_names)
    
    for tab, (category, fields) in zip(tabs, field_categories.items()):
        with tab:
            st.markdown(f"### {category}")
            
            for field_name in fields:
                if field_name in api_schema:
                    with st.expander(f"📝 {api_schema[field_name].get('description', field_name)}", expanded=False):
                        interface.render_manual_value_input(field_name)
    
    # Summary and validation
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        manual_values = interface.get_all_manual_values()
        st.metric("Manual Values Set", len(manual_values))
    
    with col2:
        valid_values, errors = interface.validate_all_manual_values()
        st.metric("Valid Values", len(valid_values))
    
    with col3:
        st.metric("Validation Errors", len(errors))
    
    # Show validation errors if any
    if errors:
        with st.expander("⚠️ Validation Errors", expanded=True):
            for error in errors:
                st.error(error)
    
    # Configuration management buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("💾 Save Manual Values", use_container_width=True):
            if valid_values:
                st.session_state.manual_values_config = valid_values
                st.success(f"✅ Saved {len(valid_values)} manual values")
            else:
                st.warning("⚠️ No valid values to save")
    
    with col2:
        if st.button("🔄 Reset All Values", use_container_width=True):
            interface.clear_manual_values()
            st.success("✅ All manual values cleared")
            st.rerun()
    
    with col3:
        if st.button("📋 Show Summary", use_container_width=True):
            st.session_state.show_manual_values_summary = True
    
    # Detailed manual values summary
    if st.session_state.get('show_manual_values_summary', False):
        with st.expander("📊 Manual Values Summary", expanded=True):
            if valid_values:
                summary_df = pd.DataFrame([
                    {
                        'Field': field,
                        'Value': value,
                        'Type': api_schema.get(field, {}).get('type', 'string'),
                        'Is Enum': 'Yes' if api_schema.get(field, {}).get('enum') else 'No'
                    }
                    for field, value in valid_values.items()
                ])
                st.dataframe(summary_df, use_container_width=True)
            else:
                st.info("No manual values configured")
    
    return valid_values