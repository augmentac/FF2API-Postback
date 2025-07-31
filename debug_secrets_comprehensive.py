#!/usr/bin/env python3
"""
Comprehensive Secrets Debugging Utility

This script provides detailed analysis of Streamlit secrets access issues
specifically designed to diagnose authentication failures in cloud deployment.
"""

import logging
import time
import sys
from typing import Dict, Any, List, Optional

# Setup detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('secrets_debug.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)

def comprehensive_secrets_analysis():
    """
    Perform comprehensive analysis of Streamlit secrets access.
    This function tests all aspects of secrets access to identify the exact failure point.
    """
    logger.info("🔍 STARTING COMPREHENSIVE SECRETS ANALYSIS")
    logger.info("=" * 80)
    
    results = {
        'streamlit_import': test_streamlit_import(),
        'secrets_basic_access': test_secrets_basic_access(),
        'secrets_timing': test_secrets_timing(),
        'load_api_section': test_load_api_section(),
        'tracking_api_section': test_tracking_api_section(),
        'secrets_conversion': test_secrets_conversion(),
        'attribute_access_patterns': test_attribute_access_patterns(),
        'error_scenarios': test_error_scenarios()
    }
    
    logger.info("=" * 80)
    logger.info("🔍 COMPREHENSIVE ANALYSIS COMPLETE")
    
    # Generate summary
    generate_analysis_summary(results)
    
    return results

def test_streamlit_import():
    """Test Streamlit import and basic functionality."""
    logger.info("🧪 Testing Streamlit import and basic functionality...")
    
    try:
        import streamlit as st
        logger.info("✅ Streamlit imported successfully")
        logger.info(f"🔍 Streamlit version: {getattr(st, '__version__', 'unknown')}")
        logger.info(f"🔍 Streamlit module path: {st.__file__ if hasattr(st, '__file__') else 'unknown'}")
        
        return {
            'success': True,
            'version': getattr(st, '__version__', 'unknown'),
            'module_path': st.__file__ if hasattr(st, '__file__') else 'unknown'
        }
        
    except ImportError as e:
        logger.error(f"❌ Failed to import Streamlit: {e}")
        return {'success': False, 'error': str(e), 'error_type': 'ImportError'}
    except Exception as e:
        logger.error(f"❌ Unexpected error importing Streamlit: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def test_secrets_basic_access():
    """Test basic secrets access and availability."""
    logger.info("🧪 Testing basic secrets access...")
    
    try:
        import streamlit as st
        
        # Test 1: Check if secrets attribute exists
        has_secrets_attr = hasattr(st, 'secrets')
        logger.info(f"🔍 hasattr(st, 'secrets'): {has_secrets_attr}")
        
        if not has_secrets_attr:
            logger.error("❌ st.secrets attribute does not exist")
            return {
                'success': False,
                'has_secrets_attr': False,
                'error': 'st.secrets attribute missing'
            }
        
        # Test 2: Try to access secrets object
        try:
            secrets_obj = st.secrets
            logger.info(f"🔍 st.secrets object type: {type(secrets_obj)}")
            logger.info(f"🔍 st.secrets object exists: {secrets_obj is not None}")
            
            # Test 3: Check if secrets object is callable/accessible
            try:
                secrets_repr = repr(secrets_obj)[:100]  # First 100 chars
                logger.info(f"🔍 st.secrets representation: {secrets_repr}...")
            except Exception as repr_error:
                logger.warning(f"⚠️ Cannot get secrets representation: {repr_error}")
            
            return {
                'success': True,
                'has_secrets_attr': True,
                'secrets_type': str(type(secrets_obj)),
                'secrets_exists': secrets_obj is not None
            }
            
        except Exception as access_error:
            logger.error(f"❌ Error accessing st.secrets object: {access_error}")
            return {
                'success': False,
                'has_secrets_attr': True,
                'error': str(access_error),
                'error_type': type(access_error).__name__
            }
            
    except Exception as e:
        logger.error(f"❌ Basic secrets access test failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }

def test_secrets_timing():
    """Test secrets access timing and retry behavior."""
    logger.info("🧪 Testing secrets access timing...")
    
    try:
        import streamlit as st
        
        access_times = []
        success_count = 0
        
        for attempt in range(10):
            start_time = time.time()
            try:
                # Try to access secrets
                has_secrets = hasattr(st, 'secrets')
                if has_secrets:
                    _ = st.secrets  # Try to access the object
                    success_count += 1
                
                end_time = time.time()
                access_time = end_time - start_time
                access_times.append(access_time)
                
                logger.info(f"🔍 Attempt {attempt + 1}: Success={has_secrets}, Time={access_time:.4f}s")
                
            except Exception as e:
                end_time = time.time()
                access_time = end_time - start_time
                access_times.append(-1)  # Mark as failed
                logger.warning(f"⚠️ Attempt {attempt + 1} failed: {e}, Time={access_time:.4f}s")
            
            time.sleep(0.1)  # Wait between attempts
        
        avg_time = sum(t for t in access_times if t > 0) / len([t for t in access_times if t > 0]) if success_count > 0 else -1
        
        logger.info(f"🔍 Timing summary: {success_count}/10 successful, avg time: {avg_time:.4f}s")
        
        return {
            'success': success_count > 0,
            'success_rate': success_count / 10,
            'access_times': access_times,
            'average_time': avg_time,
            'max_time': max(t for t in access_times if t > 0) if success_count > 0 else -1
        }
        
    except Exception as e:
        logger.error(f"❌ Timing test failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }

def test_load_api_section():
    """Test specific load_api section access."""
    logger.info("🧪 Testing load_api section access...")
    
    try:
        import streamlit as st
        
        result = {
            'section_exists': False,
            'section_accessible': False,
            'has_bearer_token': False,
            'has_api_key': False,
            'bearer_token_valid': False,
            'api_key_valid': False
        }
        
        # Test 1: Check if section exists
        try:
            section_exists_dict = 'load_api' in st.secrets
            logger.info(f"🔍 'load_api' in st.secrets: {section_exists_dict}")
            result['section_exists'] = section_exists_dict
        except Exception as e:
            logger.error(f"❌ Error checking section existence: {e}")
        
        # Test 2: Check hasattr access
        try:
            section_exists_attr = hasattr(st.secrets, 'load_api')
            logger.info(f"🔍 hasattr(st.secrets, 'load_api'): {section_exists_attr}")
        except Exception as e:
            logger.error(f"❌ Error with hasattr check: {e}")
            section_exists_attr = False
        
        if not (result['section_exists'] or section_exists_attr):
            logger.warning("⚠️ load_api section not found")
            return result
        
        # Test 3: Try to access section
        try:
            load_secrets = st.secrets.load_api
            logger.info("✅ Successfully accessed load_api section")
            logger.info(f"🔍 load_api section type: {type(load_secrets)}")
            result['section_accessible'] = True
            
            # Test bearer_token
            try:
                result['has_bearer_token'] = hasattr(load_secrets, 'bearer_token')
                if result['has_bearer_token']:
                    bearer_token = load_secrets.bearer_token
                    logger.info(f"🔍 bearer_token type: {type(bearer_token)}")
                    logger.info(f"🔍 bearer_token length: {len(str(bearer_token)) if bearer_token else 0}")
                    result['bearer_token_valid'] = bool(bearer_token and str(bearer_token).strip())
                    logger.info(f"🔍 bearer_token valid: {result['bearer_token_valid']}")
            except Exception as e:
                logger.error(f"❌ Error accessing bearer_token: {e}")
            
            # Test api_key
            try:
                result['has_api_key'] = hasattr(load_secrets, 'api_key')
                if result['has_api_key']:
                    api_key = load_secrets.api_key
                    logger.info(f"🔍 api_key type: {type(api_key)}")
                    logger.info(f"🔍 api_key length: {len(str(api_key)) if api_key else 0}")
                    result['api_key_valid'] = bool(api_key and str(api_key).strip())
                    logger.info(f"🔍 api_key valid: {result['api_key_valid']}")
            except Exception as e:
                logger.error(f"❌ Error accessing api_key: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error accessing load_api section: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ load_api section test failed: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def test_tracking_api_section():
    """Test specific tracking_api section access."""
    logger.info("🧪 Testing tracking_api section access...")
    
    try:
        import streamlit as st
        
        result = {
            'section_exists': False,
            'section_accessible': False,
            'has_bearer_token': False,
            'has_api_key': False,
            'bearer_token_valid': False,
            'api_key_valid': False
        }
        
        # Test 1: Check if section exists
        try:
            section_exists_dict = 'tracking_api' in st.secrets
            logger.info(f"🔍 'tracking_api' in st.secrets: {section_exists_dict}")
            result['section_exists'] = section_exists_dict
        except Exception as e:
            logger.error(f"❌ Error checking section existence: {e}")
        
        # Test 2: Check hasattr access
        try:
            section_exists_attr = hasattr(st.secrets, 'tracking_api')
            logger.info(f"🔍 hasattr(st.secrets, 'tracking_api'): {section_exists_attr}")
        except Exception as e:
            logger.error(f"❌ Error with hasattr check: {e}")
            section_exists_attr = False
        
        if not (result['section_exists'] or section_exists_attr):
            logger.warning("⚠️ tracking_api section not found")
            return result
        
        # Test 3: Try to access section
        try:
            tracking_secrets = st.secrets.tracking_api
            logger.info("✅ Successfully accessed tracking_api section")
            logger.info(f"🔍 tracking_api section type: {type(tracking_secrets)}")
            result['section_accessible'] = True
            
            # Test bearer_token
            try:
                result['has_bearer_token'] = hasattr(tracking_secrets, 'bearer_token')
                if result['has_bearer_token']:
                    bearer_token = tracking_secrets.bearer_token
                    logger.info(f"🔍 bearer_token type: {type(bearer_token)}")
                    logger.info(f"🔍 bearer_token length: {len(str(bearer_token)) if bearer_token else 0}")
                    result['bearer_token_valid'] = bool(bearer_token and str(bearer_token).strip())
                    logger.info(f"🔍 bearer_token valid: {result['bearer_token_valid']}")
            except Exception as e:
                logger.error(f"❌ Error accessing bearer_token: {e}")
            
            # Test api_key
            try:
                result['has_api_key'] = hasattr(tracking_secrets, 'api_key')
                if result['has_api_key']:
                    api_key = tracking_secrets.api_key
                    logger.info(f"🔍 api_key type: {type(api_key)}")
                    logger.info(f"🔍 api_key length: {len(str(api_key)) if api_key else 0}")
                    result['api_key_valid'] = bool(api_key and str(api_key).strip())
                    logger.info(f"🔍 api_key valid: {result['api_key_valid']}")
            except Exception as e:
                logger.error(f"❌ Error accessing api_key: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error accessing tracking_api section: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ tracking_api section test failed: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def test_secrets_conversion():
    """Test different ways to convert and access secrets."""
    logger.info("🧪 Testing secrets conversion methods...")
    
    try:
        import streamlit as st
        
        result = {}
        
        # Test 1: Dict conversion
        try:
            secrets_dict = dict(st.secrets)
            result['dict_conversion'] = {
                'success': True,
                'sections': list(secrets_dict.keys()),
                'total_sections': len(secrets_dict.keys())
            }
            logger.info(f"✅ Dict conversion successful: {len(secrets_dict)} sections")
            logger.info(f"🔍 Available sections: {list(secrets_dict.keys())}")
        except Exception as e:
            logger.error(f"❌ Dict conversion failed: {e}")
            result['dict_conversion'] = {'success': False, 'error': str(e)}
        
        # Test 2: Direct attribute enumeration
        try:
            attrs = [attr for attr in dir(st.secrets) if not attr.startswith('_')]
            result['attribute_enumeration'] = {
                'success': True,
                'attributes': attrs,
                'total_attributes': len(attrs)
            }
            logger.info(f"✅ Attribute enumeration successful: {len(attrs)} attributes")
            logger.info(f"🔍 Public attributes: {attrs}")
        except Exception as e:
            logger.error(f"❌ Attribute enumeration failed: {e}")
            result['attribute_enumeration'] = {'success': False, 'error': str(e)}
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Secrets conversion test failed: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def test_attribute_access_patterns():
    """Test different patterns of accessing secrets attributes."""
    logger.info("🧪 Testing attribute access patterns...")
    
    try:
        import streamlit as st
        
        result = {}
        test_sections = ['load_api', 'tracking_api', 'api']
        
        for section in test_sections:
            logger.info(f"🔍 Testing section: {section}")
            section_result = {}
            
            # Pattern 1: hasattr + getattr
            try:
                if hasattr(st.secrets, section):
                    section_obj = getattr(st.secrets, section)
                    section_result['hasattr_getattr'] = {
                        'success': True,
                        'object_type': str(type(section_obj))
                    }
                    logger.info(f"✅ hasattr+getattr for {section}: Success")
                else:
                    section_result['hasattr_getattr'] = {'success': False, 'reason': 'hasattr returned False'}
                    logger.warning(f"⚠️ hasattr+getattr for {section}: hasattr returned False")
            except Exception as e:
                section_result['hasattr_getattr'] = {'success': False, 'error': str(e)}
                logger.error(f"❌ hasattr+getattr for {section}: {e}")
            
            # Pattern 2: Direct attribute access
            try:
                section_obj = getattr(st.secrets, section, None)
                if section_obj is not None:
                    section_result['direct_access'] = {
                        'success': True,
                        'object_type': str(type(section_obj))
                    }
                    logger.info(f"✅ Direct access for {section}: Success")
                else:
                    section_result['direct_access'] = {'success': False, 'reason': 'getattr returned None'}
                    logger.warning(f"⚠️ Direct access for {section}: getattr returned None")
            except Exception as e:
                section_result['direct_access'] = {'success': False, 'error': str(e)}
                logger.error(f"❌ Direct access for {section}: {e}")
            
            # Pattern 3: Dictionary-style access
            try:
                secrets_dict = dict(st.secrets)
                if section in secrets_dict:
                    section_result['dict_access'] = {
                        'success': True,
                        'object_type': str(type(secrets_dict[section]))
                    }
                    logger.info(f"✅ Dict access for {section}: Success")
                else:
                    section_result['dict_access'] = {'success': False, 'reason': 'key not in dict'}
                    logger.warning(f"⚠️ Dict access for {section}: key not in dict")
            except Exception as e:
                section_result['dict_access'] = {'success': False, 'error': str(e)}
                logger.error(f"❌ Dict access for {section}: {e}")
            
            result[section] = section_result
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Attribute access patterns test failed: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def test_error_scenarios():
    """Test specific error scenarios that might occur."""
    logger.info("🧪 Testing error scenarios...")
    
    try:
        import streamlit as st
        
        result = {}
        
        # Scenario 1: Test with non-existent section
        try:
            non_existent = getattr(st.secrets, 'non_existent_section', 'DEFAULT')
            result['non_existent_section'] = {
                'success': True,
                'returned_default': non_existent == 'DEFAULT'
            }
            logger.info(f"✅ Non-existent section test: returned {non_existent}")
        except Exception as e:
            result['non_existent_section'] = {'success': False, 'error': str(e)}
            logger.error(f"❌ Non-existent section test: {e}")
        
        # Scenario 2: Test rapid successive access
        try:
            access_results = []
            for i in range(5):
                try:
                    _ = hasattr(st.secrets, 'load_api')
                    access_results.append(True)
                except:
                    access_results.append(False)
            
            result['rapid_access'] = {
                'success': True,
                'results': access_results,
                'success_rate': sum(access_results) / len(access_results)
            }
            logger.info(f"✅ Rapid access test: {sum(access_results)}/{len(access_results)} successful")
        except Exception as e:
            result['rapid_access'] = {'success': False, 'error': str(e)}
            logger.error(f"❌ Rapid access test: {e}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error scenarios test failed: {e}")
        return {'success': False, 'error': str(e), 'error_type': type(e).__name__}

def generate_analysis_summary(results: Dict[str, Any]):
    """Generate a summary of the analysis results."""
    logger.info("📋 ANALYSIS SUMMARY")
    logger.info("-" * 50)
    
    # Overall status
    critical_tests = ['streamlit_import', 'secrets_basic_access', 'load_api_section', 'tracking_api_section']
    critical_passed = sum(1 for test in critical_tests if results.get(test, {}).get('success', False))
    
    logger.info(f"🎯 Critical tests passed: {critical_passed}/{len(critical_tests)}")
    
    # Detailed findings
    if results.get('streamlit_import', {}).get('success'):
        logger.info("✅ Streamlit import: OK")
    else:
        logger.error("❌ Streamlit import: FAILED")
    
    if results.get('secrets_basic_access', {}).get('success'):
        logger.info("✅ Basic secrets access: OK")
    else:
        logger.error("❌ Basic secrets access: FAILED")
    
    # Section-specific findings
    load_api = results.get('load_api_section', {})
    if load_api.get('section_accessible') and (load_api.get('bearer_token_valid') or load_api.get('api_key_valid')):
        logger.info("✅ load_api configuration: OK")
    else:
        logger.error("❌ load_api configuration: ISSUES FOUND")
        if not load_api.get('section_exists'):
            logger.error("   - Section does not exist")
        if not load_api.get('section_accessible'):
            logger.error("   - Section not accessible")
        if not (load_api.get('bearer_token_valid') or load_api.get('api_key_valid')):
            logger.error("   - No valid tokens found")
    
    tracking_api = results.get('tracking_api_section', {})
    if tracking_api.get('section_accessible') and (tracking_api.get('bearer_token_valid') or tracking_api.get('api_key_valid')):
        logger.info("✅ tracking_api configuration: OK")
    else:
        logger.error("❌ tracking_api configuration: ISSUES FOUND")
        if not tracking_api.get('section_exists'):
            logger.error("   - Section does not exist")
        if not tracking_api.get('section_accessible'):
            logger.error("   - Section not accessible")
        if not (tracking_api.get('bearer_token_valid') or tracking_api.get('api_key_valid')):
            logger.error("   - No valid tokens found")
    
    logger.info("-" * 50)
    logger.info("📄 Full debug log saved to: secrets_debug.log")

if __name__ == "__main__":
    try:
        results = comprehensive_secrets_analysis()
        
        # Save results to file
        import json
        with open('secrets_analysis_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
            
        logger.info("💾 Analysis results saved to: secrets_analysis_results.json")
        
    except Exception as e:
        logger.error(f"💥 Comprehensive analysis failed: {e}")
        sys.exit(1)