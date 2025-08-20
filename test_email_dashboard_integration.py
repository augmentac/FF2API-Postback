"""
Test Email Dashboard Integration

Simple test to verify that the email processing dashboard integrates correctly 
with the email automation system without breaking existing functionality.
"""

import sys
import os
sys.path.append('.')

def test_imports():
    """Test that all modules import correctly."""
    print("Testing imports...")
    
    try:
        # Test email processing dashboard import
        from email_processing_dashboard import (
            EmailProcessingDashboard,
            render_email_processing_dashboard,
            add_email_processing_job,
            update_email_job_progress
        )
        print("✅ Email processing dashboard imports successful")
    except Exception as e:
        print(f"❌ Email processing dashboard import failed: {e}")
        return False
    
    try:
        # Test unified app imports (should not break)
        from src.frontend.unified_app import main
        print("✅ Unified app imports successful")
    except Exception as e:
        print(f"❌ Unified app import failed: {e}")
        return False
    
    try:
        # Test email automation imports (should not break)
        from src.frontend.email_automation import EmailAutomationManager
        print("✅ Email automation imports successful")
    except Exception as e:
        print(f"❌ Email automation import failed: {e}")
        return False
    
    return True

def test_dashboard_functionality():
    """Test basic dashboard functionality."""
    print("Testing dashboard functionality...")
    
    try:
        from email_processing_dashboard import EmailProcessingDashboard, EmailProcessingJob
        from datetime import datetime
        
        dashboard = EmailProcessingDashboard()
        
        # Test job creation
        job = EmailProcessingJob(
            job_id="test_job_123",
            filename="test_file.csv",
            brokerage_key="test_brokerage",
            email_source="test@example.com",
            file_size=1024,
            record_count=100,
            started_at=datetime.now()
        )
        
        # Test job serialization
        job_dict = job.to_dict()
        reconstructed_job = EmailProcessingJob.from_dict(job_dict)
        
        print("✅ Job creation and serialization works")
        
        # Test helper functions
        from email_processing_dashboard import add_email_processing_job, update_email_job_progress
        
        job_id = add_email_processing_job(
            filename="test2.csv",
            brokerage_key="test_brokerage", 
            email_source="test2@example.com",
            record_count=50
        )
        
        print(f"✅ Job creation helper works, created job: {job_id}")
        
        # Test progress update
        update_email_job_progress(job_id, "test_brokerage", "parsing_email", 10.0)
        print("✅ Progress update works")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard functionality test failed: {e}")
        return False

def test_integration_points():
    """Test that integration points work correctly."""
    print("Testing integration points...")
    
    try:
        # Test that email automation can create jobs
        from src.frontend.email_automation import EmailAutomationManager
        from email_processing_dashboard import add_email_processing_job
        
        # This should work without errors
        manager = EmailAutomationManager("test_brokerage")
        print("✅ Email automation manager creation works")
        
        # Test that unified_app functions exist
        from src.frontend.unified_app import main, authenticate_user
        print("✅ Unified app integration points exist")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration points test failed: {e}")
        return False

def run_integration_tests():
    """Run all integration tests."""
    print("🧪 Running Email Dashboard Integration Tests")
    print("=" * 50)
    
    tests = [
        ("Import Tests", test_imports),
        ("Dashboard Functionality", test_dashboard_functionality), 
        ("Integration Points", test_integration_points)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n🔍 {test_name}:")
        try:
            result = test_func()
            results.append(result)
            if result:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} CRASHED: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 ALL TESTS PASSED ({passed}/{total})")
        print("✅ Email dashboard integration is working correctly!")
        return True
    else:
        print(f"⚠️  SOME TESTS FAILED ({passed}/{total})")
        print("❌ There may be issues with the integration")
        return False

if __name__ == "__main__":
    success = run_integration_tests()
    sys.exit(0 if success else 1)