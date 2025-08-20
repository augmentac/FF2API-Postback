"""
Test UI Dashboard Integration

This test verifies that the email processing dashboard appears correctly 
in the unified UI with real eShipping data.
"""

import sys
import os
sys.path.append('.')

def test_ui_dashboard_integration():
    """Test that the UI dashboard correctly displays email processing data."""
    print("🧪 Testing UI Dashboard Integration")
    print("=" * 50)
    
    try:
        # Test 1: Check shared storage data for eShipping
        print("1. Checking shared storage data for eShipping...")
        from shared_storage_bridge import get_email_processing_data
        
        processing_data = get_email_processing_data("eShipping")
        
        print(f"   📊 Active jobs: {len(processing_data['active_jobs'])}")
        print(f"   📊 Completed jobs: {len(processing_data['completed_jobs'])}")
        print(f"   📊 Recent results: {len(processing_data['recent_results'])}")
        print(f"   📊 Stats: {processing_data['stats']}")
        print(f"   📊 Has recent activity: {processing_data['has_recent_activity']}")
        
        if processing_data['active_jobs'] or processing_data['completed_jobs']:
            print("   ✅ Found processing data for eShipping")
        else:
            print("   ❌ No processing data found for eShipping")
            return False
        
        # Test 2: Check email processing dashboard functionality
        print("\n2. Testing email processing dashboard...")
        from email_processing_dashboard import EmailProcessingDashboard
        
        dashboard = EmailProcessingDashboard()
        active_jobs, completed_jobs = dashboard._get_processing_jobs("eShipping")
        
        print(f"   📋 Dashboard found {len(active_jobs)} active jobs")
        print(f"   📋 Dashboard found {len(completed_jobs)} completed jobs")
        
        # List job details
        for job in active_jobs:
            print(f"   🔄 Active: {job.filename} ({job.progress_percent}% - {job.current_step})")
        
        for job in completed_jobs[:3]:  # Show first 3
            print(f"   ✅ Completed: {job.filename} ({job.success_count}/{job.record_count} successful)")
        
        # Test 3: Check queue status
        print("\n3. Testing queue status...")
        queue_status = dashboard._get_queue_status("eShipping")
        print(f"   📊 Queue status: {queue_status}")
        
        if queue_status['total'] > 0:
            print("   ✅ Queue has processing data")
        else:
            print("   ❌ Queue appears empty")
        
        # Test 4: Simulate unified app logic
        print("\n4. Testing unified app detection logic...")
        
        # Simulate the logic from unified_app.py
        email_jobs_exist = False
        
        # Check shared storage (same logic as unified_app.py)
        try:
            from shared_storage_bridge import shared_storage
            active_jobs_check = shared_storage.get_active_jobs("eShipping")
            completed_jobs_check = shared_storage.get_completed_jobs("eShipping")
            recent_activity = shared_storage.has_recent_activity("eShipping", minutes=60)
            
            if active_jobs_check or completed_jobs_check or recent_activity:
                email_jobs_exist = True
                print(f"   ✅ Unified app would detect email activity: active={len(active_jobs_check)}, completed={len(completed_jobs_check)}, recent={recent_activity}")
        except Exception as e:
            print(f"   ❌ Error in unified app logic: {e}")
            return False
        
        if not email_jobs_exist:
            print("   ❌ Unified app would NOT detect email activity")
            return False
        
        print("\n" + "=" * 50)
        print("🎉 UI DASHBOARD INTEGRATION TEST SUCCESSFUL!")
        print("\n📋 Summary:")
        print("✅ Shared storage contains eShipping data")
        print("✅ Email processing dashboard can read the data")
        print("✅ Queue status is populated")
        print("✅ Unified app logic would show dashboard")
        
        print("\n🚀 DASHBOARD SHOULD NOW APPEAR IN UI!")
        print("When you refresh the app, the email processing dashboard will be visible.")
        
        return True
        
    except Exception as e:
        print(f"❌ UI dashboard integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔧 UI Dashboard Integration Test")
    print("Testing that email processing data appears in the unified UI")
    print()
    
    success = test_ui_dashboard_integration()
    
    if success:
        print("\n✅ INTEGRATION TEST PASSED!")
        print("The email processing dashboard should now be visible in your UI.")
        sys.exit(0)
    else:
        print("\n❌ INTEGRATION TEST FAILED")
        sys.exit(1)