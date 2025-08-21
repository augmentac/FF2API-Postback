"""
Universal Email Processing Results Display Component
"""
import streamlit as st
import json
from pathlib import Path
from datetime import datetime

def show_email_processing_results():
    """Display email processing results from shared storage - works universally"""
    
    st.markdown("---")
    st.markdown("### 📧 Automated Email Processing Results")
    
    try:
        # Direct access to shared storage files
        storage_dir = Path(".streamlit_shared")
        
        if not storage_dir.exists():
            st.info("No email processing activity detected yet.")
            return
            
        # Read results file
        results_file = storage_dir / "email_results.json"
        jobs_file = storage_dir / "email_jobs.json"
        
        if results_file.exists():
            with open(results_file, 'r') as f:
                results_data = json.load(f)
            
            if results_data:
                st.success(f"✅ Found email processing results for {len(results_data)} brokerage(s)")
                
                for brokerage_key, results in results_data.items():
                    if results:  # If there are results for this brokerage
                        with st.expander(f"📊 {brokerage_key} - {len(results)} processed files", expanded=True):
                            
                            # Show recent results
                            recent_results = sorted(results, key=lambda x: x.get('processed_time', ''), reverse=True)[:5]
                            
                            for result in recent_results:
                                col1, col2, col3 = st.columns([2, 1, 1])
                                
                                with col1:
                                    st.write(f"📄 **{result.get('filename', 'Unknown File')}**")
                                    st.caption(f"Source: {result.get('email_source', 'Unknown')}")
                                
                                with col2:
                                    if result.get('success', False):
                                        st.success("✅ Success")
                                    else:
                                        st.error("❌ Failed")
                                        
                                with col3:
                                    processed_time = result.get('processed_time', '')
                                    if processed_time:
                                        try:
                                            dt = datetime.fromisoformat(processed_time.replace('Z', '+00:00'))
                                            st.caption(dt.strftime("%m/%d %H:%M"))
                                        except:
                                            st.caption("Recent")
                                    
                                # Show record count if available
                                record_count = result.get('record_count', 0)
                                if record_count > 0:
                                    st.caption(f"📊 {record_count} records processed")
                                    
                                st.markdown("---")
            else:
                st.info("No email processing results found yet.")
        
        # Also check jobs file for active processing
        if jobs_file.exists():
            with open(jobs_file, 'r') as f:
                jobs_data = json.load(f)
            
            active_jobs = []
            for brokerage_key, jobs in jobs_data.items():
                for job in jobs:
                    if job.get('status') in ['pending', 'processing']:
                        active_jobs.append((brokerage_key, job))
            
            if active_jobs:
                st.info(f"🔄 {len(active_jobs)} email processing job(s) currently active")
                
                for brokerage_key, job in active_jobs:
                    with st.expander(f"⏳ Processing: {job.get('filename', 'Unknown')}", expanded=False):
                        st.write(f"**Status:** {job.get('status', 'Unknown')}")
                        st.write(f"**Step:** {job.get('current_step', 'Unknown')}")
                        progress = job.get('progress_percent', 0)
                        if progress > 0:
                            st.progress(progress / 100.0)
                            st.caption(f"{progress}% complete")
    
    except Exception as e:
        st.error(f"Error loading email processing results: {str(e)}")
        
    st.markdown("---")

if __name__ == "__main__":
    show_email_processing_results()