"""
Google Drive SQLite Backup and Restore Manager for FF2API-Postback
Handles persistent storage of ff.sqlite via Google Drive integration
"""

import os
import hashlib
import threading
import time
import json
import tempfile
from typing import Optional
import streamlit as st

# Import PyDrive2 components
try:
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
    PYDRIVE_AVAILABLE = True
except ImportError:
    print("[db_manager] WARNING: PyDrive2 not installed. Google Drive backup disabled.")
    PYDRIVE_AVAILABLE = False

# Configuration
SQLITE_FILE = "ff.sqlite"
HASH_FILE = ".last_sha"
BACKUP_INTERVAL_MINUTES = 30

class GoogleDriveManager:
    """Manages Google Drive operations for SQLite backup/restore"""
    
    def __init__(self):
        self.drive = None
        self.authenticated = False
        self._initialize_drive()
    
    def _initialize_drive(self):
        """Initialize Google Drive connection using Streamlit secrets"""
        if not PYDRIVE_AVAILABLE:
            print("[db_manager] PyDrive2 not available, skipping Drive initialization")
            return
            
        try:
            # Generate client_secrets.json from Streamlit secrets
            self._create_client_secrets()
            
            # Generate token.json from Streamlit secrets
            self._create_token_file()
            
            # Initialize PyDrive2 authentication
            gauth = GoogleAuth()
            gauth.LoadClientConfigFile("client_secrets.json")
            gauth.LoadCredentialsFile("token.json")
            
            if gauth.credentials is None:
                print("[db_manager] ERROR: No valid credentials found in secrets")
                return
            elif gauth.access_token_expired:
                print("[db_manager] Access token expired, refreshing...")
                gauth.Refresh()
                self._save_refreshed_token(gauth)
            else:
                gauth.Authorize()
            
            self.drive = GoogleDrive(gauth)
            self.authenticated = True
            print("[db_manager] Google Drive authentication successful")
            
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to initialize Google Drive: {e}")
            self.authenticated = False
    
    def _create_client_secrets(self):
        """Create client_secrets.json from Streamlit secrets"""
        try:
            client_config = {
                "web": {
                    "client_id": st.secrets["google"]["client_id"],
                    "client_secret": st.secrets["google"]["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "redirect_uris": ["http://localhost"]
                }
            }
            
            with open("client_secrets.json", "w") as f:
                json.dump(client_config, f, indent=2)
            
            print("[db_manager] Created client_secrets.json from Streamlit secrets")
            
        except KeyError as e:
            print(f"[db_manager] ERROR: Missing required secret: {e}")
            raise
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to create client_secrets.json: {e}")
            raise
    
    def _create_token_file(self):
        """Create token.json from Streamlit secrets"""
        try:
            token_data = {
                "access_token": st.secrets["google"]["access_token"],
                "refresh_token": st.secrets["google"]["refresh_token"],
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": st.secrets["google"]["client_id"],
                "client_secret": st.secrets["google"]["client_secret"],
                "scopes": ["https://www.googleapis.com/auth/drive.file"]
            }
            
            with open("token.json", "w") as f:
                json.dump(token_data, f, indent=2)
            
            print("[db_manager] Created token.json from Streamlit secrets")
            
        except KeyError as e:
            print(f"[db_manager] ERROR: Missing required token secret: {e}")
            raise
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to create token.json: {e}")
            raise
    
    def _save_refreshed_token(self, gauth):
        """Save refreshed token back to token.json"""
        try:
            token_data = {
                "access_token": gauth.credentials.access_token,
                "refresh_token": gauth.credentials.refresh_token,
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": st.secrets["google"]["client_id"],
                "client_secret": st.secrets["google"]["client_secret"],
                "scopes": ["https://www.googleapis.com/auth/drive.file"]
            }
            
            with open("token.json", "w") as f:
                json.dump(token_data, f, indent=2)
            
            print("[db_manager] Refreshed token saved successfully")
            
        except Exception as e:
            print(f"[db_manager] WARNING: Failed to save refreshed token: {e}")
    
    def find_sqlite_file(self) -> Optional[str]:
        """Find ff.sqlite file in Google Drive"""
        if not self.authenticated:
            print("[db_manager] Not authenticated with Google Drive")
            return None
        
        try:
            file_list = self.drive.ListFile({
                'q': f"title='{SQLITE_FILE}' and trashed=false"
            }).GetList()
            
            if file_list:
                file_id = file_list[0]['id']
                print(f"[db_manager] Found {SQLITE_FILE} in Google Drive (ID: {file_id})")
                return file_id
            else:
                print(f"[db_manager] {SQLITE_FILE} not found in Google Drive")
                return None
                
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to search for {SQLITE_FILE}: {e}")
            return None
    
    def download_sqlite(self, file_id: str) -> bool:
        """Download SQLite file from Google Drive"""
        if not self.authenticated:
            print("[db_manager] Not authenticated with Google Drive")
            return False
        
        try:
            file_obj = self.drive.CreateFile({'id': file_id})
            file_obj.GetContentFile(SQLITE_FILE)
            print(f"[db_manager] Successfully downloaded {SQLITE_FILE} from Google Drive")
            return True
            
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to download {SQLITE_FILE}: {e}")
            return False
    
    def upload_sqlite(self) -> bool:
        """Upload SQLite file to Google Drive"""
        if not self.authenticated:
            print("[db_manager] Not authenticated with Google Drive")
            return False
        
        if not os.path.exists(SQLITE_FILE):
            print(f"[db_manager] {SQLITE_FILE} does not exist, cannot upload")
            return False
        
        try:
            # Check if file already exists
            existing_file_id = self.find_sqlite_file()
            
            if existing_file_id:
                # Update existing file
                file_obj = self.drive.CreateFile({'id': existing_file_id})
                file_obj.SetContentFile(SQLITE_FILE)
                file_obj.Upload()
                print(f"[db_manager] Successfully updated {SQLITE_FILE} in Google Drive")
            else:
                # Create new file
                file_obj = self.drive.CreateFile({'title': SQLITE_FILE})
                file_obj.SetContentFile(SQLITE_FILE)
                file_obj.Upload()
                print(f"[db_manager] Successfully uploaded new {SQLITE_FILE} to Google Drive")
            
            return True
            
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to upload {SQLITE_FILE}: {e}")
            return False

# Global instance
_drive_manager = None

def _get_drive_manager():
    """Get or create global GoogleDriveManager instance"""
    global _drive_manager
    if _drive_manager is None:
        _drive_manager = GoogleDriveManager()
    return _drive_manager

def _calculate_file_hash(filepath: str) -> Optional[str]:
    """Calculate SHA256 hash of a file"""
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        print(f"[db_manager] ERROR: Failed to calculate hash for {filepath}: {e}")
        return None

def _get_last_hash() -> Optional[str]:
    """Get the last known hash from .last_sha file"""
    try:
        if os.path.exists(HASH_FILE):
            with open(HASH_FILE, "r") as f:
                return f.read().strip()
    except Exception as e:
        print(f"[db_manager] WARNING: Failed to read {HASH_FILE}: {e}")
    return None

def _save_hash(hash_value: str):
    """Save hash to .last_sha file"""
    try:
        with open(HASH_FILE, "w") as f:
            f.write(hash_value)
    except Exception as e:
        print(f"[db_manager] WARNING: Failed to save hash to {HASH_FILE}: {e}")

# Public API functions

def restore_sqlite_if_missing():
    """
    Restore SQLite database from Google Drive if missing locally.
    If ff.sqlite exists locally → do nothing
    If not, download it from Google Drive
    """
    print("[db_manager] Checking if SQLite restore is needed...")
    
    if os.path.exists(SQLITE_FILE):
        print(f"[db_manager] {SQLITE_FILE} already exists locally, no restore needed")
        return
    
    print(f"[db_manager] {SQLITE_FILE} not found locally, attempting restore from Google Drive")
    
    try:
        drive_manager = _get_drive_manager()
        
        if not drive_manager.authenticated:
            print("[db_manager] Google Drive not authenticated, cannot restore database")
            print("[db_manager] App will continue with empty database")
            return
        
        # Find the SQLite file in Google Drive
        file_id = drive_manager.find_sqlite_file()
        
        if not file_id:
            print(f"[db_manager] {SQLITE_FILE} not found in Google Drive")
            print("[db_manager] App will start with empty database")
            return
        
        # Download the file
        success = drive_manager.download_sqlite(file_id)
        
        if success:
            print(f"[db_manager] Database restored successfully from Google Drive")
            # Update hash after successful restore
            current_hash = _calculate_file_hash(SQLITE_FILE)
            if current_hash:
                _save_hash(current_hash)
        else:
            print("[db_manager] Failed to restore database, app will start with empty database")
    
    except Exception as e:
        print(f"[db_manager] ERROR: Exception during restore: {e}")
        print("[db_manager] App will continue with empty database")

def upload_sqlite_if_changed():
    """
    Upload SQLite database to Google Drive if it has changed.
    Compares SHA256 hash with last known hash and uploads only if different.
    """
    print("[db_manager] Checking if SQLite backup is needed...")
    
    if not os.path.exists(SQLITE_FILE):
        print(f"[db_manager] {SQLITE_FILE} does not exist, no backup needed")
        return
    
    try:
        # Calculate current hash
        current_hash = _calculate_file_hash(SQLITE_FILE)
        if not current_hash:
            print("[db_manager] Failed to calculate file hash, skipping backup")
            return
        
        # Compare with last known hash
        last_hash = _get_last_hash()
        
        if last_hash == current_hash:
            print("[db_manager] Database unchanged, skipping backup")
            return
        
        print(f"[db_manager] Database changed (hash: {current_hash[:8]}...), uploading to Google Drive")
        
        # Upload to Google Drive
        drive_manager = _get_drive_manager()
        
        if not drive_manager.authenticated:
            print("[db_manager] Google Drive not authenticated, cannot backup database")
            return
        
        success = drive_manager.upload_sqlite()
        
        if success:
            _save_hash(current_hash)
            print("[db_manager] Database backup completed successfully")
        else:
            print("[db_manager] Database backup failed")
    
    except Exception as e:
        print(f"[db_manager] ERROR: Exception during backup: {e}")

def start_periodic_backup(interval_minutes: int = 30):
    """
    Start background thread for periodic SQLite backups.
    Runs upload_sqlite_if_changed() every interval_minutes.
    """
    def backup_worker():
        print(f"[db_manager] Starting periodic backup worker (interval: {interval_minutes} minutes)")
        
        while True:
            try:
                time.sleep(interval_minutes * 60)  # Convert minutes to seconds
                print("[db_manager] Running periodic backup check...")
                upload_sqlite_if_changed()
            except Exception as e:
                print(f"[db_manager] ERROR: Exception in periodic backup worker: {e}")
                # Continue running even if backup fails
    
    # Start daemon thread
    backup_thread = threading.Thread(target=backup_worker, daemon=True)
    backup_thread.start()
    print(f"[db_manager] Periodic backup started (every {interval_minutes} minutes)")

# Cleanup function for temporary files
def cleanup_temp_files():
    """Clean up temporary credential files"""
    try:
        for temp_file in ["client_secrets.json", "token.json"]:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"[db_manager] Cleaned up {temp_file}")
    except Exception as e:
        print(f"[db_manager] WARNING: Failed to cleanup temp files: {e}")

# Auto-cleanup on module import (optional)
import atexit
atexit.register(cleanup_temp_files)