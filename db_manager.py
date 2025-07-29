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
            # Check if required secrets are available
            if not self._check_secrets_available():
                print("[db_manager] Google OAuth secrets not configured - backup system will be disabled")
                return
                
            # Generate client_secrets.json from Streamlit secrets
            self._create_client_secrets()
            
            # Try to create token file from stored tokens
            if not self._create_token_file():
                print("[db_manager] No stored OAuth tokens - Google Drive setup required")
                print("[db_manager] Use the backup dashboard to complete OAuth setup")
                return
            
            # Initialize PyDrive2 authentication
            gauth = GoogleAuth()
            gauth.LoadClientConfigFile("client_secrets.json")
            gauth.LoadCredentialsFile("token.json")
            
            if gauth.credentials is None:
                print("[db_manager] ERROR: No valid credentials found")
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
    
    def _check_secrets_available(self):
        """Check if required Google OAuth client credentials are available"""
        try:
            required_keys = ['client_id', 'client_secret']
            google_secrets = st.secrets.get("google", {})
            
            for key in required_keys:
                if not google_secrets.get(key):
                    print(f"[db_manager] Missing required secret: google.{key}")
                    return False
            
            return True
            
        except Exception as e:
            print(f"[db_manager] Error checking secrets: {e}")
            return False
    
    def _get_stored_tokens(self):
        """Get OAuth tokens from database storage"""
        try:
            import sqlite3
            
            if not os.path.exists(SQLITE_FILE):
                return None
                
            conn = sqlite3.connect(SQLITE_FILE)
            cursor = conn.cursor()
            
            # Create tokens table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                    service TEXT PRIMARY KEY,
                    access_token TEXT,
                    refresh_token TEXT,
                    expires_at INTEGER,
                    created_at INTEGER
                )
            """)
            
            # Get Google Drive tokens
            cursor.execute("SELECT access_token, refresh_token, expires_at FROM oauth_tokens WHERE service = ?", ('google_drive',))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'access_token': result[0],
                    'refresh_token': result[1], 
                    'expires_at': result[2]
                }
            return None
            
        except Exception as e:
            print(f"[db_manager] Error getting stored tokens: {e}")
            return None
    
    def _store_tokens(self, access_token, refresh_token, expires_in=3600):
        """Store OAuth tokens in database"""
        try:
            import sqlite3
            import time
            
            # Ensure database exists
            conn = sqlite3.connect(SQLITE_FILE)
            cursor = conn.cursor()
            
            # Create tokens table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                    service TEXT PRIMARY KEY,
                    access_token TEXT,
                    refresh_token TEXT,
                    expires_at INTEGER,
                    created_at INTEGER
                )
            """)
            
            # Store/update tokens
            expires_at = int(time.time()) + expires_in
            created_at = int(time.time())
            
            cursor.execute("""
                INSERT OR REPLACE INTO oauth_tokens 
                (service, access_token, refresh_token, expires_at, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, ('google_drive', access_token, refresh_token, expires_at, created_at))
            
            conn.commit()
            conn.close()
            print("[db_manager] OAuth tokens stored successfully")
            return True
            
        except Exception as e:
            print(f"[db_manager] Error storing tokens: {e}")
            return False
    
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
        """Create token.json from stored database tokens"""
        try:
            # Get tokens from database
            stored_tokens = self._get_stored_tokens()
            if not stored_tokens:
                print("[db_manager] No stored tokens found - OAuth setup required")
                return False
                
            token_data = {
                "access_token": stored_tokens["access_token"],
                "refresh_token": stored_tokens["refresh_token"],
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": st.secrets["google"]["client_id"],
                "client_secret": st.secrets["google"]["client_secret"],
                "scopes": ["https://www.googleapis.com/auth/drive.file"]
            }
            
            with open("token.json", "w") as f:
                json.dump(token_data, f, indent=2)
            
            print("[db_manager] Created token.json from stored tokens")
            return True
            
        except Exception as e:
            print(f"[db_manager] ERROR: Failed to create token.json: {e}")
            return False
    
    def _save_refreshed_token(self, gauth):
        """Save refreshed token back to database and token.json"""
        try:
            access_token = gauth.credentials.access_token
            refresh_token = gauth.credentials.refresh_token
            
            # Store in database
            expires_in = getattr(gauth.credentials, 'expires_in', 3600)
            self._store_tokens(access_token, refresh_token, expires_in)
            
            # Also update token.json for current session
            token_data = {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": st.secrets["google"]["client_id"],
                "client_secret": st.secrets["google"]["client_secret"],
                "scopes": ["https://www.googleapis.com/auth/drive.file"]
            }
            
            with open("token.json", "w") as f:
                json.dump(token_data, f, indent=2)
            
            print("[db_manager] Refreshed token saved to database and session")
            
        except Exception as e:
            print(f"[db_manager] WARNING: Failed to save refreshed token: {e}")
    
    def setup_oauth_flow(self):
        """Generate OAuth authorization URL for user setup"""
        try:
            if not self._check_secrets_available():
                return None, "Client credentials not configured in secrets"
            
            # Create client secrets for OAuth flow
            self._create_client_secrets()
            
            # Generate authorization URL
            params = {
                'client_id': st.secrets["google"]["client_id"],
                'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
                'scope': 'https://www.googleapis.com/auth/drive.file',
                'response_type': 'code',
                'access_type': 'offline',
                'prompt': 'consent'
            }
            
            import urllib.parse
            base_url = "https://accounts.google.com/o/oauth2/auth"
            auth_url = f"{base_url}?{urllib.parse.urlencode(params)}"
            
            return auth_url, None
            
        except Exception as e:
            return None, f"Failed to setup OAuth flow: {e}"
    
    def complete_oauth_flow(self, authorization_code):
        """Complete OAuth flow with authorization code"""
        try:
            import requests
            
            if not self._check_secrets_available():
                return False, "Client credentials not configured"
            
            # Exchange code for tokens
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                'client_id': st.secrets["google"]["client_id"],
                'client_secret': st.secrets["google"]["client_secret"],
                'code': authorization_code,
                'grant_type': 'authorization_code',
                'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob'
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            
            tokens = response.json()
            access_token = tokens.get('access_token')
            refresh_token = tokens.get('refresh_token')
            expires_in = tokens.get('expires_in', 3600)
            
            if not access_token or not refresh_token:
                return False, "Failed to get valid tokens from Google"
            
            # Store tokens in database
            if self._store_tokens(access_token, refresh_token, expires_in):
                print("[db_manager] OAuth setup completed successfully")
                # Try to initialize Drive connection
                self._initialize_drive()
                return True, "OAuth setup completed successfully"
            else:
                return False, "Failed to store tokens"
            
        except Exception as e:
            return False, f"OAuth flow failed: {e}"
    
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

# Backup status monitoring functions

def get_backup_status() -> dict:
    """Get comprehensive backup system status including database size and health"""
    import sqlite3
    from datetime import datetime
    
    status = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'database_exists': False,
        'database_size_mb': 0,
        'record_count': 0,
        'tables': {},
        'google_drive_connected': False,
        'last_backup_hash': None,
        'backup_needed': False,
        'system_health': 'Unknown'
    }
    
    try:
        # Check if database exists and get size
        if os.path.exists(SQLITE_FILE):
            status['database_exists'] = True
            status['database_size_mb'] = round(os.path.getsize(SQLITE_FILE) / (1024 * 1024), 2)
            
            # Get table info and record counts
            try:
                conn = sqlite3.connect(SQLITE_FILE)
                cursor = conn.cursor()
                
                # Get table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                total_records = 0
                for (table_name,) in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    status['tables'][table_name] = count
                    total_records += count
                
                status['record_count'] = total_records
                conn.close()
                
            except Exception as e:
                print(f"[db_manager] WARNING: Failed to query database tables: {e}")
        
        # Check Google Drive connection
        drive_manager = _get_drive_manager()
        status['google_drive_connected'] = drive_manager.authenticated
        
        # Check backup status
        if status['database_exists']:
            current_hash = _calculate_file_hash(SQLITE_FILE)
            last_hash = _get_last_hash()
            status['last_backup_hash'] = last_hash[:8] + '...' if last_hash else 'None'
            status['backup_needed'] = current_hash != last_hash
        
        # Determine system health
        if status['google_drive_connected'] and status['database_exists']:
            if status['backup_needed']:
                status['system_health'] = 'Backup Pending'
            else:
                status['system_health'] = 'Healthy'
        elif status['database_exists']:
            status['system_health'] = 'Drive Disconnected'
        else:
            status['system_health'] = 'No Database'
            
    except Exception as e:
        print(f"[db_manager] ERROR: Failed to get backup status: {e}")
        status['system_health'] = 'Error'
    
    return status

def render_backup_status_dashboard():
    """Render the backup status dashboard in Streamlit"""
    st.subheader("🔄 Database Backup Status")
    
    status = get_backup_status()
    
    # Status overview with metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        health_color = {
            'Healthy': '🟢',
            'Backup Pending': '🟡',
            'Drive Disconnected': '🟠',
            'No Database': '⚪',
            'Error': '🔴',
            'Unknown': '⚫'
        }
        st.metric(
            "System Health",
            f"{health_color.get(status['system_health'], '⚫')} {status['system_health']}"
        )
    
    with col2:
        st.metric(
            "Database Size",
            f"{status['database_size_mb']} MB" if status['database_exists'] else "No DB",
            help="Current SQLite database file size"
        )
    
    with col3:
        st.metric(
            "Total Records",
            f"{status['record_count']:,}" if status['database_exists'] else "0",
            help="Total records across all database tables"
        )
    
    with col4:
        if status['google_drive_connected']:
            drive_status = "✅ Connected"
            drive_help = "Google Drive backup is active"
        else:
            drive_status = "❌ Not Configured"
            drive_help = "Google OAuth credentials needed in Streamlit secrets"
        
        st.metric(
            "Google Drive",
            drive_status,
            help=drive_help
        )
    
    # Detailed information
    with st.expander("📊 Database Details", expanded=False):
        if status['database_exists']:
            st.write("**Database Tables:**")
            for table, count in status['tables'].items():
                st.write(f"• {table}: {count:,} records")
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.write(f"**Last Backup Hash:** `{status['last_backup_hash']}`")
            with col_b:
                backup_status_text = "⚠️ Needed" if status['backup_needed'] else "✅ Up to date"
                st.write(f"**Backup Status:** {backup_status_text}")
        else:
            st.info("No database file found. Database will be created when data is processed.")
        
        # Show Google Drive configuration status
        if not status['google_drive_connected']:
            st.markdown("**Google Drive Setup Required:**")
            
            # Check if client secrets are available
            drive_manager = _get_drive_manager()
            if drive_manager._check_secrets_available():
                st.markdown("**Step 1: Authorize Google Drive Access**")
                
                # OAuth setup interface
                auth_url, error = drive_manager.setup_oauth_flow()
                
                if error:
                    st.error(f"Setup error: {error}")
                else:
                    st.markdown(f"[🔗 **Click here to authorize**]({auth_url})")
                    st.markdown("This will open Google's authorization page where you can grant access.")
                    
                    # Authorization code input
                    auth_code = st.text_input(
                        "Enter the authorization code:",
                        placeholder="4/0AdQt8qh...",
                        help="Copy the code from Google's authorization page"
                    )
                    
                    if st.button("✅ Complete Setup"):
                        if auth_code:
                            with st.spinner("Setting up Google Drive access..."):
                                success, message = drive_manager.complete_oauth_flow(auth_code)
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        else:
                            st.error("Please enter the authorization code")
            else:
                st.markdown("""
                **Step 1: Configure Client Credentials**
                
                Add these to your Streamlit Cloud app secrets:
                ```toml
                [google]
                client_id = "your-client-id.googleusercontent.com"
                client_secret = "your-client-secret"
                ```
                
                **Step 2: OAuth Setup**
                
                After adding client credentials, restart the app and use the OAuth flow above to automatically generate access tokens.
                """)
    
    # Action buttons
    col_x, col_y = st.columns(2)
    
    with col_x:
        if st.button("🔄 Force Backup Now", disabled=not status['database_exists'] or not status['google_drive_connected']):
            with st.spinner("Uploading database to Google Drive..."):
                upload_sqlite_if_changed()
                st.success("Backup completed!")
                st.rerun()
    
    with col_y:
        if st.button("📥 Restore from Drive", disabled=not status['google_drive_connected']):
            with st.spinner("Restoring database from Google Drive..."):
                # Backup current DB if it exists
                if status['database_exists']:
                    backup_name = f"ff_backup_{int(time.time())}.sqlite"
                    os.rename(SQLITE_FILE, backup_name)
                    st.info(f"Current database backed up as {backup_name}")
                
                restore_sqlite_if_missing()
                st.success("Database restored from Google Drive!")
                st.rerun()
    
    # Status timestamp
    st.caption(f"Last updated: {status['timestamp']}")

# Auto-cleanup on module import (optional)
import atexit
atexit.register(cleanup_temp_files)