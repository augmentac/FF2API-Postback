# FF2API Platform - Streamlit Cloud Deployment

## 📋 Overview

This guide covers deploying the FF2API Platform (including the new Postback & Enrichment system) to Streamlit Cloud for internal company use. The platform now provides two main applications:

1. **FF2API - Load Processing**: Secure CSV-to-API data processing
2. **Postback & Enrichment System**: Freight data enrichment and multi-format export

## 🔧 Prerequisites

- GitHub account with repository access
- Streamlit Cloud account (free tier sufficient)
- Team member with admin access to configure secrets

## 🚀 Deployment Steps

### 1. Repository Preparation

1. **Clone the repository** to your local machine
2. **Review configuration files** in `.streamlit/` directory
3. **Update application settings** if needed

### 2. Secrets Configuration

Create secrets in Streamlit Cloud dashboard:

#### Required Secrets

```toml
[general]
APP_NAME = "FF2API"
APP_VERSION = "1.0.0"
ENVIRONMENT = "production"

[auth]
APP_PASSWORD = "YourSecureTeamPassword123!"

[database]
ENCRYPTION_KEY = "gAAAAABhZ8x7K..."  # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

[api]
DEFAULT_API_BASE_URL = "https://api.prod.goaugment.com"
```

#### Generating Encryption Key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Streamlit Cloud Setup

1. **Connect Repository**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Connect your GitHub account
   - Select the FF2API repository

2. **Configure Deployment**
   - Main file path: `src/frontend/app.py`
   - Python version: 3.9+
   - Requirements: `requirements.txt`

3. **Add Secrets**
   - Click "Advanced settings"
   - Paste your secrets configuration
   - Save deployment

### 4. Access Configuration

The application will be available at:
```
https://your-app-name.streamlit.app
```

**Important**: Share this URL only with authorized team members.

## 🔐 Security Considerations

### Authentication
- Simple password protection for internal use
- No user management system (shared access)
- Password stored in Streamlit secrets

### Data Security
- Database encryption for sensitive data
- Ephemeral storage (data lost on restart)
- Manual backup/restore workflow

### Team Access
- All team members share the same environment
- No user isolation or permissions
- Collaborative database usage

## 💾 Backup & Recovery

### Manual Backup Process
1. **Daily Backup Routine**
   - Last person of the day downloads backup
   - Store in shared company folder
   - Include timestamp in filename

2. **Recovery Process**
   - First person of the day restores backup
   - Upload JSON file through UI
   - Verify data integrity

### Emergency Procedures
- Container restart causes data loss
- Always download backup before major operations
- Keep multiple backup versions

## 👥 Team Workflow

### Daily Operations
1. **Morning**: Check if backup restore needed
2. **During day**: Normal usage, shared database
3. **Evening**: Download backup for safety

### Best Practices
- Communicate major operations to team
- Download backup before large imports
- Keep shared folder organized

## 📊 Monitoring

### Application Health
- Container age monitoring
- Database size tracking
- Backup recommendations

### Usage Analytics
- Processing statistics
- Error rate monitoring
- Performance metrics

## 🔄 Maintenance

### Regular Tasks
- Monitor container uptime
- Review backup procedures
- Update secrets if needed

### Troubleshooting
- Check Streamlit Cloud logs
- Verify secrets configuration
- Test backup/restore functionality

## 🆘 Support

### Common Issues
1. **Access Denied**: Check APP_PASSWORD in secrets
2. **Data Missing**: Restore from backup
3. **Performance Issues**: Container restart recommended

### Contact Information
- Internal IT support for access issues
- Development team for functionality problems
- Team lead for workflow questions

## 📚 Additional Resources

- [Streamlit Cloud Documentation](https://docs.streamlit.io/streamlit-cloud)
- [Application User Guide](README.md)
- [Backup Procedures](BACKUP_PROCEDURES.md)

---

**Note**: This application is designed for internal use only. Do not share access credentials or URLs with external parties. 