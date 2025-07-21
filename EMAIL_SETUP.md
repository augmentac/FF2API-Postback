# 📧 Email Postback Setup Guide

## Overview

The FF2API Platform now supports sending enriched freight data via email automatically. This guide shows you how to set it up using Gmail for **zero cost**.

---

## 🔧 Gmail Setup (One-time)

### Step 1: Create/Use Gmail Account
- Use any Gmail account (personal or create new one for the app)
- Example: `freight-data@gmail.com`

### Step 2: Enable App Passwords
1. Go to [myaccount.google.com](https://myaccount.google.com)
2. Click **Security** → **2-Step Verification** (enable if not already)
3. Go back to **Security** → **App passwords**
4. Generate app password for "Mail"
5. **Save this password** - you'll need it for Streamlit secrets

---

## 🔐 Streamlit Cloud Configuration

### Add Email Secrets
In your Streamlit Cloud app settings, add these secrets:

```toml
[email]
SMTP_USER = "your-app-email@gmail.com"
SMTP_PASS = "your-16-char-app-password"
```

**Important:** Use the 16-character app password, not your regular Gmail password!

---

## 🎯 How to Use

### In the Application:
1. Upload your CSV/JSON file
2. Configure enrichment settings
3. **Check "Send Results via Email"**
4. **Enter recipient email address**
5. Optionally customize email subject
6. Click "Process & Enrich Data"

### Email Features:
- **Automatic delivery** of CSV attachment
- **Summary in email body** (record counts, processing time)
- **Professional email format**
- **Timestamped filenames**
- **Works with any email provider** (Gmail, Outlook, company email, etc.)

---

## 📧 Sample Email Output

**Subject:** `Freight Data Results - 25 records`

**Body:**
```
Hello,

Your freight data processing is complete.

Summary:
• Records processed: 25
• Records enriched: 23
• Records with tracking: 20
• Processing time: 2024-07-21 15:30

Please find the enriched data attached as a CSV file.

Best regards,
FF2API System
```

**Attachment:** `freight_data_20240721_153045.csv`

---

## 💡 Usage Tips

### Email-Only Mode:
- Don't select any output formats
- Just enable email
- Perfect for automated workflows

### Combined Mode:
- Select output formats AND enable email
- Download files immediately + get email copy
- Best of both worlds

### Multiple Recipients:
- Enter multiple emails separated by commas
- Example: `ops@company.com, freight@company.com`

---

## 🚨 Troubleshooting

### "Email credentials not configured"
- Check Streamlit secrets are set correctly
- Verify SMTP_USER and SMTP_PASS are exact

### "SMTP authentication failed"
- Use app password, not regular Gmail password
- Ensure 2-step verification is enabled on Gmail
- Try generating a new app password

### "Email delivery failed"
- Check recipient email address for typos
- Verify Gmail account has sending permissions
- Check if recipient's email blocks attachments

### Testing Email Setup:
1. Use your own email as recipient first
2. Check spam/junk folders
3. Try with small test file first

---

## 🔒 Security Notes

- **Gmail credentials are encrypted** in Streamlit secrets
- **Emails sent via secure SMTP** (port 587 with TLS)
- **No data stored permanently** - only sent via email
- **App password can be revoked** anytime in Gmail settings

---

## 💰 Cost Breakdown

- **Gmail account:** FREE
- **App password:** FREE  
- **SMTP sending:** FREE (Gmail's daily limits apply)
- **Streamlit hosting:** FREE
- **Total cost:** $0

---

## 📈 Gmail Limits

Gmail free accounts have these limits:
- **500 emails per day**
- **25MB attachment limit**
- **Recipients per email:** 500

These limits are **more than sufficient** for most freight data workflows!

---

**Your email postback system is ready! 📧✨**

For support, check the main application logs or contact your administrator.