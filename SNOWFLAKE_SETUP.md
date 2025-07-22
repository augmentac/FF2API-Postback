# 🏗️ Snowflake Integration Setup Guide

## Overview

The FF2API Platform now integrates with your GoAugment Snowflake warehouse to enrich freight data with real operational intelligence from your DBT models.

---

## 🔧 Snowflake Configuration

### Step 1: Create Service Account
Create a read-only service account in Snowflake:

```sql
-- Create service user
CREATE USER ff2api_service 
PASSWORD = 'secure_password_here'
DEFAULT_WAREHOUSE = 'ANALYTICS_WH'
DEFAULT_DATABASE = 'AUGMENT_DW'
DEFAULT_SCHEMA = 'MARTS';

-- Create read-only role
CREATE ROLE ff2api_reader;

-- Grant permissions
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE ff2api_reader;
GRANT USAGE ON DATABASE AUGMENT_DW TO ROLE ff2api_reader;
GRANT USAGE ON SCHEMA AUGMENT_DW.MARTS TO ROLE ff2api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA AUGMENT_DW.MARTS TO ROLE ff2api_reader;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AUGMENT_DW.MARTS TO ROLE ff2api_reader;

-- Assign role to user
GRANT ROLE ff2api_reader TO USER ff2api_service;
ALTER USER ff2api_service SET DEFAULT_ROLE = 'ff2api_reader';
```

### Step 2: Configure Streamlit Secrets
Add to your Streamlit Cloud app secrets:

```toml
[snowflake]
ACCOUNT = "your-account.snowflakecomputing.com"
USER = "ff2api_service"
PASSWORD = "secure_password_here"
DATABASE = "AUGMENT_DW"
SCHEMA = "MARTS"
WAREHOUSE = "ANALYTICS_WH"

[email]
SMTP_USER = "your-app-email@gmail.com"
SMTP_PASS = "your-app-password"
```

---

## 📊 Required DBT Models

The system expects these models from your GoAugment DBT project:

### **`fct_tracking_events`**
```sql
-- Expected columns:
pro_number (string)
current_status (string) 
scan_location (string)
scan_datetime (timestamp)
estimated_delivery_date (timestamp)
```

### **`dim_customers`** 
```sql
-- Expected columns:
customer_code (string)
customer_name (string)
account_manager (string)
payment_terms (string)
customer_tier (string)
```

### **`dim_carriers`**
```sql
-- Expected columns:
carrier_code (string)
carrier_name (string)
on_time_percentage (float)
service_levels (string)
```

### **`fct_shipments`**
```sql
-- Expected columns:
origin_zip (string)
dest_zip (string)
carrier_code (string)
transit_days (integer)
total_cost (float)
ship_date (date)
```

---

## 🎯 How It Works

### **Data Enhancement Process:**

1. **User uploads CSV** with load data
2. **System identifies join keys** (PRO, customer_code, carrier, etc.)
3. **Queries your DBT models** for matching records
4. **Adds new columns** with prefix `sf_` to avoid conflicts
5. **Returns enhanced dataset** with original + warehouse data

### **Example Enhancement:**

**Input CSV:**
```csv
load_id,carrier,PRO,customer_code,origin_zip,dest_zip
LOAD001,FEDEX,123456789,ABC123,60601,30309
```

**Enhanced Output:**
```csv
load_id,carrier,PRO,customer_code,origin_zip,dest_zip,sf_tracking_status,sf_last_scan_location,sf_customer_name,sf_account_manager,sf_carrier_name,sf_avg_transit_days
LOAD001,FEDEX,123456789,ABC123,60601,30309,In Transit,Atlanta GA Hub,ABC Manufacturing Corp,John Smith,Federal Express,2.3
```

---

## 🎨 User Interface Options

### **Available Enrichments:**
- **📍 Latest Tracking Status** - Current tracking info from `fct_tracking_events`
- **👤 Customer Information** - Customer details from `dim_customers`  
- **🚚 Carrier Details** - Carrier performance from `dim_carriers`
- **🛣️ Lane Performance** - Historical lane data from `fct_shipments`

### **Added Columns by Type:**

#### **Tracking Enrichment:**
- `sf_tracking_status` - "In Transit", "Delivered", etc.
- `sf_last_scan_location` - "Atlanta GA Hub"
- `sf_last_scan_time` - "2024-07-21T15:30:00"
- `sf_estimated_delivery` - "2024-07-23T17:00:00"

#### **Customer Enrichment:**
- `sf_customer_name` - "ABC Manufacturing Corp"
- `sf_account_manager` - "John Smith" 
- `sf_payment_terms` - "NET 30"
- `sf_customer_tier` - "Platinum"

#### **Carrier Enrichment:**
- `sf_carrier_name` - "Federal Express"
- `sf_carrier_otp` - 94.2 (on-time percentage)
- `sf_service_levels` - "Ground, Express, Freight"

#### **Lane Performance:**
- `sf_avg_transit_days` - 2.3
- `sf_avg_lane_cost` - 1250.75
- `sf_lane_volume` - 45 (shipments in last 90 days)

---

## 🚨 Troubleshooting

### **"Snowflake connector not available"**
- Ensure `snowflake-connector-python>=3.0.0` is in requirements.txt
- Redeploy Streamlit Cloud app to install dependency

### **"Snowflake credentials not configured"**
- Check Streamlit secrets are set correctly
- Verify all required fields: ACCOUNT, USER, PASSWORD, etc.

### **"Failed to connect to Snowflake"**
- Test credentials in Snowflake console first
- Verify service account has correct permissions
- Check warehouse is available and user has access

### **"No data returned from queries"**
- Verify DBT models exist in AUGMENT_DW.MARTS schema
- Check table names match expected models
- Ensure data exists for the PRO/customer_code being queried

### **Connection Timeouts:**
- Use dedicated warehouse for analytics
- Consider upgrading warehouse size for better performance
- Implement connection pooling if needed

---

## 📈 Performance Optimization

### **Query Optimization:**
- Queries use single-row lookups with indexed keys
- Connection reuse minimizes overhead
- Error handling prevents failures from blocking processing

### **Data Freshness:**
- Uses live Snowflake data (as fresh as your DBT runs)
- No caching - always gets latest information
- Relies on your existing DBT refresh schedule

### **Scaling Considerations:**
- Designed for batch sizes up to 10,000 rows
- Connection pooling handles concurrent requests
- Warehouse auto-scaling manages query load

---

## 💰 Cost Impact

### **Snowflake Costs:**
- **Query costs** based on warehouse usage (minimal for lookups)
- **Storage costs** unchanged (no new data stored)
- **Compute costs** scale with usage volume

### **Optimization Tips:**
- Use smaller warehouse for analytics queries
- Consider query result caching for repeated lookups
- Monitor warehouse usage in Snowflake console

---

**Your GoAugment warehouse integration is ready! 🏗️📊**

This transforms your basic load files into comprehensive operational intelligence using your existing data investment.