# 🔄 End-to-End Load Processing Guide

## Overview

The End-to-End Load Processing system provides a complete freight workflow that connects CSV upload, FF2API processing, load ID retrieval, Snowflake enrichment, and postback delivery in one seamless operation.

---

## 🚀 Complete Workflow

### **The Full Pipeline:**
```
📁 CSV Upload → ⚙️ FF2API Processing → 🔗 Load ID Mapping → 🏗️ Snowflake Enrichment → 📤 Postback Results
```

### **What Happens at Each Step:**

#### **1. CSV Upload & Validation** 📁
- Upload freight load data (CSV or JSON)
- Validate required fields (`load_id`)
- Preview data and check for recommended fields
- Identify fields available for processing

#### **2. FF2API Load Processing** ⚙️
- Process loads through existing FF2API system
- Create loads in carrier systems
- Capture `load.loadNumber` from successful processing
- Track success/failure for each CSV row

#### **3. Load ID Mapping** 🔗
- Call GoAugment API for each processed load:
  ```
  GET https://load.prod.goaugment.com/unstable/loads/brokerage-key/augment-brokerage/brokerage-load-id/{loadNumber}
  ```
- Extract `internal_load_id` from API response
- Map CSV rows to internal system load IDs
- Handle API failures gracefully

#### **4. Snowflake Enrichment** 🏗️
- Use `internal_load_id` for comprehensive warehouse lookups
- Query `dim_loads` table for complete load information
- Add customer, carrier, tracking, and financial data
- Include historical performance metrics

#### **5. Postback Results** 📤
- Send enhanced data via email, files, or webhooks
- Include original CSV data + processing status + warehouse enrichment
- Provide download options in multiple formats
- Track delivery success/failure

---

## 🎯 User Interface

### **Application Selection**
Choose "End-to-End Load Processing" from the main navigation dropdown.

### **Configuration Sidebar**

#### **Load Processing Settings:**
- **Brokerage Key**: Identifier for API calls (default: "augment-brokerage")
- **API Timeout**: How long to wait for API responses (10-120 seconds)
- **Retry Count**: Number of retries for failed API calls (1-5)

#### **Enrichment Settings:**
- **Enable Mock Tracking**: Add simulated tracking events
- **Enable Snowflake Enrichment**: Use real warehouse data
  - Load Tracking Data
  - Customer Information
  - Carrier Details
  - Lane Performance

#### **Postback Settings:**
- **Output Formats**: CSV, Excel, JSON
- **Email Delivery**: Send results via email
- **Advanced Settings**: Logging level, processing options

### **Main Interface**

#### **Upload Section:**
- File uploader with validation
- Data preview (first 10 rows)
- Field validation status
- Missing field warnings

#### **Processing Section:**
- Large "Start End-to-End Processing" button
- Real-time workflow progress indicators
- Step-by-step status updates
- Error messages and troubleshooting

---

## 📊 Data Enhancement

### **Input Data Requirements:**
```csv
load_id,carrier,PRO,customer_code,origin_zip,dest_zip,shipper_name
LOAD001,FEDEX,123456789,ABC123,60601,30309,ABC Manufacturing
```

### **Enhanced Output Includes:**
```csv
load_id,carrier,PRO,customer_code,origin_zip,dest_zip,shipper_name,load_number,internal_load_id,sf_load_status,sf_pickup_date,sf_delivery_date,sf_total_cost,sf_customer_name,sf_account_manager,sf_tracking_status,sf_last_scan_location,sf_estimated_delivery
LOAD001,FEDEX,123456789,ABC123,60601,30309,ABC Manufacturing,CSVLOAD00175279,LID-987654321,Active,2024-07-20T08:00:00,2024-07-23T17:00:00,1250.75,ABC Manufacturing Corp,John Smith,In Transit,Atlanta GA Hub,2024-07-23T17:00:00
```

### **New Data Columns Added:**

#### **Processing Results:**
- `load_number` - FF2API generated load number
- `internal_load_id` - GoAugment system internal ID
- `load_id_status` - API lookup success/failure

#### **Load Information:**
- `sf_load_status` - Current load status
- `sf_pickup_date` - Scheduled pickup date
- `sf_delivery_date` - Scheduled/actual delivery date
- `sf_total_cost` - Complete load cost

#### **Customer Data:**
- `sf_customer_name` - Full customer name
- `sf_account_manager` - Assigned account manager
- `sf_payment_terms` - Payment terms (NET 30, etc.)

#### **Tracking Information:**
- `sf_tracking_status` - Current tracking status
- `sf_last_scan_location` - Last scan location
- `sf_last_scan_time` - Last scan timestamp
- `sf_estimated_delivery` - Carrier estimated delivery

---

## ⚙️ Configuration

### **Streamlit Secrets Required:**

```toml
# API Authentication
[api]
API_KEY = "your_goaugment_api_key"

# Snowflake Connection
[snowflake]
ACCOUNT = "your-account.snowflakecomputing.com"
USER = "service_account"
PASSWORD = "secure_password"
DATABASE = "AUGMENT_DW"
SCHEMA = "MARTS"
WAREHOUSE = "ANALYTICS_WH"

# Email Configuration (Optional)
[email]
SMTP_USER = "your-app-email@gmail.com"
SMTP_PASS = "your-app-password"
```

### **API Endpoint Configuration:**
- **Base URL**: `https://load.prod.goaugment.com/unstable/loads`
- **Endpoint Pattern**: `/brokerage-key/{brokerage_key}/brokerage-load-id/{load_number}`
- **Authentication**: Bearer token via API_KEY

---

## 📈 Workflow Progress Tracking

### **Visual Progress Indicators:**
1. **📁 Upload** - File validation and data preview
2. **⚙️ Process** - FF2API load processing
3. **🔗 Map IDs** - Internal load ID retrieval
4. **🏗️ Enrich** - Snowflake data enhancement
5. **📤 Postback** - Results delivery

### **Success Metrics Display:**
- Total rows processed
- FF2API processing success rate
- Load IDs successfully retrieved
- Rows enriched with warehouse data
- Postback handlers success rate

### **Error Handling:**
- **Partial Success**: Some loads succeed, others fail
- **Graceful Degradation**: Continue with available data
- **Detailed Error Messages**: Specific failure reasons
- **Recovery Options**: Retry failed steps where possible

---

## 🚨 Troubleshooting

### **Common Issues:**

#### **"FF2API Processing Failed"**
- Check FF2API system availability
- Verify load data format and required fields
- Review FF2API authentication and permissions

#### **"Load ID API Calls Failing"**
- Verify API_KEY in Streamlit secrets
- Check brokerage_key configuration
- Ensure load numbers exist in GoAugment system
- Review API endpoint URL and format

#### **"Snowflake Enrichment Errors"**
- Verify Snowflake credentials and permissions
- Check that DBT models exist (dim_loads, dim_customers, etc.)
- Ensure internal_load_id format matches warehouse schema
- Review database and schema names

#### **"No Data Enhanced"**
- Check if internal_load_ids were successfully retrieved
- Verify Snowflake tables contain data for the load IDs
- Review enrichment configuration and enabled options

### **Performance Tips:**
- **Batch Size**: Process 100-1000 loads at a time for optimal performance
- **API Timeouts**: Increase timeout for large batches
- **Retry Logic**: Use retry settings for transient API failures
- **Error Monitoring**: Check logs for detailed error information

---

## 💡 Best Practices

### **Data Preparation:**
- Ensure `load_id` field is unique and meaningful
- Include carrier, PRO, customer_code for better enrichment
- Clean data before upload to minimize processing errors
- Use consistent field naming conventions

### **Workflow Management:**
- **Test with Small Batches**: Start with 10-50 loads
- **Monitor Progress**: Watch each step for issues
- **Download Backups**: Keep copies of results
- **Error Review**: Check failed loads and retry if needed

### **Result Utilization:**
- **Data Analysis**: Use enriched data for operational insights
- **Reporting**: Create dashboards from enhanced datasets
- **Integration**: Feed results into other systems via APIs
- **Archival**: Store complete datasets for historical analysis

---

This end-to-end system transforms simple CSV upload into comprehensive freight intelligence using your complete operational data stack! 🚀📊🔄

---

**Ready to process freight loads with complete operational intelligence!**