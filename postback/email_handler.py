"""Email postback handler using Gmail SMTP."""

import smtplib
import csv
import io
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Dict, Any
import logging
from datetime import datetime
from .base import PostbackHandler

logger = logging.getLogger(__name__)


class EmailPostbackHandler(PostbackHandler):
    """Handler that sends enriched rows via email using Gmail SMTP."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.smtp_server = config.get('smtp_server', 'smtp.gmail.com')
        self.smtp_port = config.get('smtp_port', 587)
        self.smtp_user = config.get('smtp_user')
        self.smtp_pass = config.get('smtp_pass')
        self.recipient = config.get('recipient')
        self.subject = config.get('subject', 'Freight Data Results')
        self.sender_name = config.get('sender_name', 'FF2API System')
        
    def validate_config(self) -> bool:
        """Validate email handler configuration."""
        if not self.smtp_user:
            logger.error("Email handler missing smtp_user")
            return False
        if not self.smtp_pass:
            logger.error("Email handler missing smtp_pass")
            return False
        if not self.recipient:
            logger.error("Email handler missing recipient")
            return False
        return True
    
    def _create_csv_content(self, rows: List[Dict[str, Any]]) -> str:
        """Create CSV content from rows."""
        if not rows:
            return ""
        
        output = io.StringIO()
        fieldnames = rows[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
        return output.getvalue()
    
    def _create_email_body(self, rows: List[Dict[str, Any]]) -> str:
        """Create email body with summary."""
        row_count = len(rows)
        enriched_count = sum(1 for row in rows if 'enrichment_timestamp' in row)
        tracking_count = sum(1 for row in rows if row.get('tracking_events_count', 0) > 0)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        
        body = f"""Hello,

Your freight data processing is complete.

Summary:
• Records processed: {row_count}
• Records enriched: {enriched_count}
• Records with tracking: {tracking_count}
• Processing time: {timestamp}

Please find the enriched data attached as a CSV file.

Best regards,
{self.sender_name}
"""
        return body
    
    def post(self, rows: List[Dict[str, Any]]) -> bool:
        """Send enriched rows via email.
        
        Args:
            rows: List of enriched data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not rows:
            logger.warning("No rows to email")
            return True
            
        try:
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = f"{self.sender_name} <{self.smtp_user}>"
            msg['To'] = self.recipient
            msg['Subject'] = f"{self.subject} - {len(rows)} records"
            
            # Add body
            body = self._create_email_body(rows)
            msg.attach(MIMEText(body, 'plain'))
            
            # Create CSV attachment
            csv_content = self._create_csv_content(rows)
            
            # Create attachment
            filename = f"freight_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            attachment = MIMEBase('application', 'octet-stream')
            attachment.set_payload(csv_content.encode('utf-8'))
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f'attachment; filename= {filename}'
            )
            msg.attach(attachment)
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()  # Enable TLS encryption
            server.login(self.smtp_user, self.smtp_pass)
            
            text = msg.as_string()
            server.sendmail(self.smtp_user, self.recipient, text)
            server.quit()
            
            logger.info(f"Successfully sent email to {self.recipient} with {len(rows)} rows")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logger.error("SMTP authentication failed - check email credentials")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error occurred: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False