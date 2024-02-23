import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import email_host, email_port, email_username, email_password, use_ssl_tls, cc_email
import os

class EmailSenderError(Exception):
    pass

def send_email(emails, subject, body, attachment_file_paths=None, adminEmail=False):
    try:
        if use_ssl_tls:
            # Create a secure SSL/TLS connection to the SMTP server
            server = smtplib.SMTP_SSL(email_host, email_port)
        else:
            # Create a non-secure SMTP connection
            server = smtplib.SMTP(email_host, email_port)

        # Start TLS (if not using SSL/TLS) and login to the server
        if not use_ssl_tls:
            server.starttls()

        server.login(email_username, email_password)

        for email in emails:
            # Create a message
            msg = MIMEMultipart()
            msg['From'] = email_username
            msg['To'] = email
            msg['Subject'] = subject

            # Add CC recipients
            if cc_email:
                msg['Cc'] = ', '.join(cc_email)

            # Add the email body
            msg.attach(MIMEText(body, 'plain'))

            # Attach files (if provided)
            if attachment_file_paths:
                for attachment_file_path in attachment_file_paths:
                    file_name = os.path.basename(attachment_file_path)
                    with open(attachment_file_path, "rb") as attachment:
                        part = MIMEApplication(attachment.read(), Name=file_name)
                    # Add header for attachment
                    part['Content-Disposition'] = f'attachment; filename="{file_name}"'
                    msg.attach(part)

            if(adminEmail == False):
                # Combine 'To' and 'Cc' recipients into a single list
                all_recipients = emails + cc_email if cc_email else emails
            else:
                # Combine 'To' and 'Cc' recipients into a single list
                all_recipients = emails 

            # Send the message
            server.sendmail(email_username, all_recipients, msg.as_string())

        # Close the connection
        server.quit()
        return True

    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")
        raise EmailSenderError("Error sending email") from e
