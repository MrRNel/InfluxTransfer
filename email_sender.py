import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from project_secrets import EMAIL_HOST, EMAIL_PORT, EMAIL_USERNAME, EMAIL_PASSWORD, USE_SSL_TLS, CC_EMAIL
import os

class EmailSenderError(Exception):
    pass

def send_email(emails, subject, body, attachment_file_paths=None, adminEmail=False):
    try:
        if USE_SSL_TLS:
            # Create a secure SSL/TLS connection to the SMTP server
            server = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
        else:
            # Create a non-secure SMTP connection
            server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)

        # Start TLS (if not using SSL/TLS) and login to the server
        if not USE_SSL_TLS:
            server.starttls()

        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)

        for email in emails:
            # Create a message
            msg = MIMEMultipart()
            msg['From'] = EMAIL_USERNAME
            msg['To'] = email
            msg['Subject'] = subject

            # Add CC recipients
            if CC_EMAIL:
                msg['Cc'] = ', '.join(CC_EMAIL)

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
                all_recipients = emails + CC_EMAIL if CC_EMAIL else emails
            else:
                # Combine 'To' and 'Cc' recipients into a single list
                all_recipients = emails 

            # Send the message
            server.sendmail(EMAIL_USERNAME, all_recipients, msg.as_string())

        # Close the connection
        server.quit()
        return True

    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")
        raise EmailSenderError("Error sending email") from e
