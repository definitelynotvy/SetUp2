import os
import smtplib
import requests
import logging
import win32com.client as win32
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from O365 import Account

# Initialize dotenv to load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO) 

class MailSender:
    """
    A class for sending emails using various methods, including the Outlook desktop app,
    Microsoft Graph API, and SMTP.

    Parameters
    ----------
    use_banana_style : bool, default=True
        If True, uses the Outlook desktop application as the default email sending method.
        If False, attempts to use the Microsoft Graph API (requires configured permissions).

    Methods
    -------
    send_mail(to_email, subject, body, method=None)
        Sends an email using the specified method ('outlook', 'token', 'smtp', or 'o365').
    """

    def __init__(self, use_banana_style=True):
        self.use_banana_style = use_banana_style
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize O365 Account only if not using banana style
        if not self.use_banana_style:
            credentials = (os.getenv('GRAPH_CLIENT_ID'), os.getenv('GRAPH_CLIENT_SECRET'))
            self.account = Account(credentials, tenant_id=os.getenv('GRAPH_TENANT_ID'))

            # Authenticate with Microsoft Graph API if not already authenticated
            if not self.account.is_authenticated:
                self.logger.info("Authenticating with Microsoft Graph API...")
                self.account.authenticate(
                    scopes=['https://graph.microsoft.com/Mail.Send'],
                    redirect_uri='https://login.microsoftonline.com/common/oauth2/nativeclient'
                )
                self.logger.info("Authentication successful.")

    def send_mail(self, to_email, subject, body, method=None):
        """
        Sends an email using the specified method.

        Parameters
        ----------
        to_email : str
            The recipient's email address.
        subject : str
            The subject of the email.
        body : str
            The body content of the email.
        method : str, optional, default=None
            The method to use for sending ('outlook', 'token', 'smtp', or 'o365').
            If None, defaults to 'outlook' if use_banana_style is True, otherwise 'o365'.

        Returns
        -------
        str
            A message indicating the result of the email sending operation.
        """
        if method is None:
            method = "outlook" if self.use_banana_style else "o365"
        self.logger.info(f"Sending email via {method} method...")

        if method == "outlook":
            return self._send_mail_via_outlook(to_email, subject, body)
        elif method == "token":
            return self._send_mail_via_graph(to_email, subject, body)
        elif method == "smtp":
            return self._send_mail_via_smtp(to_email, subject, body)
        elif method == "o365":
            return self._send_mail_via_o365(to_email, subject, body)
        else:
            raise ValueError("Invalid method specified. Use 'outlook', 'token', 'smtp', or 'o365'.")

    def _send_mail_via_outlook(self, to_email, subject, body, attachment=None):
        """
        Sends an email using the locally installed Outlook application.

        Parameters
        ----------
        to_email : str
            The recipient's email address.
        subject : str
            The subject of the email.
        body : str
            The body content of the email.
        attachment : str, optional
            Path to a file to attach to the email.

        Returns
        -------
        str
            A message indicating the result of the email sending operation.
        """
        try:
            outlook = win32.Dispatch('outlook.application')
            mail = outlook.CreateItem(0)
            mail.To = to_email
            mail.Subject = subject
            mail.Body = body
            mail.HTMLBody = f"<p>{body}</p>"

            # Attach a file if provided
            if attachment:
                mail.Attachments.Add(attachment)

            mail.Send()
            self.logger.info("Email sent successfully via Outlook.")
            return "Email sent successfully via Outlook."
        except Exception as e:
            self.logger.error(f"Failed to send email via Outlook: {str(e)}")
            return f"Failed to send email via Outlook: {str(e)}"

    def _send_mail_via_o365(self, to_email, subject, body):
        """
        Sends an email using Microsoft Graph API through the O365 library.

        Parameters
        ----------
        to_email : str
            The recipient's email address.
        subject : str
            The subject of the email.
        body : str
            The body content of the email.

        Returns
        -------
        str
            A message indicating the result of the email sending operation.
        """
        try:
            mailbox = self.account.mailbox()
            message = mailbox.new_message()
            message.to.add(to_email)
            message.subject = subject
            message.body = body
            message.sender.address = os.getenv("GRAPH_GROUP_EMAIL")
            message.send()
            self.logger.info("Email sent successfully via Microsoft Graph API (O365).")
            return "Email sent successfully via Microsoft Graph API (O365)."
        except Exception as e:
            self.logger.error(f"Failed to send email via Microsoft Graph API (O365): {str(e)}")
            return f"Failed to send email via Microsoft Graph API (O365): {str(e)}"

    def _send_mail_via_graph(self, to_email, subject, body):
        """
        Sends an email using the Microsoft Graph API sendMail endpoint directly via requests.

        Parameters
        ----------
        to_email : str
            The recipient's email address.
        subject : str
            The subject of the email.
        body : str
            The body content of the email.

        Returns
        -------
        str
            A message indicating the result of the email sending operation.
        """
        token = self.account.connection.token_backend.get_token()  

        if not token:
            self.logger.error("Failed to acquire token.")
            return "Failed to acquire token."

        send_mail_url = f"https://graph.microsoft.com/v1.0/users/{os.getenv('GRAPH_USER_ID')}/sendMail"
        email_data = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "Text",
                    "content": body
                },
                "toRecipients": [
                    {"emailAddress": {"address": to_email}}
                ],
                "from": {
                    "emailAddress": {"address": os.getenv("GRAPH_GROUP_EMAIL")}
                }
            },
            "saveToSentItems": "true"
        }

        headers = {
            "Authorization": f"Bearer {token['access_token']}",
            "Content-Type": "application/json"
        }

        response = requests.post(send_mail_url, headers=headers, json=email_data)

        if response.status_code == 202:
            self.logger.info("Email sent successfully via Microsoft Graph API (requests).")
            return "Email sent successfully via Microsoft Graph API (requests)."
        else:
            self.logger.error(f"Failed to send email via Microsoft Graph API (requests): {response.json()}")
            return f"Failed to send email via Microsoft Graph API (requests): {response.json()}"

    def _send_mail_via_smtp(self, to_email, subject, body):
        """
        Sends an email using SMTP with password-based authentication.

        Parameters
        ----------
        to_email : str
            The recipient's email address.
        subject : str
            The subject of the email.
        body : str
            The body content of the email.

        Returns
        -------
        str
            A message indicating the result of the email sending operation.
        """
        msg = MIMEMultipart()
        msg['From'] = os.getenv('SMTP_USERNAME')
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP(os.getenv('SMTP_SERVER'), int(os.getenv('SMTP_PORT'))) as server:
                server.starttls()
                server.login(os.getenv('SMTP_USERNAME'), os.getenv('SMTP_PASSWORD'))
                server.sendmail(msg['From'], msg['To'], msg.as_string())
                self.logger.info("Email sent successfully via SMTP.")
                return "Email sent successfully via SMTP."
        except Exception as e:
            self.logger.error(f"Failed to send email via SMTP: {str(e)}")
            return f"Failed to send email via SMTP: {str(e)}"

# Main script (e.g., if this script is used as an entry point)
if __name__ == "__main__":
    mail_sender = MailSender(use_banana_style=True)
    res = mail_sender.send_mail(os.getenv("DEFAULT_FROM"), "Test Subject", "Test Body")
    print(res)
