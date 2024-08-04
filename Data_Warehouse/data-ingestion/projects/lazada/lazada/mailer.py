import smtplib
from email.mime.text import MIMEText
from scrapy.utils.project import get_project_settings


class Mailer:

    @staticmethod
    def send_error_email(method_name, error_message):
        settings = get_project_settings()
        smtp_server = settings.get('MAIL_HOST')

        # Log in to the SMTP server
        smtp_user = settings.get('MAIL_USER')
        smtp_password = settings.get('MAIL_PASS')
        smtp_connection = smtplib.SMTP(smtp_server, settings.get('MAIL_PORT'))
        smtp_connection.starttls()
        smtp_connection.login(smtp_user, smtp_password)

        # Composing the Email
        email_body = error_message
        email_message = MIMEText(email_body)
        email_message['Subject'] = f"Error in {method_name}"
        email_message['From'] = smtp_user
        email_message['To'] = settings.get('MAIL_TO')

        smtp_connection.sendmail(smtp_user, email_message['To'], email_message.as_string())

        smtp_connection.quit()
