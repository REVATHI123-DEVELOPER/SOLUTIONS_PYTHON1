import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Email configuration for custom SMTP server
SMTP_SERVER = "10.30.0.48"  # Replace with your SMTP server
SMTP_PORT = 25  # Using port 25 for your own SMTP server
EMAIL_USER = "do-not-reply@buckman.com"  # Replace with your email address
EMAIL_PASSWORD = "your-email-password"  # Replace with your email password


def __create_html_content() -> str:

    html_content = """
    <html>
    <head>
    <style>
        body {
            font-family: Arial, sans-serif;
            color: #333;
            padding: 20px;
        }
    </style>
    </head>
    <body>
    """

    html_content += """
        Please debug collect job immediately.
    """

    return html_content


def send_mail(
    hostname: str,
    subject: str,
    recipients: list,
    environment: str,
    outputdir: str,
):

    html_content = __create_html_content()

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = "ddaniel@buckman.com"
    msg["Subject"] = "Insight4 Collect Job Failure"

    # Attach the HTML content
    msg.attach(MIMEText(html_content, "html"))

    # Send the email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            # Uncomment if your server requires login (otherwise comment out)
            # server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, recipients, msg.as_string())
            print("Mail Sent Successfully")
    except Exception as e:
        print(f"Exception Type: {type(e).__name__}")
        print(f"Exception Message: {str(e)}")


if __name__ == "__main__":
    send_mail(
        "budig-bb-qa-05-d",
        "Test Email Using Buckman SMTP Server",
        ["ddaniel@buckman.com"],
        "PRODUCTION",
        "/var/www/html/adfTestLogs/2025-04-08/07-32-34/",
    )
