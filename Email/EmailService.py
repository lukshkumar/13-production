import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def SendEmail(subject, body):

    sender_email = "jackcharlessmith3@gmail.com"
    receiver_emails = ["lukshkumar97@gmail.com"]
    password = "hdecfihgrasafvqc"

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails)

    # Create the plain-text and HTML version of your message
    text = "The Email Faield to Load HTML Content, yet it managed to send the email."

    html = "<html><body>" + body + "</body></html>"

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_emails, message.as_string())