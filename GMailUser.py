from smtplib import SMTP, SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException, SMTPRecipientsRefused, SMTPSenderRefused, SMTPDataError
from email.mime.text import MIMEText
from socket import gaierror
from argparse import ArgumentParser
from TLSUser import TLSUser


class GMailUser(TLSUser):
    def __init__(self, user, password, verify_parameters=False):
        self.host = "smtp.gmail.com"
        self.port = 587
        super().__init__(self.host, self.port, user, password, verify_parameters=verify_parameters)
