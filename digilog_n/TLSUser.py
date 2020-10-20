from email.mime.text import MIMEText
from smtplib import SMTP, SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException, SMTPRecipientsRefused, SMTPSenderRefused, SMTPDataError
from socket import gaierror


class TLSUser:
    def __init__(self, host, port, user, password, verify_parameters=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.smtp = None

        if verify_parameters:
            # Attempt to connect to the server on object creation, so that if
            # parameters are incorrect, the user finds out that the object
            # is invalid now, rather than later. Assume that any smtp servers
            # going down would continue to be down for the send() call
            # anyway.
            self._login()
            self._disconnect()

    def _login(self):
        # create and initialze the SMTP object
        smtp = SMTP(self.host, self.port)
        # starttls() performs connection negotiation before login
        smtp.starttls()
        # log into TLS Secured connection with username and password.
        smtp.login(self.user, self.password)

        # if no exceptions occurred, then this object is good.
        # allow member tobe non-null w/good objects only.
        self.smtp = smtp
        #self.smtp.set_debuglevel(1)

    def _disconnect(self):
        self.smtp.quit()
        # once quit() has been called, the object does not appear to remain valid.
        self.smtp = None

    def send(self, subject, message, list_of_recipients, hangup=True):
        '''
        Simple send, where multiple recipients are always visible to each other.
        :param subject: subject line of the message
        :param message: the text of the message
        :param list_of_recipients: a list of one or more email addresses.
        :param hangup: hangs up after sending message. Set to False to prevent having to reconnect w/in a loop.
        :return: None if successful. Raises Error if unsuccessful.
        '''
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.user
        msg['To'] = ", ".join(list_of_recipients)

        if not self.smtp:
            self._login()

        # any email addresses in the list that are obviously invalid such as 'il.com' or 'foo' will be returned in
        # invalid_addresses. Addresses that are valid, but happen to not exist, such as 'non.existant.user@gmail.com'
        # will still be successful.
        invalid_addresses = self.smtp.sendmail(self.user, list_of_recipients, msg.as_string())

        if invalid_addresses:
            raise ValueError("The following addresses are invalid: %s" % ' '.join(invalid_addresses.keys()))

        if hangup:
            self._disconnect()


class GMailUser(TLSUser):
    def __init__(self, user, password, verify_parameters=False):
        self.host = "smtp.gmail.com"
        self.port = 587
        super().__init__(self.host, self.port, user, password, verify_parameters=verify_parameters)
