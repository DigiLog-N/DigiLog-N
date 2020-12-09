##############################################################################
# Email.py
# https://github.com/DigiLog-N/DigiLog-N
# Copyright 2020 Canvass Labs, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################
from digilog_n.GMailUser import GMailUser
from smtplib import SMTPHeloError, SMTPAuthenticationError, SMTPNotSupportedError, SMTPException, SMTPRecipientsRefused, SMTPSenderRefused, SMTPDataError 
from socket import gaierror


def email_main(user, password, subject, message, addressed_to):
    try:
        email_alert = GMailUser(user, password, verify_parameters=False)
        email_alert.send(subject, message, addressed_to, hangup=True)
    except SMTPHeloError as e:
        raise ValueError("I said HELO, but the server ignored me: %s" % str(e))
    except SMTPAuthenticationError as e:
        raise ValueError("The SMTP server did not accept your username and/or password: %s" % str(e))
    except SMTPNotSupportedError as e:
        raise ValueError("The AUTH and/or SMTPUTF8 command is not supported by this server: %s" % str(e))
    except SMTPException as e:
        raise ValueError("A suitable authentication method couldn't be found: %s" % str(e))
    except gaierror as e:
        raise ValueError("Invalid SMTP host name: %s" % str(e))
    except RuntimeError as e:
        raise EnvironmentError("TLS and/or SSL support is not available to your Python interpreter: %s" % str(e))
    except SMTPRecipientsRefused as e:
        raise ValueError("All recipients were refused. Please verify your list of recipients is correct.")
    except SMTPSenderRefused as e:
        raise ValueError("Sender refused From=%s: %s" % (user, str(e)))
    except SMTPDataError as e:
        # TODO: This should probably be a different kind of error.
        #  consider wrapping all errors in a digilog-N specific error type.
        #  catching all of these errors here and re-raising them as a merged set of types is mainly
        #  to prevent downstream code from having to know/handle all of these very specific errors.
        #  Yet, we want to be fairly robust; we need to know if a user did not receive an email.
        raise ValueError("Server replied w/unexpected error code: %s" % str(e))
