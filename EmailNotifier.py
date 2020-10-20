from digilog_n.DataSourceRegistry import DataSourceRegistry
from digilog_n.PlasmaReader import PlasmaReader
from digilog_n.GMailUser import GMailUser
from time import sleep


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


def main():
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    data_source = dsr.get_data_source('DigiLog-N Notifications')

    if not data_source:
        print("Error: Could not locate Notifications data-source.")
        exit(1)

    pr = PlasmaReader(data_source.get_path_to_plasma_file(), 'NOTIFY', remove_after_reading=True)

    print("Looking for notifications...")
    print("This system will poll Plasma once every second to check for any new notifications.")
    print("You will only be notified when a new result is found. Silence = no new results found.")

    while True:
        pdf = pr.to_pandas()
        if pdf is None:
            #print("No new notifications")
            pass
        else:
            print("New notifications!")
            pdf = pdf.sort_values(by=['epoch_timestamp'])

            user = "cowartcharles1@gmail.com"
            password = "D9AZm244L3UbYwBf"

            for index, row in pdf.iterrows():
                recipients = row['recipients'].split(',')
                subject = row['subject']
                message = row['message']

                email_main(user, password, subject, message, recipients)

        # sleep an arbitrary amount before checking for more notifications 
        # John and I agree that Plasma shouldn't have a problem polling at 1s intervals.
        sleep(1)

if __name__ == '__main__':
    main()
