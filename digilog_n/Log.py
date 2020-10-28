
def log_file_main(user, password, subject, message, addressed_to):
    with open('/tmp/demo.log', 'a') as f:
        f.write("From (User): %s\n" % user)
        f.write("To: %s\n" % addressed_to)
        f.write("Password: %s\n" % password)
        f.write("Subject: %s\n" % subject)
        f.write("Message: %s\n" % message)
        f.write("########################################\n")
