from os.path import exists, isdir
from os import remove
from time import sleep
from subprocess import Popen
from shutil import chown


class PlasmaBroker:
    '''
    The purpose of this class is to manage the creation and graceful deletion
    of Plasma objects. The intention is for a central DigiLog-N server process
    to manage creation of Plasma objects and CT2Arrow either assumes that the
    file already exists in the agreed location, or tests for the existence of
    the file.

    Although we are using just a single Plasma object for our demo, it may
    still be useful to have it managed at some point using this Broker.
    '''
    def __init__(self, root_path, group_name):
        # store all plasmas in an organized location.
        self.root_path = root_path

        if exists(root_path):
            if isdir(root_path):
                self.root_path = root_path
            else:
                raise ValueError("'%s' is not a directory" % root_path)
        else:
            raise ValueError("'%s' does not exist" % root_path)

        # plasma files are created with the default group set to the user
        # of this program. in order for the producer of data and the consumer
        # to be different users, they both need membership in a shared unix
        # group.
        #
        # this confirms the existence of the specified group.
        with open('/etc/group', 'r') as f:
            groups = f.readlines()
            groups = [x.split(':')[0] for x in groups]
            if group_name in groups:
                self.group_name = group_name
            else:
                raise ValueError("'%s' is not a valid group name" % group_name)

        # maintain a dictionary of all plasma file metadata keyed by name.
        self.plasma_files = {}

    def __del__(self):
        # As a safety, delete all plasma files created by this object
        # when this object falls out of scope. This will mean that this
        # object needs to be maintained in some type of daemon process.
        names = list(self.plasma_files.keys())
        for name in names:
            self.delete_plasma_file(name)

    def create_plasma_file(self, name):
        file_path = '%s/%s' % (self.root_path, name)

        if exists(file_path):
            # assume it's a file, not a directory, and return the following.
            raise ValueError("'%s' already exists" % name)

        # the life of the Plasma object is tied to the lifetime of this child
        # process. Instantiating the child this way, we are not waiting on it
        # to complete, and if this process is killed, the child will continue
        # to run.
        #
        # TODO: for now, set limit for Plasma object growth in RAM at 1GB.
        child = Popen(["plasma_store", "-m", "1000000000", "-s", file_path])

        does_exist = False
        for i in range(0, 10):
            # wait for the child process to create the Plasma file so that
            # the group can be set afterward.
            if exists(file_path):
                does_exist = True
                break
            else:
                sleep(1)

        if does_exist:
            chown(file_path, group=self.group_name)
        else:
            # TODO: Change error type.
            raise ValueError("Could not create '%s'" % file_path)

        # save the child process information, including pid, and the methods
        # used to terminate the process.
        self.plasma_files[name] = child


    def delete_plasma_file(self, name):
        # the manner in which we create Plasma files, we need to explicitly
        # clean up after them. This includes killing the child process,
        # letting its child process become a zombie and get reaped, and
	# finally, removing the socket file. This last part is important
        # because this object checks for existing objects before creating
        # new ones, and because the socket file will only generate an error
        # for the user, anyway.
        if name in self.plasma_files:
            child = self.plasma_files[name]
            child.kill()
            file_path = '%s/%s' % (self.root_path, name)
            remove(file_path)
            del(self.plasma_files[name])
        else:
            raise ValueError("'%s' is not a valid plasma file" % name)


if __name__ == '__main__':
    # create a broker, using /tmp as the storage location.
    # set the group ownership of any plasma file to 'dev'
    # so that the producer and consumer(s) do not need to
    # be the same person.
    broker = PlasmaBroker('/tmp', 'dev')

    broker.create_plasma_file('ctest1')
    broker.create_plasma_file('ctest2')
    broker.create_plasma_file('ctest3')
    broker.create_plasma_file('ctest4')

    # wait, give it time to create, user can confirm the
    # file in /tmp.
    sleep(10)

    # delete the plasma object and confirm the file
    # is not present before ending.
    broker.delete_plasma_file('ctest4')
    sleep(10)

    # observe if remaining socket files are deleted
    # once the broker object falls out of scope.


